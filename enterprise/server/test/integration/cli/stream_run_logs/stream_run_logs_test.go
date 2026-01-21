package stream_run_logs_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/stream_run_logs"
	"github.com/buildbuddy-io/buildbuddy/cli/testutil/testcli"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
)

// Integration test that runs `bb run` and does the full build through the CLI.
func TestWithBuild(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bazelversion": fmt.Sprintf("%s\n%s\n", testcli.BinaryPath(t), testbazel.BinaryPath(t)),
		"BUILD":         `sh_binary(name = "echo", srcs = ["echo.sh"])`,
		"echo.sh": `#!/bin/bash
echo "hello world"
echo "goodbye world"
`,
	})

	_, webClient, setupOpts := setup(t)

	cmd := append([]string{"run", ":echo", "--invocation_id=" + setupOpts.InvocationID}, getFlags(setupOpts)...)
	out := runWithCLI(t, ws, cmd)

	// Verify that the script ran as expected.
	require.Contains(t, out, "hello world")
	require.Contains(t, out, "goodbye world")

	// Verify that the run logs look as expected.
	logRsp := &elpb.GetEventLogChunkResponse{}
	err := webClient.RPC("GetEventLogChunk", &elpb.GetEventLogChunkRequest{
		RequestContext: webClient.RequestContext,
		InvocationId:   setupOpts.InvocationID,
		Type:           elpb.LogType_RUN_LOG,
	}, logRsp)
	require.NoError(t, err)

	runLogs := string(logRsp.Buffer)
	require.Equal(t, "hello world\ngoodbye world\n", runLogs)

	invRsp := &inpb.GetInvocationResponse{}
	err = webClient.RPC("GetInvocation", &inpb.GetInvocationRequest{
		RequestContext: webClient.RequestContext,
		Lookup: &inpb.InvocationLookup{
			InvocationId: setupOpts.InvocationID,
		},
	}, invRsp)
	require.NoError(t, err)
	require.Equal(t, 1, len(invRsp.Invocation))
	require.Equal(t, inspb.OverallStatus_SUCCESS, invRsp.Invocation[0].GetRunStatus())
}

func TestFlushingMultipleChunks(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"echo.sh": `#!/bin/bash
echo "hello world"
`,
	})

	app, webClient, setupOpts := setup(t)

	// Set a very small buffer size to test that the plugin correctly flushes the buffer when it's full.
	oldBufferSize := stream_run_logs.UploadBufferSize
	stream_run_logs.UploadBufferSize = 2
	t.Cleanup(func() {
		stream_run_logs.UploadBufferSize = oldBufferSize
	})

	mockBuild(t, app, webClient.RequestContext.GetGroupId(), setupOpts.InvocationID)

	script := ws + "/echo.sh"
	exitCode, err := stream_run_logs.Execute(script, *setupOpts)
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)

	// Verify that the run logs look as expected.
	logRsp := &elpb.GetEventLogChunkResponse{}
	err = webClient.RPC("GetEventLogChunk", &elpb.GetEventLogChunkRequest{
		RequestContext: webClient.RequestContext,
		InvocationId:   setupOpts.InvocationID,
		Type:           elpb.LogType_RUN_LOG,
	}, logRsp)
	require.NoError(t, err)

	runLogs := string(logRsp.Buffer)
	require.Equal(t, "hello world\n", runLogs)

	invRsp := &inpb.GetInvocationResponse{}
	err = webClient.RPC("GetInvocation", &inpb.GetInvocationRequest{
		RequestContext: webClient.RequestContext,
		Lookup: &inpb.InvocationLookup{
			InvocationId: setupOpts.InvocationID,
		},
	}, invRsp)
	require.NoError(t, err)
	require.Equal(t, 1, len(invRsp.Invocation))
	require.Equal(t, inspb.OverallStatus_SUCCESS, invRsp.Invocation[0].GetRunStatus())
}

func TestUploadFailure(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"echo.sh": `#!/bin/bash
echo "hello world"
`,
	})

	// Point to an invalid server to simulate log upload failure.
	invalidOpts := stream_run_logs.Opts{
		BesBackend:   "grpc://invalid.buildbuddy.io",
		ApiKey:       "",
		InvocationID: "",
	}

	exitCode, err := stream_run_logs.Execute(ws+"/echo.sh", invalidOpts)
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
}

func setup(t *testing.T) (*app.App, *buildbuddy_enterprise.WebClient, *stream_run_logs.Opts) {
	app := buildbuddy_enterprise.Run(t)
	webClient := buildbuddy_enterprise.LoginAsDefaultSelfAuthUser(t, app)

	rsp := &akpb.CreateApiKeyResponse{}
	err := webClient.RPC("CreateApiKey", &akpb.CreateApiKeyRequest{
		RequestContext: webClient.RequestContext,
		Capability:     []cappb.Capability{cappb.Capability_ORG_ADMIN},
	}, rsp)
	require.NoError(t, err)

	iid := uuid.New().String()

	return app, webClient, &stream_run_logs.Opts{
		BesBackend:   app.GRPCAddress(),
		ApiKey:       rsp.ApiKey.Value,
		InvocationID: iid,
	}
}

func getFlags(opts *stream_run_logs.Opts) []string {
	return []string{
		"--bes_backend=" + opts.BesBackend,
		"--remote_header=x-buildbuddy-api-key=" + opts.ApiKey,
		"--stream_run_logs=1",
	}
}

func runWithCLI(t *testing.T, ws string, cmdArgs []string) string {
	cmd := testcli.BazeliskCommand(t, ws, cmdArgs...)
	b, err := testcli.CombinedOutput(cmd)
	require.NoErrorf(t, err, "output: %s", string(b))
	fmt.Print(string(b))
	output := strings.ReplaceAll(string(b), "\r\n", "\n")
	return output
}

// Rather than running a full build, mock the build by creating an invocation to make the test faster.
func mockBuild(t *testing.T, app *app.App, groupID string, iid string) {
	db := app.DB()
	err := db.Create(&tables.Invocation{
		InvocationID: iid,
		GroupID:      groupID,
		Perms:        perms.GROUP_READ | perms.GROUP_WRITE,
	}).Error
	require.NoError(t, err)
}

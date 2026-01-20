package upload_run_logs_test

import (
	"fmt"
	"os/exec"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
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

func TestUploadRunLogs(t *testing.T) {
	// Create a simple script to run.
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"echo.sh": `#!/bin/bash
echo "hello world"
echo "goodbye world"
`,
	})
	testfs.MakeExecutable(t, ws, "echo.sh")

	iid := uuid.New().String()
	webClient, flags := setup(t, iid)

	// Run the plugin with the echo script.
	echoScript := ws + "/echo.sh"
	out := runPlugin(t, webClient, flags, echoScript)

	// Verify that the echo script runs as expected.
	require.Equal(t, "hello world\r\ngoodbye world\r\n", out)

	// Verify that the run logs look as expected.
	logRsp := &elpb.GetEventLogChunkResponse{}
	err := webClient.RPC("GetEventLogChunk", &elpb.GetEventLogChunkRequest{
		RequestContext: webClient.RequestContext,
		InvocationId:   iid,
		Type:           elpb.LogType_RUN_LOG,
	}, logRsp)
	require.NoError(t, err)

	runLogs := string(logRsp.Buffer)
	require.Equal(t, "hello world\ngoodbye world\n", runLogs)

	invRsp := &inpb.GetInvocationResponse{}
	err = webClient.RPC("GetInvocation", &inpb.GetInvocationRequest{
		RequestContext: webClient.RequestContext,
		Lookup: &inpb.InvocationLookup{
			InvocationId: iid,
		},
	}, invRsp)
	require.NoError(t, err)
	require.Equal(t, 1, len(invRsp.Invocation))
	require.Equal(t, inspb.OverallStatus_SUCCESS, invRsp.Invocation[0].GetRunStatus())
}

func TestLargeUpload(t *testing.T) {
	// Create a simple script.
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"echo.sh": `#!/bin/bash
echo "hello world"
`,
	})
	testfs.MakeExecutable(t, ws, "echo.sh")

	// Set a very small buffer size to test that the plugin correctly flushes the buffer when it's full.
	iid := uuid.New().String()
	webClient, flags := setup(t, iid, "--buffer_size=2")

	// Run the plugin with the echo script.
	script := ws + "/echo.sh"
	_ = runPlugin(t, webClient, flags, script)

	// Verify that the run logs look as expected.
	logRsp := &elpb.GetEventLogChunkResponse{}
	err := webClient.RPC("GetEventLogChunk", &elpb.GetEventLogChunkRequest{
		RequestContext: webClient.RequestContext,
		InvocationId:   iid,
		Type:           elpb.LogType_RUN_LOG,
	}, logRsp)
	require.NoError(t, err)

	runLogs := string(logRsp.Buffer)
	require.Equal(t, "hello world\n", runLogs)

	invRsp := &inpb.GetInvocationResponse{}
	err = webClient.RPC("GetInvocation", &inpb.GetInvocationRequest{
		RequestContext: webClient.RequestContext,
		Lookup: &inpb.InvocationLookup{
			InvocationId: iid,
		},
	}, invRsp)
	require.NoError(t, err)
	require.Equal(t, 1, len(invRsp.Invocation))
	require.Equal(t, inspb.OverallStatus_SUCCESS, invRsp.Invocation[0].GetRunStatus())
}

func TestUploadFailure(t *testing.T) {
	// Create a simple script.
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"echo.sh": `#!/bin/bash
echo "hello world"
`,
	})
	testfs.MakeExecutable(t, ws, "echo.sh")

	// Point to an invalid server to simulate log upload failure.
	iid := uuid.New().String()
	webClient, flags := setup(t, iid, "--target=grpc://invalid.buildbuddy.io")

	// Run the plugin with the echo script.
	script := ws + "/echo.sh"
	out := runPlugin(t, webClient, flags, script)

	// The echo script should've still run successfully.
	require.Contains(t, out, "hello world")
}

func setup(t *testing.T, iid string, extraFlags ...string) (*buildbuddy_enterprise.WebClient, []string) {
	app := buildbuddy_enterprise.Run(t)
	webClient := buildbuddy_enterprise.LoginAsDefaultSelfAuthUser(t, app)

	rsp := &akpb.CreateApiKeyResponse{}
	err := webClient.RPC("CreateApiKey", &akpb.CreateApiKeyRequest{
		RequestContext: webClient.RequestContext,
		Capability:     []cappb.Capability{cappb.Capability_ORG_ADMIN},
	}, rsp)
	require.NoError(t, err)

	// Create an invocation for the associated build, because the plugin assumes one already exists.
	db := app.DB()
	err = db.Create(&tables.Invocation{
		InvocationID: iid,
		GroupID:      webClient.RequestContext.GetGroupId(),
		Perms:        perms.GROUP_READ | perms.GROUP_WRITE,
	}).Error
	require.NoError(t, err)

	// Configure plugin to talk to local server.
	flags := append([]string{
		"--target=" + app.GRPCAddress(),
		"--invocation_id=" + iid,
		"--api_key=" + rsp.ApiKey.Value,
	}, extraFlags...)
	return webClient, flags
}

func runPlugin(t *testing.T, webClient *buildbuddy_enterprise.WebClient, flags []string, script string) string {
	binPath, err := runfiles.Rlocation("_main/cli/plugins/upload_run_logs/upload_run_logs_/upload_run_logs")
	require.NoError(t, err)

	args := append(flags, "--", script)
	cmd := exec.Command(
		binPath,
		args...,
	)
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "`bazel run :echo` failed: %s", string(out))
	fmt.Println(string(out))
	return string(out)
}

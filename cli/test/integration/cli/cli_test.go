package cli_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/testutil/testcli"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

func TestBazelVersion(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	cmd := testcli.Command(t, ws, "version")

	b, err := testcli.CombinedOutput(cmd)
	output := string(b)
	require.NoError(t, err, "output: %s", string(b))

	require.Contains(t, output, "Build label: "+testbazel.Version)
	// Make sure we don't print any warnings.
	require.NotContains(t, output, log.WarningPrefix)
}

func TestBazelBuildWithLocalPlugin(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"plugins/test/pre_bazel.sh": `
			echo 'Hello from pre_bazel.sh!'
			if grep '\--build_metadata=FOO=bar' "$1" >/dev/null ; then
				echo "--build_metadata FOO=bar was canonicalized as expected!"
			fi
		`,
		"plugins/test/post_bazel.sh": `echo 'Hello from post_bazel.sh!'`,
		"plugins/test/handle_bazel_output.sh": `
			if grep 'Build completed successfully'; then
				echo 'Hello from handle_bazel_output.sh! Build was successful.'
			fi
		`,
	})

	// Install the workspace-local plugin
	cmd := testcli.Command(t, ws, "install", "--path=plugins/test")

	err := cmd.Run()
	require.NoError(t, err)

	testfs.WriteAllFileContents(t, ws, map[string]string{"BUILD": ``})

	cmd = testcli.Command(t, ws, "build", "//...", "--build_metadata", "FOO=bar")

	b, err := testcli.CombinedOutput(cmd)

	require.NoError(t, err)
	output := strings.ReplaceAll(string(b), "\r\n", "\n")

	require.Contains(t, output, "Hello from pre_bazel.sh!")
	require.Contains(t, output, "--build_metadata FOO=bar was canonicalized as expected!")
	require.Contains(t, output, "Hello from handle_bazel_output.sh! Build was successful.")
	require.Contains(t, output, "Hello from post_bazel.sh!")
	// Make sure we don't print any warnings.
	require.NotContains(t, output, log.WarningPrefix)
}

func TestBazelBuildWithBuildBuddyServices(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testgit.ConfigureRemoteOrigin(t, ws, "https://secretUser:secretToken@github.com/test-org/test-repo")
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"BUILD":  `sh_binary(name = "nop", srcs = ["nop.sh"])`,
		"nop.sh": "",
	})
	testfs.MakeExecutable(t, ws, "nop.sh")
	app := buildbuddy.Run(t, "--cache.detailed_stats_enabled=true")
	args := []string{"build", ":nop"}
	args = append(args, app.BESBazelFlags()...)
	args = append(args, app.RemoteCacheBazelFlags()...)
	args = append(args, "--remote_upload_local_results")
	uid, err := uuid.NewRandom()
	iid := uid.String()
	require.NoError(t, err)
	args = append(args, "--invocation_id="+iid)

	cmd := testcli.Command(t, ws, args...)

	err = cmd.Run()

	require.NoError(t, err)
	bbs := app.BuildBuddyServiceClient(t)

	ctx := context.Background()
	var invocationResponse *inpb.GetInvocationResponse
	retryUntilSuccess(t, func() error {
		invReq := &inpb.GetInvocationRequest{
			Lookup: &inpb.InvocationLookup{InvocationId: iid},
		}
		inv, err := bbs.GetInvocation(ctx, invReq)
		if err != nil {
			return err
		}
		invocationResponse = inv

		scReq := &capb.GetCacheScoreCardRequest{
			InvocationId: iid,
		}
		sc, err := bbs.GetCacheScoreCard(ctx, scReq)
		if err != nil {
			return err
		}

		if len(sc.Results) == 0 {
			return fmt.Errorf("scorecard results list is empty")
		}

		return nil
	})

	invocation := invocationResponse.GetInvocation()[0]
	require.Equal(
		t, "https://github.com/test-org/test-repo", invocation.GetRepoUrl(),
		"CLI should set repo URL metadata, stripping URL credentials")
	require.Equal(
		t, testgit.CurrentBranch(t, ws), invocation.GetBranchName(),
		"CLI should set branch name metadata")
	require.Equal(
		t, testgit.CurrentCommitSHA(t, ws), invocation.GetCommitSha(),
		"CLI should set commit SHA metadata")
}

func retryUntilSuccess(t *testing.T, f func() error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := retry.DefaultWithContext(ctx)
	var err error
	for r.Next() {
		err = f()
		if err == nil {
			return
		}
	}
	// testcli.DumpSidecarLog(t)
	require.FailNowf(t, "timed out waiting for function to succeed", "last error: %s", err)
}

package cli_test

import (
	"context"
	"fmt"
	"os"
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

	// Note: this test makes sure that the version output appears in stdout
	// (not stderr), so that tools can do things like `bb version | grep ...`
	// the same way they can with vanilla bazel.
	b, err := testcli.Output(cmd)
	output := string(b)
	require.NoError(t, err, "output: %s", string(b))

	require.Contains(t, output, "Build label: "+testbazel.Version)
	// Make sure we don't print any warnings.
	require.NotContains(t, output, log.WarningPrefix)
}

func TestInvokeViaBazelisk(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bazelversion": fmt.Sprintf("%s\n%s\n", testcli.BinaryPath(t), testbazel.BinaryPath(t)),
	})

	{
		// Make sure we can invoke the CLI under bazelisk, using the
		// .bazelversion trick.
		cmd := testcli.BazeliskCommand(t, ws, "version")
		b, err := testcli.CombinedOutput(cmd)

		require.NoError(t, err, "output: %s", string(b))
		require.Contains(t, string(b), "bb unknown")
		require.Contains(t, string(b), "Build label: "+testbazel.Version)
	}
	{
		// Make sure that if we're using the .bazelversion trick, we still have
		// a way to override the bazel version via env var
		// (BB_USE_BAZEL_VERSION).
		cmd := testcli.BazeliskCommand(t, ws, "version")
		cmd.Env = append(os.Environ(), "BB_USE_BAZEL_VERSION=6.0.0")
		// Sanity check: make sure testbazel.Version is different from the one
		// we're testing here.
		require.NotEqual(t, "6.0.0", testbazel.Version)
		b, err := testcli.CombinedOutput(cmd)

		require.NoError(t, err, "output: %s", string(b))
		require.Contains(t, string(b), "bb unknown")
		require.Contains(t, string(b), "Build label: 6.0.0")
	}
}

func TestBazelHelp(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	cmd := testcli.Command(t, ws, "help", "completion")

	// Note: this test makes sure that the help output appears in stdout (not
	// stderr), so that tools can do things like `eval $(bb help completion)`
	// the same way they can with vanilla bazel.
	b, err := testcli.Output(cmd)
	output := string(b)
	require.NoError(t, err, "output: %s", string(b))

	require.Contains(t, output, `BAZEL_STARTUP_OPTIONS="`)
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

func TestBazelRunWithLocalPlugin(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testgit.ConfigureRemoteOrigin(t, ws, "https://secretUser:secretToken@github.com/test-org/test-repo")
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"BUILD":   `sh_binary(name = "echo", srcs = ["echo.sh"])`,
		"echo.sh": "echo $@",
	})
	testfs.MakeExecutable(t, ws, "echo.sh")

	testfs.WriteAllFileContents(t, ws, map[string]string{
		"plugins/test/pre_bazel.sh": `
			echo 'Hello from pre_bazel.sh!'
			if grep '\--build_metadata=FOO=bar' "$1" >/dev/null ; then
				echo "--build_metadata FOO=bar was canonicalized as expected!"
			fi
			if grep 'Hello' "$EXEC_ARGS_FILE" >/dev/null ; then
				echo "'Hello' was recognized as a positional argument to forward to the executable!"
			fi
			echo "World" >> $EXEC_ARGS_FILE
		`,
	})

	// Install the workspace-local plugin
	cmd := testcli.Command(t, ws, "install", "--path=plugins/test")

	err := cmd.Run()
	require.NoError(t, err)

	app := buildbuddy.Run(t, "--cache.detailed_stats_enabled=true")

	args := []string{"run", ":echo"}
	args = append(args, "--build_metadata", "FOO=bar")
	args = append(args, app.BESBazelFlags()...)
	args = append(args, app.RemoteCacheBazelFlags()...)
	args = append(args, "--remote_upload_local_results")
	uid, err := uuid.NewRandom()
	iid := uid.String()
	require.NoError(t, err)
	args = append(args, "--invocation_id="+iid)
	args = append(args, "--")
	args = append(args, "Hello")

	cmd = testcli.Command(t, ws, args...)

	b, err := testcli.CombinedOutput(cmd)

	require.NoError(t, err)
	output := strings.ReplaceAll(string(b), "\r\n", "\n")

	require.Contains(t, output, "Hello from pre_bazel.sh!")
	require.Contains(t, output, "'Hello' was recognized as a positional argument to forward to the executable!")
	require.Contains(t, output, "--build_metadata FOO=bar was canonicalized as expected!")
	require.Contains(t, output, "Hello World")
	// Make sure we don't print any warnings.
	require.NotContains(t, output, log.WarningPrefix)

	args = []string{"run", ":echo"}
	args = append(args, "Hello")
	args = append(args, "--build_metadata", "FOO=bar")
	args = append(args, app.BESBazelFlags()...)
	args = append(args, app.RemoteCacheBazelFlags()...)
	args = append(args, "--remote_upload_local_results")
	uid, err = uuid.NewRandom()
	iid = uid.String()
	require.NoError(t, err)
	args = append(args, "--invocation_id="+iid)

	cmd = testcli.Command(t, ws, args...)

	b, err = testcli.CombinedOutput(cmd)

	require.NoError(t, err)
	output = strings.ReplaceAll(string(b), "\r\n", "\n")

	require.Contains(t, output, "Hello from pre_bazel.sh!")
	// TODO (tempoz): fix arg parsing so that this is handled correctly
	// require.Contains(t, output, "'Hello' was recognized as a positional argument to forward to the executable!")
	require.Contains(t, output, "--build_metadata FOO=bar was canonicalized as expected!")
	require.Contains(t, output, "Hello World")
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

func TestTerminalOutput(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"test.sh": `#!/usr/bin/env bash
			for i in {1..5}; do
				echo "$i"
				sleep 0.1
			done
		`,
		"BUILD": `sh_test(name = "test", srcs = ["test.sh"])`,
	})

	term := testcli.PTY(t)
	term.Run(testcli.Command(t, ws, "test", "...", "--test_output=streamed"))

	// Make sure Bazel's progress output doesn't get interspersed with the test
	// output.
	require.Contains(t, term.Render(), "1\n2\n3\n4\n5\n")
	// Make sure Bazel understands that it's connected to a terminal - it should
	// produce colorful output.
	require.Contains(t, term.Render(), "\x1b[32mINFO")
}

func TestTargetPatternFile(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"BUILD":       `sh_test(name = "nop", srcs = ["nop.sh"])`,
		"nop.sh":      "",
		"targets.txt": "//:nop",
	})

	_, err := testcli.CombinedOutput(testcli.Command(t, ws, "build", "--target_pattern_file=targets.txt"))
	require.NoError(t, err)

	_, err = testcli.CombinedOutput(testcli.Command(t, ws, "test", "--target_pattern_file=targets.txt"))
	require.NoError(t, err)
}

func TestQueryFile(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"BUILD":       `sh_test(name = "nop", srcs = ["nop.sh"])`,
		"nop.sh":      "",
		"targets.txt": "//:nop",
	})

	_, err := testcli.CombinedOutput(testcli.Command(t, ws, "query", "--query_file=targets.txt"))
	require.NoError(t, err)
}

func TestFix(t *testing.T) {
	ws := testcli.NewWorkspace(t)

	cmd := testcli.Command(t, ws, "fix", "-mode", "diff")
	b, err := testcli.CombinedOutput(cmd)

	require.NoError(t, err, "output:\n%s", string(b))
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

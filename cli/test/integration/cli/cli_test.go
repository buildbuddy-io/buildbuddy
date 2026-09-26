package cli_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
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

func init() {
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

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

func TestBazelRun(t *testing.T) {
	// Bazel run writes a script; the CLI must execute it and preserve its exit
	// status, rather than returning the successful build's exit status.
	ws := newFakeBazelWorkspace(t, `
for arg in "$@"; do
  if [[ "$arg" == --script_path=* ]]; then
    printf '#!/usr/bin/env bash\nexit 42\n' > "${arg#--script_path=}"
    exit 0
  fi
done
echo 'missing --script_path' >&2
exit 1
`)
	cmd := testcli.BazelCommand(t, ws, "run", ":fail")
	b, err := testcli.CombinedOutput(cmd)
	require.Error(t, err, "output: %s", string(b))
	require.Equal(t, 42, cmd.ProcessState.ExitCode())
}

func TestParseGlobalFlags(t *testing.T) {
	ws := newFakeBazelWorkspace(t, `
script_path=""
for arg in "$@"; do
  if [[ "$arg" == --script_path=* ]]; then
    script_path="${arg#--script_path=}"
  fi
done
# Generate a launch script with the executable args quoted just as Bazel would.
printf '#!/usr/bin/env bash\nprintf "<%%s>\\n" ' > "$script_path"
for arg in "$@"; do
  if [[ "${forward:-0}" == 1 ]]; then
    printf '%q ' "$arg" >> "$script_path"
  elif [[ "$arg" == -- ]]; then
    forward=1
  fi
done
printf '\n' >> "$script_path"
`)
	cmd := testcli.BazelCommand(t, ws, "run", ":print_args", "--", "--before", "--verbose", "hello world", "--after")
	b, err := testcli.Output(cmd)
	require.NoError(t, err, "output: %s", string(b))
	require.Equal(t, "<--before>\n<--verbose>\n<hello world>\n<--after>\n", string(b))
}

func TestInvokeViaBazelisk(t *testing.T) {
	ws := newFakeBazelWorkspace(t, `echo "Build label: workspace-bazel"`)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"override-bazel.sh": strings.ReplaceAll(testfs.ReadFileAsString(t, ws, "fake-bazel.sh"), "workspace-bazel", "override-bazel"),
	})
	testfs.MakeExecutable(t, ws, "override-bazel.sh")
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bazelversion": fmt.Sprintf("%s\n%s\n", testcli.BinaryPath(t), ws+"/fake-bazel.sh"),
	})

	{
		// Make sure we can invoke the CLI under bazelisk, using the
		// .bazelversion trick.
		cmd := testcli.BazeliskCommand(t, ws, "version")
		b, err := testcli.CombinedOutput(cmd)

		require.NoError(t, err, "output: %s", string(b))
		require.Regexp(t, `(?m)^bb (unknown|\d+\.\d+\.\d+)$`, string(b))
		require.Contains(t, string(b), "Build label: workspace-bazel")
	}
	{
		// Make sure that if we're using the .bazelversion trick, we still have
		// a way to override the bazel version via env var
		// (BB_USE_BAZEL_VERSION).
		cmd := testcli.BazeliskCommand(t, ws, "version")
		cmd.Env = append(os.Environ(), "BB_USE_BAZEL_VERSION="+ws+"/override-bazel.sh")
		b, err := testcli.CombinedOutput(cmd)

		require.NoError(t, err, "output: %s", string(b))
		require.Regexp(t, `(?m)^bb (unknown|\d+\.\d+\.\d+)$`, string(b))
		require.Contains(t, string(b), "Build label: override-bazel")
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

func TestBazelHelp_IgnoresBBRC(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		// This would fail if help attempted to parse the workspace .bbrc.
		".bbrc": `startup --not_a_bb_flag`,
	})

	// Explicit bbrc options should also not be forwarded to Bazel.
	cmd := testcli.Command(t, ws,
		"--bbrc="+ws+"/missing.bbrc",
		"help",
		"--bb_config=missing",
		"build",
	)
	b, err := testcli.CombinedOutput(cmd)
	output := string(b)
	require.NoErrorf(t, err, "output: %s", output)
	require.Contains(t, output, "Usage:")
}

func TestBazelHelp_UsesBazelrc(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bazelrc": `help --announce_rc`,
	})

	cmd := testcli.Command(t, ws, "help", "build")
	b, err := testcli.CombinedOutput(cmd)
	output := string(b)
	require.NoErrorf(t, err, "output: %s", output)
	require.Contains(t, output, "Reading rc options for 'help'")
	require.Contains(t, output, "--announce_rc")
}

func TestHelpWithoutHomeEnv(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	cmd := testcli.Command(t, ws, "--help")

	// Keep USERPROFILE empty and set HOME to a temp dir so we don't inherit
	// user-specific rc files from the test harness environment.
	homeDir := t.TempDir()
	configDir := t.TempDir()
	cacheDir := t.TempDir()
	env := make([]string, 0, len(os.Environ())+4)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "HOME=") || strings.HasPrefix(kv, "USERPROFILE=") || strings.HasPrefix(kv, "XDG_CONFIG_HOME=") || strings.HasPrefix(kv, "XDG_CACHE_HOME=") {
			continue
		}
		env = append(env, kv)
	}
	cmd.Env = append(env, "HOME="+homeDir, "USERPROFILE=", "XDG_CONFIG_HOME="+configDir, "XDG_CACHE_HOME="+cacheDir)

	stdout, stderr, err := testcli.SplitOutput(cmd)
	require.NoError(t, err, "stdout: %s\nstderr: %s", string(stdout), string(stderr))
	require.Contains(t, string(stdout), "Usage: bb <command> <options> ...")
}

func TestBazelBuildWithLocalPlugin(t *testing.T) {
	ws := newFakeBazelWorkspace(t, `
printf 'BAZEL_ARG:<%s>\n' "$@"
echo 'Build completed successfully' >&2
`)
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

	b, err := cmd.CombinedOutput()
	require.NoError(t, err, "output: %s", string(b))

	cmd = testcli.BazelCommand(t, ws, "build", "//...", "--build_metadata", "FOO=bar")

	b, err = testcli.CombinedOutput(cmd)

	require.NoError(t, err, "output: %s", string(b))
	output := strings.ReplaceAll(string(b), "\r\n", "\n")

	require.Contains(t, output, "BAZEL_ARG:<build>")
	require.Contains(t, output, "BAZEL_ARG:<//...>")
	require.Contains(t, output, "BAZEL_ARG:<--build_metadata=FOO=bar>")
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
		"BUILD": `load("@rules_shell//shell:sh_binary.bzl", "sh_binary")
sh_binary(name = "echo", srcs = ["echo.sh"])`,
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

	b, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "output: %s", string(b))

	app := buildbuddy.Run(t, "--cache.detailed_stats_enabled=true")

	args := []string{"run", ":echo"}
	args = append(args, "--build_metadata", "FOO=bar")
	args = append(args, app.BESBazelFlags()...)
	args = append(args, app.RemoteCacheBazelFlags()...)
	args = append(args, "--remote_upload_local_results")
	uid, err := uuid.NewRandom()
	require.NoError(t, err)
	iid := uid.String()
	args = append(args, "--invocation_id="+iid)
	args = append(args, "--")
	args = append(args, "Hello")

	cmd = testcli.BazelCommand(t, ws, args...)

	b, err = testcli.CombinedOutput(cmd)

	require.NoErrorf(t, err, "output: %s", string(b))
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
	require.NoError(t, err)
	iid = uid.String()
	args = append(args, "--invocation_id="+iid)

	cmd = testcli.BazelCommand(t, ws, args...)

	b, err = testcli.CombinedOutput(cmd)

	require.NoErrorf(t, err, "output: %s", string(b))
	output = strings.ReplaceAll(string(b), "\r\n", "\n")

	require.Contains(t, output, "Hello from pre_bazel.sh!")
	// TODO (tempoz): fix arg parsing so that this is handled correctly
	// require.Contains(t, output, "'Hello' was recognized as a positional argument to forward to the executable!")
	require.Contains(t, output, "--build_metadata FOO=bar was canonicalized as expected!")
	require.Contains(t, output, "Hello World")
	// Make sure we don't print any warnings.
	require.NotContains(t, output, log.WarningPrefix)
}

func TestBBRC_Watcher(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	writeFakeBazel(t, ws)
	customBBRC := ws + "/custom.bbrc"
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"custom.bbrc": `
startup --watch
run:ci --stream_run_logs --on_stream_run_logs_failure=warn
`,
		"fake-godemon.sh": `#!/usr/bin/env bash
set -euo pipefail
if [[ "${FAKE_GODEMON_ACTIVE:-}" == "1" ]]; then
  echo "nested watcher invocation" >&2
  exit 1
fi
export FAKE_GODEMON_ACTIVE=1
# Skip Godemon's --watch and --lockfile arguments, then execute bb once.
shift 4
printf 'WATCHER_CHILD_ARGS:%s\n' "$*"
exec "$@"
`,
	})
	testfs.MakeExecutable(t, ws, "fake-godemon.sh")

	cmd := testcli.Command(t, ws,
		"--bbrc="+customBBRC,
		"run",
		"--bb_config=ci",
		":target",
	)
	cmd.Env = append(os.Environ(),
		"GODEMON_BINARY_PATH="+ws+"/fake-godemon.sh",
		"BB_DISABLE_SIDECAR=1",
	)
	b, err := testcli.CombinedOutput(cmd)
	output := strings.ReplaceAll(string(b), "\r\n", "\n")
	require.NoErrorf(t, err, "output: %s", output)

	// Even when the watcher restarts the bb process, the BBRC flags should be preserved.
	require.Contains(t, output, "WATCHER_CHILD_ARGS:")
	require.Contains(t, output, "--bbrc="+customBBRC)
	require.Contains(t, output, "--bb_config=ci")
	// This log is only printed when the stream_run_logs flag is correctly set.
	require.Contains(t, output, "streaming run logs is only supported")
	// The Bazel wrapper should have been successfully invoked.
	require.Contains(t, output, "FAKE_BAZEL_ARGS:")
}

func TestBBRC_Plugin(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	writeFakeBazel(t, ws)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bbrc": `run:ci --stream_run_logs --on_stream_run_logs_failure=warn`,
		"buildbuddy.yaml": `plugins:
  - path: testplugin
`,
		"testplugin/pre_bazel.sh": `#!/usr/bin/env bash
set -euo pipefail
echo '--build_metadata=FROM_PLUGIN' >> "$FORWARDED_BAZEL_ARGS_FILE"
`,
	})
	testfs.MakeExecutable(t, ws, "testplugin/pre_bazel.sh")

	cmd := testcli.Command(t, ws, "run", "--bb_config=ci", ":target")
	cmd.Env = append(os.Environ(), "BB_DISABLE_SIDECAR=1")
	b, err := testcli.CombinedOutput(cmd)
	output := strings.ReplaceAll(string(b), "\r\n", "\n")
	require.NoErrorf(t, err, "output: %s", output)

	// Run-log streaming came from the named BB config, so this warning proves
	// the config was still active after the plugin arguments were reparsed.
	require.Contains(t, output, "streaming run logs is only supported")
	require.Contains(t, output, "--build_metadata=FROM_PLUGIN")
}

func TestBBRC_SidecarRollback(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	writeFakeBazel(t, ws)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bbrc": `run:ci --stream_run_logs --on_stream_run_logs_failure=warn`,
	})

	cmd := testcli.Command(t, ws,
		"run",
		"--bb_config=ci",
		"--remote_cache=grpc://127.0.0.1:1",
		":target",
	)
	// An unmatched quote makes sidecar argument parsing fail immediately. The
	// CLI then restores its original arguments and continues without a sidecar.
	cmd.Env = append(os.Environ(), "BB_SIDECAR_ARGS='")
	b, err := testcli.CombinedOutput(cmd)
	output := strings.ReplaceAll(string(b), "\r\n", "\n")
	require.NoErrorf(t, err, "output: %s", output)

	require.Contains(t, output, "Sidecar could not be initialized")
	// This setting came from --bb_config=ci. Seeing it after the sidecar
	// rollback proves that restoring the arguments retained the config choice.
	require.Contains(t, output, "streaming run logs is only supported")
	require.Contains(t, output, "FAKE_BAZEL_ARGS:")
}

// newFakeBazelWorkspace exercises the CLI/Bazelisk boundary without starting a
// Bazel server. The option metadata query uses the same fixture as parser tests;
// all other invocations run the test-specific script with the forwarded args.
func newFakeBazelWorkspace(t *testing.T, script string) string {
	t.Helper()
	ws := testcli.NewWorkspace(t)
	writeFakeBazelBinary(t, ws, script)
	return ws
}

func writeFakeBazelBinary(t *testing.T, ws, script string) {
	t.Helper()
	// Embed the metadata: Bazelisk copies local binaries into its cache, so
	// the script cannot rely on files relative to its own path.
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bazelversion": ws + "/fake-bazel.sh",
		"fake-bazel.sh": `#!/usr/bin/env bash
set -euo pipefail
for arg in "$@"; do
  if [[ "$arg" == flags-as-proto ]]; then
    printf '%s\n' "` + strings.TrimSpace(test_data.BazelHelpFlagsAsProtoOutput) + `"
    exit 0
  fi
done
` + script,
	})
	testfs.MakeExecutable(t, ws, "fake-bazel.sh")
}

// writeFakeBazel installs a workspace Bazel wrapper that completes `run`
// commands without starting Bazel. It also rejects BB-only arguments, since
// those must be consumed by the CLI before it invokes Bazelisk.
func writeFakeBazel(t *testing.T, ws string) {
	t.Helper()
	writeFakeBazelBinary(t, ws, `echo "unexpected direct Bazel invocation" >&2; exit 1`)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"tools/bazel": `#!/usr/bin/env bash
set -euo pipefail
# Answer metadata queries with the fixture, and intercept the final run.
for arg in "$@"; do
  if [[ "$arg" == "help" ]]; then
    exec "$BAZEL_REAL" "$@"
  fi
done
script_path=""
for arg in "$@"; do
  case "$arg" in
    --bbrc*|--bb_config*|--ignore_all_bb_rc_files*|--stream_run_logs*|--on_stream_run_logs_failure*)
      echo "BB-only argument leaked to Bazel: $arg" >&2
      exit 1
      ;;
    --script_path=*)
      script_path="${arg#--script_path=}"
      ;;
  esac
done
printf 'FAKE_BAZEL_ARGS:%s\n' "$*"
if [[ -n "$script_path" ]]; then
  printf '#!/usr/bin/env bash\nexit 0\n' > "$script_path"
  chmod +x "$script_path"
fi
`,
	})
	testfs.MakeExecutable(t, ws, "tools/bazel")
}

func TestBazelBuildWithBuildBuddyServices(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testgit.ConfigureRemoteOrigin(t, ws, "https://secretUser:secretToken@github.com/test-org/test-repo")
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"BUILD": `load("@rules_shell//shell:sh_binary.bzl", "sh_binary")
sh_binary(name = "nop", srcs = ["nop.sh"])`,
		"nop.sh": "",
	})
	testfs.MakeExecutable(t, ws, "nop.sh")
	app := buildbuddy.Run(t, "--cache.detailed_stats_enabled=true")
	args := []string{"build", ":nop"}
	args = append(args, app.BESBazelFlags()...)
	args = append(args, app.RemoteCacheBazelFlags()...)
	args = append(args, "--remote_upload_local_results")
	// Make the sidecar finish cache writes before the invocation is
	// finalized, so that they are reflected in the cache scorecard.
	args = append(args, "--bes_upload_mode=wait_for_upload_complete")
	uid, err := uuid.NewRandom()
	require.NoError(t, err)
	iid := uid.String()
	args = append(args, "--invocation_id="+iid)

	cmd := testcli.BazelCommand(t, ws, args...)

	b, err := testcli.CombinedOutput(cmd)
	require.NoErrorf(t, err, "output: %s", string(b))

	// Sidecar should not log any errors.
	require.NotContains(t, string(b), "sidecar errors")

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
		"BUILD": `load("@rules_shell//shell:sh_test.bzl", "sh_test")
sh_test(name = "test", srcs = ["test.sh"])`,
	})

	term := testcli.PTY(t)
	exitCode, err := term.Run(testcli.BazelCommand(t, ws, "test", "...", "--test_output=streamed"))
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)

	// Make sure Bazel's progress output doesn't get interspersed with the test
	// output.
	require.Contains(t, term.Render(), "1\n2\n3\n4\n5\n")
	// Make sure Bazel understands that it's connected to a terminal - it should
	// produce colorful output.
	require.Contains(t, term.Render(), "\x1b[32mINFO")
}

func TestTargetPatternFile(t *testing.T) {
	for _, tc := range []struct {
		name              string
		args              []string
		wantDefaultTarget bool
	}{
		{name: "build direct", args: []string{"build", "--target_pattern_file=targets.txt"}},
		{name: "test direct", args: []string{"test", "--target_pattern_file=targets.txt"}},
		{name: "build config", args: []string{"build", "--config=pattern-file"}},
		{name: "test config", args: []string{"test", "--config=pattern-file"}},
		{name: "test default", args: []string{"test"}, wantDefaultTarget: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ws := newFakeBazelWorkspace(t, `printf '%s\n' "$@"`)
			testfs.WriteAllFileContents(t, ws, map[string]string{
				".bazelrc":    `build:pattern-file --target_pattern_file=targets.txt`,
				"targets.txt": "//:pass",
			})

			b, err := testcli.Output(testcli.BazelCommand(t, ws, tc.args...))
			require.NoErrorf(t, err, "output: %s", string(b))
			args := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
			require.Contains(t, args, tc.args[0])
			if tc.wantDefaultTarget {
				require.Contains(t, args, "//...")
				require.NotContains(t, args, "--target_pattern_file=targets.txt")
			} else {
				// Configs are resolved for CLI decisions, but left intact for
				// Bazel to expand. Either form must suppress the default target.
				require.Contains(t, args, tc.args[1])
				require.NotContains(t, args, "//...")
			}
		})
	}
}

func TestQueryFile(t *testing.T) {
	ws := newFakeBazelWorkspace(t, `printf '%s\n' "$@"`)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"targets.txt": "//:nop",
	})

	b, err := testcli.Output(testcli.BazelCommand(t, ws, "query", "--query_file=targets.txt"))
	require.NoErrorf(t, err, "output: %s", string(b))
	args := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	require.Contains(t, args, "query")
	require.Contains(t, args, "--query_file=targets.txt")
	require.NotContains(t, args, "//...")
}

func TestFixDiff(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"MODULE.bazel": `module(  name = "cli_test"    )`,
	})
	cmd := testcli.Command(t, ws, "fix", "--diff")
	stdout, stderr, err := testcli.SplitOutput(cmd)
	// TODO: a non-empty diff probably *should* return an error (exit code 1)
	require.NoError(t, err, "stdout: %q\nstderr: %q", string(stdout), string(stderr))
	require.NotEmpty(t, string(stdout))
}

func TestCLIDoesNotRestartBazelServer(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"BUILD": "",
		".bazelrc": `
startup --host_jvm_args=-DBAZEL_TRACK_SOURCE_DIRECTORIES=1
`,
	})

	cmd := testcli.BazelCommand(t, ws, "query", "//...")
	b, err := testcli.CombinedOutput(cmd)
	require.NoErrorf(t, err, "output: %s", string(b))
	require.NotContains(t, string(b), "Running Bazel server needs to be killed")
}

func TestBazelModDumpRepoMappingEmptyString(t *testing.T) {
	ws := newFakeBazelWorkspace(t, `
args=("$@")
[[ ${#args[@]} -ge 3 ]]
[[ "${args[${#args[@]}-1]}" == "" ]]
[[ "${args[${#args[@]}-2]}" == "dump_repo_mapping" ]]
printf '%s\n' 'empty repo mapping argument preserved'
`)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		// Add a nop plugin to make sure we properly handle args when there is
		// at least one plugin in the pre-bazel plugin pipeline.
		"testplugin/pre_bazel.sh": `#!/usr/bin/env bash`,
		"buildbuddy.yaml": `
plugins:
- path: testplugin
`,
	})
	cmd := testcli.Command(t, ws, "mod", "dump_repo_mapping", "")
	b, err := testcli.Output(cmd)
	require.NoErrorf(t, err, "output: %s", string(b))
	require.Equal(t, "empty repo mapping argument preserved\n", string(b))
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

func TestLateBazelrcAddedByPreBazelPlugin(t *testing.T) {
	for _, tc := range []struct {
		name             string
		pluginStartupArg string
		wantSuccess      bool
	}{
		{
			name:             "add bazelrc",
			pluginStartupArg: "--bazelrc=${BUILD_WORKSPACE_DIRECTORY}/required.bazelrc",
			wantSuccess:      true,
		},
		{name: "add --ignore_all_rc_files", pluginStartupArg: "--ignore_all_rc_files", wantSuccess: false},
		{name: "add --bazelrc=/dev/null", pluginStartupArg: "--bazelrc=/dev/null", wantSuccess: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ws := newWorkspaceWithRequiredRC(t)
			testfs.WriteAllFileContents(t, ws, map[string]string{
				"buildbuddy.yaml": `
plugins:
- path: testplugin
`,
				// This plugin prepends startup args before the forwarded Bazel args.
				"testplugin/pre_bazel.sh": `#!/usr/bin/env bash
tmp="$(mktemp)"
printf '%s\n' "` + tc.pluginStartupArg + `" > "$tmp"
cat "$FORWARDED_BAZEL_ARGS_FILE" >> "$tmp"
mv "$tmp" "$FORWARDED_BAZEL_ARGS_FILE"
`})
			testfs.MakeExecutable(t, ws, "testplugin/pre_bazel.sh")

			args := []string{"--bazelrc=" + ws + "/unrelated.bazelrc", "test", "--test_output=all", ":needs_required_rc"}
			b, err := testcli.CombinedOutput(testcli.BazelCommand(t, ws, args...))
			output := strings.ReplaceAll(string(b), "\r\n", "\n")

			if tc.wantSuccess {
				require.NoErrorf(t, err, "output: %s", output)
				require.Contains(t, output, "REQUIRED_RC_VALUE=1")
				require.Contains(t, output, "UNRELATED_RC_VALUE=1")
			} else {
				require.Errorf(t, err, "output: %s", output)
				require.Contains(t, output, "required bazelrc was not applied")
				require.NotContains(t, output, "REQUIRED_RC_VALUE=1")
				require.NotContains(t, output, "UNRELATED_RC_VALUE=1")
			}
		})
	}
}

func TestLateBazelrcAddedByBazeliskWrapper(t *testing.T) {
	for _, tc := range []struct {
		name              string
		wrapperStartupArg string
		wantSuccess       bool
	}{
		{
			name:              "add bazelrc",
			wrapperStartupArg: "--bazelrc=$WORKSPACE/required.bazelrc",
			wantSuccess:       true,
		},
		{name: "add --ignore_all_rc_files", wrapperStartupArg: "--ignore_all_rc_files", wantSuccess: false},
		{name: "add --bazelrc=/dev/null", wrapperStartupArg: "--bazelrc=/dev/null", wantSuccess: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ws := newWorkspaceWithRequiredRC(t)
			wrapperStartupArg := strings.ReplaceAll(tc.wrapperStartupArg, "$WORKSPACE", ws)
			testfs.WriteAllFileContents(t, ws, map[string]string{
				// If tools/bazel is present in the workspace root, Bazelisk will run it instead of the regular bazel binary.
				// This wrapper script injects a startup arg before the forwarded Bazel args.
				"tools/bazel": `#!/usr/bin/env bash
set -euo pipefail
exec "$BAZEL_REAL" "` + wrapperStartupArg + `" "$@"
`,
			})
			testfs.MakeExecutable(t, ws, "tools/bazel")

			args := []string{"--bazelrc=" + ws + "/unrelated.bazelrc", "test", "--test_output=all", ":needs_required_rc"}
			b, err := testcli.CombinedOutput(testcli.BazelCommand(t, ws, args...))
			output := strings.ReplaceAll(string(b), "\r\n", "\n")

			if tc.wantSuccess {
				require.NoErrorf(t, err, "output: %s", output)
				require.Contains(t, output, "REQUIRED_RC_VALUE=1")
				require.Contains(t, output, "UNRELATED_RC_VALUE=1")
			} else {
				require.Errorf(t, err, "output: %s", output)
				require.Contains(t, output, "required bazelrc was not applied")
				require.NotContains(t, output, "REQUIRED_RC_VALUE=1")
				require.NotContains(t, output, "UNRELATED_RC_VALUE=1")
			}
		})
	}
}

func TestLateConfigAddedByBazeliskWrapper(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bazelrc": `
test:rbe --test_env=RBE_CONFIG_VALUE=1
test:hello --test_env=HELLO_CONFIG_VALUE=1
`,
		"BUILD": `load("@rules_shell//shell:sh_test.bzl", "sh_test")

sh_test(name = "needs_rbe_config", srcs = ["needs_rbe_config.sh"])
`,
		"needs_rbe_config.sh": `#!/usr/bin/env bash
if [[ "${HELLO_CONFIG_VALUE:-}" != "1" ]]; then
  echo "hello config was not applied" >&2
  exit 1
fi
if [[ "${RBE_CONFIG_VALUE:-}" != "1" ]]; then
  echo "rbe config was not applied" >&2
  exit 1
fi
`,
		// If tools/bazel is present in the workspace root, Bazelisk will run it
		// instead of the regular bazel binary.
		// This wrapper injects --config=rbe after the bazel command is specified.
		"tools/bazel": `#!/usr/bin/env bash
set -euo pipefail
args=()
inserted=0
for arg in "$@"; do
	args+=("$arg")
	if [[ "$inserted" -eq 0 && "$arg" != -* ]]; then
		if [[ "$arg" == "test" ]]; then
			args+=("--config=rbe")
		fi
		inserted=1
	fi
done
echo "args=${args[*]}" >&2
exec "$BAZEL_REAL" "${args[@]}"
`,
	})
	testfs.MakeExecutable(t, ws, "needs_rbe_config.sh")
	testfs.MakeExecutable(t, ws, "tools/bazel")

	args := []string{"test", "--test_output=errors", ":needs_rbe_config", "--config=hello"}
	b, err := testcli.CombinedOutput(testcli.BazelCommand(t, ws, args...))
	output := strings.ReplaceAll(string(b), "\r\n", "\n")

	require.NoErrorf(t, err, "output: %s", output)
	require.NotContains(t, output, "rbe config was not applied")
	require.NotContains(t, output, "hello config was not applied")
}

// Creates a workspace that verifies --bazelrc=required.bazelrc is applied.
func newWorkspaceWithRequiredRC(t *testing.T) string {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"BUILD": `load("@rules_shell//shell:sh_test.bzl", "sh_test")

sh_test(name = "needs_required_rc", srcs = ["needs_required_rc.sh"])
`,
		"needs_required_rc.sh": `#!/usr/bin/env bash
echo "REQUIRED_RC_VALUE=${REQUIRED_RC_VALUE:-}"
echo "UNRELATED_RC_VALUE=${UNRELATED_RC_VALUE:-}"
if [[ "${REQUIRED_RC_VALUE:-}" == "1" ]]; then
  exit 0
fi
echo "required bazelrc was not applied" >&2
exit 1
`,
		"required.bazelrc":  `test --test_env=REQUIRED_RC_VALUE=1`,
		"unrelated.bazelrc": `test --test_env=UNRELATED_RC_VALUE=1`,
	})
	testfs.MakeExecutable(t, ws, "needs_required_rc.sh")
	return ws
}

func TestWorkspaceBazelrcAppliedOnce(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bazelrc": `test --test_arg=workspace-rc`,
		"BUILD": `load("@rules_shell//shell:sh_test.bzl", "sh_test")

sh_test(name = "assert_single_workspace_rc_arg", srcs = ["assert_single_workspace_rc_arg.sh"])
`,
		"assert_single_workspace_rc_arg.sh": `#!/usr/bin/env bash
count=0
for arg in "$@"; do
  if [[ "$arg" == "workspace-rc" ]]; then
    count=$((count + 1))
  fi
done
if [[ "$count" -ne 1 ]]; then
  echo "expected workspace .bazelrc to be applied once" >&2
  echo "count=$count args=$*" >&2
  exit 1
fi
`,
	})
	testfs.MakeExecutable(t, ws, "assert_single_workspace_rc_arg.sh")

	b, err := testcli.CombinedOutput(testcli.BazelCommand(t, ws, "test", "--test_output=errors", ":assert_single_workspace_rc_arg"))
	require.NoErrorf(t, err, "output: %s", string(b))
}

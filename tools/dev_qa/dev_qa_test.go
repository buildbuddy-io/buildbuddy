// Package dev_qa contains the dev QA test: a single test target whose subtests
// each run one "inner" bazel or bb invocation against a deployed BuildBuddy
// environment and fail if that invocation fails.
//
// Each subtest shows up as its own test case on the outer invocation's page,
// with the inner invocation URL(s) logged at the end of its output.
//
// Non-secret parameters are flags (pass them with --test_arg). Secrets are
// read from the environment (pass them with --test_env=NAME so that the values
// never appear in the outer invocation's options).
//
// Suites:
//
//	abseil_cpp, rules_python, bazel_gazelle, buildbuddy
//	    Download a release tarball, inject the BuildBuddy RBE toolchain and
//	    run `bazel test //...` against dev RBE (see dev_qa_test_runner.sh).
//	    Needs BB_QA_DEV_KEY.
//	remote_bazel
//	    `bb remote test //server/util/lru/...` on a Linux dev remote runner.
//	    Needs BB_QA_DEV_KEY and -buildbuddy_commit.
//	mac_remote_runner
//	    `bb remote test //server/util/...` on a Mac arm64 dev remote runner.
//	    Needs BB_DEV_MAC_QA_KEY and -buildbuddy_commit.
//	webdriver
//	    Builds and runs the webdriver invocation tests on dev RBE, driving a
//	    browser against the dev app. Needs BB_QA_DEV_KEY, DEV_UI_PROBER_SLUG
//	    and -buildbuddy_commit.
//	mac_rbe
//	    A few abseil_cpp tests driven from a macOS host so that the inner
//	    bazel targets Mac RBE executors. Skipped on other hosts.
//	    Needs BB_DEV_MAC_QA_KEY.
//
// Run a subset with --test_filter, e.g. --test_filter=TestDevQA/mac_rbe.
package dev_qa

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/stretchr/testify/require"
)

var (
	buildbuddyCommit  = flag.String("buildbuddy_commit", "", "buildbuddy-io/buildbuddy commit SHA to run the remote_bazel, mac_remote_runner and webdriver suites against.")
	buildbuddyRepoURL = flag.String("buildbuddy_repo_url", "https://github.com/buildbuddy-io/buildbuddy.git", "Git URL that buildbuddy is cloned from.")

	qaAppEndpoint  = flag.String("qa_app_endpoint", "buildbuddy-qa-dev.buildbuddy.dev", "App host for the OSS repo, remote_bazel and webdriver suites.")
	qaGRPCEndpoint = flag.String("qa_grpc_endpoint", "buildbuddy-qa-dev.buildbuddy.dev", "gRPC host for the OSS repo, remote_bazel and webdriver suites.")

	macAppEndpoint  = flag.String("mac_app_endpoint", "app.buildbuddy.dev", "App host for the mac_rbe and mac_remote_runner suites.")
	macGRPCEndpoint = flag.String("mac_grpc_endpoint", "remote.buildbuddy.dev", "gRPC host for the mac_rbe and mac_remote_runner suites.")

	remoteRunner = flag.String("remote_runner", "grpcs://remote.buildbuddy.dev", "Remote runner target for the bb remote suites.")

	webdriverAppEndpoint = flag.String("webdriver_app_endpoint", "https://app.buildbuddy.dev", "App URL that the webdriver suite drives a browser against (the browser logs in to the group that -remote_sso_slug selects there).")

	// Injected via x_defs.
	runnerScriptRlocationpath string
	bazel8Rlocationpath       string
	bazel9Rlocationpath       string
	bbRlocationpath           string
)

// Environment variables holding secrets.
const (
	qaDevKeyEnv   = "BB_QA_DEV_KEY"
	macQAKeyEnv   = "BB_DEV_MAC_QA_KEY"
	proberSlugEnv = "DEV_UI_PROBER_SLUG"
)

// ossRepo is a third-party repo that dev_qa_test_runner.sh downloads and runs
// bazel in. env is passed to the runner script verbatim.
type ossRepo struct {
	name string
	env  map[string]string
}

var (
	abseilCpp = ossRepo{
		name: "abseil_cpp",
		env: map[string]string{
			"QA_TARBALL_URL":   "https://github.com/abseil/abseil-cpp/archive/refs/tags/20250814.1.tar.gz",
			"QA_STRIP_PREFIX":  "abseil-cpp-20250814.1",
			"QA_BAZEL_COMMAND": "test //...",
			// Exclude tests that need timezone data (missing in RBE containers) and benchmarks.
			"QA_EXTRA_BAZEL_FLAGS": "--test_tag_filters=-performance,-webdriver,-docker,-bare,-benchmark -- -//absl/time:time_test -//absl/time/internal/cctz:time_zone_format_test -//absl/time/internal/cctz:time_zone_lookup_test -//absl/random/internal:randen_benchmarks",
		},
	}

	// abseilCppSmoke is abseilCpp cut down to a few tests: enough to prove that
	// an executor pool can compile, link and run C++ without waiting for all of
	// //... (which takes ~7 minutes on Mac RBE).
	abseilCppSmoke = ossRepo{
		name: "abseil_cpp_smoke",
		env: map[string]string{
			"QA_TARBALL_URL":   abseilCpp.env["QA_TARBALL_URL"],
			"QA_STRIP_PREFIX":  abseilCpp.env["QA_STRIP_PREFIX"],
			"QA_BAZEL_COMMAND": "test //absl/base:config_test //absl/strings:str_cat_test //absl/container:flat_hash_map_test",
		},
	}

	ossRepos = []ossRepo{
		abseilCpp,
		{
			name: "rules_python",
			env: map[string]string{
				"QA_TARBALL_URL":       "https://github.com/bazelbuild/rules_python/archive/refs/tags/1.6.3.tar.gz",
				"QA_STRIP_PREFIX":      "rules_python-1.6.3",
				"QA_BAZEL_COMMAND":     "test //...",
				"QA_EXTRA_BAZEL_FLAGS": "--test_tag_filters=-performance,-webdriver,-docker,-bare,-integration-test,-acceptance-test --build_tag_filters=-integration-test -- -//tests/implicit_namespace_packages/... -//tests/whl_with_build_files/... -//tests/toolchains/...",
			},
		},
		{
			name: "bazel_gazelle",
			env: map[string]string{
				"QA_TARBALL_URL":       "https://github.com/bazel-contrib/bazel-gazelle/archive/refs/tags/v0.53.0.tar.gz",
				"QA_STRIP_PREFIX":      "bazel-gazelle-0.53.0",
				"QA_BAZEL_COMMAND":     "test //...",
				"QA_EXTRA_BAZEL_FLAGS": "-- -//internal:bazel_test -//cmd/gazelle:gazelle_test -//docs/... -//tests/bzl_deps/...",
			},
		},
		{
			name: "buildbuddy",
			env: map[string]string{
				"QA_TARBALL_URL":   "https://github.com/buildbuddy-io/buildbuddy/tarball/eb9115eafcdcd3c356fc470649c58a473910ae3c",
				"QA_STRIP_PREFIX":  "buildbuddy-io-buildbuddy-eb9115e",
				"QA_BAZEL_COMMAND": "test //...",
				"INJECT_TOOLCHAIN": "false",
				"UPDATE_LOCKFILE":  "false",
			},
		},
	}
)

func TestDevQA(t *testing.T) {
	for _, repo := range ossRepos {
		t.Run(repo.name, func(t *testing.T) {
			t.Parallel()
			runOSSRepo(t, repo, *qaAppEndpoint, *qaGRPCEndpoint, secret(t, qaDevKeyEnv))
		})
	}

	t.Run("remote_bazel", func(t *testing.T) {
		t.Parallel()
		apiKey := secret(t, qaDevKeyEnv)
		repoDir := cloneBuildBuddy(t)
		runBB(t, repoDir,
			"remote",
			"--run_from_commit="+*buildbuddyCommit,
			"--remote_runner="+*remoteRunner,
			"--env=GIT_REPO_DEFAULT_BRANCH=master",
			"test", "//server/util/lru/...",
			"--nocache_test_results",
			"--bes_results_url=https://"+*qaAppEndpoint+"/invocation/",
			"--bes_backend=grpcs://"+*qaGRPCEndpoint,
			"--remote_cache=grpcs://"+*qaGRPCEndpoint,
			"--remote_executor=grpcs://"+*qaGRPCEndpoint,
			"--remote_header=x-buildbuddy-api-key="+apiKey,
		)
	})

	t.Run("mac_remote_runner", func(t *testing.T) {
		t.Parallel()
		apiKey := secret(t, macQAKeyEnv)
		repoDir := cloneBuildBuddy(t)
		runBB(t, repoDir,
			"remote",
			"--os=darwin",
			"--arch=arm64",
			"--run_from_commit="+*buildbuddyCommit,
			"--remote_runner="+*remoteRunner,
			"--env=GIT_REPO_DEFAULT_BRANCH=master",
			"--timeout=45m",
			"test", "//server/util/...",
			"--test_tag_filters=-performance,-webdriver,-docker,-bare",
			"--nocache_test_results",
			"--bes_results_url=https://"+*macAppEndpoint+"/invocation/",
			"--bes_backend=grpcs://"+*macGRPCEndpoint,
			"--remote_cache=grpcs://"+*macGRPCEndpoint,
			"--remote_header=x-buildbuddy-api-key="+apiKey,
		)
	})

	t.Run("webdriver", func(t *testing.T) {
		t.Parallel()
		apiKey := secret(t, qaDevKeyEnv)
		slug := secret(t, proberSlugEnv)
		repoDir := cloneBuildBuddy(t)
		// probers-shared selects the RBE platforms and toolchains (and
		// minimal downloads) without choosing an endpoint; those are given
		// explicitly below.
		err := os.WriteFile(filepath.Join(repoDir, "user.bazelrc"), []byte("build --config=probers-shared\n"), 0644)
		require.NoError(t, err)
		runCommand(t, repoDir, nil, runfile(t, bazel9Rlocationpath),
			"test", "enterprise/server/test/webdriver/invocation/...",
			"--remote_header=x-buildbuddy-api-key="+apiKey,
			"--bes_results_url=https://"+*qaAppEndpoint+"/invocation/",
			"--bes_backend=grpcs://"+*qaGRPCEndpoint,
			"--remote_cache=grpcs://"+*qaGRPCEndpoint,
			"--remote_executor=grpcs://"+*qaGRPCEndpoint,
			"--test_arg=-webdriver_target=remote",
			"--test_arg=-remote_app_endpoint="+*webdriverAppEndpoint,
			"--test_arg=-remote_sso_slug="+slug,
			"--test_arg=-webdriver_implicit_wait_timeout=10s",
			"--test_arg=-webdriver_verbose",
			"--flaky_test_attempts=3",
			"--verbose_failures",
		)
	})

	t.Run("mac_rbe", func(t *testing.T) {
		if runtime.GOOS != "darwin" {
			t.Skip("mac_rbe must be driven from a macOS host so that the inner bazel targets Mac RBE executors")
		}
		t.Parallel()
		runOSSRepo(t, abseilCppSmoke, *macAppEndpoint, *macGRPCEndpoint, secret(t, macQAKeyEnv))
	})
}

// runOSSRepo runs dev_qa_test_runner.sh for the given repo against the given
// BuildBuddy endpoints.
func runOSSRepo(t *testing.T, repo ossRepo, appEndpoint, grpcEndpoint, apiKey string) {
	env := []string{
		"DEV_QA_BAZEL_BINARY=" + runfile(t, bazel8Rlocationpath),
		"DEV_QA_WORKSPACE_DIR=" + t.TempDir(),
		"BB_APP_ENDPOINT=" + appEndpoint,
		"BB_GRPC_ENDPOINT=" + grpcEndpoint,
		"BB_API_KEY=" + apiKey,
	}
	for k, v := range repo.env {
		env = append(env, k+"="+v)
	}
	runCommand(t, "", env, "bash", runfile(t, runnerScriptRlocationpath))
}

// runBB runs the pinned bb CLI in the given buildbuddy checkout.
func runBB(t *testing.T, repoDir string, args ...string) {
	runNamedCommand(t, "bb", repoDir, []string{"GIT_REPO_DEFAULT_BRANCH=master"}, runfile(t, bbRlocationpath), args...)
}

// cloneBuildBuddy makes a shallow clone of buildbuddy at -buildbuddy_commit and
// returns its path. A fresh clone is used rather than the outer workspace so
// that inner bazel invocations don't contend with the outer bazel server.
func cloneBuildBuddy(t *testing.T) string {
	require.NotEmpty(t, *buildbuddyCommit, "-buildbuddy_commit is required for this suite (pass --test_arg=-buildbuddy_commit=<sha>)")
	dir := filepath.Join(t.TempDir(), "buildbuddy")
	require.NoError(t, os.MkdirAll(dir, 0755))
	git := func(args ...string) {
		runCommand(t, dir, nil, "git", args...)
	}
	git("init", "-q")
	git("remote", "add", "origin", *buildbuddyRepoURL)
	git("fetch", "-q", "--depth=1", "origin", *buildbuddyCommit)
	git("checkout", "-q", "FETCH_HEAD")
	return dir
}

// secret returns the value of the given environment variable, failing the test
// if it's unset. The value is redacted from anything this test logs.
func secret(t *testing.T, name string) string {
	value := os.Getenv(name)
	require.NotEmptyf(t, value, "%s must be set (pass --test_env=%s)", name, name)
	redactor.add(value)
	return value
}

func runfile(t *testing.T, rlocationpath string) string {
	path, err := runfiles.Rlocation(rlocationpath)
	require.NoError(t, err, "look up runfile %q", rlocationpath)
	return path
}

// runCommand runs a command with the given extra environment, streaming its
// output to the test log, and fails the test if it exits non-zero.
func runCommand(t *testing.T, dir string, env []string, name string, args ...string) {
	t.Helper()
	runNamedCommand(t, filepath.Base(name), dir, env, name, args...)
}

// runNamedCommand is runCommand with an explicit display name for the command
// (the pinned binaries have unhelpful file names like "downloaded").
func runNamedCommand(t *testing.T, displayName, dir string, env []string, name string, args ...string) {
	t.Helper()

	ctx, cancel := commandContext(t)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	// Give each command its own HOME so that nested bazel servers, bb config
	// etc. land in the test's temp dir.
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
	cmd.Env = append(cmd.Env, env...)
	// Run in a new process group and kill the whole group on timeout so that
	// nested bazel servers don't outlive the test.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 10 * time.Second

	out := newOutput(t.Name(), os.Stdout)
	cmd.Stdout = out
	cmd.Stderr = out

	t.Logf("Running: %s", redactor.redact(strings.Join(append([]string{displayName}, args...), " ")))
	err := cmd.Run()
	out.flush()
	for _, url := range out.invocationURLs {
		t.Logf("Inner invocation: %s", url)
	}
	require.NoError(t, err, "%s failed", displayName)
}

// commandContext returns a context that expires shortly before the test
// deadline, leaving time to kill the child and flush its output.
func commandContext(t *testing.T) (context.Context, context.CancelFunc) {
	if deadline, ok := t.Deadline(); ok {
		return context.WithDeadline(context.Background(), deadline.Add(-30*time.Second))
	}
	return context.WithCancel(context.Background())
}

var invocationURLRegexp = regexp.MustCompile(`https?://\S+/invocation/[0-9a-fA-F-]+`)

// output prefixes each line of a command's output with the subtest name (so
// that the output of parallel subtests can be told apart), redacts secrets,
// and collects any invocation URLs seen.
type output struct {
	mu             sync.Mutex
	prefix         string
	w              io.Writer
	partial        bytes.Buffer
	invocationURLs []string
	seen           map[string]bool
}

func newOutput(prefix string, w io.Writer) *output {
	return &output{prefix: "[" + prefix + "] ", w: w, seen: map[string]bool{}}
}

func (o *output) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.partial.Write(p)
	for {
		line, err := o.partial.ReadString('\n')
		if err != nil {
			// Incomplete line; keep it for the next write.
			o.partial.Reset()
			o.partial.WriteString(line)
			break
		}
		o.emit(line)
	}
	return len(p), nil
}

func (o *output) flush() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.partial.Len() > 0 {
		o.emit(o.partial.String() + "\n")
		o.partial.Reset()
	}
}

func (o *output) emit(line string) {
	line = redactor.redact(line)
	for _, url := range invocationURLRegexp.FindAllString(line, -1) {
		if !o.seen[url] {
			o.seen[url] = true
			o.invocationURLs = append(o.invocationURLs, url)
		}
	}
	fmt.Fprint(o.w, o.prefix+line)
}

// secretRedactor replaces known secret values in strings before they're logged.
type secretRedactor struct {
	mu     sync.Mutex
	values []string
}

var redactor = &secretRedactor{}

func (r *secretRedactor) add(value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values = append(r.values, value)
}

func (r *secretRedactor) redact(s string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, v := range r.values {
		s = strings.ReplaceAll(s, v, "<redacted>")
	}
	return s
}

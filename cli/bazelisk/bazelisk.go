package bazelisk

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/bazelbuild/bazelisk/core"
	"github.com/bazelbuild/bazelisk/repositories"
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
)

var (
	setVersionOnce sync.Once
	setVersionErr  error
)

type RunOpts struct {
	// Stdout is the Writer where bazelisk should write its stdout.
	// Defaults to os.Stdout if nil.
	Stdout io.Writer

	// Stderr is the Writer where bazelisk should write its stderr.
	// Defaults to os.Stderr if nil.
	Stderr io.Writer
}

func Run(args []string, opts *RunOpts) (exitCode int, err error) {
	// If we were already invoked via bazelisk, then set the bazel version to
	// the next version appearing in the .bazelversion file so that bazelisk
	// doesn't just invoke us again (resulting in an infinite loop).
	if err := setBazelVersion(); err != nil {
		return -1, fmt.Errorf("failed to set bazel version: %s", err)
	}
	gcs := &repositories.GCSRepo{}
	gitHub := repositories.CreateGitHubRepo(core.GetEnvOrConfig("BAZELISK_GITHUB_TOKEN"))
	// Fetch releases, release candidates and Bazel-at-commits from GCS, forks from GitHub
	repos := core.CreateRepositories(gcs, gcs, gitHub, gcs, gitHub, true)

	if opts.Stdout != nil {
		close, err := redirectStdio(opts.Stdout, &os.Stdout)
		if err != nil {
			return -1, err
		}
		defer close()
	}
	if opts.Stderr != nil {
		close, err := redirectStdio(opts.Stderr, &os.Stderr)
		if err != nil {
			return -1, err
		}
		defer close()
	}
	return core.RunBazelisk(args, repos)
}

// ConfigureRunScript adds `--script_path` to a bazel run command so that we can
// invoke the build and the run separately.
func ConfigureRunScript(args []string) (newArgs []string, scriptPath string, err error) {
	if arg.GetCommand(args) != "run" {
		return args, "", nil
	}
	// If --script_path is already set, don't create a run script ourselves,
	// since the caller probably has the intention to invoke it on their own.
	existingScript := arg.Get(args, "script_path")
	if existingScript != "" {
		return args, "", nil
	}
	script, err := os.CreateTemp("", "bb-run-*")
	if err != nil {
		return nil, "", err
	}
	defer script.Close()
	scriptPath = script.Name()
	args = append(args, "--script_path="+scriptPath)
	return args, scriptPath, nil
}

func InvokeRunScript(path string) (exitCode int, err error) {
	if err := os.Chmod(path, 0755); err != nil {
		return -1, err
	}
	// TODO: Exec() replaces the current process, so it prevents us from running
	// post-run hooks (if we decide those will be supported). If we want to use
	// exec.Command() here instead of exec(), then we might need to manually
	// forward signals from the parent file watcher process.
	if err := syscall.Exec(path, nil, os.Environ()); err != nil {
		return -1, err
	}
	panic("unreachable")
}

// IsInvokedByBazelisk returns whether the CLI was invoked by bazelisk itself.
// This will return true when referencing a CLI release in .bazelversion such as
// "buildbuddy-io/0.0.13" and then running `bazelisk`.
func IsInvokedByBazelisk() bool {
	return os.Getenv("BAZELISK_SKIP_WRAPPER") == "true"
}

// makePipeWriter adapts a writer to an *os.File by using an os.Pipe().
// The returned file should not be closed; instead the returned closeFunc
// should be called to ensure that all data from the pipe is flushed to the
// underlying writer.
func makePipeWriter(w io.Writer) (pw *os.File, closeFunc func(), err error) {
	pr, pw, err := os.Pipe()
	if err != nil {
		return
	}
	done := make(chan struct{})
	go func() {
		io.Copy(w, pr)
		close(done)
	}()
	closeFunc = func() {
		pw.Close()
		// Wait until the pipe contents are flushed to w.
		<-done
	}
	return
}

// Redirects either os.Stdout or os.Stderr to the given writer. Calling the
// returned close function stops redirection.
func redirectStdio(w io.Writer, stdio **os.File) (close func(), err error) {
	original := *stdio
	var closePipe func()
	f, ok := w.(*os.File)
	if !ok {
		pw, c, err := makePipeWriter(w)
		if err != nil {
			return nil, err
		}
		closePipe = c
		f = pw
	}
	*stdio = f
	close = func() {
		if closePipe != nil {
			closePipe()
		}
		*stdio = original
	}
	return close, nil
}

func createRepositories() *core.Repositories {
	gcs := &repositories.GCSRepo{}
	gitHub := repositories.CreateGitHubRepo(core.GetEnvOrConfig("BAZELISK_GITHUB_TOKEN"))
	// Fetch releases, release candidates and Bazel-at-commits from GCS, forks from GitHub
	return core.CreateRepositories(gcs, gcs, gitHub, gcs, gitHub, true)
}

func getBazeliskHome() (string, error) {
	bazeliskHome := core.GetEnvOrConfig("BAZELISK_HOME")
	if bazeliskHome != "" {
		return bazeliskHome, nil
	}
	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("could not get the user's cache directory: %v", err)
	}
	return filepath.Join(userCacheDir, "bazelisk"), nil
}

// ResolveVersion returns the actual bazel version that will be used by
// bazelisk.
// This will either return a version string like "5.0.0" or an absolute path to
// a local bazel binary (which bazelisk also supports).
func ResolveVersion() (string, error) {
	if err := setBazelVersion(); err != nil {
		return "", err
	}
	rawVersion := os.Getenv("USE_BAZEL_VERSION")
	// If using a file path "version", return the file path directly.
	if filepath.IsAbs(rawVersion) {
		return rawVersion, nil
	}
	bazeliskHome, err := getBazeliskHome()
	if err != nil {
		return "", err
	}
	// TODO: Support forks?
	fork := ""
	repos := createRepositories()
	version, _, err := repos.ResolveVersion(bazeliskHome, fork, rawVersion)
	if err != nil {
		return "", fmt.Errorf("failed to resolve bazel version %s: %s", version, err)
	}
	return version, nil
}

func setBazelVersion() error {
	setVersionOnce.Do(func() {
		setVersionErr = setBazelVersionImpl()
	})
	return setVersionErr
}

func setBazelVersionImpl() error {
	// If USE_BAZEL_VERSION is already set and not pointing to us (the BB CLI),
	// preserve that value.
	envVersion := os.Getenv("USE_BAZEL_VERSION")
	if envVersion != "" && !strings.HasPrefix(envVersion, "buildbuddy-io/") {
		return nil
	}

	// TODO: Handle the cases where we were invoked via .bazeliskrc
	// or USE_BAZEL_FALLBACK_VERSION (less common).

	ws, err := workspace.Path()
	if err != nil {
		return err
	}
	parts, err := ParseVersionDotfile(filepath.Join(ws, ".bazelversion"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	// If we appear first in .bazelversion, ignore that version to prevent
	// bazelisk from invoking us recursively.
	if IsInvokedByBazelisk() {
		for len(parts) > 0 && strings.HasPrefix(parts[0], "buildbuddy-io/") {
			parts = parts[1:]
		}
	}
	// If we couldn't find a non-BB bazel version in .bazelversion at this
	// point, default to "latest".
	if len(parts) == 0 {
		return os.Setenv("USE_BAZEL_VERSION", "latest")
	}

	return os.Setenv("USE_BAZEL_VERSION", parts[0])
}

// ParseVersionDotfile returns the non-empty lines from the given .bazelversion
// path.
func ParseVersionDotfile(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(string(b), "\n")
	var out []string
	for _, p := range parts {
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

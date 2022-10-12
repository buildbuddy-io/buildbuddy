package bazelisk

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/bazelbuild/bazelisk/core"
	"github.com/bazelbuild/bazelisk/repositories"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/plugin"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func RunWithPlugins(args []string, outputPath string, plugins []*plugin.Plugin) (int, error) {
	// Build the pipeline of bazel output handlers
	wc, err := plugin.PipelineWriter(os.Stdout, plugins)
	if err != nil {
		return -1, err
	}
	// Note, it's important that the Close() here happens just after the
	// bazelisk run completes and before we run post_bazel hooks, since this
	// waits for all plugins to finish writing to stdout. Otherwise, the
	// post_bazel output will get intermingled with bazel output.
	defer wc.Close()

	return Run(args, outputPath, wc)
}

func Run(args []string, outputPath string, w io.Writer) (int, error) {
	// If we were already invoked via bazelisk, then set the bazel version to
	// the next version appearing in the .bazelversion file so that bazelisk
	// doesn't just invoke us again (resulting in an infinite loop).
	if err := setBazelVersion(); err != nil {
		log.Fatalf("failed to set bazel version: %s", err)
	}

	log.Debugf("Calling bazelisk with %+v", args)

	gcs := &repositories.GCSRepo{}
	gitHub := repositories.CreateGitHubRepo(core.GetEnvOrConfig("BAZELISK_GITHUB_TOKEN"))
	// Fetch releases, release candidates and Bazel-at-commits from GCS, forks from GitHub
	repos := core.CreateRepositories(gcs, gcs, gitHub, gcs, gitHub, true)

	output, err := os.Create(outputPath)
	if err != nil {
		return 0, status.FailedPreconditionErrorf("failed to create output file: %s", err)
	}
	defer output.Close()

	// Temporarily redirect stdout/stderr so that we can capture the bazelisk
	// output.
	// Note, we might be able to use Bazel's "command.log" file instead, but
	// would need to find a way to avoid a race condition if another Bazel
	// command is run concurrently (since it will clear out command.log as
	// soon as our bazelisk invocation below is complete and it is able to
	// grab the workspace lock).
	originalStdout, originalStderr := os.Stdout, os.Stderr
	defer func() {
		os.Stdout, os.Stderr = originalStdout, originalStderr
	}()
	stdoutPipe, closeStdoutPipe, err := makePipeWriter(io.MultiWriter(output, w))
	if err != nil {
		return 0, err
	}
	defer closeStdoutPipe()
	os.Stdout = stdoutPipe
	stderrPipe, closeStderrPipe, err := makePipeWriter(io.MultiWriter(output, w))
	if err != nil {
		return 0, err
	}
	defer closeStderrPipe()
	os.Stderr = stderrPipe

	exitCode, err := core.RunBazelisk(args, repos)
	if err != nil {
		log.Fatalf("error running bazelisk: %s", err)
	}
	return exitCode, nil
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

func setBazelVersion() error {
	ws, err := workspace.Path()
	if err != nil {
		return err
	}
	b, err := disk.ReadFile(context.TODO(), filepath.Join(ws, ".bazelversion"))
	if err != nil {
		if !status.IsNotFoundError(err) {
			return err
		}
	}
	parts := strings.Split(string(b), "\n")
	// TODO: Handle the cases where we were invoked via .bazeliskrc,
	// USE_BAZEL_VERSION, or USE_BAZEL_FALLBACK_VERSION.

	// Bazelisk probably chose us because we were specified first in
	// .bazelversion. Delete the first line, if it exists.
	if len(parts) > 0 {
		parts = parts[1:]
	}
	// If we couldn't find a non-BB bazel version in .bazelversion at this
	// point, default to "latest".
	if len(parts) == 0 {
		return os.Setenv("USE_BAZEL_VERSION", "latest")
	}

	return os.Setenv("USE_BAZEL_VERSION", parts[0])
}

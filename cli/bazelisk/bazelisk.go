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
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func Run(args []string) (int, *Output, error) {
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

	output, err := os.CreateTemp("", "bazelisk-output-*")
	if err != nil {
		return 0, nil, status.FailedPreconditionErrorf("failed to create output file: %s", err)
	}

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
	stdoutPipe, closeStdoutPipe, err := makePipeWriter(io.MultiWriter(output, os.Stdout))
	if err != nil {
		return 0, nil, err
	}
	defer closeStdoutPipe()
	os.Stdout = stdoutPipe
	stderrPipe, closeStderrPipe, err := makePipeWriter(io.MultiWriter(output, os.Stderr))
	if err != nil {
		return 0, nil, err
	}
	defer closeStderrPipe()
	os.Stderr = stderrPipe

	exitCode, err := core.RunBazelisk(args, repos)
	if err != nil {
		log.Fatalf("error running bazelisk: %s", err)
	}
	return exitCode, &Output{output}, nil
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

// Output points to the stdout/stderr written by bazelisk. It is like a regular
// file, but the Close() method also deletes the file to avoid accumulating too
// many logs on disk.
type Output struct{ *os.File }

func (o *Output) Close() error {
	o.File.Close()
	return os.Remove(o.File.Name())
}

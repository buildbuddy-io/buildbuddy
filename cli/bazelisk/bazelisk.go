package bazelisk

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/bazelbuild/bazelisk/core"
	"github.com/bazelbuild/bazelisk/repositories"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	bblog "github.com/buildbuddy-io/buildbuddy/cli/logging"
)

func Run(args []string) {
	// If we were already invoked via bazelisk, then set the bazel version to
	// the next version appearing in the .bazelversion file so that bazelisk
	// doesn't just invoke us again (resulting in an infinite loop).
	if err := setBazelVersion(); err != nil {
		log.Fatal(err)
	}

	bblog.Printf("Calling bazelisk with %+v", args)

	gcs := &repositories.GCSRepo{}
	gitHub := repositories.CreateGitHubRepo(core.GetEnvOrConfig("BAZELISK_GITHUB_TOKEN"))
	// Fetch releases, release candidates and Bazel-at-commits from GCS, forks from GitHub
	repos := core.CreateRepositories(gcs, gcs, gitHub, gcs, gitHub, true)

	exitCode, err := core.RunBazelisk(args, repos)
	if err != nil {
		log.Fatal(err)
	}

	os.Exit(exitCode)
}

func setBazelVersion() error {
	ws, err := findWorkspacePath()
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

func findWorkspacePath() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		for _, basename := range []string{"WORKSPACE", "WORKSPACE.bazel"} {
			ex, err := disk.FileExists(context.TODO(), filepath.Join(dir, basename))
			if err != nil {
				return "", err
			}
			if ex {
				return dir, nil
			}
		}
		next := filepath.Dir(dir)
		if dir == next {
			// We've reached the root dir without finding a WORKSPACE file
			return "", status.FailedPreconditionError("not within a bazel workspace (could not find WORKSPACE or WORKSPACE.bazel file)")
		}
		dir = next
	}
}

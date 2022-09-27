package bazelisk

import (
	"log"
	"os"

	"github.com/bazelbuild/bazelisk/core"
	"github.com/bazelbuild/bazelisk/repositories"

	bblog "github.com/buildbuddy-io/buildbuddy/cli/logging"
)

func Run(args []string) {
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

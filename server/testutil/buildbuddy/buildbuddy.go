package buildbuddy

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
)

var (
	// set by x_defs in BUILD file
	buildbuddyRunfilePath  string
	localConfigRunfilePath string
)

// Run a local BuildBuddy server for the scope of the given test case.
func Run(t *testing.T, args ...string) *app.App {
	cacheDir := testfs.MakeTempDir(t)
	commandArgs := append([]string{
		"--cache.disk.root_directory=" + cacheDir,
	}, args...)
	return app.Run(
		t,
		/* commandPath= */ buildbuddyRunfilePath,
		commandArgs,
		/* configPath= */ localConfigRunfilePath,
	)
}

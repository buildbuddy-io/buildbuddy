package buildbuddy

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
)

var (
	// set by x_defs in BUILD file
	buildbuddyRunfilePath  string
	localConfigRunfilePath string
)

// Run a local BuildBuddy server for the scope of the given test case.
func Run(t *testing.T, args ...string) *app.App {
	return app.Run(
		t,
		/* commandPath= */ buildbuddyRunfilePath,
		/* commandArgs= */ args,
		/* configPath= */ localConfigRunfilePath,
	)
}

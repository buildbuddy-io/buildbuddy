package buildbuddy

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
)

// Run a local BuildBuddy server for the scope of the given test case.
func Run(t *testing.T, args ...string) *app.App {
	return app.Run(
		t,
		/* commandPath= */ "server/cmd/buildbuddy/buildbuddy_/buildbuddy",
		/* commandArgs= */ args,
		/* configPath= */ "config/buildbuddy.local.yaml",
	)
}

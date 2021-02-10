package buildbuddy

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
)

// Run a local BuildBuddy server for the scope of the given test case.
func Run(t *testing.T) *app.App {
	return app.Run(
		t,
		app.WithBuildBuddyCmdPath("server/cmd/buildbuddy/buildbuddy_/buildbuddy"),
		app.WithConfigPath("config/buildbuddy.local.yaml"),
	)
}

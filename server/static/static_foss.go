//go:build buildbuddy_foss

package static

import (
	"github.com/buildbuddy-io/buildbuddy/server/environment"
)

func githubAuthEnabled(env environment.Env) bool {
	return false
}

func trendsHeatmapEnabled() bool {
	return false
}

func remoteExecutionEnabled() bool {
	return false
}

func userOwnedExecutorsEnabled() bool {
	return false
}

func forceUserOwnedDarwinExecutors() bool {
	return false
}

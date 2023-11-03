//go:build !buildbuddy_foss

package static

import (
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/githubauth"
	"github.com/buildbuddy-io/buildbuddy/server/environment"

	iss_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_stat_service/config"
	remote_execution_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/config"
	scheduler_server_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_server/config"
)

func githubAuthEnabled(env environment.Env) bool {
	return githubauth.IsEnabled(env)
}

func trendsHeatmapEnabled() bool {
	return iss_config.TrendsHeatmapEnabled()
}

func remoteExecutionEnabled() bool {
	return remote_execution_config.RemoteExecutionEnabled()
}

func userOwnedExecutorsEnabled() bool {
	return scheduler_server_config.UserOwnedExecutorsEnabled()
}

func forceUserOwnedDarwinExecutors() bool {
	return scheduler_server_config.ForceUserOwnedDarwinExecutors()
}

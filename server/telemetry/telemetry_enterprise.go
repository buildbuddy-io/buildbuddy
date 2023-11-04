//go:build !buildbuddy_foss

package telemetry

import remote_execution_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/config"

func remoteExecutionEnabled() bool {
	return remote_execution_config.RemoteExecutionEnabled()
}

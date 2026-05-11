package config

import "github.com/buildbuddy-io/buildbuddy/server/util/flag"

var (
	enableUserOwnedExecutors          = flag.Bool("remote_execution.enable_user_owned_executors", false, "If enabled, users can register their own executors with the scheduler.")
	forceUserOwnedDarwinExecutors     = flag.Bool("remote_execution.force_user_owned_darwin_executors", false, "If enabled, darwin actions will always run on user-owned executors.")
	forceUserOwnedWindowsExecutors    = flag.Bool("remote_execution.force_user_owned_windows_executors", false, "If enabled, windows actions will always run on user-owned executors.")
	disableAnonymousArmLinuxExecution = flag.Bool("remote_execution.disable_anonymous_linux_arm64_execution", false, "If enabled, anonymous requests for ARM Linux remote build execution will be rejected.", flag.Internal)
)

func UserOwnedExecutorsEnabled() bool {
	return *enableUserOwnedExecutors
}

func ForceUserOwnedDarwinExecutors() bool {
	return *forceUserOwnedDarwinExecutors
}

func ForceUserOwnedWindowsExecutors() bool {
	return *forceUserOwnedWindowsExecutors
}

func DisableAnonymousArmLinuxExecution() bool {
	return *disableAnonymousArmLinuxExecution
}

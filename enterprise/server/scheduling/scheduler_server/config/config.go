package config

import "flag"

var (
	enableUserOwnedExecutors      = flag.Bool("remote_execution.enable_user_owned_executors", false, "If enabled, users can register their own executors with the scheduler.")
	forceUserOwnedDarwinExecutors = flag.Bool("remote_execution.force_user_owned_darwin_executors", false, "If enabled, darwin actions will always run on user-owned executors.")
)

func UserOwnedExecutorsEnabled() bool {
	return *enableUserOwnedExecutors
}

func ForceUserOwnedDarwinExecutors() bool {
	return *forceUserOwnedDarwinExecutors
}

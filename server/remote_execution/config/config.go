package config

import "flag"

var remoteExecEnableRemoteExec = flag.Bool("remote_execution.enable_remote_exec", true, "If true, enable remote-exec. ** Enterprise only **")

func RemoteExecutionEnabled() bool {
	return *remoteExecEnableRemoteExec
}

//go:build windows

package ci_runner_util

import "github.com/buildbuddy-io/buildbuddy/server/util/platform"

const (
	ExecutableName = platform.CIRunnerExecutableBaseName + ".exe"
	CLIBinaryName  = "bb.exe"
)

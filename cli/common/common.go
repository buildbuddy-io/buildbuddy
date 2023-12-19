package common

const (
	// If a cli subcommand handler returns this exit code, we should forward
	// the subcommand to bazel
	ForwardCommandToBazelExitCode = -10
)

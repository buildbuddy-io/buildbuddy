// Package commandtypes defines lightweight types for command execution
// results. It exists as a separate package so that commandutil (and by
// extension goinit) can reference these types without pulling in the
// heavy server/interfaces package.
package commandtypes

import (
	"io"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// Stdio specifies standard input / output readers for a command.
type Stdio struct {
	// Stdin is an optional stdin source for the executed process.
	Stdin io.Reader
	// Stdout is an optional stdout sink for the executed process.
	Stdout io.Writer
	// Stderr is an optional stderr sink for the executed process.
	Stderr io.Writer
}

// CommandResult captures the output and details of an executed command.
type CommandResult struct {
	// Error is populated only if the command was unable to be started, or if it was
	// started but never completed.
	//
	// In particular, if the command runs and returns a non-zero exit code (such as 1),
	// this is considered a successful execution, and this error will NOT be populated.
	//
	// In some cases, the command may have failed to start due to an issue unrelated
	// to the command itself. For example, the runner may execute the command in a
	// sandboxed environment but fail to create the sandbox. In these cases, the
	// Error field here should be populated with a gRPC error code indicating why the
	// command failed to start, and the ExitCode field should contain the exit code
	// from the sandboxing process, rather than the command itself.
	//
	// If the call to `exec.Cmd#Run` returned -1, meaning that the command was killed or
	// never exited, this field should be populated with a gRPC error code indicating the
	// reason, such as DEADLINE_EXCEEDED (if the command times out), UNAVAILABLE (if
	// there is a transient error that can be retried), or RESOURCE_EXHAUSTED (if the
	// command ran out of memory while executing).
	Error error
	// CommandDebugString indicates the command that was run, for debugging purposes only.
	CommandDebugString string
	// Stdout from the command. This may contain data even if there was an Error.
	Stdout []byte
	// Stderr from the command. This may contain data even if there was an Error.
	Stderr []byte
	// AuxiliaryLogs contain extra logs associated with the task that may be
	// useful to present to the user.
	AuxiliaryLogs map[string][]byte

	// DoNotRecycle indicates that a runner should not be reused after this
	// command has executed.
	DoNotRecycle bool

	// ExitCode is one of the following:
	// * The exit code returned by the executed command
	// * -1 if the process was killed or did not exit
	// * -2 (NoExitCode) if the exit code could not be determined because it returned
	//   an error other than exec.ExitError. This case typically means it failed to start.
	ExitCode int

	// UsageStats holds the command's measured resource usage. It may be nil if
	// resource measurement is not implemented by the command's isolation type.
	UsageStats *repb.UsageStats

	// VfsStats holds VFS-specific stats if VFS workspaces are enabled.
	VfsStats *repb.VfsStats

	// VMMetadata associated with the VM that ran the task, if applicable.
	VMMetadata *fcpb.VMMetadata
}

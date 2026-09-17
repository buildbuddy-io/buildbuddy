package agentutil

import (
	"io"
	"os"
)

const (
	Claude = "claude"
	Codex  = "codex"
)

const (
	SandboxReadOnly       = "read-only"
	SandboxWorkspaceWrite = "workspace-write"
	SandboxFullAccess     = "danger-full-access"
)

type RunRequest struct {
	Agent           string
	Model           string
	ReasoningEffort string
	Prompt          string

	// Output overrides the default output stream for the agent's result.
	Output io.Writer

	// Progress overrides the default output stream for the agent's
	// turn-by-turn activity and diagnostics.
	Progress io.Writer

	// ClaudeAllowedTools restricts which tools Claude may call.
	ClaudeAllowedTools []string

	// CodexSandbox is the filesystem access Codex runs with, defaulting to
	// SandboxReadOnly.
	CodexSandbox string

	// CodexArgs contains additional arguments passed directly to Codex.
	CodexArgs []string
}

// OutputWriter returns the writer for the agent's result.
func (r *RunRequest) OutputWriter() io.Writer {
	if r.Output != nil {
		return r.Output
	}
	return os.Stdout
}

// ProgressWriter returns the writer for the agent's turn-by-turn activity and
// diagnostics.
func (r *RunRequest) ProgressWriter() io.Writer {
	if r.Progress != nil {
		return r.Progress
	}
	return os.Stderr
}

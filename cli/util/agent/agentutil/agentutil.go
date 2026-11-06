package agentutil

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/armon/circbuf"
)

const StderrTailBytes int64 = 4 * 1024

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

	// Progress receives the agent's turn-by-turn activity and diagnostics.
	// By default, this output is discarded.
	//
	// This is only supported for Codex, which streams progress to stderr.
	// Claude does not separate its output into separate streams.
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
	return io.Discard
}

// FormatCommandError wraps a process error with the retained stderr tail.
func FormatCommandError(agent string, err error, stderr *circbuf.Buffer) error {
	err = fmt.Errorf("%s failed: %w", agent, err)
	output := bytes.TrimSpace(stderr.Bytes())
	if len(output) == 0 {
		return err
	}
	if stderr.TotalWritten() > stderr.Size() {
		return fmt.Errorf("%w\n[stderr truncated to last %d bytes]\n%s", err, stderr.Size(), output)
	}
	return fmt.Errorf("%w\n%s", err, output)
}

package agentutil

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

	// ClaudeAllowedTools restricts which tools Claude may call.
	ClaudeAllowedTools []string

	// CodexSandbox is the filesystem access Codex runs with, defaulting to
	// SandboxReadOnly.
	CodexSandbox string
}

type RunResponse struct {
	Output        string
	SessionID     string
	ResumeCommand string
}

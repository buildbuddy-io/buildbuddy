package agentutil

const (
	Claude = "claude"
	Codex  = "codex"
)

type RunRequest struct {
	Agent           string
	Model           string
	ReasoningEffort string
	Prompt          string
	AllowedTools    []string
}

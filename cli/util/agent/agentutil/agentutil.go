package agentutil

import (
	"os"
	"strings"
)

const (
	Claude = "claude"
	Codex  = "codex"
)

const repoTokenEnvVarName = "REPO_TOKEN"

type RunRequest struct {
	Agent           string
	Model           string
	ReasoningEffort string
	Prompt          string
	AllowedTools    []string
	// WritableWorkspace allows the agent to write within its sandboxed
	// workspace and temporary directories. The agent still runs without
	// approval prompts or unrestricted host access.
	WritableWorkspace bool
}

type RunResponse struct {
	Output        string
	SessionID     string
	ResumeCommand string
}

// ChildProcessEnv returns the environment that should be passed to an agent
// process. Repository credentials are used by the runner to check out the
// workspace, but the agent does not need direct access to them.
func ChildProcessEnv() []string {
	env := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if name == repoTokenEnvVarName {
			continue
		}
		env = append(env, entry)
	}
	return env
}

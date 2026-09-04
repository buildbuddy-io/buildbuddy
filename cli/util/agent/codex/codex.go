package codex

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/util/agent/agentutil"
)

// Run executes a Codex agent command.
//
// Codex doesn't support a tool allowlist, so request.AllowedTools is ignored.
// Instead, commands are run in a read-only sandbox.
func Run(ctx context.Context, request *agentutil.RunRequest) error {
	if _, err := exec.LookPath("codex"); err != nil {
		return fmt.Errorf("codex is not installed or not in PATH")
	}

	args := commandArgs(request)
	cmd := exec.CommandContext(ctx, "codex", args...)
	cmd.Stdin = strings.NewReader(request.Prompt)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stdout
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("codex failed: %w", err)
	}
	return nil
}

func commandArgs(request *agentutil.RunRequest) []string {
	// Codex does not support a tool allowlist equivalent to Claude's
	// --allowedTools. Run without approvals in a read-only sandbox.
	args := []string{
		"exec",
		"--sandbox", "read-only",
		"--config", `approval_policy="never"`,
		// Support not running in a git repository.
		"--skip-git-repo-check",
	}
	if request.Model != "" {
		args = append(args, "--model", request.Model)
	}
	if request.ReasoningEffort != "" {
		args = append(args, "--config", fmt.Sprintf("model_reasoning_effort=%q", request.ReasoningEffort))
	}
	return args
}

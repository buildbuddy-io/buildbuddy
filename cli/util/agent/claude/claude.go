package claude

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/util/agent/agentutil"
)

func Run(ctx context.Context, request *agentutil.RunRequest) error {
	if _, err := exec.LookPath("claude"); err != nil {
		return fmt.Errorf("claude is not installed or not in PATH")
	}

	args := commandArgs(request)
	cmd := exec.CommandContext(ctx, "claude", args...)
	cmd.Stdin = strings.NewReader(request.Prompt)
	cmd.Stdout = request.OutputWriter()
	cmd.Stderr = request.ProgressWriter()
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("claude failed: %w", err)
	}
	return nil
}

func commandArgs(request *agentutil.RunRequest) []string {
	// Run in dontAsk mode so unapproved tool calls are denied rather than prompting.
	args := []string{"--print", "--permission-mode", "dontAsk"}
	if request.Model != "" {
		args = append(args, "--model", request.Model)
	}
	if request.ReasoningEffort != "" {
		args = append(args, "--effort", request.ReasoningEffort)
	}
	if len(request.ClaudeAllowedTools) > 0 {
		args = append(args, "--allowedTools")
		args = append(args, request.ClaudeAllowedTools...)
	}
	return args
}

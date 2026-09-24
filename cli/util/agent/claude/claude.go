package claude

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/armon/circbuf"
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
	stderr, err := circbuf.NewBuffer(agentutil.StderrTailBytes)
	if err != nil {
		return fmt.Errorf("create stderr buffer: %w", err)
	}
	cmd.Stderr = io.MultiWriter(request.ProgressWriter(), stderr)
	if err := cmd.Run(); err != nil {
		return agentutil.FormatCommandError("claude", err, stderr)
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

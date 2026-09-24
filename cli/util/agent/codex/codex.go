package codex

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/armon/circbuf"
	"github.com/buildbuddy-io/buildbuddy/cli/util/agent/agentutil"
)

// Run executes a Codex agent command.
func Run(ctx context.Context, request *agentutil.RunRequest) error {
	if _, err := exec.LookPath("codex"); err != nil {
		return fmt.Errorf("codex is not installed or not in PATH")
	}

	args := commandArgs(request)
	cmd := exec.CommandContext(ctx, "codex", args...)
	cmd.Stdin = strings.NewReader(request.Prompt)
	cmd.Stdout = request.OutputWriter()
	stderr, err := circbuf.NewBuffer(agentutil.StderrTailBytes)
	if err != nil {
		return fmt.Errorf("create stderr buffer: %w", err)
	}
	cmd.Stderr = io.MultiWriter(request.ProgressWriter(), stderr)
	if err := cmd.Run(); err != nil {
		return agentutil.FormatCommandError("codex", err, stderr)
	}
	return nil
}

func commandArgs(request *agentutil.RunRequest) []string {
	// Run without approvals, read-only unless the caller asked for more.
	sandbox := request.CodexSandbox
	if sandbox == "" {
		sandbox = agentutil.SandboxReadOnly
	}
	args := []string{
		"exec",
		"--sandbox", sandbox,
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
	if len(request.CodexArgs) > 0 {
		args = append(args, request.CodexArgs...)
	}
	return args
}

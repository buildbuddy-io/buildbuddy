package git

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
)

// Run executes git in dir and writes stdout to stdout. An empty dir uses the
// current working directory.
func Run(ctx context.Context, dir string, stdout io.Writer, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Stdout = stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		secretValues := redact.CollectSensitiveEnvValues(os.Environ())
		command := redact.RedactTextWithValues(strings.Join(args, " "), secretValues)
		message := redact.RedactTextWithValues(strings.TrimSpace(stderr.String()), secretValues)
		return fmt.Errorf("git %s: %s: %w", command, message, err)
	}
	return nil
}

// Output returns trimmed stdout from a git command.
func Output(ctx context.Context, dir string, args ...string) (string, error) {
	var output bytes.Buffer
	if err := Run(ctx, dir, &output, args...); err != nil {
		return "", err
	}
	return strings.TrimSpace(output.String()), nil
}

// Root returns the absolute root of the current Git worktree.
func Root(ctx context.Context) (string, error) {
	root, err := Output(ctx, "", "rev-parse", "--show-toplevel")
	if err != nil {
		return "", fmt.Errorf("find git repository root: %w", err)
	}
	return root, nil
}

// CurrentBranch returns the checked-out branch name. It returns an error when
// HEAD is detached.
func CurrentBranch(ctx context.Context, dir string) (string, error) {
	return Output(ctx, dir, "symbolic-ref", "--short", "HEAD")
}

// HeadCommit returns the commit SHA at HEAD.
func HeadCommit(ctx context.Context, dir string) (string, error) {
	return Output(ctx, dir, "rev-parse", "HEAD")
}

// BranchRemote returns the remote configured for branch, or origin if none is
// configured.
func BranchRemote(ctx context.Context, dir, branch string) string {
	remote, err := Output(ctx, dir, "config", "--get", "branch."+branch+".remote")
	if err != nil || remote == "" {
		return "origin"
	}
	return remote
}

// IsWorktreeClean reports whether tracked and untracked files are unchanged.
func IsWorktreeClean(ctx context.Context) (bool, error) {
	root, err := Root(ctx)
	if err != nil {
		return false, err
	}
	var output bytes.Buffer
	if err := Run(ctx, root, &output, "status", "--porcelain=v1", "-z", "--untracked-files=all"); err != nil {
		return false, fmt.Errorf("inspect git worktree: %w", err)
	}
	return output.Len() == 0, nil
}

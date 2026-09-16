package fix

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	cligit "github.com/buildbuddy-io/buildbuddy/cli/util/git"
)

type pushTarget struct {
	root   string // Absolute path to the Git worktree root.
	remote string // Remote that contains the branch to update.
	branch string // Checked-out branch to push.
	base   string // HEAD before the agent runs; must match the remote branch.
}

// checkPushPreconditions returns metadata required to push to if the push is possible.
func checkPushPreconditions(ctx context.Context) (*pushTarget, error) {
	clean, err := cligit.IsWorktreeClean(ctx)
	if err != nil {
		return nil, err
	}
	if !clean {
		return nil, fmt.Errorf("--push requires a clean git worktree")
	}

	root, err := cligit.Root(ctx)
	if err != nil {
		return nil, err
	}
	branch, err := cligit.CurrentBranch(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("--push requires a checked-out branch: %w", err)
	}
	base, err := cligit.HeadCommit(ctx, root)
	if err != nil {
		return nil, err
	}
	remote := cligit.BranchRemote(ctx, root, branch)

	// Verify the remote branch's commit SHA matches the local HEAD.
	var remoteHead bytes.Buffer
	if err := cligit.Run(ctx, root, &remoteHead, "ls-remote", "--exit-code", remote, "refs/heads/"+branch); err != nil {
		return nil, fmt.Errorf("cannot read remote branch %s; check Git credentials: %w", branch, err)
	}
	fields := strings.Fields(remoteHead.String())
	if len(fields) != 2 || fields[0] != base {
		return nil, fmt.Errorf("--push requires HEAD to match remote branch %s", branch)
	}
	return &pushTarget{root: root, remote: remote, branch: branch, base: base}, nil
}

func commitAndPush(ctx context.Context, target *pushTarget, summary, invocationID string) error {
	head, err := cligit.HeadCommit(ctx, target.root)
	if err != nil {
		return err
	}
	if head != target.base {
		return fmt.Errorf("HEAD changed while the agent ran; refusing to push")
	}
	var output bytes.Buffer
	if err := cligit.Run(ctx, target.root, &output, "status", "--porcelain=v1", "-z", "--untracked-files=all"); err != nil {
		return err
	}
	if output.Len() == 0 {
		log.Printf("Agent made no git changes; nothing to push")
		return nil
	}
	log.Printf("Generating commit message...")
	message, err := generateCommitMessage(ctx, summary)
	if err != nil {
		log.Warnf("Could not generate a commit message: %s", err)
		message = fallbackCommitMessage(invocationID, summary)
	}
	if err := cligit.Run(ctx, target.root, &output, "add", "--all"); err != nil {
		return err
	}
	commitArgs := []string{"commit", "-m", message, "-m", "Original failing invocation: " + invocationID}
	if err := cligit.Run(ctx, target.root, &output, commitArgs...); err != nil {
		return err
	}
	if err := cligit.Run(ctx, target.root, io.Discard, "push", target.remote, "HEAD:refs/heads/"+target.branch); err != nil {
		return fmt.Errorf("committed the fix, but push to branch %s failed; check write access, branch protection, and whether the branch changed: %w", target.branch, err)
	}
	log.Printf("Committed and pushed fix to branch %s", target.branch)
	return nil
}

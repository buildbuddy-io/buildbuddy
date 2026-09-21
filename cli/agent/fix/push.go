package fix

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"

	cligit "github.com/buildbuddy-io/buildbuddy/cli/util/git"
)

type pushTarget struct {
	root         string // Absolute path to the Git worktree root.
	remote       string // Remote that contains the branch to update.
	branch       string // Branch checked out before the agent runs, or empty if HEAD is detached.
	base         string // HEAD before the agent runs; must match the remote branch.
	createBranch bool   // Whether to create and push to a new branch, rather than pushing to the current branch.
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
	base, err := cligit.HeadCommit(ctx, root)
	if err != nil {
		return nil, err
	}
	branch, err := cligit.CurrentBranch(ctx, root)
	if err != nil {
		// HEAD is detached, so push the fix to a new branch.
		remote := detachedPushRemote(ctx, root)
		// Fail fast if the remote is unreachable or we lack read access.
		if err := cligit.Run(ctx, root, io.Discard, "ls-remote", "--exit-code", remote, "HEAD"); err != nil {
			return nil, fmt.Errorf("cannot read remote %s; check Git credentials: %w", remote, err)
		}
		return &pushTarget{root: root, remote: remote, base: base, createBranch: true}, nil
	}
	remote := pushRemote(ctx, root, branch)

	// Fetch the branch tip and the remote's default branch.
	var remoteRefs bytes.Buffer
	if err := cligit.Run(ctx, root, &remoteRefs, "ls-remote", "--symref", "--exit-code", remote, "HEAD", "refs/heads/"+branch); err != nil {
		return nil, fmt.Errorf("cannot read remote branch %s; check Git credentials: %w", branch, err)
	}
	remoteHead, defaultBranch := parseRemoteRefs(remoteRefs.String(), branch)
	if remoteHead != base {
		return nil, fmt.Errorf("--push requires HEAD to match remote branch %s", branch)
	}
	return &pushTarget{
		root: root, remote: remote, branch: branch, base: base,
		createBranch: branch == defaultBranch || branch == "main" || branch == "master",
	}, nil
}

func pushRemote(ctx context.Context, root, branch string) string {
	// On a remote runner, push to fork if present, or origin otherwise.
	if os.Getenv("BUILDBUDDY_CI_RUNNER_ROOT_DIR") != "" {
		if _, err := cligit.Output(ctx, root, "config", "--get", "remote.fork.url"); err == nil {
			return "fork"
		}
		return "origin"
	}
	// Locally, use the configured Git push remote.
	return cligit.PushRemote(ctx, root, branch)
}

// detachedPushRemote returns the remote to push to when HEAD is detached and
// there is no branch-specific config. If the default remote doesn't exist,
// falls back to the repo's only remote.
func detachedPushRemote(ctx context.Context, root string) string {
	remote := pushRemote(ctx, root, "")
	if _, err := cligit.Output(ctx, root, "config", "--get", "remote."+remote+".url"); err == nil {
		return remote
	}
	remotes, err := cligit.Output(ctx, root, "remote")
	if err == nil && remotes != "" && !strings.Contains(remotes, "\n") {
		return remotes
	}
	return remote
}

func parseRemoteRefs(output, branch string) (branchHead, defaultBranch string) {
	for line := range strings.SplitSeq(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if len(fields) == 3 && fields[0] == "ref:" && fields[2] == "HEAD" {
			defaultBranch = strings.TrimPrefix(fields[1], "refs/heads/")
		} else if fields[1] == "refs/heads/"+branch {
			branchHead = fields[0]
		}
	}
	return branchHead, defaultBranch
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
	pushBranch := target.branch
	if target.createBranch {
		shortID := invocationID
		if len(shortID) > 8 {
			shortID = shortID[:8]
		}
		pushBranch = "bb-agent-fix/" + shortID + "-" + uuid.New()[:8]
		if err := cligit.Run(ctx, target.root, io.Discard, "checkout", "-b", pushBranch); err != nil {
			return fmt.Errorf("create fix branch %s: %w", pushBranch, err)
		}
		log.Printf("Created fix branch %s", pushBranch)
	}
	commitArgs := []string{"commit", "-m", message, "-m", "Original failing invocation: " + invocationID}
	if err := cligit.Run(ctx, target.root, &output, commitArgs...); err != nil {
		return err
	}
	if err := cligit.Run(ctx, target.root, io.Discard, "push", target.remote, "HEAD:refs/heads/"+pushBranch); err != nil {
		return fmt.Errorf("committed the fix, but push to branch %s failed; check write access, branch protection, and whether the branch changed: %w", pushBranch, err)
	}
	log.Printf("Committed and pushed fix to branch %s", pushBranch)
	return nil
}

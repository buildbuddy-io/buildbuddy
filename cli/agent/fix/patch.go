package fix

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
)

const remoteRunnerArtifactsDirectoryEnvVar = "BUILDBUDDY_ARTIFACTS_DIRECTORY"

func isGitWorktreeClean(ctx context.Context) (bool, error) {
	root, err := gitRoot(ctx)
	if err != nil {
		return false, err
	}
	var output bytes.Buffer
	if err := runGit(ctx, root, &output, "status", "--porcelain=v1", "-z", "--untracked-files=all"); err != nil {
		return false, fmt.Errorf("inspect git worktree: %w", err)
	}
	return output.Len() == 0, nil
}

// writeGitPatch writes all tracked and untracked worktree changes to the
// remote runner artifact directory. It returns an empty path if there are no
// changes.
func writeGitPatch(ctx context.Context, invocationID string) (string, error) {
	artifactsDir := os.Getenv(remoteRunnerArtifactsDirectoryEnvVar)
	if artifactsDir == "" {
		return "", nil
	}
	root, err := gitRoot(ctx)
	if err != nil {
		return "", err
	}

	var untracked bytes.Buffer
	if err := runGit(ctx, root, &untracked, "ls-files", "--others", "--exclude-standard", "-z"); err != nil {
		return "", fmt.Errorf("list untracked files: %w", err)
	}
	patchPath := filepath.Join(artifactsDir, fmt.Sprintf("bb-agent-fix-%s.patch", invocationID))
	patchFile, err := os.OpenFile(patchPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return "", fmt.Errorf("create patch artifact: %w", err)
	}
	success := false
	defer func() {
		patchFile.Close()
		if !success {
			os.Remove(patchPath)
		}
	}()

	if err := runGit(ctx, root, patchFile, "diff", "--binary", "HEAD", "--"); err != nil {
		return "", fmt.Errorf("create patch for tracked files: %w", err)
	}
	for pathBytes := range bytes.SplitSeq(untracked.Bytes(), []byte{0}) {
		if len(pathBytes) == 0 {
			continue
		}
		path := string(pathBytes)
		info, err := os.Lstat(filepath.Join(root, path))
		if err != nil {
			return "", fmt.Errorf("inspect untracked file %q: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			log.Warnf("Skipping untracked symlink %q in patch artifact", path)
			continue
		}
		if err := runGit(ctx, root, patchFile, "diff", "--no-index", "--binary", "--", "/dev/null", path); err != nil {
			return "", fmt.Errorf("create patch for untracked file %q: %w", path, err)
		}
	}
	if err := patchFile.Close(); err != nil {
		return "", fmt.Errorf("close patch artifact: %w", err)
	}
	info, err := os.Stat(patchPath)
	if err != nil {
		return "", fmt.Errorf("inspect patch artifact: %w", err)
	}
	if info.Size() == 0 {
		return "", nil
	}
	success = true
	return patchPath, nil
}

func gitRoot(ctx context.Context) (string, error) {
	var output bytes.Buffer
	if err := runGit(ctx, "", &output, "rev-parse", "--show-toplevel"); err != nil {
		return "", fmt.Errorf("find git repository root: %w", err)
	}
	return strings.TrimSpace(output.String()), nil
}

func runGit(ctx context.Context, dir string, stdout io.Writer, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Stdout = stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 && len(args) > 1 && args[0] == "diff" && args[1] == "--no-index" {
		// git diff --no-index returns 1 when the files differ.
		return nil
	}
	if err != nil {
		return fmt.Errorf("git %v: %s: %w", args, bytes.TrimSpace(stderr.Bytes()), err)
	}
	return nil
}

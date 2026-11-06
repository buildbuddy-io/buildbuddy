package fix

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	cligit "github.com/buildbuddy-io/buildbuddy/cli/util/git"
)

const remoteRunnerArtifactsDirectoryEnvVar = "BUILDBUDDY_ARTIFACTS_DIRECTORY"

// writeGitPatch writes all tracked and untracked worktree changes to the
// remote runner artifact directory. It returns an empty path if there are no
// changes.
func writeGitPatch(ctx context.Context, invocationID string) (string, error) {
	artifactsDir := os.Getenv(remoteRunnerArtifactsDirectoryEnvVar)
	if artifactsDir == "" {
		return "", nil
	}
	root, err := cligit.Root(ctx)
	if err != nil {
		return "", err
	}

	var untracked bytes.Buffer
	if err := cligit.Run(ctx, root, &untracked, "ls-files", "--others", "--exclude-standard", "-z"); err != nil {
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

	if err := cligit.Run(ctx, root, patchFile, "diff", "--binary", "HEAD", "--"); err != nil {
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
		if err := cligit.Run(ctx, root, patchFile, "diff", "--no-index", "--binary", "--", "/dev/null", path); err != nil {
			var exitErr *exec.ExitError
			// git diff --no-index returns 1 when the files differ.
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
				return "", fmt.Errorf("create patch for untracked file %q: %w", path, err)
			}
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

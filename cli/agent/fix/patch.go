package fix

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

const (
	// remoteRunnerArtifactsDirectoryEnvVar is set by remote BuildBuddy runners to the directory
	// whose contents are uploaded as invocation artifacts when the command exits.
	remoteRunnerArtifactsDirectoryEnvVar = "BUILDBUDDY_ARTIFACTS_DIRECTORY"
	downloadDirectoryName                = "bb-download"
)

// ensureCleanGitWorktree verifies that an exported patch will contain only
// changes made after this check.
func ensureCleanGitWorktree(ctx context.Context) error {
	output, err := runGit(ctx, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return fmt.Errorf("inspect git worktree: %w", err)
	}
	if len(output) != 0 {
		return fmt.Errorf("git worktree is not clean: %s", output)
	}
	return nil
}

// writeGitPatch writes all tracked and untracked worktree changes to the
// remote runner artifact directory. It returns an empty path if there are no
// changes.
func writeGitPatch(ctx context.Context, name string) (string, error) {
	artifactsDir := os.Getenv(remoteRunnerArtifactsDirectoryEnvVar)
	if artifactsDir == "" {
		return "", nil
	}
	patch, err := runGit(ctx, "diff", "--binary", "HEAD", "--")
	if err != nil {
		return "", fmt.Errorf("create patch for tracked files: %w", err)
	}
	untracked, err := runGit(ctx, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return "", fmt.Errorf("list untracked files: %w", err)
	}
	for pathBytes := range bytes.SplitSeq(untracked, []byte{0}) {
		if len(pathBytes) == 0 {
			continue
		}
		untrackedPatch, err := runGit(ctx, "diff", "--no-index", "--binary", "--", "/dev/null", string(pathBytes))
		if err != nil {
			return "", fmt.Errorf("create patch for untracked file %q: %w", pathBytes, err)
		}
		patch = append(patch, untrackedPatch...)
	}
	if len(patch) == 0 {
		return "", nil
	}

	downloadDir := filepath.Join(artifactsDir, downloadDirectoryName)
	if err := os.MkdirAll(downloadDir, 0755); err != nil {
		return "", fmt.Errorf("create artifact directory: %w", err)
	}
	info, err := os.Lstat(downloadDir)
	if err != nil {
		return "", fmt.Errorf("inspect artifact directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("artifact path %q is not a directory", downloadDir)
	}

	outputPath := filepath.Join(downloadDir, name)
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return "", fmt.Errorf("create patch artifact: %w", err)
	}
	success := false
	defer func() {
		file.Close()
		if !success {
			os.Remove(outputPath)
		}
	}()
	if _, err := file.Write(patch); err != nil {
		return "", fmt.Errorf("write patch artifact: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close patch artifact: %w", err)
	}
	success = true
	return outputPath, nil
}

func runGit(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 && len(args) > 1 && args[0] == "diff" && args[1] == "--no-index" {
		// git diff --no-index returns 1 when the files differ.
		return stdout.Bytes(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("git %v: %s: %w", args, bytes.TrimSpace(stderr.Bytes()), err)
	}
	return stdout.Bytes(), nil
}

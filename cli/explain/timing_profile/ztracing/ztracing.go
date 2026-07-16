package ztracing

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
)

const (
	ztracingRepoURL    = "https://github.com/coeuvre/ztracing.git"
	ztracingCommitSHA  = "2652640077baa8852d10f0e33a592a5085206ebe"
	bazelOutputDirName = "bazel-output"
)

type Installation struct {
	RepoPath   string
	BinaryPath string
	SkillDir   string
}

// Setup returns a pinned ztracing installation, cloning and building it when
// necessary. Only the checkout for the configured commit is retained.
func Setup(ctx context.Context) (*Installation, error) {
	cacheDir, err := storage.CacheDir()
	if err != nil {
		return nil, fmt.Errorf("get BuildBuddy cache directory: %w", err)
	}
	rootDir := filepath.Join(cacheDir, "tools", "ztracing")
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("create ztracing cache directory: %w", err)
	}
	if err := removeOldCheckouts(rootDir, ztracingCommitSHA); err != nil {
		return nil, err
	}

	installation := installationAt(rootDir, ztracingCommitSHA)
	if isValidInstallation(installation) {
		if err := addToPath(installation.BinaryPath); err != nil {
			return nil, err
		}
		log.Printf("Using existing ztracing installation at %s", installation.RepoPath)
		return installation, nil
	}
	// Remove an interrupted or otherwise incomplete installation before
	// attempting the clone again.
	versionDir := filepath.Dir(installation.RepoPath)
	if err := os.RemoveAll(versionDir); err != nil {
		return nil, fmt.Errorf("remove incomplete ztracing checkout: %w", err)
	}
	if err := os.MkdirAll(versionDir, 0755); err != nil {
		return nil, fmt.Errorf("create ztracing version directory: %w", err)
	}

	log.Printf("Downloading ztracing at commit %s...", ztracingCommitSHA)
	if err := run(ctx, rootDir, "git", "init", installation.RepoPath); err != nil {
		return nil, fmt.Errorf("initialize ztracing repository: %w", err)
	}
	if err := run(ctx, installation.RepoPath, "git", "remote", "add", "origin", ztracingRepoURL); err != nil {
		return nil, fmt.Errorf("configure ztracing repository: %w", err)
	}
	if err := run(ctx, installation.RepoPath, "git", "fetch", "--depth=1", "origin", ztracingCommitSHA); err != nil {
		return nil, fmt.Errorf("fetch ztracing commit %s: %w", ztracingCommitSHA, err)
	}
	if err := run(ctx, installation.RepoPath, "git", "checkout", "--detach", ztracingCommitSHA); err != nil {
		return nil, fmt.Errorf("check out ztracing commit %s: %w", ztracingCommitSHA, err)
	}

	log.Printf("Building ztracing at commit %s...", ztracingCommitSHA)
	outputUserRoot := filepath.Join(rootDir, bazelOutputDirName)
	if err := run(ctx, installation.RepoPath, "bazel", "--output_user_root="+outputUserRoot, "build", "//src:ztracing"); err != nil {
		return nil, fmt.Errorf("build ztracing: %w", err)
	}
	if !isValidInstallation(installation) {
		return nil, fmt.Errorf("ztracing setup completed but expected artifacts were not found in %q", installation.RepoPath)
	}
	if err := addToPath(installation.BinaryPath); err != nil {
		return nil, err
	}
	return installation, nil
}

func addToPath(binaryPath string) error {
	path := filepath.Dir(binaryPath)
	if existingPath := os.Getenv("PATH"); existingPath != "" {
		path += string(os.PathListSeparator) + existingPath
	}
	if err := os.Setenv("PATH", path); err != nil {
		return fmt.Errorf("add ztracing to PATH: %w", err)
	}
	return nil
}

func installationAt(rootDir, commit string) *Installation {
	repoPath := filepath.Join(rootDir, commit, "repo")
	return &Installation{
		RepoPath:   repoPath,
		BinaryPath: filepath.Join(repoPath, "bazel-bin", "src", "ztracing"),
		SkillDir:   filepath.Join(repoPath, ".agents", "skills", "trace-analyzer"),
	}
}

// removeOldCheckouts removes all ztracing checkouts except the one at the given commit.
func removeOldCheckouts(rootDir, keep string) error {
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return fmt.Errorf("read ztracing cache directory: %w", err)
	}
	for _, entry := range entries {
		if entry.Name() == keep || entry.Name() == bazelOutputDirName {
			continue
		}
		if err := os.RemoveAll(filepath.Join(rootDir, entry.Name())); err != nil {
			return fmt.Errorf("remove old ztracing checkout %q: %w", entry.Name(), err)
		}
	}
	return nil
}

func isValidInstallation(installation *Installation) bool {
	if _, err := os.Stat(filepath.Join(installation.RepoPath, ".git")); err != nil {
		return false
	}
	if !isExecutable(installation.BinaryPath) {
		return false
	}
	info, err := os.Stat(filepath.Join(installation.SkillDir, "SKILL.md"))
	return err == nil && info.Mode().IsRegular()
}

func isExecutable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0
}

func run(ctx context.Context, dir, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

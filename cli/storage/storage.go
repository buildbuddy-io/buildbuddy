package storage

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
)

const (
	// The section in .git/config where we write all repo-local configuration
	// for the CLI.
	gitConfigSection = "buildbuddy"
)

// ConfigDir returns a user-specific directory for storing BuildBuddy
// configuration files.
func ConfigDir() (string, error) {
	configDir := os.Getenv("BUILDBUDDY_CONFIG_DIR")
	if configDir == "" {
		userConfigDir, err := os.UserConfigDir()
		if err != nil {
			return "", err
		}
		configDir = filepath.Join(userConfigDir, "buildbuddy")
	}
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return "", err
	}
	return configDir, nil
}

// CacheDir returns a user-specific directory for storing results of expensive
// computations. The user may clear this dir at any time (e.g. to free up disk
// space), so longer-term data (like config files) should not be placed here.
func CacheDir() (string, error) {
	cacheDir := os.Getenv("BUILDBUDDY_CACHE_DIR")
	if cacheDir == "" {
		userCacheDir, err := os.UserCacheDir()
		if err != nil {
			return "", err
		}
		cacheDir = filepath.Join(userCacheDir, "buildbuddy")
	}
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return "", err
	}
	return cacheDir, nil
}

func repoRootPath() (string, error) {
	ws, err := workspace.Path()
	if err != nil {
		return "", err
	}
	dotgit, err := os.Stat(filepath.Join(ws, ".git"))
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("not inside a git repository")
		}
		return "", fmt.Errorf("failed to determine git repo root: %s", err)
	}
	if dotgit.Mode().IsDir() {
		return ws, nil
	}
	return "", fmt.Errorf("failed to determine git repo root: %q is not a directory", filepath.Join(ws, ".git"))
}

// ReadRepoConfig reads a repository-local configuration setting.
func ReadRepoConfig(key string) (string, error) {
	dir, err := repoRootPath()
	if err != nil {
		return "", err
	}
	fullKey := gitConfigSection + "." + key
	cmd := exec.Command("git", "config", "--local", "--get", fullKey)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := stderr.String()
		if msg == "" {
			return "", nil
		}
		return "", fmt.Errorf("failed to read %q from .git/config: %s", fullKey, msg)
	}

	out := strings.TrimSpace(stdout.String())
	if out == "" {
		return out, fmt.Errorf("empty value for %s", key)
	}

	return out, nil
}

// WriteRepoConfig writes a repository-local configuration setting.
func WriteRepoConfig(key, value string) error {
	dir, err := repoRootPath()
	if err != nil {
		return err
	}
	fullKey := gitConfigSection + "." + key
	cmd := exec.Command("git", "config", "--local", "--replace-all", fullKey, value)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"failed to update %q in .git/config (%s): %s",
			fullKey, err, stderr.String())
	}
	return nil
}

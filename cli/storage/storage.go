package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

const (
	// The section in .git/config where we write all repo-local configuration
	// for the CLI.
	gitConfigSection = "buildbuddy"

	// Exit code used by `git config --unset-all` to report that the setting
	// does not exist. See the EXIT STATUS section of git-config(1).
	gitConfigExitCodeKeyNotFound = 5
)

// ErrNotInRepo reports that the working directory is not inside a git
// repository. Callers distinguish it from every other reason the repo root might
// be unavailable, because having no repo-local config is normal while being
// unable to read it is not.
var ErrNotInRepo = errors.New("not in a git repository")

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

// RepoRootPath returns the root of the git repository containing the working
// directory. It returns ErrNotInRepo if there is no such repository.
var RepoRootPath = sync.OnceValues(func() (string, error) {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").CombinedOutput()
	return repoRootFromGitOutput(out, err)
})

// repoRootFromGitOutput interprets the result of `git rev-parse
// --show-toplevel`. git exits 128 for every failure, so only the message
// distinguishes "not in a repo" from an unreadable or rejected repository.
func repoRootFromGitOutput(out []byte, err error) (string, error) {
	msg := strings.TrimSpace(string(out))
	if err != nil {
		if strings.Contains(msg, "not a git repository") {
			return "", ErrNotInRepo
		}
		if msg == "" {
			return "", fmt.Errorf("git rev-parse --show-toplevel: %w", err)
		}
		return "", fmt.Errorf("git rev-parse --show-toplevel: %s: %w", msg, err)
	}
	return msg, nil
}

// ReadRepoConfig reads a repository-local configuration setting.
// It returns an empty string if the configuration value is not set.
func ReadRepoConfig(key string) (string, error) {
	dir, err := RepoRootPath()
	if err != nil {
		return "", err
	}
	fullKey := gitConfigSection + "." + key
	cmd := exec.Command("git", "config", "--local", "--get", "--default=", fullKey)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := stderr.String()
		if msg == "" {
			return "", fmt.Errorf("failed to read %q from .git/config: 'git config' command failed: %w", fullKey, err)
		}
		return "", fmt.Errorf("failed to read %q from .git/config: %s", fullKey, msg)
	}

	return strings.TrimSpace(stdout.String()), nil
}

// WriteRepoConfig writes a repository-local configuration setting.
func WriteRepoConfig(key, value string) error {
	dir, err := RepoRootPath()
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

// UnsetRepoConfig removes a repository-local configuration setting, leaving no
// entry behind. Writing an empty value with WriteRepoConfig is not equivalent:
// that leaves the key present with an empty value.
//
// It is not an error for the setting to be absent already.
func UnsetRepoConfig(key string) error {
	dir, err := RepoRootPath()
	if err != nil {
		return err
	}
	fullKey := gitConfigSection + "." + key
	cmd := exec.Command("git", "config", "--local", "--unset-all", fullKey)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == gitConfigExitCodeKeyNotFound {
			return nil
		}
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			return fmt.Errorf("failed to unset %q in .git/config: 'git config' command failed: %w", fullKey, err)
		}
		return fmt.Errorf("failed to unset %q in .git/config: %s: %w", fullKey, msg, err)
	}
	return nil
}

package storage

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
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

var RepoRootPath = sync.OnceValues(func() (string, error) {
	dir, err := exec.Command("git", "rev-parse", "--show-toplevel").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git rev-parse --show-toplevel: %w", err)
	}
	return strings.TrimSpace(string(dir)), nil
})

// ReadRepoConfig reads a repository-local configuration setting.
// It returns an empty string if the configuration value is not set.
func ReadRepoConfig(key string) (string, error) {
	dir, err := RepoRootPath()
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

const (
	InvocationIDFlagName  = "invocation_id"
	BesResultsUrlFlagName = "bes_results_url"

	// Use GetLastBackend instead of directly reading this flag.
	besBackendFlagName = "bes_backend"
)

func SaveFlags(args []string) []string {
	command := arg.GetCommand(args)
	if command == "build" || command == "test" || command == "run" || command == "query" || command == "cquery" {
		saveFlag(args, besBackendFlagName, "", 1)
		saveFlag(args, BesResultsUrlFlagName, "", 1)
		args = saveFlag(args, InvocationIDFlagName, uuid.New(), 2)
	}
	return args
}

// GetPreviousFlag returns the previous value of a flag, or an empty string if
// the flag has not been set before.
func GetPreviousFlag(flag string) (string, error) {
	return GetNthPreviousFlag(flag, 1)
}

// GetNthPreviousFlag returns the nth previous value of a flag, or an empty
// string if the flag has not been set n times (n >= 1).
func GetNthPreviousFlag(flag string, n int) (string, error) {
	lastValue, err := os.ReadFile(getPreviousFlagPath(flag))
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	values := strings.Split(string(lastValue), "\n")
	if len(values) < n {
		return "", nil
	}
	return values[n-1], nil
}

// GetLastBackend returns the last BES backend used by the CLI.
func GetLastBackend() (string, error) {
	lastBackend, err := GetPreviousFlag(besBackendFlagName)
	if lastBackend == "" || err != nil {
		log.Printf("The previous invocation didn't have the --bes_backend= set.")
		return "", err
	}

	if !strings.HasPrefix(lastBackend, "grpc://") && !strings.HasPrefix(lastBackend, "grpcs://") && !strings.HasPrefix(lastBackend, "unix://") {
		lastBackend = "grpcs://" + lastBackend
	}
	return lastBackend, nil
}

func saveFlag(args []string, flag, backup string, maxValues int) []string {
	value := arg.Get(args, flag)
	if value == "" {
		value = backup
	}
	args = append(args, "--"+flag+"="+value)
	path := getPreviousFlagPath(flag)
	if path == "" {
		log.Debugf("Failed to get path for flag %q", flag)
		return args
	}
	var newContent string
	oldContent, err := os.ReadFile(path)
	if err != nil {
		newContent = value
	} else {
		oldEntries := strings.Split(string(oldContent), "\n")
		if len(oldEntries) >= maxValues {
			newContent = strings.Join(append([]string{value}, oldEntries[:maxValues-1]...), "\n")
		} else {
			newContent = strings.Join(append([]string{value}, oldEntries...), "\n")
		}
	}
	os.WriteFile(path, []byte(newContent), 0777)
	return args
}

func getPreviousFlagPath(flagName string) string {
	workspaceDir, err := workspace.Path()
	if err != nil {
		return ""
	}
	cacheDir, err := CacheDir()
	if err != nil {
		return ""
	}
	flagsDir := filepath.Join(cacheDir, "last_flag_values", hash.String(workspaceDir))
	if err := os.MkdirAll(flagsDir, 0755); err != nil {
		return ""
	}
	return filepath.Join(flagsDir, flagName+".txt")
}

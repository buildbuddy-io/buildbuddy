package flaghistory

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
)

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
	cacheDir, err := storage.CacheDir()
	if err != nil {
		return ""
	}
	flagsDir := filepath.Join(cacheDir, "last_flag_values", hash.String(workspaceDir))
	if err := os.MkdirAll(flagsDir, 0755); err != nil {
		return ""
	}
	return filepath.Join(flagsDir, flagName+".txt")
}

package login

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
)

const (
	apiKeyRepoSetting = "api-key"
	apiKeyHeader      = "remote_header=x-buildbuddy-api-key"
	loginURL          = "https://app.buildbuddy.io/settings/cli-login"
)

func HandleLogin(args []string) (exitCode int, err error) {
	if arg.GetCommand(args) != "login" {
		return -1, nil
	}

	if err := openInBrowser(loginURL); err != nil {
		log.Printf("Failed to open browser: %s", err)
		log.Printf("Copy and paste the URL below into a browser window:")
		log.Printf("    %s", loginURL)
	}

	io.WriteString(os.Stderr, "Enter your API key from "+loginURL+": ")

	var apiKey string
	if _, err := fmt.Scanln(&apiKey); err != nil {
		return -1, fmt.Errorf("failed to read input: %s", err)
	}
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return -1, fmt.Errorf("invalid input: API key is empty")
	}

	if err := storage.WriteRepoConfig(apiKeyRepoSetting, apiKey); err != nil {
		return -1, fmt.Errorf("failed to write API key to local .git/config: %s", err)
	}

	log.Printf("Wrote API key to .git/config")

	log.Printf("You are now logged in!")

	return 0, nil
}

func openInBrowser(url string) error {
	cmd := "open"
	if runtime.GOOS == "linux" {
		cmd = "xdg-open"
	}
	return exec.Command(cmd, url).Run()
}

func ConfigureAPIKey(args []string) ([]string, error) {
	if cmd, _ := parser.GetBazelCommandAndIndex(args); !isSupportedCommand(cmd) {
		return args, nil
	}

	// TODO(siggisim): find a more graceful way of finding headers if we change the way we parse flags.
	if arg.Has(args, apiKeyHeader) {
		return args, nil
	}

	apiKey, err := storage.ReadRepoConfig(apiKeyRepoSetting)
	if err != nil {
		// If we're not in a git repo, we'll fail to read the repo-specific
		// config.
		// Making this fatal would be inconvenient for new workspaces,
		// so just log a debug message and move on.
		log.Debugf("failed to configure API key from .git/config: %s", err)
		return nil, nil
	}

	return append(args, "--"+apiKeyHeader+"="+strings.TrimSpace(apiKey)), nil
}

// Commands that support the `--remote_header` bazel flag
func isSupportedCommand(command string) bool {
	switch command {
	case "aquery":
		fallthrough
	case "build":
		fallthrough
	case "clean":
		fallthrough
	case "config":
		fallthrough
	case "coverage":
		fallthrough
	case "cquery":
		fallthrough
	case "canonicalize-flags":
		fallthrough
	case "fetch":
		fallthrough
	case "info":
		fallthrough
	case "mobile-install":
		fallthrough
	case "print_action":
		fallthrough
	case "query":
		fallthrough
	case "remote":
		fallthrough
	case "run":
		fallthrough
	case "sync":
		fallthrough
	case "test":
		return true
	}
	return false
}

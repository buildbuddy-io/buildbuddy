package login

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
)

const (
	apiKeyFileName = "apikey"
	apiKeyHeader   = "remote_header=x-buildbuddy-api-key"
)

func HandleLogin(args []string) []string {
	if arg.GetCommand(args) != "login" {
		return args
	}
	log.Printf("Enter your api key from https://app.buildbuddy.io/settings/org/api-keys")

	var apiKey string
	fmt.Scanln(&apiKey)

	log.Printf("You are now logged in!")

	dir, err := storage.ConfigDir()
	if err != nil {
		log.Fatalf("error getting config directory: %s", err)
	}
	apiKeyFile := filepath.Join(dir, apiKeyFileName)
	err = os.WriteFile(apiKeyFile, []byte(apiKey), 0644)
	if err != nil {
		log.Fatalf("error saving api key: %s", err)
	}
	os.Exit(0)
	return args
}

func ConfigureAPIKey(args []string) []string {
	if !isSupportedCommand(arg.GetCommand(args)) {
		return args
	}

	dir, err := storage.ConfigDir()
	if err != nil {
		log.Fatalf("error getting config directory: %s", err)
	}
	apiKeyFile := filepath.Join(dir, apiKeyFileName)
	apiKeyBytes, err := os.ReadFile(apiKeyFile)
	if err != nil {
		log.Debug(err)
		return args
	}

	// TODO(siggisim): find a more graceful way of finding headers if we change the way we parse flags.
	if !arg.Has(args, apiKeyHeader) {
		return append(args, "--"+apiKeyHeader+"="+string(apiKeyBytes))
	}
	return args
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

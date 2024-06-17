package login

import (
	"context"
	"flag"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
)

const (
	apiKeyRepoSetting = "api-key"
	apiKeyHeader      = "remote_header=x-buildbuddy-api-key"
)

var (
	flags = flag.NewFlagSet("login", flag.ContinueOnError)

	check         = flags.Bool("check", false, "Just check whether logged in. Exits with code 0 if logged in, code 1 if not logged in, or 2 if there is an error.")
	allowExisting = flags.Bool("allow_existing", false, "Don't force re-login if the current credentials are valid.")

	loginURL  = flags.String("url", "https://app.buildbuddy.io/settings/cli-login", "Web URL for user to login")
	apiTarget = flags.String("target", "grpcs://remote.buildbuddy.io", "BuildBuddy gRPC target")

	usage = `
bb ` + flags.Name() + ` [--allow_existing] [--check]

Logs into BuildBuddy, saving your personal API key to .git/config.

By default, this command will always prompt for login. To skip prompting if
the current credentials are valid, use the --allow_existing flag.

The --check option checks whether you are logged in.
The exit code indicates the result of the check:
	0: credentials are valid
	1: credentials are invalid
	2: error validating credentials
`
)

func authenticate(apiKey string) error {
	conn, err := grpc_client.DialSimple(*apiTarget)
	if err != nil {
		return fmt.Errorf("dial %s: %w", *apiTarget, err)
	}
	defer conn.Close()
	client := bbspb.NewBuildBuddyServiceClient(conn)

	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)

	_, err = client.GetUser(ctx, &uspb.GetUserRequest{})
	if err != nil {
		if status.IsNotFoundError(err) && strings.Contains(err.Error(), "user not found") {
			// Org-level key is used.
			return nil
		}
		return err
	}
	return nil
}

func HandleLogin(args []string) (exitCode int, err error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}

	if *check || *allowExisting {
		apiKey, err := storage.ReadRepoConfig(apiKeyRepoSetting)
		if err != nil {
			return -1, fmt.Errorf("read .git/config: %w", err)
		}
		code := 0
		if err := authenticate(apiKey); err != nil {
			if status.IsUnauthenticatedError(err) {
				code = 1
			} else {
				log.Printf("Failed to authenticate API key: %s", err)
				code = 2
			}
		}
		if *check {
			// In check mode, always exit.
			return code, nil
		}
		if *allowExisting {
			if code == 0 {
				// Success, skip login.
				return 0, nil
			}
			if code != 1 {
				// Error, exit immediately without proceeding to login.
				return code, nil
			}
			// Unauthenticated - proceed to login.
		}
	}

	log.Printf("Press Enter to open %s in your browser...", *loginURL)
	if _, err := fmt.Scanln(); err != nil {
		return -1, fmt.Errorf("failed to read input: %s", err)
	}
	if err := openInBrowser(*loginURL); err != nil {
		log.Printf("Failed to open browser: %s", err)
		log.Printf("Copy and paste the URL below into a browser window:")
		log.Printf("    %s", *loginURL)
	}

	io.WriteString(os.Stderr, "Enter your API key from "+*loginURL+": ")

	var apiKey string
	if _, err := fmt.Scanln(&apiKey); err != nil {
		return -1, fmt.Errorf("failed to read input: %s", err)
	}
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return -1, fmt.Errorf("invalid input: API key is empty")
	}

	if err := authenticate(apiKey); err != nil {
		return -1, fmt.Errorf("authenticate API key: %w", err)
	}

	if err := storage.WriteRepoConfig(apiKeyRepoSetting, apiKey); err != nil {
		return -1, fmt.Errorf("failed to write API key to local .git/config: %s", err)
	}

	log.Printf("Wrote API key to .git/config")
	log.Printf("You are now logged in!")

	return 0, nil
}

func HandleLogout(args []string) (exitCode int, err error) {
	if err := storage.WriteRepoConfig(apiKeyRepoSetting, ""); err != nil {
		return -1, fmt.Errorf("failed to clear api key from local .git/config: %s", err)
	}

	log.Printf("You are now logged out!")

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
		log.Debugf("Failed to read API key from .git/config: %s", err)
		return args, nil
	}
	if apiKey == "" {
		return args, nil
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

package login

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"

	_ "embed"

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
	DefaultApiTarget  = "grpcs://remote.buildbuddy.io"
)

var (
	flags = flag.NewFlagSet("login", flag.ContinueOnError)

	group            = flags.String("org", "", "If set, log in with this org identifier (slug), like 'my-org'")
	check            = flags.Bool("check", false, "Just check whether logged in. Exits with code 0 if logged in, code 1 if not logged in, or 2 if there is an error.")
	allowExisting    = flags.Bool("allow_existing", false, "Don't force re-login if the current credentials are valid.")
	noLaunchBrowser  = flags.Bool("no_launch_browser", false, "Never launch a browser window from this script.")
	promptForBrowser = flags.Bool("prompt_for_browser", false, "Prompt before opening the browser. Has no effect if -no_launch_browser is set.")

	loginURL  = flags.String("url", "https://app.buildbuddy.io", "Web URL for user to login")
	apiTarget = flags.String("target", DefaultApiTarget, "BuildBuddy gRPC target")

	usage = `
bb ` + flags.Name() + ` [--allow_existing | --check] [--org=my-org]

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
	buildbuddyURL, err := url.Parse(*loginURL)
	if err != nil {
		return -1, fmt.Errorf("invalid -url: %w", err)
	}
	buildbuddyURL.Path = ""

	repoRoot, err := storage.RepoRootPath()
	if err != nil {
		return -1, fmt.Errorf("locate .git repo root path: %w", err)
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

	userInputCh := make(chan Result[string])
	go func() {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			userInputCh <- Result[string]{Val: s.Text()}
		}
		if s.Err() != nil {
			userInputCh <- Result[string]{Err: s.Err()}
		} else {
			userInputCh <- Result[string]{Err: io.EOF}
		}
	}()

	loginServer, err := startServer(buildbuddyURL.String(), repoRoot)
	if err != nil {
		return -1, fmt.Errorf("failed to start login server: %w", err)
	}
	defer loginServer.Close()

	log.Printf("Running BuildBuddy login server at %s", loginServer.LocalAuthURL())
	log.Printf("BuildBuddy login URL: %s", loginServer.BuildBuddyAuthURL())

	// TODO: don't show browser prompt or auto-open browser if there is no
	// display (e.g. ssh)
	if *promptForBrowser && !*noLaunchBrowser {
		log.Printf("Press Enter to open this URL in the browser...")
		input := <-userInputCh
		if input.Err != nil {
			return -1, fmt.Errorf("failed to read input: %s", err)
		}
	}
	launchedBrowser := false
	if !*noLaunchBrowser {
		if err := openInBrowser(loginServer.BuildBuddyAuthURL()); err != nil {
			log.Printf("Failed to open browser: %s", err)
		} else {
			launchedBrowser = true
		}
	}
	if !launchedBrowser {
		log.Printf("Open the URL below in your browser to continue:")
		log.Printf("    %s", loginServer.BuildBuddyAuthURL())
	}
	io.WriteString(os.Stderr, "Follow the login instructions, or visit "+buildbuddyURL.String()+"/settings/cli-login and enter your API key: ")

	var apiKey string
	for apiKey == "" {
		select {
		case input := <-userInputCh:
			if input.Err == io.EOF {
				// Stdin is not available.
				continue
			}
			if input.Err != nil {
				return -1, fmt.Errorf("failed to read stdin: %w", err)
			}
			apiKey = input.Val
		case res := <-loginServer.ResultChan():
			if res.Err != nil {
				return -1, fmt.Errorf("login failed: %w", err)
			}
			apiKey = res.Val
			// Before we return, reply back to the login server so that we can
			// display the success/failure status in the UI. e.g. if we failed
			// to write to .git/config for some reason, then we can show "login
			// failed" in the UI.
			defer func() { loginServer.SetErr(err) }()
		}
	}

	// Terminate API key prompt.
	log.Printf("")

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
	log.Printf("You are now building with BuildBuddy!")

	return 0, nil
}

func HandleLogout(args []string) (exitCode int, err error) {
	if err := storage.WriteRepoConfig(apiKeyRepoSetting, ""); err != nil {
		return -1, fmt.Errorf("failed to clear api key from local .git/config: %s", err)
	}

	log.Printf("You are now logged out!")

	return 0, nil
}

type Result[T any] struct {
	Val T
	Err error
}

// Login server which redirects to the BB UI and consumes the token when we
// are redirected back from BuildBuddy.
type server struct {
	wg       sync.WaitGroup
	lis      net.Listener
	repoRoot string
	loginURL string
	resultCh chan Result[string]
	errCh    chan error
}

var _ http.Handler = (*server)(nil)

func startServer(loginURL, repoRoot string) (*server, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	s := &server{
		lis:      lis,
		loginURL: loginURL,
		repoRoot: repoRoot,
		resultCh: make(chan Result[string], 1),
		errCh:    make(chan error, 1),
	}
	go http.Serve(lis, s)
	return s, nil
}

func (s *server) LocalAuthURL() string {
	return fmt.Sprintf("http://localhost:%d", s.lis.Addr().(*net.TCPAddr).Port)
}

func (s *server) BuildBuddyAuthURL() string {
	return fmt.Sprintf("%s/cli-login?cli_url=%s&org=%s&workspace=%s", s.loginURL, url.QueryEscape(s.LocalAuthURL()), url.QueryEscape(*group), url.QueryEscape(s.repoRoot))
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.wg.Add(1)
	defer s.wg.Done()

	if token := r.URL.Query().Get("token"); token != "" {
		s.resultCh <- Result[string]{Val: token, Err: nil}
		// Wait for SetErr() to be called, which indicates whether we handled
		// the token successfully. Then redirect back to the UI with a success
		// or error page accordingly.
		var errParam string
		if err := <-s.errCh; err != nil {
			errParam = "&cliLoginError=1"
		}
		http.Redirect(w, r, s.BuildBuddyAuthURL()+"&complete=1"+errParam, http.StatusTemporaryRedirect)
	} else {
		log.Debugf("Redirecting to %s", s.BuildBuddyAuthURL())
		http.Redirect(w, r, s.BuildBuddyAuthURL(), http.StatusTemporaryRedirect)
	}
}

func (s *server) ResultChan() chan Result[string] {
	return s.resultCh
}

// SetErr sets the result of processing the API key.
func (s *server) SetErr(err error) {
	s.errCh <- err
}

func (s *server) Close() error {
	s.wg.Wait()
	return s.lis.Close()
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

// GetAPIKeyInteractively attempts to read an API key from the
// BUILDBUDDY_API_KEY environment variable and, if not set, from the buildbuddy
// config set at the key `buildbuddy.api-key` in .git/config. If neither is set,
// will prompt the user to set it.
func GetAPIKeyInteractively() (string, error) {
	var err error
	apiKey := os.Getenv("BUILDBUDDY_API_KEY")
	if apiKey != "" {
		return apiKey, nil
	}
	apiKey, err = storage.ReadRepoConfig("api-key")
	if err != nil {
		log.Debugf("Could not read api key from bb config: %s", err)
	} else {
		log.Debugf("API key read from `buildbuddy.api-key` in .git/config.")
		return apiKey, nil
	}
	// If an API key is not set, prompt the user to set it in their cli config.
	if _, err = HandleLogin([]string{}); err != nil {
		return "", status.WrapError(err, "handle login")
	}
	apiKey, err = storage.ReadRepoConfig("api-key")
	if err != nil {
		return "", status.WrapError(err, "read api key from bb config")
	}
	if apiKey == "" {
		return "", status.NotFoundErrorf("API key not set after login")
	}
	return apiKey, nil
}

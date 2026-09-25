package login

import (
	"bufio"
	"context"
	"errors"
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
	"time"

	_ "embed"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
)

const (
	apiKeyRepoSetting = "api-key"
	apiKeyHeader      = "remote_header=x-buildbuddy-api-key"
	envAPIKeyVarName  = "BUILDBUDDY_API_KEY"
	DefaultApiTarget  = "grpcs://remote.buildbuddy.io"
	DefaultHTTPTarget = "https://app.buildbuddy.io"
)

// apiKeySource identifies which credential source supplied an API key, for use
// in messages to the user 
type apiKeySource int

const (
	apiKeySourceNone apiKeySource = iota
	apiKeySourceEnv
	apiKeySourceRepo
)

func (s apiKeySource) String() string {
	switch s {
	case apiKeySourceEnv:
		return "$" + envAPIKeyVarName
	case apiKeySourceRepo:
		return ".git/config " + gitConfigAPIKeyName
	default:
		return "no credential source"
	}
}

// gitConfigAPIKeyName is the fully qualified .git/config key, for use in
// messages to the user.
const gitConfigAPIKeyName = "buildbuddy." + apiKeyRepoSetting

// envAPIKey returns the API key supplied by the environment, or an empty string
// if there is none.
func envAPIKey() string {
	return strings.TrimSpace(os.Getenv(envAPIKeyVarName))
}

var (
	flags = flag.NewFlagSet("login", flag.ContinueOnError)
	Flags = flags

	group            = flags.String("org", "", "If set, log in with this org identifier (slug), like 'my-org'")
	check            = flags.Bool("check", false, "Just check whether logged in. Exits with code 0 if logged in, code 1 if not logged in, or 2 if there is an error.")
	allowExisting    = flags.Bool("allow_existing", false, "Don't force re-login if the current credentials are valid.")
	noLaunchBrowser  = flags.Bool("no_launch_browser", false, "Never launch a browser window from this script.")
	promptForBrowser = flags.Bool("prompt_for_browser", false, "Prompt before opening the browser. Has no effect if -no_launch_browser is set.")

	loginURL  = flags.String("url", DefaultHTTPTarget, "Web URL for user to login")
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

--check and --allow_existing each consider the BUILDBUDDY_API_KEY environment
variable as well as the key saved in .git/config. BUILDBUDDY_API_KEY takes
precedence: if it is set, that is the key that gets checked, and a key written
to .git/config by a later login will not be used until the variable is unset.
`
)

var (
	logoutFlags = flag.NewFlagSet("logout", flag.ContinueOnError)
	LogoutFlags = logoutFlags

	logoutUsage = `
bb ` + logoutFlags.Name() + `

Removes the personal API key that bb saved to .git/config.

This only clears what bb itself wrote. It cannot clear a key supplied by the
BUILDBUDDY_API_KEY environment variable, or by a
--remote_header=x-buildbuddy-api-key flag in a .bazelrc; remove those yourself
if you want builds to stop authenticating.
`
)

// authenticateFn is replaced in tests to drive the credential-checking paths
// without a network round-trip. Production code always uses authenticate.
var authenticateFn = authenticate

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

	// --check reports on the current credentials and --allow_existing logs in
	// only if they are unusable. Silently letting --check win hides which
	// request was dropped.
	if *check && *allowExisting {
		log.Printf("--check and --allow_existing cannot be used together: --check only reports on the current credentials, while --allow_existing may log in.")
		return 2, nil
	}

	if *check || *allowExisting {
		// Check every credential source a build would use, not just
		// .git/config, so that --check agrees with the build.
		apiKey, source, err := resolveAPIKey()
		if err != nil {
			// Exit 2 ("error validating credentials") rather than returning the
			// error: returning it reaches log.Fatal in main and exits 1, the
			// documented code for "credentials are invalid", which is not what
			// we established.
			log.Printf("Failed to read API key: %s", err)
			return 2, nil
		}
		code := 0
		if apiKey == "" {
			code = 1
		} else if err := authenticateFn(apiKey); err != nil {
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
		if code == 0 {
			// Success, skip login. Say which credential satisfied the check:
			// otherwise this is a silent no-op, and a user who expected a
			// personal key in .git/config has no hint that one was not written.
			log.Printf("Already logged in using %s. Skipping login.", source)
			return 0, nil
		}
		if code != 1 {
			// Error, exit immediately without proceeding to login.
			return code, nil
		}
		// Unauthenticated - proceed to login.
		if source == apiKeySourceEnv {
			log.Warnf("%s is set but its key was rejected. Logging in will write a new key to .git/config, but %s takes precedence and will still be used until you unset it.", envAPIKeyVarName, envAPIKeyVarName)
		}
	}

	// Only the interactive login flow needs a repo to write the key to, so this
	// is resolved after the --check path above, which must work outside a git
	// repo.
	repoRoot, err := storage.RepoRootPath()
	if err != nil {
		return -1, fmt.Errorf("locate .git repo root path: %w", err)
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
	// The key written above is not necessarily the key builds will use: the
	// environment wins over .git/config, so an unconditional success message
	// would be wrong.
	if envAPIKey() != "" {
		log.Warnf("%s is set in the environment and takes precedence, so builds will keep using that key instead of the one just saved. Unset it to use your new key.", envAPIKeyVarName)
	} else {
		log.Printf("You are now building with BuildBuddy!")
	}

	return 0, nil
}

func HandleLogout(args []string) (exitCode int, err error) {
	if err := arg.ParseFlagSet(logoutFlags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(logoutUsage)
			return 1, nil
		}
		return -1, err
	}

	if err := storage.UnsetRepoConfig(apiKeyRepoSetting); err != nil {
		// Being outside a git repo is not a failure to log out: there is no
		// repo-local key to clear, and the environment check below is still
		// worth running.
		if !errors.Is(err, storage.ErrNotInRepo) {
			return -1, fmt.Errorf("failed to clear api key from local .git/config: %w", err)
		}
		log.Printf("Not in a git repo, so there is no saved API key to clear.")
	} else {
		log.Printf("Cleared the saved API key from .git/config.")
	}

	// Logout can only clear what bb itself wrote. If another source still
	// supplies a key then builds will keep using it, so don't claim to be logged
	// out.
	if envAPIKey() != "" {
		log.Warnf("%s is still set in the environment and will still be used. Unset it to finish logging out.", envAPIKeyVarName)
		return 0, nil
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
	lis      net.Listener
	srv      *http.Server
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
	s.srv = &http.Server{Handler: s}
	go s.srv.Serve(lis)
	return s, nil
}

func (s *server) LocalAuthURL() string {
	return fmt.Sprintf("http://localhost:%d", s.lis.Addr().(*net.TCPAddr).Port)
}

func (s *server) BuildBuddyAuthURL() string {
	return fmt.Sprintf("%s/cli-login?cli_url=%s&org=%s&workspace=%s", s.loginURL, url.QueryEscape(s.LocalAuthURL()), url.QueryEscape(*group), url.QueryEscape(s.repoRoot))
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	// Gracefully shut down so that any in-flight redirect response has a
	// chance to be fully written to the underlying TCP connection before
	// the process exits. Without this, the browser can miss the final
	// redirect back to the "CLI login complete" page, since the process
	// would exit immediately after the handler returns but before the
	// response bytes are flushed.
	//
	// Once the handler returns, the connection becomes idle and Shutdown
	// closes it immediately, so this should complete in milliseconds on
	// localhost. The timeout is just a safety net so that a stuck client
	// connection can't hang the CLI on exit.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

func openInBrowser(url string) error {
	cmd := "open"
	if runtime.GOOS == "linux" {
		cmd = "xdg-open"
	}
	return exec.Command(cmd, url).Run()
}

func ConfigureAPIKey(args *arg.BazelArgs) error {
	if cmd := args.GetCommand(); !isSupportedCommand(cmd) {
		return nil
	}

	// TODO(siggisim): find a more graceful way of finding headers if we change the way we parse flags.
	if args.Get(apiKeyHeader) != "" {
		return nil
	}

	// TODO: consider always starting an interactive login, or maybe only when
	// using an org-specific subdomain.
	apiKey, err := getAPIKey(false /*=interactive*/)
	if err != nil {
		// Having no API key is the normal state for a new workspace, so this
		// reports "no key available" rather than a read failure. Making it fatal
		// would break unauthenticated builds.
		log.Debugf("No BuildBuddy API key available: %s", err)
		return nil
	}
	if apiKey == "" {
		return nil
	}

	if err := args.Append("--" + apiKeyHeader + "=" + strings.TrimSpace(apiKey)); err != nil {
		return err
	}
	return nil
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

// GetAPIKey attempts to read an API key from the
// BUILDBUDDY_API_KEY environment variable and, if not set, from the buildbuddy
// config set at the key `buildbuddy.api-key` in .git/config. If neither is set,
// and we're running in a tty, this will start the login flow.
func GetAPIKey() (string, error) {
	return getAPIKey(true /*=interactive*/)
}

// resolveAPIKey returns the API key from the first credential source that has
// one set, checking BUILDBUDDY_API_KEY before repo-local .git/config, along with
// the source it came from. It never starts an interactive login, and returns an
// empty string and apiKeySourceNone if no source has a key.
//
// Not being inside a git repo is not an error here: there is no repo-local key
// to read, and BUILDBUDDY_API_KEY on its own is a valid way to be logged in.
func resolveAPIKey() (string, apiKeySource, error) {
	if apiKey := envAPIKey(); apiKey != "" {
		debugAPIKey(apiKeySourceEnv, apiKey)
		return apiKey, apiKeySourceEnv, nil
	}
	apiKey, err := storage.ReadRepoConfig(apiKeyRepoSetting)
	if err != nil {
		if errors.Is(err, storage.ErrNotInRepo) {
			log.Debugf("Not in a git repo, so there is no repo-local API key.")
			return "", apiKeySourceNone, nil
		}
		return "", apiKeySourceNone, err
	}
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		log.Debugf("API key is empty")
		return "", apiKeySourceNone, nil
	}
	debugAPIKey(apiKeySourceRepo, apiKey)
	return apiKey, apiKeySourceRepo, nil
}

func getAPIKey(interactive bool) (string, error) {
	apiKey, _, err := resolveAPIKey()
	if err != nil {
		// Reading repo-local config can fail in ways that shouldn't be fatal
		// here (see ConfigureAPIKey), so fall through to the login flow.
		log.Debugf("Could not read api key from bb config: %s", err)
	} else if apiKey != "" {
		return apiKey, nil
	}
	// If an API key is not set, and we're running in a terminal, start the
	// login flow.
	if interactive && terminal.IsTTY(os.Stdin) && terminal.IsTTY(os.Stdout) && terminal.IsTTY(os.Stderr) {
		// HandleLogin parses into the package-global flag set, which may already
		// hold values from an earlier parse in this process. Pass the flags this
		// call depends on explicitly rather than inheriting them: an interactive
		// login triggered from here must prompt.
		if _, err := HandleLogin([]string{"--check=false", "--allow_existing=false"}); err != nil {
			return "", status.WrapError(err, "handle login")
		}
		// Read .git/config directly rather than going through resolveAPIKey: the
		// login above wrote the key there, and resolveAPIKey would prefer a
		// stale BUILDBUDDY_API_KEY over the key the user just entered.
		apiKey, err := storage.ReadRepoConfig(apiKeyRepoSetting)
		if err != nil {
			return "", status.WrapError(err, "read api key from bb config")
		}
		if apiKey == "" {
			return "", status.NotFoundErrorf("API key not set after login")
		}
		debugAPIKey(apiKeySourceRepo, apiKey)
		return apiKey, nil
	}
	return "", status.NotFoundErrorf("API key not set")
}

func debugAPIKey(source apiKeySource, apiKey string) {
	log.Debugf("Using BuildBuddy API key from %s: %s", source, apiKeyDebugString(apiKey))
}

// apiKeyDebugString renders an API key for a debug log without revealing any of
// its characters or its length. Verbose logs get pasted into issues and CI
// output.
func apiKeyDebugString(apiKey string) string {
	if apiKey == "" {
		return "(empty)"
	}
	return "(redacted)"
}

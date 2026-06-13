package install_mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	mcpServerName = "buildbuddy"
	defaultMCPURL = "https://app.buildbuddy.io/mcp"

	buildBuddyAPIKeyEnvVar = "BUILDBUDDY_API_KEY"
	mcpAPIKeyEnvVar        = "BUILDBUDDY_MCP_API_KEY"
)

var (
	flags = flag.NewFlagSet("install-mcp", flag.ContinueOnError)
	Flags = flags

	lookPath = exec.LookPath
)

const usage = `
usage: bb install-mcp [--url=URL] [--clients=codex,claude] [--force]
       bb install-mcp proxy [--url=URL]

Installs the BuildBuddy MCP server for AI coding agents.

Commands:
  proxy  Run a stdio MCP proxy that authenticates to BuildBuddy. Used by AI agents.
`

const setupUsage = `
usage: bb install-mcp [--url=URL] [--clients=codex,claude] [--force]

Configures supported AI coding agents on PATH to use BuildBuddy's MCP server.
By default, installed Codex and Claude Code clients are configured.

The installed MCP server command is:
  bb install-mcp proxy --url=URL

If no BuildBuddy API key is configured, this command starts the standard
'bb login' flow. The proxy later reads authentication from BUILDBUDDY_MCP_API_KEY,
BUILDBUDDY_API_KEY, or the API key saved by 'bb login' in the current git repo.
`

const proxyUsage = `
usage: bb install-mcp proxy [--url=URL]

Runs a stdio MCP proxy that forwards JSON-RPC requests to BuildBuddy's HTTP MCP
endpoint while injecting BuildBuddy API key auth.
`

type agentClient struct {
	Name       string
	BinaryName string
	getArgs    []string
	removeArgs []string
	addArgs    func(mcpURL string) []string
}

func supportedClients() []agentClient {
	return []agentClient{
		{
			Name:       "Codex",
			BinaryName: "codex",
			getArgs:    []string{"mcp", "get", mcpServerName},
			removeArgs: []string{"mcp", "remove", mcpServerName},
			addArgs: func(mcpURL string) []string {
				return []string{"mcp", "add", mcpServerName, "--", "bb", "install-mcp", "proxy", "--url", mcpURL}
			},
		},
		{
			Name:       "Claude Code",
			BinaryName: "claude",
			getArgs:    []string{"mcp", "get", mcpServerName},
			removeArgs: []string{"mcp", "remove", mcpServerName},
			addArgs: func(mcpURL string) []string {
				return []string{"mcp", "add", "--scope", "user", mcpServerName, "--", "bb", "install-mcp", "proxy", "--url", mcpURL}
			},
		},
	}
}

func HandleInstallMCP(args []string) (exitCode int, err error) {
	if err := chdirToBazelWorkingDirectory(); err != nil {
		return 1, err
	}
	if len(args) == 0 {
		return handleSetup(nil)
	}
	switch args[0] {
	case "-h", "--help", "help":
		log.Print(usage)
		return 0, nil
	case "proxy", "mcp-proxy":
		return handleMCPProxy(args[1:])
	default:
		return handleSetup(args)
	}
}

func chdirToBazelWorkingDirectory() error {
	for _, envVar := range []string{"BUILD_WORKING_DIRECTORY", "BUILD_WORKSPACE_DIRECTORY"} {
		if dir := os.Getenv(envVar); dir != "" {
			if err := os.Chdir(dir); err != nil {
				return fmt.Errorf("chdir to %s: %w", envVar, err)
			}
			return nil
		}
	}
	return nil
}

func handleSetup(args []string) (exitCode int, err error) {
	setupFlags := flag.NewFlagSet("install-mcp", flag.ContinueOnError)
	mcpURL := setupFlags.String("url", defaultMCPURL, "BuildBuddy MCP endpoint URL")
	clientsFlag := setupFlags.String("clients", "", "Comma-separated list of clients to configure: codex,claude. Defaults to all detected clients.")
	force := setupFlags.Bool("force", false, "Replace any existing BuildBuddy MCP server configuration.")
	if err := arg.ParseFlagSet(setupFlags, args); err != nil {
		log.Print(setupUsage)
		if err == flag.ErrHelp {
			return 1, nil
		}
		return 1, err
	}
	if setupFlags.NArg() > 0 {
		log.Print(usage)
		return 1, fmt.Errorf("unknown install-mcp command %q", setupFlags.Arg(0))
	}
	if err := Setup(context.Background(), SetupOptions{
		MCPURL:  *mcpURL,
		Clients: parseClientNames(*clientsFlag),
		Force:   *force,
	}); err != nil {
		return 1, err
	}
	return 0, nil
}

func handleMCPProxy(args []string) (exitCode int, err error) {
	proxyFlags := flag.NewFlagSet("install-mcp proxy", flag.ContinueOnError)
	mcpURL := proxyFlags.String("url", defaultMCPURL, "BuildBuddy MCP endpoint URL")
	if err := arg.ParseFlagSet(proxyFlags, args); err != nil {
		log.Print(proxyUsage)
		if err == flag.ErrHelp {
			return 1, nil
		}
		return 1, err
	}
	if err := RunMCPProxy(context.Background(), os.Stdin, os.Stdout, os.Stderr, *mcpURL); err != nil {
		return 1, err
	}
	return 0, nil
}

type SetupOptions struct {
	MCPURL  string
	Clients []string
	Force   bool
}

func Setup(ctx context.Context, opts SetupOptions) error {
	if opts.MCPURL == "" {
		opts.MCPURL = defaultMCPURL
	}
	clients, missing := selectClients(opts.Clients)
	for _, name := range missing {
		log.Warnf("Unsupported AI client %q. Supported clients are: codex, claude.", name)
	}
	installed := make([]agentClient, 0, len(clients))
	for _, client := range clients {
		if _, err := lookPath(client.BinaryName); err != nil {
			continue
		}
		installed = append(installed, client)
	}
	if len(installed) == 0 {
		if len(opts.Clients) == 0 {
			log.Print("No supported AI coding agents found on PATH. Install Codex or Claude Code, then run 'bb install-mcp'.")
		} else {
			log.Print("None of the requested AI coding agents were found on PATH.")
		}
		return nil
	}

	if err := ensureAPIKeyConfigured(); err != nil {
		return err
	}

	for _, client := range installed {
		if opts.Force && clientIsConfigured(ctx, client) {
			log.Printf("Removing existing BuildBuddy MCP configuration for %s.", client.Name)
			if err := runClientCommand(ctx, client.BinaryName, client.removeArgs, os.Stdout, os.Stderr); err != nil {
				return fmt.Errorf("remove existing %s MCP server: %w", client.Name, err)
			}
		}
		if clientIsConfigured(ctx, client) {
			log.Printf("%s already has a BuildBuddy MCP server configured.", client.Name)
			continue
		}
		log.Printf("Configuring BuildBuddy MCP for %s.", client.Name)
		if err := runClientCommand(ctx, client.BinaryName, client.addArgs(opts.MCPURL), os.Stdout, os.Stderr); err != nil {
			return fmt.Errorf("configure %s MCP server: %w", client.Name, err)
		}
	}
	log.Print("BuildBuddy MCP setup complete. Restart your AI coding agent to pick up the new MCP server.")
	return nil
}

func ensureAPIKeyConfigured() error {
	// Start the normal bb login flow if there is no API key in the environment or
	// in repo-local BuildBuddy config. This should happen during explicit install,
	// not when an AI agent later launches the non-interactive MCP proxy.
	_, err := login.GetAPIKey()
	return err
}

func parseClientNames(clientsFlag string) []string {
	if strings.TrimSpace(clientsFlag) == "" {
		return nil
	}
	clients := strings.Split(clientsFlag, ",")
	out := make([]string, 0, len(clients))
	for _, client := range clients {
		client = strings.TrimSpace(strings.ToLower(client))
		if client != "" {
			out = append(out, client)
		}
	}
	return out
}

func selectClients(names []string) ([]agentClient, []string) {
	clients := supportedClients()
	if len(names) == 0 {
		return clients, nil
	}
	byName := make(map[string]agentClient, len(clients))
	for _, client := range clients {
		byName[client.BinaryName] = client
	}
	selected := make([]agentClient, 0, len(names))
	missing := make([]string, 0)
	for _, name := range names {
		client, ok := byName[name]
		if !ok {
			missing = append(missing, name)
			continue
		}
		selected = append(selected, client)
	}
	return selected, missing
}

func clientIsConfigured(ctx context.Context, client agentClient) bool {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, client.BinaryName, client.getArgs...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run() == nil
}

func runClientCommand(ctx context.Context, binary string, args []string, stdout, stderr io.Writer) error {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	return cmd.Run()
}

func detectedClients() []agentClient {
	clients := supportedClients()
	out := make([]agentClient, 0, len(clients))
	for _, client := range clients {
		if _, err := lookPath(client.BinaryName); err == nil {
			out = append(out, client)
		}
	}
	return out
}

func RunMCPProxy(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, mcpURL string) error {
	if mcpURL == "" {
		mcpURL = defaultMCPURL
	}
	client := &http.Client{Timeout: 5 * time.Minute}
	scanner := bufio.NewScanner(stdin)
	// MCP messages can be large when returning logs, so allow reasonably large
	// newline-delimited JSON-RPC messages on stdio.
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	writer := bufio.NewWriter(stdout)
	defer writer.Flush()
	for scanner.Scan() {
		body := bytes.TrimSpace(scanner.Bytes())
		if len(body) == 0 {
			continue
		}
		response, err := forwardMCPRequest(ctx, client, mcpURL, body)
		if err != nil {
			fmt.Fprintf(stderr, "BuildBuddy MCP proxy error: %s\n", err)
			response = jsonRPCError(body, -32603, status.Message(err))
		}
		if len(response) == 0 {
			continue
		}
		if _, err := writer.Write(bytes.TrimSpace(response)); err != nil {
			return err
		}
		if err := writer.WriteByte('\n'); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func forwardMCPRequest(ctx context.Context, client *http.Client, mcpURL string, body []byte) ([]byte, error) {
	apiKey, err := getAPIKeyForProxy()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, mcpURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	response, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusAccepted {
		return nil, nil
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		msg := strings.TrimSpace(string(response))
		if msg == "" {
			msg = res.Status
		}
		return nil, fmt.Errorf("BuildBuddy MCP request failed with HTTP %d: %s", res.StatusCode, msg)
	}
	return response, nil
}

func getAPIKeyForProxy() (string, error) {
	for _, envVar := range []string{mcpAPIKeyEnvVar, buildBuddyAPIKeyEnvVar} {
		if apiKey := strings.TrimSpace(os.Getenv(envVar)); apiKey != "" {
			return apiKey, nil
		}
	}
	apiKey, err := storage.ReadRepoConfig("api-key")
	if err != nil {
		return "", status.NotFoundErrorf("BuildBuddy API key not found. Run 'bb login' in this repo or set %s.", buildBuddyAPIKeyEnvVar)
	}
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return "", status.NotFoundErrorf("BuildBuddy API key not found. Run 'bb login' in this repo or set %s.", buildBuddyAPIKeyEnvVar)
	}
	return apiKey, nil
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Error   rpcError        `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func jsonRPCError(requestBody []byte, code int, message string) []byte {
	id, ok := jsonRPCID(requestBody)
	if ok && jsonRPCIDIsNotification(id) {
		return nil
	}
	if !ok {
		id = json.RawMessage("null")
	}
	response, err := json.Marshal(rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: rpcError{
			Code:    code,
			Message: message,
		},
	})
	if err != nil {
		return nil
	}
	return response
}

func jsonRPCID(body []byte) (json.RawMessage, bool) {
	var request struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, false
	}
	return request.ID, true
}

func jsonRPCIDIsNotification(id json.RawMessage) bool {
	trimmed := bytes.TrimSpace(id)
	return len(trimmed) == 0 || slices.Equal(trimmed, []byte("null"))
}

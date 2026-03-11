package mcp

import (
	"context"
	"flag"
	"fmt"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/mcp/mcptools"
	"github.com/buildbuddy-io/buildbuddy/cli/version"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	serverName = "buildbuddy-mcp"
)

var (
	flags = flag.NewFlagSet("mcp", flag.ContinueOnError)

	target = flags.String("target", login.DefaultApiTarget, "BuildBuddy gRPC target")

	usage = `
usage: bb mcp [--target=grpcs://remote.buildbuddy.io]

Starts a BuildBuddy MCP server over stdio.

Quick start:
  1. Run 'bb login' (or set BUILDBUDDY_API_KEY)
  2. Configure a "buildbuddy" MCP server using the "bb mcp" command:

# claude
claude mcp add --scope project buildbuddy -- bb mcp

# codex
codex mcp add buildbuddy -- bb mcp

# gemini
gemini mcp add --scope project buildbuddy bb mcp
`
)

type mcpServer struct {
	mcpServer *mcpsdk.Server
	tools     *mcptools.Service
}

func HandleMCP(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return 1, err
	}
	if flags.NArg() != 0 {
		log.Print(usage)
		return 1, nil
	}
	if *target == "" {
		return 1, fmt.Errorf("--target must be non-empty")
	}

	server := newMCPServer(*target)
	if err := server.Serve(); err != nil {
		return 1, err
	}
	return 0, nil
}

func newMCPServer(target string) *mcpServer {
	toolService := mcptools.New(target)
	server := &mcpServer{
		mcpServer: mcpsdk.NewServer(&mcpsdk.Implementation{
			Name:    serverName,
			Version: version.String(),
		}, nil),
		tools: toolService,
	}
	server.registerTools()
	return server
}

func (s *mcpServer) Serve() error {
	defer func() {
		_ = s.close()
	}()
	return s.mcpServer.Run(context.Background(), &mcpsdk.StdioTransport{})
}

func (s *mcpServer) close() error {
	return s.tools.Close()
}

func (s *mcpServer) registerTools() {
	tools := s.tools.Tools()
	names := make([]string, 0, len(tools))
	for name := range tools {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		tool := tools[name]
		mcpsdk.AddTool(s.mcpServer, tool.MCPTool, s.handleToolCall(tool))
	}
}

func (s *mcpServer) handleToolCall(tool *mcptools.Tool) func(context.Context, *mcpsdk.CallToolRequest, map[string]any) (*mcpsdk.CallToolResult, any, error) {
	return func(ctx context.Context, _ *mcpsdk.CallToolRequest, args map[string]any) (*mcpsdk.CallToolResult, any, error) {
		if args == nil {
			args = map[string]any{}
		}
		if err := s.tools.EnsureAuthenticated(); err != nil {
			return nil, nil, err
		}
		if err := s.tools.EnsureClients(); err != nil {
			return nil, nil, err
		}
		callCtx := ctx
		if tool.Timeout > 0 {
			var cancel context.CancelFunc
			callCtx, cancel = context.WithTimeout(ctx, tool.Timeout)
			defer cancel()
		}
		result, err := tool.Handler(callCtx, args)
		if err != nil {
			return nil, nil, err
		}
		return nil, result, nil
	}
}

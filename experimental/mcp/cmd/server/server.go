// Adapted from https://cs.opensource.google/go/x/tools/+/refs/tags/v0.34.0:internal/mcp/examples/sse/main.go
package main

import (
	"context"
	"flag"
	"log"
	"net/http"

	mcp "github.com/buildbuddy-io/buildbuddy/experimental/mcp/sdk"
)

var httpAddr = flag.String("http", "", "use SSE HTTP at this address")

type SayHiParams struct {
	Name string `json:"name"`
}

func SayHi(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[SayHiParams]) (*mcp.CallToolResultFor[SayHiParams], error) {
	return &mcp.CallToolResultFor[SayHiParams]{
		Content: []*mcp.Content{
			mcp.NewTextContent("Hi " + params.Name),
		},
	}, nil
}

func main() {
	flag.Parse()

	if httpAddr == nil || *httpAddr == "" {
		log.Fatal("http address not set")
	}

	server1 := mcp.NewServer("greeter1", "v0.0.1", nil)
	server1.AddTools(mcp.NewTool("greet1", "say hi", SayHi))

	server2 := mcp.NewServer("greeter2", "v0.0.1", nil)
	server2.AddTools(mcp.NewTool("greet2", "say hello", SayHi))

	log.Printf("MCP servers serving at %s\n", *httpAddr)
	handler := mcp.NewSSEHandler(func(request *http.Request) *mcp.Server {
		url := request.URL.Path
		log.Printf("Handling request for URL %s\n", url)
		switch url {
		case "/greeter1":
			return server1
		case "/greeter2":
			return server2
		default:
			return nil
		}
	})
	http.ListenAndServe(*httpAddr, handler)
}

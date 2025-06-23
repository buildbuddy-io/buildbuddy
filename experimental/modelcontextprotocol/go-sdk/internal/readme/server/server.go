// Copyright 2025 The Go MCP SDK Authors. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// !+
package main

import (
	"context"
	"log"

	"github.com/buildbuddy-io/buildbuddy/experimental/modelcontextprotocol/go-sdk/mcp"
)

type HiParams struct {
	Name string `json:"name"`
}

func SayHi(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[HiParams]) (*mcp.CallToolResultFor[any], error) {
	return &mcp.CallToolResultFor[any]{
		Content: []*mcp.Content{mcp.NewTextContent("Hi " + params.Name)},
	}, nil
}

func main() {
	// Create a server with a single tool.
	server := mcp.NewServer("greeter", "v1.0.0", nil)
	server.AddTools(mcp.NewServerTool("greet", "say hi", SayHi))
	// Run the server over stdin/stdout, until the client disconnects
	if err := server.Run(context.Background(), mcp.NewStdioTransport()); err != nil {
		log.Fatal(err)
	}
}

// !-

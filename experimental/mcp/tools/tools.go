package tools

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/experimental/modelcontextprotocol/go-sdk/mcp"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
)

type ToolHandler struct {
	env environment.Env
}

func NewHandler(env environment.Env) (*ToolHandler, error) {
	return &ToolHandler{
		env: env,
	}, nil
}

func (t *ToolHandler) GetAllTools() []*mcp.ServerTool {
	return []*mcp.ServerTool{
		mcp.NewServerTool("echo", "echo any string back", t.Echo),
		mcp.NewServerTool("GetInvocation", "list all invocations or get specific invocations by ID", t.GetInvocation),
	}
}

type EchoParams struct {
	Value string `json:"value"`
}

func (t *ToolHandler) Echo(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[EchoParams]) (*mcp.CallToolResultFor[any], error) {
	return &mcp.CallToolResultFor[any]{
		Content: []*mcp.Content{
			mcp.NewTextContent(fmt.Sprintf("~%q~", params.Arguments.Value)),
		},
	}, nil
}

type GetInvocationParams struct {
	InvocationID *string `json:"invocation_id"`
}

func (t *ToolHandler) GetInvocation(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[GetInvocationParams]) (*mcp.CallToolResultFor[any], error) {
	if t.env.GetApiClient() == nil {
		return nil, status.InternalError("no registered api client")
	}
	req := &apipb.GetInvocationRequest{}
	if params.Arguments.InvocationID != nil {
		req.Selector = &apipb.InvocationSelector{
			InvocationId: *params.Arguments.InvocationID,
		}
	}
	_, err := t.env.GetApiClient().GetInvocation(ctx, req)
	if err != nil {
		return nil, err // TODO(tylerw): handle
	}
	return &mcp.CallToolResultFor[any]{
		Content: []*mcp.Content{
			mcp.NewResourceContent(&mcp.ResourceContents{
				URI: "https://app.buildbuddy.io/invocation/" + *params.Arguments.InvocationID,
			}),
		},
	}, nil
}

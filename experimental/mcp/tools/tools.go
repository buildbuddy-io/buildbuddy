package tools

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/experimental/modelcontextprotocol/go-sdk/mcp"

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
	log.Infof("Echo function called with session: %+v, params: %+v", cc, params)
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
	log.Infof("GetInvocation function called with session: %+v, params: %+v", cc, params)
	if t.env.GetApiClient() == nil {
		return nil, status.InternalError("no registered api client")
	}
	rsp, err := t.env.GetApiClient().GetInvocation(ctx, &apipb.GetInvocationRequest{})
	if err != nil {
		return nil, err // TODO(tylerw): handle
	}
	log.Infof("rsp: %+v", rsp)
	return &mcp.CallToolResultFor[any]{
		Content: []*mcp.Content{
			mcp.NewTextContent(fmt.Sprintf("~%q~", params.Arguments.InvocationID)),
		},
	}, nil
}

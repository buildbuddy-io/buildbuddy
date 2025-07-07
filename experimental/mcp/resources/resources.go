package resources

import (
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type ResourceHandler struct {
	env environment.Env
}

func NewHandler(env environment.Env) (*ResourceHandler, error) {
	return &ResourceHandler{
		env: env,
	}, nil
}

func (r *ResourceHandler) Register(server *mcp.Server) error {
	return nil
}

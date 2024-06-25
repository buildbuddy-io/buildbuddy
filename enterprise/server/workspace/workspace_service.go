package workspace

import (
	"context"
	"flag"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	wspb "github.com/buildbuddy-io/buildbuddy/proto/workspace"
)

var (
	enabled = flag.Bool("workspace.enabled", false, "If true, enable workspaces.")
)

type workspaceService struct {
	env environment.Env
}

func Register(env *real_environment.RealEnv) error {
	if *enabled {
		env.SetWorkspaceService(New(env))
	}
	return nil
}

func New(env environment.Env) *workspaceService {
	return &workspaceService{
		env: env,
	}
}

func (s *workspaceService) GetWorkspace(ctx context.Context, req *wspb.GetWorkspaceRequest) (*wspb.GetWorkspaceResponse, error) {
	return nil, status.UnimplementedError("Not implemented")
}

func (s *workspaceService) SaveWorkspace(ctx context.Context, req *wspb.SaveWorkspaceRequest) (*wspb.SaveWorkspaceResponse, error) {
	return nil, status.UnimplementedError("Not implemented")
}

func (s *workspaceService) GetWorkspaceDirectory(ctx context.Context, req *wspb.GetWorkspaceDirectoryRequest) (*wspb.GetWorkspaceDirectoryResponse, error) {
	return nil, status.UnimplementedError("Not implemented")
}

func (s *workspaceService) GetWorkspaceFile(ctx context.Context, req *wspb.GetWorkspaceFileRequest) (*wspb.GetWorkspaceFileResponse, error) {
	return nil, status.UnimplementedError("Not implemented")
}

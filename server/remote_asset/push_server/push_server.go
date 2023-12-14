package push_server

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
)

type PushServer struct {
	env environment.Env
}

func Register(env *real_environment.RealEnv) error {
	// OPTIONAL CACHE API -- only enable if configured.
	if env.GetCache() == nil {
		return nil
	}
	env.SetPushServer(NewPushServer(env))
	return nil
}

func NewPushServer(env environment.Env) *PushServer {
	return &PushServer{
		env: env,
	}
}

func (p *PushServer) PushBlob(ctx context.Context, req *rapb.PushBlobRequest) (*rapb.PushBlobResponse, error) {
	return nil, status.UnimplementedError("PushBlob is not yet implemented")
}

func (p *PushServer) PushDirectory(ctx context.Context, req *rapb.PushDirectoryRequest) (*rapb.PushDirectoryResponse, error) {
	return nil, status.UnimplementedError("PushDirectory is not yet implemented")
}

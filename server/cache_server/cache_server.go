package cache_server

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	cspb "github.com/buildbuddy-io/buildbuddy/proto/cache_service"
)

type CacheServer struct {
	env environment.Env
}

func Register(env *real_environment.RealEnv) error {
	if env.GetCache() == nil {
		return status.FailedPreconditionErrorf("no cache configured")
	}
	env.SetCacheServer(New(env))
	return nil
}

func New(env environment.Env) cspb.CacheServer {
	return &CacheServer{
		env: env,
	}
}

func (s *CacheServer) GetMetadata(ctx context.Context, req *capb.GetCacheMetadataRequest) (*capb.GetCacheMetadataResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env.GetAuthenticator())
	if err != nil {
		return nil, err
	}

	metadata, err := s.env.GetCache().Metadata(ctx, req.GetResourceName())
	if err != nil {
		return nil, err
	}

	return &capb.GetCacheMetadataResponse{
		StoredSizeBytes: metadata.StoredSizeBytes,
		DigestSizeBytes: metadata.DigestSizeBytes,
		LastAccessUsec:  metadata.LastAccessTimeUsec,
		LastModifyUsec:  metadata.LastModifyTimeUsec,
	}, nil
}

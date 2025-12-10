package cache_service

import (
	"context"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	cache_service_pb "github.com/buildbuddy-io/buildbuddy/proto/cache_service"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type Service struct {
	env environment.Env
}

var _ cache_service_pb.CacheServiceServer = (*Service)(nil)

func Register(env *real_environment.RealEnv) error {
	if env.GetCache() == nil {
		return nil
	}
	env.SetCacheService(New(env))
	return nil
}

func New(env environment.Env) cache_service_pb.CacheServiceServer {
	return &Service{
		env: env,
	}
}

func (s *Service) GetMetadata(ctx context.Context, req *capb.GetCacheMetadataRequest) (*capb.GetCacheMetadataResponse, error) {
	if req == nil {
		return nil, status.InvalidArgumentError("request is required")
	}
	if s.env.GetCache() == nil {
		return nil, status.UnimplementedError("cache not configured")
	}
	if req.GetResourceName() == nil {
		return nil, status.InvalidArgumentError("resource_name is required")
	}

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

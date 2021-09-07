package usage_service

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"

	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

type usageService struct {
	env environment.Env
}

func New(env environment.Env) *usageService {
	return &usageService{env}
}

func (s *usageService) GetUsage(ctx context.Context, req *usagepb.GetUsageRequest) (*usagepb.GetUsageResponse, error) {
	return &usagepb.GetUsageResponse{}, nil
}

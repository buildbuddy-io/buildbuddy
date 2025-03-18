package remoteusage

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"

	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

func Register(env *real_environment.RealEnv) {
	env.SetRemoteUsageService(&Service{})
}

type Service struct {
}

func (u *Service) Record(ctx context.Context, req *usagepb.RecordRequest) (*usagepb.RecordResponse, error) {
	// TODO(iain): verify caller identity before recording usage. This should
	// be done automatically by usage.go, but confirm.
	return &usagepb.RecordResponse{}, nil
}

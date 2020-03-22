package buildbuddy_server

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	inpb "proto/invocation"
)

type BuildBuddyServer struct {
	env environment.Env
}

func NewBuildBuddyServer(env environment.Env) (*BuildBuddyServer, error) {
	return &BuildBuddyServer{
		env: env,
	}, nil
}

func (s *BuildBuddyServer) GetInvocation(ctx context.Context, req *inpb.GetInvocationRequest) (*inpb.GetInvocationResponse, error) {
	inv, err := build_event_handler.LookupInvocation(s.env, ctx, req.Lookup.InvocationId)
	if err != nil {
		return nil, err
	}
	return &inpb.GetInvocationResponse{
		Invocation: []*inpb.Invocation{
			inv,
		},
	}, nil
}

func (s *BuildBuddyServer) SearchInvocation(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error) {
	searcher := s.env.GetSearcher()
	if searcher == nil {
		return nil, fmt.Errorf("No searcher was configured")
	}
	if req.Query == nil {
		return nil, fmt.Errorf("A query must be provided")
	}
	return searcher.QueryInvocations(ctx, req)
}

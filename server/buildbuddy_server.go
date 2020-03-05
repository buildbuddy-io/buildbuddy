package buildbuddy_server

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	inpb "proto/invocation"
)

type BuildBuddyServer struct {
	eventHandler *build_event_handler.BuildEventHandler
	searcher     interfaces.Searcher
}

func NewBuildBuddyServer(h *build_event_handler.BuildEventHandler, s interfaces.Searcher) (*BuildBuddyServer, error) {
	return &BuildBuddyServer{
		eventHandler: h,
		searcher:     s,
	}, nil
}

func (s *BuildBuddyServer) GetInvocation(ctx context.Context, req *inpb.GetInvocationRequest) (*inpb.GetInvocationResponse, error) {
	inv, err := s.eventHandler.LookupInvocation(ctx, req.Lookup.InvocationId)
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
	if s.searcher == nil {
		return nil, fmt.Errorf("No searcher was configured")
	}
	if req.Query == nil {
		return nil, fmt.Errorf("A query must be provided")
	}
	return s.searcher.QueryInvocations(ctx, req)
}

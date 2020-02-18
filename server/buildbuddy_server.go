package buildbuddy_server

import (
	"context"
	"fmt"

	"github.com/tryflame/buildbuddy/server/build_event_handler"
	inpb "proto/invocation"
)

type BuildBuddyServer struct {
	eventHandler *build_event_handler.BuildEventHandler
}

func NewBuildBuddyServer(h *build_event_handler.BuildEventHandler) (*BuildBuddyServer, error) {
	return &BuildBuddyServer{
		eventHandler: h,
	}, nil
}

func (s *BuildBuddyServer) GetInvocation(ctx context.Context, req *inpb.GetInvocationRequest) (*inpb.GetInvocationResponse, error) {
	fmt.Printf("GetInvocation called: %s\n", req)

	inv, err := s.eventHandler.LookupInvocation(req.Query.InvocationId)
	if err != nil {
		return nil, err
	}
	return &inpb.GetInvocationResponse{
		Invocation: []*inpb.Invocation{
			inv,
		},
	}, nil
}

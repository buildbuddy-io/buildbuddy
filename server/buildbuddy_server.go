package buildbuddy_server

import (
	"context"
	"fmt"

	inpb "proto/invocation"
)

type BuildBuddyServer struct {
}

func NewBuildBuddyServer() (*BuildBuddyServer, error) {
	return &BuildBuddyServer{}, nil
}

func (s *BuildBuddyServer) GetInvocation(ctx context.Context, req *inpb.GetInvocationRequest) (*inpb.GetInvocationResponse, error) {
	fmt.Printf("GetInvocation called: %s\n", req)
	return nil, fmt.Errorf("not implmented yet, sorry!")
}

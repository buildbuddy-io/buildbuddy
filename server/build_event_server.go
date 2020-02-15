package build_event_server

import (
	"context"
	"fmt"
	"io"

	"github.com/golang/protobuf/ptypes/empty"

	bpb "proto"

	// Used when parsing the "any* in PublishBuildEvent
	_ "proto/build_event_stream"
)

type BuildEventProtocolServer struct {
}

func NewBuildEventProtocolServer() (*BuildEventProtocolServer, error) {
	return &BuildEventProtocolServer{}, nil
}

func (s *BuildEventProtocolServer) PublishLifecycleEvent(ctx context.Context, req *bpb.PublishLifecycleEventRequest) (*empty.Empty, error) {
	fmt.Printf("in: %+v\n\n", req)
	// Safe to ignore the response for now because we're sending
	// these from the flame-local-server.
	return &empty.Empty{}, nil
}

func (s *BuildEventProtocolServer) PublishBuildToolEventStream(stream bpb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		fmt.Printf("in: %+v\n\n", in)
	}
	// Safe to ignore the response for now because we're sending
	// these from the flame-local-server.
	return nil
}

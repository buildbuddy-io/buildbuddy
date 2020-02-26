package build_event_server

import (
	"context"
	"io"
	"log"
	"sort"

	_ "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/tryflame/buildbuddy/server/build_event_handler"

	bpb "proto"
	inpb "proto/invocation"
	"proto/build_event_stream"
)

type BuildEventProtocolServer struct {
	eventHandler *build_event_handler.BuildEventHandler
}

func NewBuildEventProtocolServer(h *build_event_handler.BuildEventHandler) (*BuildEventProtocolServer, error) {
	return &BuildEventProtocolServer{
		eventHandler: h,
	}, nil
}

func (s *BuildEventProtocolServer) chompBuildEvent(obe *bpb.OrderedBuildEvent) (*inpb.InvocationEvent, error) {
	switch buildEvent := obe.Event.Event.(type) {
	case *bpb.BuildEvent_ComponentStreamFinished:
		return nil, nil
	case *bpb.BuildEvent_BazelEvent:
		var bazelBuildEvent build_event_stream.BuildEvent
		if err := ptypes.UnmarshalAny(buildEvent.BazelEvent, &bazelBuildEvent); err != nil {
			return nil, err
		}
		invocationEvent := &inpb.InvocationEvent{
			EventTime: obe.Event.EventTime,
			BuildEvent: &bazelBuildEvent,
		}

		return invocationEvent, nil
	}
	return nil, nil
}

func (s *BuildEventProtocolServer) PublishLifecycleEvent(ctx context.Context, req *bpb.PublishLifecycleEventRequest) (*empty.Empty, error) {
	// We don't currently handle these events.
	return &empty.Empty{}, nil
}

// Handles Streaming BuildToolEvent
// From the bazel client: (Read more in BuildEventServiceUploader.java.)
// {@link BuildEventServiceUploaderCommands#OPEN_STREAM} is the first event and opens a
// bidi streaming RPC for sending build events and receiving ACKs.
// {@link BuildEventServiceUploaderCommands#SEND_REGULAR_BUILD_EVENT} sends a build event to
// the server. Sending of the Nth build event does
// does not wait for the ACK of the N-1th build event to have been received.
// {@link BuildEventServiceUploaderCommands#SEND_LAST_BUILD_EVENT} sends the last build event
// and half closes the RPC.
// {@link BuildEventServiceUploaderCommands#ACK_RECEIVED} is executed for every ACK from
// the server and checks that the ACKs are in the correct order.
// {@link BuildEventServiceUploaderCommands#STREAM_COMPLETE} checks that all build events
// have been sent and all ACKs have been received. If not it invokes a retry logic that may
// decide to re-send every build event for which an ACK has not been received. If so, it
// adds an OPEN_STREAM event.
func (s *BuildEventProtocolServer) PublishBuildToolEventStream(stream bpb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	// Semantically, the protocol requires we ack events in order.
	acks := make([]int, 0)
	invocationEvents := make([]*inpb.InvocationEvent, 0)
	streamID := &bpb.StreamId{}

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		streamID = in.OrderedBuildEvent.StreamId
		log.Printf("streamID: %+v", streamID)
		invocationEvent, err := s.chompBuildEvent(in.OrderedBuildEvent)
		if err != nil {
			return err
		}
		if invocationEvent != nil {
			invocationEvents = append(invocationEvents, invocationEvent)
		}
		acks = append(acks, int(in.OrderedBuildEvent.SequenceNumber))
	}

	// Check that we have received all acks! If we haven't bail out since we
	// don't want to ack *anything*. This forces the client to retransmit
	// everything all at once, which means we don't need to worry about
	// cross-server consistency here.
	sort.Sort(sort.IntSlice(acks))
	for i, ack := range acks {
		if ack != i+1 {
			log.Printf("Missing ack: saw %d and wanted %d. Bailing!", ack, i+1)
			return io.EOF
		}
	}

	if err := s.eventHandler.HandleEvents(stream.Context(), streamID.InvocationId, invocationEvents); err != nil {
		log.Printf("Error processing build events: %s", err)
		return io.EOF
	}

	// Finally, ack everything.
	for _, ack := range acks {
		rsp := &bpb.PublishBuildToolEventStreamResponse{
			StreamId:       streamID,
			SequenceNumber: int64(ack),
		}
		stream.Send(rsp)
	}
	return nil
}

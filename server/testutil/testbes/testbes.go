package testbes

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

type HandlerFunc = func(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer, streamID *bepb.StreamId, event *pepb.PublishBuildToolEventStreamRequest) error

// TestBuildEventServer can be used as a proxy target or for testing BES clients.
// It collects build events and allows controlling responses on the stream.
//
// Set EventHandler to override the default behavior, which is to simply ack
// each event as soon as it is received.
type TestBuildEventServer struct {
	t testing.TB

	EventHandler HandlerFunc
}

func NewTestBuildEventServer(t testing.TB) *TestBuildEventServer {
	return &TestBuildEventServer{t: t}
}

func (s *TestBuildEventServer) PublishLifecycleEvent(ctx context.Context, req *pepb.PublishLifecycleEventRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *TestBuildEventServer) PublishBuildToolEventStream(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	var streamID *bepb.StreamId
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.UnknownErrorf("recv: %s", err)
		}
		if streamID == nil {
			streamID = msg.OrderedBuildEvent.GetStreamId()
		}
		handler := s.EventHandler
		if handler == nil {
			handler = Ack
		}
		if err := handler(stream, streamID, msg); err != nil {
			return err
		}
	}
}

func Ack(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer, streamID *bepb.StreamId, event *pepb.PublishBuildToolEventStreamRequest) error {
	err := stream.Send(&pepb.PublishBuildToolEventStreamResponse{
		StreamId:       streamID,
		SequenceNumber: event.OrderedBuildEvent.GetSequenceNumber(),
	})
	if err != nil {
		return status.UnknownErrorf("send failed: %s", err)
	}
	return nil
}

// FailWith returns a BES handler that always replies with the given error.
func FailWith(err error) HandlerFunc {
	return func(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer, streamID *bepb.StreamId, event *pepb.PublishBuildToolEventStreamRequest) error {
		return err
	}
}

// Run starts a test BES server and returns both the server and a client connected to it.
// The server uses an in-memory bufconn connection and is automatically cleaned up when the test ends.
func Run(t testing.TB) (*TestBuildEventServer, pepb.PublishBuildEventClient) {
	server := NewTestBuildEventServer(t)

	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	pepb.RegisterPublishBuildEventServer(gs, server)

	go func() {
		_ = gs.Serve(lis)
	}()
	t.Cleanup(func() {
		gs.Stop()
	})

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithInsecure(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})
	client := pepb.NewPublishBuildEventClient(conn)

	return server, client
}

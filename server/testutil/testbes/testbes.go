package testbes

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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
//
// Note: Like a real BES server, events are persisted as they arrive and are ACK'd.
// If a stream fails and the client retries, duplicate events will be stored.
// This mirrors production behavior where failed streams don't rollback already-persisted events.
type TestBuildEventServer struct {
	t testing.TB

	EventHandler HandlerFunc

	mu             sync.Mutex
	events         []*pepb.PublishBuildToolEventStreamRequest
	attempts       [][]*pepb.PublishBuildToolEventStreamRequest
	streamMetadata metadata.MD
}

func NewTestBuildEventServer(t testing.TB) *TestBuildEventServer {
	return &TestBuildEventServer{t: t}
}

// GetEvents returns all events received by the server.
func (s *TestBuildEventServer) GetEvents() []*pepb.PublishBuildToolEventStreamRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]*pepb.PublishBuildToolEventStreamRequest, len(s.events))
	copy(events, s.events)
	return events
}

// GetAttempts returns the events received in each individual stream attempt.
func (s *TestBuildEventServer) GetAttempts() [][]*pepb.PublishBuildToolEventStreamRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	attempts := make([][]*pepb.PublishBuildToolEventStreamRequest, 0, len(s.attempts))
	for _, attempt := range s.attempts {
		copyAttempt := make([]*pepb.PublishBuildToolEventStreamRequest, len(attempt))
		copy(copyAttempt, attempt)
		attempts = append(attempts, copyAttempt)
	}
	return attempts
}

// GetMetadata returns the metadata from the most recent stream.
func (s *TestBuildEventServer) GetMetadata() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streamMetadata.Copy()
}

// Reset clears all captured events and metadata.
func (s *TestBuildEventServer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = nil
	s.attempts = nil
	s.streamMetadata = nil
}

func (s *TestBuildEventServer) PublishLifecycleEvent(ctx context.Context, req *pepb.PublishLifecycleEventRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *TestBuildEventServer) PublishBuildToolEventStream(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	// Capture metadata from the stream
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		s.mu.Lock()
		s.streamMetadata = md
		s.mu.Unlock()
	}

	var streamEvents []*pepb.PublishBuildToolEventStreamRequest
	var streamID *bepb.StreamId
	recordAttempt := func() {
		if len(streamEvents) == 0 {
			return
		}
		copyAttempt := make([]*pepb.PublishBuildToolEventStreamRequest, len(streamEvents))
		copy(copyAttempt, streamEvents)
		s.mu.Lock()
		s.attempts = append(s.attempts, copyAttempt)
		s.mu.Unlock()
	}
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			recordAttempt()
			return nil
		}
		if err != nil {
			recordAttempt()
			return status.UnknownErrorf("recv: %s", err)
		}
		if streamID == nil {
			streamID = msg.OrderedBuildEvent.GetStreamId()
		}

		streamEvents = append(streamEvents, msg)

		// Store the event immediately, like a real BES server.
		// Events are persisted as they arrive, even if the stream later fails.
		// This means retries will create duplicate events in the log.
		s.mu.Lock()
		s.events = append(s.events, msg)
		s.mu.Unlock()

		handler := s.EventHandler
		if handler == nil {
			handler = Ack
		}
		if err := handler(stream, streamID, msg); err != nil {
			// Even though we're returning an error, the events we've already
			// stored stay persisted (no rollback), matching real BES behavior.
			recordAttempt()
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

// FailNTimesThenSucceed returns a BES handler that fails the first N times it's called,
// then succeeds (ACKs) for all subsequent calls.
func FailNTimesThenSucceed(n int, err error) HandlerFunc {
	count := 0
	mu := &sync.Mutex{}
	return func(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer, streamID *bepb.StreamId, event *pepb.PublishBuildToolEventStreamRequest) error {
		mu.Lock()
		defer mu.Unlock()
		if count < n {
			count++
			return err
		}
		return Ack(stream, streamID, event)
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

// RunTCP starts a test BES server on a random TCP port and returns the server and its address.
// This is useful for testing clients that need to dial a real address.
// The returned address is prefixed with "grpc://" to indicate an insecure connection.
func RunTCP(t testing.TB) (*TestBuildEventServer, string) {
	server := NewTestBuildEventServer(t)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	gs := grpc.NewServer()
	pepb.RegisterPublishBuildEventServer(gs, server)

	go func() {
		_ = gs.Serve(lis)
	}()
	t.Cleanup(func() {
		gs.Stop()
	})

	// Prefix with grpc:// to indicate insecure connection
	return server, "grpc://" + lis.Addr().String()
}

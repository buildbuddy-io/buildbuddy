package build_event_server_test

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

func TestPublishBuildToolEventStream_NoEvents(t *testing.T) {
	env := testenv.GetTestEnv(t)
	env.SetBuildEventHandler(NewTestBuildEventHandler())
	server, err := build_event_server.NewBuildEventProtocolServer(env, false /*=synchronous*/)
	require.NoError(t, err)
	grpcServer, runServer, lis := testenv.RegisterLocalGRPCServer(t, env)
	pepb.RegisterPublishBuildEventServer(grpcServer, server)
	go runServer()

	// Make a PublishBuildToolEventStream RPC but close it without sending
	// anything.
	ctx := context.Background()
	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	client := pepb.NewPublishBuildEventClient(conn)
	stream, err := client.PublishBuildToolEventStream(ctx)
	require.NoError(t, err)
	err = stream.CloseSend()
	require.NoError(t, err)

	_, err = stream.Recv()
	require.Equal(t, io.EOF, err)
}

type proxyTestCase struct {
	Name            string
	Synchronous     bool
	UpstreamHandler HandlerFunc
	ExpectedError   error
}

func TestPublishBuildToolEventStream_Proxy(t *testing.T) {
	for _, test := range []proxyTestCase{
		{
			Name:            "can proxy events to upstream in sync mode",
			Synchronous:     true,
			UpstreamHandler: Ack,
			ExpectedError:   nil,
		},
		{
			Name:            "can proxy events to upstream in async mode",
			Synchronous:     false,
			UpstreamHandler: Ack,
			ExpectedError:   nil,
		},
		{
			Name:            "should ignore upstream recv errors in async mode",
			Synchronous:     false,
			UpstreamHandler: FailWith(status.UnavailableErrorf("test error")),
			ExpectedError:   nil,
		},
		{
			Name:            "should return upstream recv errors in sync mode",
			Synchronous:     true,
			UpstreamHandler: FailWith(status.UnavailableErrorf("test error")),
			ExpectedError:   status.UnavailableError("recv from proxy stream: test error"),
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			testPublishBuildToolEventStreamProxy(t, test)
		})
	}
}

func testPublishBuildToolEventStreamProxy(t *testing.T, test proxyTestCase) {
	env := testenv.GetTestEnv(t)
	env.SetBuildEventHandler(NewTestBuildEventHandler())

	// Run a test BE server and configure it as the upstream client.
	upstream, upstreamClient := runTestBuildEventServer(t)
	upstream.EventHandler = test.UpstreamHandler
	env.SetBuildEventProxyClients([]pepb.PublishBuildEventClient{upstreamClient})

	server, err := build_event_server.NewBuildEventProtocolServer(env, test.Synchronous)
	require.NoError(t, err)
	grpcServer, runServer, lis := testenv.RegisterLocalGRPCServer(t, env)
	pepb.RegisterPublishBuildEventServer(grpcServer, server)
	go runServer()

	ctx := context.Background()
	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	client := pepb.NewPublishBuildEventClient(conn)
	stream, err := client.PublishBuildToolEventStream(ctx)
	require.NoError(t, err)

	// Publish one message to trigger an error from the upstream server.
	err = stream.Send(&pepb.PublishBuildToolEventStreamRequest{
		OrderedBuildEvent: &pepb.OrderedBuildEvent{
			StreamId:       &bepb.StreamId{InvocationId: uuid.New()},
			SequenceNumber: 1,
		},
	})
	require.NoError(t, err)

	err = stream.CloseSend()
	require.NoError(t, err)

	rsp, err := stream.Recv()
	if test.ExpectedError == nil {
		require.NoError(t, err)
		assert.Equal(t, int64(1), rsp.GetSequenceNumber())
	} else {
		require.Equal(t, test.ExpectedError.Error(), err.Error())
	}
}

func runTestBuildEventServer(t testing.TB) (*TestBuildEventServer, pepb.PublishBuildEventClient) {
	server := NewTestBuildEventServer(t)

	sock := testfs.MakeSocket(t, "proxy.sock")
	gs := grpc.NewServer()
	pepb.RegisterPublishBuildEventServer(gs, server)
	lis, err := net.Listen("unix", sock)
	require.NoError(t, err)
	go func() {
		_ = gs.Serve(lis)
	}()
	t.Cleanup(func() {
		gs.Stop()
	})
	conn, err := grpc_client.DialSimple(
		"unix://"+sock,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	client := pepb.NewPublishBuildEventClient(conn)

	return server, client
}

type TestBuildEventHandler struct{}

func NewTestBuildEventHandler() *TestBuildEventHandler {
	return &TestBuildEventHandler{}
}

func (h *TestBuildEventHandler) OpenChannel(ctx context.Context, invocationID string) (interfaces.BuildEventChannel, error) {
	return &TestBuildEventChannel{ctx: ctx}, nil
}

type TestBuildEventChannel struct{ ctx context.Context }

func (c *TestBuildEventChannel) HandleEvent(event *pepb.PublishBuildToolEventStreamRequest) error {
	return nil
}
func (c *TestBuildEventChannel) GetNumDroppedEvents() uint64                  { return 0 }
func (c *TestBuildEventChannel) GetInitialSequenceNumber() int64              { return 1 }
func (c *TestBuildEventChannel) Context() context.Context                     { return c.ctx }
func (c *TestBuildEventChannel) FinalizeInvocation(invocationID string) error { return nil }
func (c *TestBuildEventChannel) Close()                                       {}

type HandlerFunc = func(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer, streamID *bepb.StreamId, event *pepb.PublishBuildToolEventStreamRequest) error

// TestBuildEventServer can be used as a proxy target.
// It collects build events and allows controlling responses on the stream.
//
// Set EventHandler to override the default behavior, which is to simply ack
// each event as soon as it is received.
type TestBuildEventServer struct {
	t testing.TB

	// EventHandler can be used to override the default Ack handler.
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

// Ack sends an acknowledgement for the given event.
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

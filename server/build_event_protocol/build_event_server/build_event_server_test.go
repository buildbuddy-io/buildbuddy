package build_event_server_test

import (
	"context"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbes"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	UpstreamHandler testbes.HandlerFunc
	ExpectedError   error
}

func TestPublishBuildToolEventStream_Proxy(t *testing.T) {
	for _, test := range []proxyTestCase{
		{
			Name:            "can proxy events to upstream in sync mode",
			Synchronous:     true,
			UpstreamHandler: testbes.Ack,
			ExpectedError:   nil,
		},
		{
			Name:            "can proxy events to upstream in async mode",
			Synchronous:     false,
			UpstreamHandler: testbes.Ack,
			ExpectedError:   nil,
		},
		{
			Name:            "should ignore upstream recv errors in async mode",
			Synchronous:     false,
			UpstreamHandler: testbes.FailWith(status.UnavailableErrorf("test error")),
			ExpectedError:   nil,
		},
		{
			Name:            "should return upstream recv errors in sync mode",
			Synchronous:     true,
			UpstreamHandler: testbes.FailWith(status.UnavailableErrorf("test error")),
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
	upstream, upstreamClient := testbes.Run(t)
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

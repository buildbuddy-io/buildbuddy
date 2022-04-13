package build_event_protocol_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

var (
	workspaceContents = map[string]string{
		"WORKSPACE": `workspace(name = "integration_test")`,
		"BUILD":     `genrule(name = "hello_txt", outs = ["hello.txt"], cmd_bash = "echo 'Hello world' > $@")`,
	}
)

func TestBuildWithBESFlags_Success(t *testing.T) {
	app := buildbuddy.Run(t)
	ctx := context.Background()
	ws := testbazel.MakeTempWorkspace(t, workspaceContents)
	buildFlags := []string{"//:hello.txt"}
	buildFlags = append(buildFlags, app.BESBazelFlags()...)

	result := testbazel.Invoke(ctx, t, ws, "build", buildFlags...)

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
}

func TestBuildWithRetry_InjectFailureAfterBuildStarted(t *testing.T) {
	testInjectFailureAfterBazelEvent(t, &bespb.BuildEvent_Started{})
}

func TestBuildWithRetry_InjectFailureAfterProgress(t *testing.T) {
	testInjectFailureAfterBazelEvent(t, &bespb.BuildEvent_Progress{})
}

func TestBuildWithRetry_InjectFailureAfterWorkspaceStatus(t *testing.T) {
	testInjectFailureAfterBazelEvent(t, &bespb.BuildEvent_WorkspaceStatus{})
}

func TestBuildWithRetry_InjectFailureAfterBuildFinished(t *testing.T) {
	testInjectFailureAfterBazelEvent(t, &bespb.BuildEvent_Finished{})
}

func testInjectFailureAfterBazelEvent(t *testing.T, payloadMsg interface{}) {
	app := buildbuddy.Run(t)
	bepClient := app.PublishBuildEventClient(t)
	proxy := StartBEPProxy(t, bepClient)
	proxy.FailOnce(AfterForwardBazelEvent(t, payloadMsg))

	ctx := context.Background()
	ws := testbazel.MakeTempWorkspace(t, workspaceContents)
	buildFlags := append([]string{"//:hello.txt"}, app.BESBazelFlags()...)
	buildFlags = append(buildFlags, "--bes_backend="+proxy.GRPCAddress())

	result := testbazel.Invoke(ctx, t, ws, "build", buildFlags...)

	require.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
	bbService := app.BuildBuddyServiceClient(t)
	res, err := bbService.GetInvocation(
		context.Background(),
		&inpb.GetInvocationRequest{Lookup: &inpb.InvocationLookup{InvocationId: result.InvocationID}})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(res.Invocation), 0)
	assert.Equal(t, inpb.Invocation_COMPLETE_INVOCATION_STATUS, res.Invocation[0].GetInvocationStatus())
	assert.Equal(t, true, res.Invocation[0].Success)
}

// StreamEvent represents an event occuring in the stream proxy as it forwards
// messages between the client and server during a streaming RPC. Exactly one
// of ServerRecv or ClientRecv will be non-nil.
type StreamEvent struct {
	// WasForwarded marks whether the event has yet been forwarded to its
	// destination. For every message, a StreamEvent will be fired both before
	// and after forwarding.
	WasForwarded bool

	// ServerRecv contains the message received from the server to be forwarded
	// to the client, if applicable.
	ServerRecv *serverRecv

	// ClientRecv contains the message received from the client to be forwarded
	// to the server, if applicable.
	ClientRecv *clientRecv
}

// StreamErrorInjector inspects a stream event and optionally returns an error
// to be returned to the client.
type StreamErrorInjector func(*StreamEvent) error

// AfterForwardBazelEvent returns an error injector that injects an error after
// a Bazel event with the same payload type as the given message is successfully
// forwarded to the build event server. The given message must be assignable to
// BuildEvent.Payload, otherwise the test immediately fails.
func AfterForwardBazelEvent(t *testing.T, payloadMsg interface{}) StreamErrorInjector {
	payloadType := reflect.TypeOf(payloadMsg)
	payloadSuperType := reflect.TypeOf(&(&bespb.BuildEvent{}).Payload).Elem()
	if !payloadType.AssignableTo(payloadSuperType) {
		require.FailNowf(t, "invalid argument", "%s is not assignable to %s", payloadType, payloadSuperType)
		return nil
	}
	return func(event *StreamEvent) error {
		if !event.WasForwarded || event.ClientRecv == nil || event.ClientRecv.req == nil {
			return nil
		}
		req := event.ClientRecv.req
		bazelEvent, err := readBazelEvent(req.GetOrderedBuildEvent())
		if err != nil {
			return err
		}
		if bazelEvent == nil {
			return nil
		}
		if reflect.TypeOf(bazelEvent.GetPayload()).AssignableTo(reflect.TypeOf(payloadType)) {
			return status.UnavailableError("Proxy: Injected error for test")
		}
		return nil
	}
}

// BEPProxyServer sits in between Bazel and the build event server, proxying
// messages directly. It allows injecting errors into the stream to ensure that
// they are handled properly.
type BEPProxyServer struct {
	t             *testing.T
	backend       pepb.PublishBuildEventClient
	port          int
	errorInjector StreamErrorInjector
}

func StartBEPProxy(t *testing.T, backend pepb.PublishBuildEventClient) *BEPProxyServer {
	proxy := &BEPProxyServer{
		t:       t,
		backend: backend,
	}

	lis, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err, "failed to start BES proxy")
	port := lis.Addr().(*net.TCPAddr).Port

	server := grpc.NewServer()
	pepb.RegisterPublishBuildEventServer(server, proxy)
	go func() {
		log.Infof("Proxy: Listening on 0.0.0.0:%d", port)
		err := server.Serve(lis)
		require.NoError(t, err)
	}()
	t.Cleanup(func() {
		server.GracefulStop()
	})

	proxy.port = port

	return proxy
}

func (p *BEPProxyServer) GRPCAddress() string {
	return fmt.Sprintf("grpc://localhost:%d", p.port)
}

// FailOnce registers an error injector that can fire at most once for this
// proxy server instance. In other words, it is unregistered after returning an
// error for the first time.
func (p *BEPProxyServer) FailOnce(f StreamErrorInjector) {
	p.errorInjector = f
}

func (p *BEPProxyServer) maybeInjectError(event *StreamEvent) error {
	if p.errorInjector == nil {
		return nil
	}
	if err := p.errorInjector(event); err != nil {
		// Clear the error injector after it injects an error, to ensure we inject
		// the error only once per stream proxy instance.
		p.errorInjector = nil
		return err
	}
	return nil
}

func (p *BEPProxyServer) PublishLifecycleEvent(ctx context.Context, req *pepb.PublishLifecycleEventRequest) (*emptypb.Empty, error) {
	log.Info("Proxy: /PublishLifecycleEvent")

	return p.backend.PublishLifecycleEvent(ctx, req)
}

func (p *BEPProxyServer) PublishBuildToolEventStream(client pepb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	log.Info("Proxy: /PublishBuildToolEventStream")

	ctx := client.Context()

	server, err := p.backend.PublishBuildToolEventStream(ctx)
	require.NoError(p.t, err)

	serverClosed := false
	defer func() {
		if serverClosed {
			return
		}
		err := server.CloseSend()
		require.NoError(p.t, err)
	}()

	clientRecvs := make(chan *clientRecv)
	go func() {
		for {
			req, err := client.Recv()
			clientRecvs <- &clientRecv{req: req, err: err}
			if err != nil {
				close(clientRecvs)
				return
			}
		}
	}()

	serverRecvs := make(chan *serverRecv)
	go func() {
		for {
			res, err := server.Recv()
			serverRecvs <- &serverRecv{res: res, err: err}
			if err != nil {
				close(serverRecvs)
				return
			}
		}
	}()

	for clientRecvs != nil || serverRecvs != nil {
		select {
		case msg, ok := <-clientRecvs:
			if !ok {
				clientRecvs = nil
				continue
			}
			if msg.err == io.EOF {
				err := server.CloseSend()
				serverClosed = true
				require.NoError(p.t, err)
				continue
			}
			require.NoError(p.t, msg.err, "unexpected error from Bazel")
			log.Debugf("Proxy: bazel ==> BB: %v", msg)
			if err := p.maybeInjectError(&StreamEvent{ClientRecv: msg}); err != nil {
				return err
			}
			err := server.Send(msg.req)
			require.NoError(p.t, err, "unexpected error forwarding the build event to BB")
			if err := p.maybeInjectError(&StreamEvent{ClientRecv: msg, WasForwarded: true}); err != nil {
				return err
			}
		case msg, ok := <-serverRecvs:
			if !ok {
				serverRecvs = nil
				continue
			}
			if msg.err == io.EOF {
				return nil
			}
			if msg.err != nil {
				return msg.err
			}
			log.Debugf("Proxy: bazel <-- BB: %v", msg)
			if err := p.maybeInjectError(&StreamEvent{ServerRecv: msg}); err != nil {
				return err
			}
			err := client.Send(msg.res)
			if err := p.maybeInjectError(&StreamEvent{ServerRecv: msg, WasForwarded: true}); err != nil {
				return err
			}
			require.NoError(p.t, err, "unexpected error forwarding reply to bazel")
		}
	}

	require.FailNow(p.t, "expected either EOF or error from server")
	return nil
}

type clientRecv struct {
	req *pepb.PublishBuildToolEventStreamRequest
	err error
}

func (r *clientRecv) String() string {
	if r.err != nil {
		return fmt.Sprintf("err: %s", r.err)
	}
	obe := r.req.GetOrderedBuildEvent()
	bazelEvent, err := readBazelEvent(obe)
	if err != nil {
		return fmt.Sprintf("<UnmarshalAny(BazelEvent) err! %s>", err)
	}
	if bazelEvent != nil {
		idJSON, err := protojson.Marshal(bazelEvent.GetId())
		if err != nil {
			return fmt.Sprintf("<JSON marshal err! %s>", err)
		}
		return fmt.Sprintf("%d: BazelEvent %s", obe.SequenceNumber, idJSON)
	}

	if obe.GetEvent().GetComponentStreamFinished() != nil {
		return fmt.Sprintf("%d: ComponentStreamFinished", obe.SequenceNumber)
	}

	return fmt.Sprintf("%d: <not a BazelEvent>", obe.SequenceNumber)
}

func readBazelEvent(obe *pepb.OrderedBuildEvent) (*bespb.BuildEvent, error) {
	switch buildEvent := obe.Event.Event.(type) {
	case *bepb.BuildEvent_BazelEvent:
		out := &bespb.BuildEvent{}
		if err := buildEvent.BazelEvent.UnmarshalTo(out); err != nil {
			return nil, err
		}
		return out, nil
	}
	return nil, nil
}

type serverRecv struct {
	res *pepb.PublishBuildToolEventStreamResponse
	err error
}

func (r *serverRecv) String() string {
	if r.err != nil {
		return fmt.Sprintf("err: %s", r.err)
	}
	return fmt.Sprintf("ACK %d", r.res.SequenceNumber)
}

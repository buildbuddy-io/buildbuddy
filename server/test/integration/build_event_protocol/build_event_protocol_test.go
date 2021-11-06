package build_event_protocol_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
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

// Baseline test to make sure a build can succeed using a bepProxy that doesn't
// try to inject any errors.
func TestBuildWithRetry_Baseline(t *testing.T) {
	app := buildbuddy.Run(t)
	bepClient := app.PublishBuildEventClient(t)
	proxy := StartBEPProxy(t, bepClient)
	ctx := context.Background()
	ws := testbazel.MakeTempWorkspace(t, workspaceContents)
	buildFlags := append([]string{"//:hello.txt"}, app.BESBazelFlags()...)
	buildFlags = append(buildFlags, "--bes_backend="+proxy.GRPCAddress())

	result := testbazel.Invoke(ctx, t, ws, "build", buildFlags...)

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
}

func TestBuildWithRetry_InjectFailureAfterBuildFinished(t *testing.T) {
	app := buildbuddy.Run(t)
	bepClient := app.PublishBuildEventClient(t)
	proxy := StartBEPProxy(t, bepClient)
	proxy.FailOnce(AfterForwardBazelEvent(&bespb.BuildEvent_Finished{}))

	ctx := context.Background()
	ws := testbazel.MakeTempWorkspace(t, workspaceContents)
	buildFlags := append([]string{"//:hello.txt"}, app.BESBazelFlags()...)
	buildFlags = append(buildFlags, "--bes_backend="+proxy.GRPCAddress())

	result := testbazel.Invoke(ctx, t, ws, "build", buildFlags...)

	assert.NoError(t, result.Error)
	assert.Contains(t, result.Stderr, "Build completed successfully")
}

type StreamEvent struct {
	ServerRecv   *serverRecv
	ClientRecv   *clientRecv
	WasForwarded bool
}

type StreamErrorInjector func(*StreamEvent) error

func AfterForwardBazelEvent(payloadType interface{}) StreamErrorInjector {
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
		fmt.Println("FORWARDED BAZEL EVENT:", reflect.TypeOf(bazelEvent.GetPayload()))
		if reflect.TypeOf(bazelEvent.GetPayload()).AssignableTo(reflect.TypeOf(payloadType)) {
			return status.UnknownError("Proxy: Injected error for test")
		}
		return nil
	}
}

// BEPProxyServer sits in between Bazel and the build event server, proxying
// messages directly. It allows injecting errors into the stream to ensure that
// they are handled properly.
type BEPProxyServer struct {
	t                *testing.T
	backend          pepb.PublishBuildEventClient
	port             int
	failurePredicate StreamErrorInjector
}

func StartBEPProxy(t *testing.T, backend pepb.PublishBuildEventClient) *BEPProxyServer {
	proxy := &BEPProxyServer{
		t:       t,
		backend: backend,
	}

	lis, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err, "failed to start BES proxy")
	port := lis.Addr().(*net.TCPAddr).Port

	server := grpc.NewServer(
		// Set to avoid errors: Bandwidth exhausted HTTP/2 error code: ENHANCE_YOUR_CALM Received Goaway too_many_pings
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second, // If a client pings more than once every 10 seconds, terminate the connection
			PermitWithoutStream: true,             // Allow pings even when there are no active streams
		}),
	)
	pepb.RegisterPublishBuildEventServer(server, proxy)
	go func() {
		t.Logf("Proxy: Listening on 0.0.0.0:%d", port)
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

func (p *BEPProxyServer) FailOnce(f StreamErrorInjector) {
	p.failurePredicate = f
}

func (p *BEPProxyServer) maybeFail(event *StreamEvent) error {
	if p.failurePredicate == nil {
		return nil
	}
	if err := p.failurePredicate(event); err != nil {
		// Zero out the failure condition so we only return it once.
		p.failurePredicate = nil
		return err
	}
	return nil
}

func (p *BEPProxyServer) PublishLifecycleEvent(ctx context.Context, req *pepb.PublishLifecycleEventRequest) (*empty.Empty, error) {
	p.t.Log("Proxy: /PublishLifecycleEvent")

	return p.backend.PublishLifecycleEvent(ctx, req)
}

func (p *BEPProxyServer) PublishBuildToolEventStream(client pepb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	p.t.Log("Proxy: /PublishBuildToolEventStream")

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

	for {
		select {
		case msg, ok := <-clientRecvs:
			if !ok {
				clientRecvs = nil
			}
			if msg.err == io.EOF {
				err := server.CloseSend()
				serverClosed = true
				require.NoError(p.t, err)
				continue
			}
			require.NoError(p.t, msg.err, "unexpected error from Bazel")
			p.t.Logf("Proxy: bazel ==> BB: %v", msg)
			if err := p.maybeFail(&StreamEvent{ClientRecv: msg}); err != nil {
				return err
			}
			err := server.Send(msg.req)
			require.NoError(p.t, err, "unexpected error forwarding the build event to BB")
			if err := p.maybeFail(&StreamEvent{ClientRecv: msg, WasForwarded: true}); err != nil {
				return err
			}
		case msg, ok := <-serverRecvs:
			if !ok {
				serverRecvs = nil
			}
			if msg.err == io.EOF {
				return nil
			}
			if msg.err != nil {
				return msg.err
			}
			p.t.Logf("Proxy: bazel <-- BB: %v", msg)
			if err := p.maybeFail(&StreamEvent{ServerRecv: msg}); err != nil {
				return err
			}
			err := client.Send(msg.res)
			if err := p.maybeFail(&StreamEvent{ServerRecv: msg, WasForwarded: false}); err != nil {
				return err
			}
			require.NoError(p.t, err, "unexpected error forwarding reply to bazel")
		}
	}
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
		idJSON, err := (&jsonpb.Marshaler{}).MarshalToString(bazelEvent.GetId())
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
		if err := ptypes.UnmarshalAny(buildEvent.BazelEvent, out); err != nil {
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

package rpcutil_test

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rpcutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/test/bufconn"

	dto "github.com/prometheus/client_model/go"
	hlpb "google.golang.org/grpc/health/grpc_health_v1"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	hugeTimeout = 1_000_000 * time.Hour
)

type message[T proto.Message] struct {
	Val T
	Err error
}

type stream[T proto.Message] struct {
	ch          chan message[T]
	closeRecvCh chan message[T]
}

type blockingSendStream[T proto.Message] struct {
	ctx          context.Context
	sendStarted  chan struct{}
	sendReturned chan struct{}
}

func (s *blockingSendStream[T]) Send(T) error {
	close(s.sendStarted)
	<-s.ctx.Done()
	close(s.sendReturned)
	return s.ctx.Err()
}

func (s *blockingSendStream[T]) CloseAndRecv() (T, error) {
	var zero T
	return zero, nil
}

func (s *stream[T]) Recv() (T, error) {
	var zero T
	msg, ok := <-s.ch
	if !ok {
		return zero, io.EOF
	}
	return msg.Val, msg.Err
}

func (s *stream[T]) Send(msg T) error {
	s.ch <- message[T]{Val: msg}
	return nil
}

func (s *stream[T]) CloseAndRecv() (T, error) {
	if s.closeRecvCh != nil {
		msg := <-s.closeRecvCh
		return msg.Val, msg.Err
	}
	var zero T
	return zero, nil
}

// orderingStream lets a test control/observe the ordering of calls on a stream.
type orderingStream[T proto.Message] struct {
	sendBlock        chan struct{}
	sendReturned     chan struct{}
	closeRecvStarted chan struct{}
	closeRecvReturn  chan struct{}
}

func (s *orderingStream[T]) Send(T) error {
	<-s.sendBlock
	close(s.sendReturned)
	return nil
}

func (s *orderingStream[T]) CloseAndRecv() (T, error) {
	close(s.closeRecvStarted)
	<-s.closeRecvReturn
	var zero T
	return zero, nil
}

func TestReceiver(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ch := make(chan message[*tspb.Timestamp])
	stream := &stream[*tspb.Timestamp]{ch: ch}
	receiver := rpcutil.NewReceiver(ctx, stream)
	val := tspb.Now()
	cause := fmt.Errorf("test-cause")

	// Should recv message successfully
	go func() { ch <- message[*tspb.Timestamp]{Val: val} }()
	msg, err := receiver.RecvWithTimeoutCause(hugeTimeout, cause)
	require.Equal(t, val, msg)
	require.NoError(t, err)

	// Should return cause when timed out
	msg, err = receiver.RecvWithTimeoutCause(0, cause)
	require.Nil(t, nil, msg)
	require.Equal(t, cause, err)

	// unblock the `stream.Recv` method
	ch <- message[*tspb.Timestamp]{Val: val}
}

func TestSender(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx := t.Context()
	ch := make(chan message[*tspb.Timestamp])
	stream := &stream[*tspb.Timestamp]{ch: ch}
	sender := rpcutil.NewSender(ctx, stream)
	val := tspb.Now()
	cause := fmt.Errorf("test-cause")

	// Should return cause when timed out
	err := sender.SendWithTimeoutCause(val, 0, cause)
	require.Equal(t, cause, err)
	<-ch
	sender.CloseAndRecvWithTimeoutCause(hugeTimeout, cause)
}

func TestSender_AllowsMultipleSuccessfulSends(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx := t.Context()
	ch := make(chan message[*tspb.Timestamp], 2)
	stream := &stream[*tspb.Timestamp]{ch: ch}
	sender := rpcutil.NewSender(ctx, stream)
	cause := fmt.Errorf("test-cause")
	val1 := tspb.Now()
	val2 := tspb.New(val1.AsTime().Add(time.Second))

	require.NoError(t, sender.SendWithTimeoutCause(val1, hugeTimeout, cause))
	require.NoError(t, sender.SendWithTimeoutCause(val2, hugeTimeout, cause))

	require.Equal(t, val1, (<-ch).Val)
	require.Equal(t, val2, (<-ch).Val)
	sender.CloseAndRecvWithTimeoutCause(hugeTimeout, cause)
}

func TestCloseAndRecv(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx := t.Context()
	cause := fmt.Errorf("test-cause")
	val := tspb.Now()

	// Should return response successfully
	closeRecvCh := make(chan message[*tspb.Timestamp], 1)
	s := &stream[*tspb.Timestamp]{ch: make(chan message[*tspb.Timestamp]), closeRecvCh: closeRecvCh}
	sender := rpcutil.NewSender(ctx, s)
	closeRecvCh <- message[*tspb.Timestamp]{Val: val}
	msg, err := sender.CloseAndRecvWithTimeoutCause(hugeTimeout, cause)
	require.NoError(t, err)
	require.Equal(t, val, msg)

	// Should return cause when timed out
	closeRecvChTimeout := make(chan message[*tspb.Timestamp])
	s = &stream[*tspb.Timestamp]{ch: make(chan message[*tspb.Timestamp]), closeRecvCh: closeRecvChTimeout}
	sender = rpcutil.NewSender(ctx, s)
	msg, err = sender.CloseAndRecvWithTimeoutCause(0, cause)
	require.Nil(t, msg)
	require.Equal(t, cause, err)

	// Unblock CloseAndRecv goroutine to avoid leaking it in the timeout case.
	close(closeRecvChTimeout)
}

func TestSender_CloseAndRecvDoesNotLeakSenderGoroutine(t *testing.T) {
	// Use a background context that is never cancelled, so the only way
	// the sender goroutine can exit is via sendChan being closed.
	defer goleak.VerifyNone(t)

	ch := make(chan message[*tspb.Timestamp], 1)
	closeRecvCh := make(chan message[*tspb.Timestamp], 1)
	s := &stream[*tspb.Timestamp]{ch: ch, closeRecvCh: closeRecvCh}
	sender := rpcutil.NewSender(t.Context(), s)

	require.NoError(t, sender.SendWithTimeoutCause(tspb.Now(), hugeTimeout, fmt.Errorf("cause")))
	<-ch
	closeRecvCh <- message[*tspb.Timestamp]{Val: tspb.Now()}
	_, err := sender.CloseAndRecvWithTimeoutCause(hugeTimeout, fmt.Errorf("cause"))
	require.NoError(t, err)
}

func TestSender_CloseAndRecvWithoutSendsDoesNotLeak(t *testing.T) {
	defer goleak.VerifyNone(t)

	closeRecvCh := make(chan message[*tspb.Timestamp], 1)
	s := &stream[*tspb.Timestamp]{ch: make(chan message[*tspb.Timestamp]), closeRecvCh: closeRecvCh}
	sender := rpcutil.NewSender(t.Context(), s)

	closeRecvCh <- message[*tspb.Timestamp]{Val: tspb.Now()}
	_, err := sender.CloseAndRecvWithTimeoutCause(hugeTimeout, fmt.Errorf("cause"))
	require.NoError(t, err)
}

func TestSender_SendTimeoutDoesNotLeakAfterCancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(t.Context())
	stream := &blockingSendStream[*tspb.Timestamp]{
		ctx:          ctx,
		sendStarted:  make(chan struct{}),
		sendReturned: make(chan struct{}),
	}
	sender := rpcutil.NewSender(ctx, stream)

	err := sender.SendWithTimeoutCause(tspb.Now(), time.Millisecond, fmt.Errorf("test-cause"))
	require.Error(t, err)
	select {
	case <-stream.sendStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sender goroutine to start Send")
	}

	cancel()
	select {
	case <-stream.sendReturned:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocked send to return")
	}
}

// After a SendWithTimeoutCause times out, CloseAndRecvWithTimeoutCause must
// not call stream.CloseAndRecv until the background sender goroutine has
// returned from its in-flight stream.Send — gRPC client streams don't
// support concurrent Send and CloseSend/CloseAndRecv on the same stream.
func TestSender_CloseAndRecvWaitsForInFlightSend(t *testing.T) {
	defer goleak.VerifyNone(t)

	stream := &orderingStream[*tspb.Timestamp]{
		sendBlock:        make(chan struct{}),
		sendReturned:     make(chan struct{}),
		closeRecvStarted: make(chan struct{}),
		closeRecvReturn:  make(chan struct{}),
	}
	sender := rpcutil.NewSender(t.Context(), stream)

	// Make a Send that times out, blocking the sender goroutine in stream.Send.
	err := sender.SendWithTimeoutCause(tspb.Now(), time.Millisecond, fmt.Errorf("send-cause"))
	require.Error(t, err)

	// Start CloseAndRecv in a goroutine; it should wait for the in-flight
	// Send to return before calling stream.CloseAndRecv.
	closeDone := make(chan error, 1)
	go func() {
		_, err := sender.CloseAndRecvWithTimeoutCause(hugeTimeout, fmt.Errorf("close-cause"))
		closeDone <- err
	}()

	// Ensure stream.CloseAndRecv isn't called yet, Send is still blocked.
	select {
	case <-stream.closeRecvStarted:
		t.Fatal("stream.CloseAndRecv was called before the in-flight Send returned")
	case <-time.After(50 * time.Millisecond):
	}

	// Unblock Send. The sender goroutine exits, done closes, and
	// CloseAndRecvWithTimeoutCause now proceeds to call stream.CloseAndRecv.
	close(stream.sendBlock)
	select {
	case <-stream.sendReturned:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Send to return after unblock")
	}
	select {
	case <-stream.closeRecvStarted:
	case <-time.After(time.Second):
		t.Fatal("stream.CloseAndRecv was not called after Send returned")
	}

	// Let stream.CloseAndRecv return so the test cleans up.
	close(stream.closeRecvReturn)
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("CloseAndRecvWithTimeoutCause did not return")
	}
}

// If the background sender goroutine is still stuck in stream.Send (because
// nobody has canceled the underlying ctx), CloseAndRecvWithTimeoutCause must
// still honor its own timeout and return the given cause without blocking
// forever waiting for the sender goroutine to drain.
func TestSender_CloseAndRecvHonorsTimeoutWhileSendStuck(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithCancel(t.Context())
	stream := &blockingSendStream[*tspb.Timestamp]{
		ctx:          ctx,
		sendStarted:  make(chan struct{}),
		sendReturned: make(chan struct{}),
	}
	sender := rpcutil.NewSender(ctx, stream)

	err := sender.SendWithTimeoutCause(tspb.Now(), time.Millisecond, fmt.Errorf("send-cause"))
	require.Error(t, err)
	<-stream.sendStarted

	closeCause := fmt.Errorf("close-cause")
	_, err = sender.CloseAndRecvWithTimeoutCause(time.Millisecond, closeCause)
	require.Equal(t, closeCause, err)

	// Cancel to let the stuck Send unblock so the test doesn't leak the
	// background goroutine.
	cancel()
	<-stream.sendReturned
}

// TestMeterProviderGRPCViews runs a gRPC call through otelgrpc's client and
// server stats handlers wired to the shared MeterProvider and asserts on the
// exported series. The Views in MeterProvider match otelgrpc's instruments by
// name, so an otelgrpc upgrade that renames instruments or records new
// attributes silently disables the coarse buckets and the attribute
// allowlist. This test fails in that case.
func TestMeterProviderGRPCViews(t *testing.T) {
	t.Skip("Skip until upgrading gRPC")
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithMeterProvider(rpcutil.MeterProvider()))),
	)
	hlpb.RegisterHealthServer(srv, health.NewServer())
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithMeterProvider(rpcutil.MeterProvider()))),
	)
	require.NoError(t, err)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = hlpb.NewHealthClient(conn).Check(ctx, &hlpb.HealthCheckRequest{})
	require.NoError(t, err)

	// Depending on the exporter version, in-process names keep semconv dots
	// ("rpc.client.call.duration_seconds"); scrapers see the escaped form
	// ("rpc_client_call_duration_seconds"). Normalize before comparing.
	normalize := func(name string) string { return strings.ReplaceAll(name, ".", "_") }
	gather := func() map[string]*dto.MetricFamily {
		metricFamilies, err := prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		families := map[string]*dto.MetricFamily{}
		for _, f := range metricFamilies {
			families[normalize(f.GetName())] = f
		}
		return families
	}
	// The server emits its stats.End event after the client call returns, so
	// the server family may lag the Gather by a moment.
	require.Eventually(t, func() bool {
		families := gather()
		return families["rpc_client_call_duration_seconds"] != nil &&
			families["rpc_server_call_duration_seconds"] != nil
	}, 5*time.Second, 10*time.Millisecond,
		"rpc.{client,server}.call.duration were not exported; if otelgrpc renamed its instruments, update the Views in rpcutil.MeterProvider to match")
	families := gather()

	// The Views only cover the call-duration instruments. Any other RPC
	// family here (e.g. the retired *.size / *_per_rpc instruments coming
	// back in an otelgrpc upgrade) would export unfiltered default-bucket
	// histograms and needs a View before it ships.
	var rpcFamilies []string
	for name := range families {
		if strings.HasPrefix(name, "rpc_") {
			rpcFamilies = append(rpcFamilies, name)
		}
	}
	require.ElementsMatch(t,
		[]string{"rpc_client_call_duration_seconds", "rpc_server_call_duration_seconds"},
		rpcFamilies,
		"unexpected RPC metric families; add Views in rpcutil.MeterProvider for new otelgrpc instruments")

	coarseBoundaries := []float64{0.005, 0.025, 0.1, 0.5, 1, 5, 10, 30}
	for _, tc := range []struct {
		name string
		// nil means the metric keeps otelgrpc's default (finer-grained)
		// buckets.
		wantBoundaries []float64
	}{
		{name: "rpc_client_call_duration_seconds", wantBoundaries: coarseBoundaries},
		{name: "rpc_server_call_duration_seconds", wantBoundaries: nil},
	} {
		for _, m := range families[tc.name].GetMetric() {
			labelNames := map[string]bool{}
			for _, lp := range m.GetLabel() {
				labelNames[normalize(lp.GetName())] = true
			}
			for _, banned := range []string{"server_address", "server_port"} {
				require.False(t, labelNames[banned],
					"per-peer attribute %s on %s must be filtered out by the View's AttributeFilter", banned, tc.name)
			}
			for _, want := range []string{"rpc_method", "rpc_response_status_code", "rpc_system_name"} {
				require.True(t, labelNames[want],
					"expected %s label on %s; if otelgrpc renamed its attributes, update the allowlist in rpcutil.MeterProvider", want, tc.name)
			}
			var boundaries []float64
			for _, b := range m.GetHistogram().GetBucket() {
				if !math.IsInf(b.GetUpperBound(), 1) {
					boundaries = append(boundaries, b.GetUpperBound())
				}
			}
			if tc.wantBoundaries != nil {
				require.Equal(t, tc.wantBoundaries, boundaries,
					"unexpected %s buckets; the View in rpcutil.MeterProvider did not match the otelgrpc instrument", tc.name)
			} else {
				require.Greater(t, len(boundaries), len(coarseBoundaries),
					"expected %s to keep otelgrpc's default buckets", tc.name)
			}
		}
	}
}

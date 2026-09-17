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
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
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

// blockingSendStream blocks in Send and CloseAndRecv until ctx is done, the
// way a gRPC client stream does when the peer stops reading.
type blockingSendStream[T proto.Message] struct {
	ctx          context.Context
	sendStarted  chan struct{}
	sendReturned chan struct{}
	sendCalls    int
}

func (s *blockingSendStream[T]) Send(T) error {
	s.sendCalls++
	close(s.sendStarted)
	<-s.ctx.Done()
	close(s.sendReturned)
	return s.ctx.Err()
}

func (s *blockingSendStream[T]) CloseAndRecv() (zero T, err error) {
	<-s.ctx.Done()
	return zero, s.ctx.Err()
}

// lateSuccessStream blocks in Send until ctx is done and then reports
// success, modeling a send that completes just as the Sender's timeout fires.
type lateSuccessStream[T proto.Message] struct {
	ctx context.Context
}

func (s *lateSuccessStream[T]) Send(T) error {
	<-s.ctx.Done()
	return nil
}

func (s *lateSuccessStream[T]) CloseAndRecv() (zero T, err error) {
	return zero, nil
}

// eofSendStream fails every Send with io.EOF, the way a gRPC client stream
// does once the stream is over, and reports the stream's status from
// CloseAndRecv.
type eofSendStream[T proto.Message] struct {
	closeRecvVal T
	closeRecvErr error
	sendCalls    int
}

func (s *eofSendStream[T]) Send(T) error {
	s.sendCalls++
	return io.EOF
}

func (s *eofSendStream[T]) CloseAndRecv() (T, error) {
	return s.closeRecvVal, s.closeRecvErr
}

// closeSendStream mimics a gRPC client stream after CloseAndRecv: further
// sends fail with an Internal error without reaching the network.
type closeSendStream[T proto.Message] struct {
	closed    bool
	sendCalls int
}

func (s *closeSendStream[T]) Send(T) error {
	s.sendCalls++
	if s.closed {
		return status.InternalError("SendMsg called after CloseSend")
	}
	return nil
}

func (s *closeSendStream[T]) CloseAndRecv() (T, error) {
	s.closed = true
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

func TestSender_SendTimeoutCancelsStream(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream := &blockingSendStream[*tspb.Timestamp]{
		ctx:          ctx,
		sendStarted:  make(chan struct{}),
		sendReturned: make(chan struct{}),
	}
	sender := rpcutil.NewSender(cancel, stream)

	// The timeout cancels the stream context, which is what unblocks the
	// synchronous Send.
	err := sender.SendWithTimeout(tspb.Now(), time.Millisecond)
	require.True(t, status.IsDeadlineExceededError(err), "expected DeadlineExceeded, got %v", err)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	<-stream.sendReturned

	// The stream is dead, so later calls report the same timeout without
	// touching it.
	require.Equal(t, err, sender.SendWithTimeout(tspb.Now(), hugeTimeout))
	_, closeErr := sender.CloseAndRecvWithTimeout(hugeTimeout)
	require.Equal(t, err, closeErr)
	require.Equal(t, 1, stream.sendCalls)
}

func TestSender_ZeroTimeoutReturnsDeadlineExceeded(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream := &blockingSendStream[*tspb.Timestamp]{
		ctx:          ctx,
		sendStarted:  make(chan struct{}),
		sendReturned: make(chan struct{}),
	}
	sender := rpcutil.NewSender(cancel, stream)

	err := sender.SendWithTimeout(tspb.Now(), 0)
	require.True(t, status.IsDeadlineExceededError(err), "expected DeadlineExceeded, got %v", err)
}

// If the timeout fires while Send is completing successfully, the stream has
// still been canceled, so the Sender must report the timeout rather than a
// success it can't follow up on.
func TestSender_TimeoutRacingWithSuccessReportsTimeout(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream := &lateSuccessStream[*tspb.Timestamp]{ctx: ctx}
	sender := rpcutil.NewSender(cancel, stream)

	err := sender.SendWithTimeout(tspb.Now(), time.Millisecond)
	require.True(t, status.IsDeadlineExceededError(err), "expected DeadlineExceeded, got %v", err)
	_, closeErr := sender.CloseAndRecvWithTimeout(hugeTimeout)
	require.Equal(t, err, closeErr)
}

func TestSender_AllowsMultipleSuccessfulSends(t *testing.T) {
	defer goleak.VerifyNone(t)
	_, cancel := context.WithCancel(t.Context())
	defer cancel()
	ch := make(chan message[*tspb.Timestamp], 2)
	closeRecvCh := make(chan message[*tspb.Timestamp], 1)
	stream := &stream[*tspb.Timestamp]{ch: ch, closeRecvCh: closeRecvCh}
	sender := rpcutil.NewSender(cancel, stream)
	val1 := tspb.Now()
	val2 := tspb.New(val1.AsTime().Add(time.Second))
	rspVal := tspb.New(val1.AsTime().Add(2 * time.Second))

	require.NoError(t, sender.SendWithTimeout(val1, hugeTimeout))
	require.NoError(t, sender.SendWithTimeout(val2, hugeTimeout))
	require.Equal(t, val1, (<-ch).Val)
	require.Equal(t, val2, (<-ch).Val)

	closeRecvCh <- message[*tspb.Timestamp]{Val: rspVal}
	rsp, err := sender.CloseAndRecvWithTimeout(hugeTimeout)
	require.NoError(t, err)
	require.Equal(t, rspVal, rsp)
}

func TestCloseAndRecv(t *testing.T) {
	defer goleak.VerifyNone(t)
	val := tspb.Now()

	// Should return response successfully
	_, cancel := context.WithCancel(t.Context())
	defer cancel()
	closeRecvCh := make(chan message[*tspb.Timestamp], 1)
	s := &stream[*tspb.Timestamp]{ch: make(chan message[*tspb.Timestamp]), closeRecvCh: closeRecvCh}
	sender := rpcutil.NewSender(cancel, s)
	closeRecvCh <- message[*tspb.Timestamp]{Val: val}
	msg, err := sender.CloseAndRecvWithTimeout(hugeTimeout)
	require.NoError(t, err)
	require.Equal(t, val, msg)

	// Should cancel the stream and return DeadlineExceeded when timed out
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	blocking := &blockingSendStream[*tspb.Timestamp]{
		ctx:          ctx,
		sendStarted:  make(chan struct{}),
		sendReturned: make(chan struct{}),
	}
	sender = rpcutil.NewSender(cancel, blocking)
	msg, err = sender.CloseAndRecvWithTimeout(time.Millisecond)
	require.Nil(t, msg)
	require.True(t, status.IsDeadlineExceededError(err), "expected DeadlineExceeded, got %v", err)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

// gRPC reports a transport error from Send as io.EOF and rejects further
// sends itself, so the Sender passes them through unchanged rather than
// refusing them: a repeat send gets the stream's answer again, and the
// stream's status is still available from CloseAndRecv.
func TestSender_SendErrorsArePassedThrough(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	streamErr := fmt.Errorf("transport is closing")
	s := &eofSendStream[*tspb.Timestamp]{closeRecvErr: streamErr}
	sender := rpcutil.NewSender(cancel, s)

	require.Equal(t, io.EOF, sender.SendWithTimeout(tspb.Now(), hugeTimeout))
	require.Equal(t, io.EOF, sender.SendWithTimeout(tspb.Now(), hugeTimeout))
	require.Equal(t, 2, s.sendCalls)
	// A failed send does not cancel the stream.
	require.NoError(t, ctx.Err())

	_, err := sender.CloseAndRecvWithTimeout(hugeTimeout)
	require.Equal(t, streamErr, err)
}

// A send after CloseAndRecv is likewise left to the stream to reject.
func TestSender_SendAfterCloseAndRecvIsPassedThrough(t *testing.T) {
	defer goleak.VerifyNone(t)
	_, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := &closeSendStream[*tspb.Timestamp]{}
	sender := rpcutil.NewSender(cancel, s)

	require.NoError(t, sender.SendWithTimeout(tspb.Now(), hugeTimeout))
	_, err := sender.CloseAndRecvWithTimeout(hugeTimeout)
	require.NoError(t, err)

	err = sender.SendWithTimeout(tspb.Now(), hugeTimeout)
	require.True(t, status.IsInternalError(err), "expected Internal, got %v", err)
	require.Equal(t, 2, s.sendCalls)
}

// When the stream context is canceled by someone other than the Sender (for
// example the caller's deadline), the stream's own error is returned rather
// than a timeout.
func TestSender_ExternalCancelReturnsStreamError(t *testing.T) {
	defer goleak.VerifyNone(t)
	ctx, cancel := context.WithCancel(t.Context())
	stream := &blockingSendStream[*tspb.Timestamp]{
		ctx:          ctx,
		sendStarted:  make(chan struct{}),
		sendReturned: make(chan struct{}),
	}
	sender := rpcutil.NewSender(cancel, stream)
	go func() {
		<-stream.sendStarted
		cancel()
	}()

	err := sender.SendWithTimeout(tspb.Now(), hugeTimeout)
	require.ErrorIs(t, err, context.Canceled)
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

func TestTracingMessageEvents(t *testing.T) {
	for _, clientSide := range []bool{false, true} {
		for _, streaming := range []bool{false, true} {
			for _, tc := range []struct {
				name, header       string
				ignore, wantEvents bool
			}{
				{name: "absent"}, {name: "empty", header: ""}, {name: "other", header: "true"},
				{name: "force", header: "force", wantEvents: true},
				{name: "ignored", header: "force", ignore: true},
			} {
				t.Run(fmt.Sprintf("client=%t/streaming=%t/%s", clientSide, streaming, tc.name), func(t *testing.T) {
					flags.Set(t, "app.ignore_forced_tracing_header", tc.ignore)
					recorder := tracetest.NewSpanRecorder()
					tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(recorder))
					defer tp.Shutdown(context.Background())
					lis := bufconn.Listen(1 << 20)
					defer lis.Close()
					var serverOpts []grpc.ServerOption
					var clientOpts []grpc.DialOption
					if clientSide {
						clientOpts = append(clientOpts, grpc.WithStatsHandler(rpcutil.WithTracingClientMessageEvents(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(tp)))))
					} else {
						serverOpts = append(serverOpts, grpc.StatsHandler(rpcutil.WithTracingMessageEvents(otelgrpc.NewServerHandler(otelgrpc.WithTracerProvider(tp)))))
					}
					srv := grpc.NewServer(serverOpts...)
					hlpb.RegisterHealthServer(srv, health.NewServer())
					go srv.Serve(lis)
					defer srv.Stop()
					clientOpts = append(clientOpts,
						grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
						grpc.WithTransportCredentials(insecure.NewCredentials()),
					)
					conn, err := grpc.NewClient("passthrough:///bufnet", clientOpts...)
					require.NoError(t, err)
					defer conn.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if tc.name != "absent" {
						ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-trace", tc.header)
					}
					client := hlpb.NewHealthClient(conn)
					if streaming {
						stream, err := client.Watch(ctx, &hlpb.HealthCheckRequest{})
						require.NoError(t, err)
						response, err := stream.Recv()
						require.NoError(t, err)
						require.Equal(t, hlpb.HealthCheckResponse_SERVING, response.GetStatus())
						cancel()
					} else {
						response, err := client.Check(ctx, &hlpb.HealthCheckRequest{})
						require.NoError(t, err)
						require.Equal(t, hlpb.HealthCheckResponse_SERVING, response.GetStatus())
					}
					require.Eventually(t, func() bool { return len(recorder.Ended()) == 1 }, 5*time.Second, time.Millisecond)
					events := recorder.Ended()[0].Events()
					if !tc.wantEvents {
						require.Empty(t, events)
						return
					}
					if clientSide {
						names := map[string]bool{}
						for _, e := range events {
							names[e.Name] = true
						}
						for _, name := range []string{"grpc.begin", "grpc.out_header", "grpc.in_header", "grpc.end"} {
							require.True(t, names[name], name)
						}
						if !streaming {
							require.True(t, names["grpc.in_trailer"])
						}
						payloads := events[:0]
						for _, e := range events {
							if e.Name == "grpc.in_payload" || e.Name == "grpc.out_payload" {
								payloads = append(payloads, e)
							}
						}
						events = payloads
						// The client sends the request and receives the response.
						require.Len(t, events, 2)
						require.Equal(t, "grpc.out_payload", events[0].Name)
						require.Equal(t, "grpc.in_payload", events[1].Name)
					} else {
						require.Len(t, events, 2)
						require.Equal(t, "grpc.in_payload", events[0].Name)
						require.Equal(t, "grpc.out_payload", events[1].Name)
					}
					for i, event := range events {
						attrs := map[string]int64{}
						for _, a := range event.Attributes {
							attrs[string(a.Key)] = a.Value.AsInt64()
						}
						require.Equal(t, int64(i*2), attrs["bytes"])
						require.Equal(t, int64(i*2+5), attrs["wire_bytes"])
					}
				})
			}
		}
	}
}

func TestTracingMessageEventLimit(t *testing.T) {
	flags.Set(t, "app.ignore_forced_tracing_header", false)
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(context.Background())
	handler := rpcutil.WithTracingMessageEvents(otelgrpc.NewServerHandler(otelgrpc.WithTracerProvider(tp)))
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-buildbuddy-trace", "force"))
	ctx = handler.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/google.bytestream.ByteStream/Read"})
	now := time.Now()
	handler.HandleRPC(ctx, &stats.Begin{BeginTime: now})
	handler.HandleRPC(ctx, &stats.InPayload{Length: 10, WireLength: 15})
	for range 200 {
		handler.HandleRPC(ctx, &stats.OutPayload{Length: 100, WireLength: 105})
	}
	handler.HandleRPC(ctx, &stats.End{BeginTime: now, EndTime: time.Now()})
	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Zero(t, spans[0].DroppedEvents())
	events := spans[0].Events()
	require.Len(t, events, 65)
	require.Equal(t, "grpc.in_payload", events[0].Name)
	require.Equal(t, "grpc.message_summary", events[64].Name)
	attrs := map[string]int64{}
	for _, a := range spans[0].Attributes() {
		if strings.HasPrefix(string(a.Key), "grpc.") {
			attrs[string(a.Key)] = a.Value.AsInt64()
		}
	}
	require.Equal(t, map[string]int64{
		"grpc.messages_received": 1, "grpc.messages_sent": 200,
		"grpc.bytes_received": 10, "grpc.bytes_sent": 20000,
		"grpc.wire_bytes_received": 15, "grpc.wire_bytes_sent": 21000,
		"grpc.message_events_omitted": 137,
	}, attrs)
}

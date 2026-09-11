package rpcutil

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/vtprotocodec"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/stats"
	grpcstatus "google.golang.org/grpc/status"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const GRPCMaxSizeBytes = int64(4 * 1000 * 1000)

var OTELGRPCMessageEventsEnabled = flag.Bool("grpc_otel_message_events_enabled", true,
	"Record up to 64 payload trace events per RPC and client lifecycle events when incoming or (for clients) outgoing x-buildbuddy-trace is force and app.ignore_forced_tracing_header is false, with totals and an omitted-event summary for longer streams.")

const maxMessageEvents = 64

// WithTracingMessageEvents adds bounded message diagnostics to force-traced RPCs.
func WithTracingMessageEvents(handler stats.Handler) stats.Handler {
	return &tracingMessageHandler{Handler: handler}
}

// WithTracingClientMessageEvents adds bounded message diagnostics and lifecycle events to force-traced client RPCs.
func WithTracingClientMessageEvents(handler stats.Handler) stats.Handler {
	return &tracingMessageHandler{Handler: handler, client: true}
}

type tracingMessageHandler struct {
	stats.Handler
	client bool
}
type messageTraceKey struct{}
type messageTrace struct {
	client                           bool
	span                             trace.Span
	mu                               sync.Mutex
	received, sent                   int64
	receivedBytes, sentBytes         int64
	receivedWireBytes, sentWireBytes int64
	recorded                         int64
}

func (h *tracingMessageHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	ctx = h.Handler.TagRPC(ctx, info)
	if tracing.IsForcedTrace(ctx) || (h.client && tracing.IsOutgoingForcedTrace(ctx)) {
		if span := trace.SpanFromContext(ctx); span.IsRecording() {
			ctx = context.WithValue(ctx, messageTraceKey{}, &messageTrace{span: span, client: h.client})
		}
	}
	return ctx
}

func (h *tracingMessageHandler) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if state, ok := ctx.Value(messageTraceKey{}).(*messageTrace); ok {
		state.record(event)
	}
	h.Handler.HandleRPC(ctx, event)
}

func (m *messageTrace) record(event stats.RPCStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.client {
		var name string
		var attrs []attribute.KeyValue
		switch e := event.(type) {
		case *stats.Begin:
			name = "grpc.begin"
			attrs = append(attrs, attribute.Bool("transparent_retry", e.IsTransparentRetryAttempt))
		case *stats.DelayedPickComplete:
			name = "grpc.connection_selected"
		case *stats.OutHeader:
			name = "grpc.out_header"
			if e.LocalAddr != nil {
				attrs = append(attrs, attribute.String("local_addr", e.LocalAddr.String()))
			}
			if e.RemoteAddr != nil {
				attrs = append(attrs, attribute.String("remote_addr", e.RemoteAddr.String()))
			}
		case *stats.InHeader:
			name = "grpc.in_header"
		case *stats.InTrailer:
			name = "grpc.in_trailer"
		case *stats.OutTrailer:
			name = "grpc.out_trailer"
		case *stats.End:
			name = "grpc.end"
			attrs = append(attrs, attribute.String("grpc_code", grpcstatus.Code(e.Error).String()))
		}
		if name != "" {
			m.span.AddEvent(name, trace.WithAttributes(attrs...))
		}
	}
	var name string
	var size, wireSize int
	switch e := event.(type) {
	case *stats.InPayload:
		name, size, wireSize = "grpc.in_payload", e.Length, e.WireLength
		m.received++
		m.receivedBytes += int64(size)
		m.receivedWireBytes += int64(wireSize)
	case *stats.OutPayload:
		// Handoff to gRPC does not prove delivery to the peer.
		name, size, wireSize = "grpc.out_payload", e.Length, e.WireLength
		m.sent++
		m.sentBytes += int64(size)
		m.sentWireBytes += int64(wireSize)
	case *stats.End:
		omitted := m.received + m.sent - m.recorded
		attrs := []attribute.KeyValue{
			attribute.Int64("grpc.messages_received", m.received),
			attribute.Int64("grpc.messages_sent", m.sent),
			attribute.Int64("grpc.bytes_received", m.receivedBytes),
			attribute.Int64("grpc.bytes_sent", m.sentBytes),
			attribute.Int64("grpc.wire_bytes_received", m.receivedWireBytes),
			attribute.Int64("grpc.wire_bytes_sent", m.sentWireBytes),
			attribute.Int64("grpc.message_events_omitted", omitted),
		}
		m.span.SetAttributes(attrs...)
		if omitted > 0 {
			m.span.AddEvent("grpc.message_summary", trace.WithAttributes(attrs...))
		}
		return
	default:
		return
	}
	if m.recorded >= maxMessageEvents {
		return
	}
	m.recorded++
	m.span.AddEvent(name, trace.WithAttributes(attribute.Int("bytes", size), attribute.Int("wire_bytes", wireSize)))
}

func init() {
	vtprotocodec.Register()

	// Change the default buffer pool. This is like the normal default, but with
	// more tiers. This improves bytestream reads and writes by 10-20% in
	// benchmarks, while allocating 10% less.
	//
	// With the previous tiers, payloads of 32KiB+1byte would fall in the 1MiB
	// tier, so they would allocate a lot more memory, plus every time we would
	// get a 1MiB from the pool, we would clear the whole thing, even though we
	// only use a part of it.
	experimental.SetDefaultBufferPool(mem.NewTieredBufferPool(
		256,
		4<<10,   // 4KiB
		16<<10,  // 16KiB (max HTTP/2 frame size used by gRPC)
		32<<10,  // 32KiB (default buffer size for io.Copy)
		64<<10,  // 64KiB
		128<<10, // 128KiB
		256<<10, // 256KiB
		512<<10, // 512KiB
		1<<20,   // 1MiB
	))
}

type StreamMsg[T proto.Message] struct {
	Data  T
	Error error
}

type RecvStream[T proto.Message] interface {
	Recv() (T, error)
}

type Receiver[T proto.Message] struct {
	ctx      context.Context
	recvChan chan StreamMsg[T]
}

// RecvWithTimeoutCause waits for a message on the underlying stream, waiting
// a maximum of timeout. If timeout is reached, the given cause is returned
// as the error.
func (r *Receiver[T]) RecvWithTimeoutCause(timeout time.Duration, cause error) (T, error) {
	ctx, cancel := context.WithTimeoutCause(r.ctx, timeout, cause)
	defer cancel()
	select {
	case msg := <-r.recvChan:
		return msg.Data, msg.Error
	case <-ctx.Done():
		return *new(T), context.Cause(ctx)
	}
}

// NewReceiver returns a stream handle that can be used to implement more
// advanced stream handling, such as per-receive timeouts.
//
// Example usage:
//
// receiver := rpcutil.NewReceiver[*bspb.ReadResponse](ctx, stream)
//
//	for {
//		rsp, err := receiver.RecvWithTimeoutCause(5 * time.Second, status.DeadlineExceededError("blah blah blah"))
//		// handle rsp and err
//	}
func NewReceiver[T proto.Message](ctx context.Context, stream RecvStream[T]) Receiver[T] {
	streamMsgs := make(chan StreamMsg[T])
	go func() {
		for {
			rsp, err := stream.Recv()
			select {
			case streamMsgs <- StreamMsg[T]{rsp, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return Receiver[T]{ctx, streamMsgs}
}

type SendStream[S proto.Message, R proto.Message] interface {
	Send(S) error
	CloseAndRecv() (R, error)
}

type Sender[S proto.Message, R proto.Message] struct {
	ctx      context.Context
	stream   SendStream[S, R]
	sendChan chan S
	errChan  chan error

	// done channel that's closed when the sending goroutine exists, to ensure
	// that there are no in-flight stream.Send calls when calling CloseAndRecv.
	done chan struct{}
}

// SendWithTimeoutCause attempts to send a message on the underlying stream,
// waiting a maximum of timeout. If timeout is reached, the given cause is
// returned as the error. If this function returns an error, it must not be
// called again on the same Sender.
//
// Note that gRPC sends are asynchronous in the sense that the protocol does not
// acknowledge individual messages. A timeout will only occur if the sender
// exhausts the flow-control window and the receiver does not increase it.
//
// Must not be called after CloseAndRecvWithTimeoutCause.
func (s *Sender[S, R]) SendWithTimeoutCause(msg S, timeout time.Duration, cause error) error {
	if s.sendChan == nil {
		return status.UnavailableError("Send channel closed")
	}
	s.sendChan <- msg

	ctx, cancel := context.WithTimeoutCause(s.ctx, timeout, cause)
	defer cancel()
	select {
	case err := <-s.errChan:
		if err != nil {
			close(s.sendChan)
			s.sendChan = nil
		}
		return err
	case <-ctx.Done():
		close(s.sendChan)
		s.sendChan = nil
		return context.Cause(ctx)
	}
}

// CloseAndRecvWithTimeoutCause calls CloseAndRecv on the underlying stream,
// waiting a maximum of timeout. If timeout is reached, the given cause is
// returned as the error.
func (s *Sender[S, R]) CloseAndRecvWithTimeoutCause(timeout time.Duration, cause error) (R, error) {
	if s.sendChan != nil {
		close(s.sendChan)
		s.sendChan = nil
	}
	ctx, cancel := context.WithTimeoutCause(s.ctx, timeout, cause)
	defer cancel()

	// gRPC client streams don't support concurrent Send and Close* operations.
	// If a previous SendWithTimeoutCause timed out, the sending goroutine may
	// be stuck in stream.Send — let it exit before touching the stream again.
	select {
	case <-s.done:
	case <-ctx.Done():
		return *new(R), context.Cause(ctx)
	}

	ch := make(chan StreamMsg[R], 1)
	go func() {
		rsp, err := s.stream.CloseAndRecv()
		ch <- StreamMsg[R]{rsp, err}
	}()
	select {
	case msg := <-ch:
		return msg.Data, msg.Error
	case <-ctx.Done():
		return *new(R), context.Cause(ctx)
	}
}

// NewSender returns a stream handle that can be used to implement more
// advanced stream handling, such as per-send timeouts.
//
// Example usage:
//
// sender := rpcutil.NewSender[*bspb.WriteRequest, *bspb.WriteResponse](ctx, stream)
//
//	for {
//		err := sender.SendWithTimeoutCause(5 * time.Second, status.DeadlineExceededError("blah blah blah"))
//		// handle err
//	}
func NewSender[S proto.Message, R proto.Message](ctx context.Context, stream SendStream[S, R]) Sender[S, R] {
	sendChan := make(chan S, 1)
	errChan := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case req, ok := <-sendChan:
				if !ok {
					return
				}
				err := stream.Send(req)
				select {
				case errChan <- err:
					if err != nil {
						return
					}
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return Sender[S, R]{ctx, stream, sendChan, errChan, done}
}

// Provides an OpenTelemetry MeterProvider that exports metrics to Prometheus.
// Wrapped in a sync.Once to avoid registering Prometheus metrics multiple
// times in case there are multiple gRPC clients or servers.
var MeterProvider = sync.OnceValue(func() metric.MeterProvider {
	exporter, err := prometheus.New()
	if err != nil {
		alert.UnexpectedEvent("Error creating prometheus metrics exporter")
		return noop.NewMeterProvider()
	}
	// Override otelgrpc's default 16-bucket histograms for client-side RPC
	// metrics with coarser bucket sets. The defaults explode cardinality
	// through the cross product of (rpc_service, rpc_method,
	// rpc_grpc_status_code, instance) × 16 buckets × 5 histogram families.
	// The metrics are named `rpc.{client,server}.{duration, request.size, response.size, requests_per_rpc, responses_per_rpc}`
	durationView := sdkmetric.NewView(
		sdkmetric.Instrument{Name: "rpc.client.duration"},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			Boundaries: []float64{5, 25, 100, 500, 1000, 5000, 10000, 30000}, // in ms
		}},
	)
	sizeView := sdkmetric.NewView(
		sdkmetric.Instrument{Name: "rpc.client.*.size"},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			// 1KiB, 32KiB, 1MiB, 4MiB, 8MiB
			Boundaries: []float64{1024, 32768, 1048576, 4194304, 8388608},
		}},
	)
	perRPCView := sdkmetric.NewView(
		sdkmetric.Instrument{Name: "rpc.client.*_per_rpc"},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			Boundaries: []float64{1, 10, 100, 1000},
		}},
	)

	// Allowlist the attributes on RPC metrics: client-side metrics carry
	// per-peer server.address/server.port attributes, which explode
	// cardinality through the cross product of (rpc_method,
	// rpc_response_status_code, server_address, server_port, instance) × 15
	// buckets. Client-side metrics also get a coarser bucket set; server-side
	// metrics keep the default buckets since their cardinality is bounded.
	// The metrics are named `rpc.{client,server}.call.duration`, measured in
	// seconds.
	metricAttrs := attribute.NewAllowKeysFilter(
		// rpc.method holds the full "package.Service/Method" path; the new
		// semconv has no separate rpc.service attribute.
		"rpc.method",
		"rpc.response.status_code",
		"rpc.system.name",
	)
	// Overrides for the new style metrics once we upgrade gRPC.
	clientCallDurationView := sdkmetric.NewView(
		sdkmetric.Instrument{Name: "rpc.client.call.duration"},
		sdkmetric.Stream{
			Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: []float64{0.005, 0.025, 0.1, 0.5, 1, 5, 10, 30}, // in seconds
			},
			AttributeFilter: metricAttrs,
		},
	)
	serverCallDurationView := sdkmetric.NewView(
		sdkmetric.Instrument{Name: "rpc.server.call.duration"},
		sdkmetric.Stream{AttributeFilter: metricAttrs},
	)
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithView(durationView, sizeView, perRPCView, clientCallDurationView, serverCallDurationView),
	)
})

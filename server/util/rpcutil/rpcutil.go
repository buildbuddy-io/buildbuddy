package rpcutil

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/vtprotocodec"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const GRPCMaxSizeBytes = int64(4 * 1000 * 1000)

func init() {
	vtprotocodec.Register()
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
}

// SendWithTimeoutCause attempts to send a message on the underlying stream,
// waiting a maximum of timeout. If timeout is reached, the given cause is
// returned as the error.
//
// Note that gRPC sends are asynchronous in the sense that the protocol does not
// acknowledge individual messages. A timeout wil only occur if the sender
// exhausts the flow-control window and the receiver does not increase it.
func (s *Sender[S, R]) SendWithTimeoutCause(msg S, timeout time.Duration, cause error) error {
	s.sendChan <- msg

	ctx, cancel := context.WithTimeoutCause(s.ctx, timeout, cause)
	defer cancel()
	select {
	case err := <-s.errChan:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// CloseAndRecvWithTimeoutCause calls CloseAndRecv on the underlying stream,
// waiting a maximum of timeout. If timeout is reached, the given cause is
// returned as the error.
func (s *Sender[S, R]) CloseAndRecvWithTimeoutCause(timeout time.Duration, cause error) (R, error) {
	ch := make(chan StreamMsg[R], 1)
	go func() {
		rsp, err := s.stream.CloseAndRecv()
		ch <- StreamMsg[R]{rsp, err}
	}()
	ctx, cancel := context.WithTimeoutCause(s.ctx, timeout, cause)
	defer cancel()
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
	errChan := make(chan error)
	go func() {
		for {
			select {
			case req := <-sendChan:
				err := stream.Send(req)
				errChan <- err
			case <-ctx.Done():
				return
			}
		}
	}()
	return Sender[S, R]{ctx, stream, sendChan, errChan}
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
	return sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
})

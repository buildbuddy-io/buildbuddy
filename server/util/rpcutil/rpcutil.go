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

type SendStream[Req, Res proto.Message] interface {
	Send(Req) error
	CloseAndRecv() (Res, error)
}

type Sender[Req, Res proto.Message] struct {
	ctx                context.Context
	sendChan           chan Req
	sendErrChan        chan error
	closeAndRecvChan   chan struct{}
	closeAndRecvResult chan StreamMsg[Res]
}

// SendWithTimeoutCause attempts to send a message on the underlying stream,
// waiting a maximum of timeout. If timeout is reached, the given cause is
// returned as the error.
//
// Note that gRPC sends are asynchronous in the sense that the protocol does not
// acknowledge individual messages. A timeout wil only occur if the sender
// exhausts the flow-control window and the receiver does not increase it.
func (s *Sender[Req, Res]) SendWithTimeoutCause(msg Req, timeout time.Duration, cause error) error {
	s.sendChan <- msg

	ctx, cancel := context.WithTimeoutCause(s.ctx, timeout, cause)
	defer cancel()
	select {
	case err := <-s.sendErrChan:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// CloseAndRecvWithTimeoutCause closes the send direction of the underlying
// stream and waits up to timeout for the final response.
func (s *Sender[Req, Res]) CloseAndRecvWithTimeoutCause(timeout time.Duration, cause error) (Res, error) {
	s.closeAndRecvChan <- struct{}{}

	ctx, cancel := context.WithTimeoutCause(s.ctx, timeout, cause)
	defer cancel()
	var zero Res
	select {
	case msg := <-s.closeAndRecvResult:
		return msg.Data, msg.Error
	case <-ctx.Done():
		return zero, context.Cause(ctx)
	}
}

// NewSender returns a stream handle that can be used to implement more
// advanced stream handling, such as per-send timeouts.
//
// Example usage:
//
// sender := rpcutil.NewSender[*bspb.WriteRequest](ctx, stream)
//
//	for {
//		err := sender.SendWithTimeoutCause(5 * time.Second, status.DeadlineExceededError("blah blah blah"))
//		// handle err
//	}
func NewSender[Req, Res proto.Message](ctx context.Context, stream SendStream[Req, Res]) Sender[Req, Res] {
	sendChan := make(chan Req, 1)
	sendErrChan := make(chan error)
	closeAndRecvChan := make(chan struct{}, 1)
	closeAndRecvResult := make(chan StreamMsg[Res], 1)
	go func() {
		for {
			select {
			case req := <-sendChan:
				err := stream.Send(req)
				sendErrChan <- err
			case <-closeAndRecvChan:
				rsp, err := stream.CloseAndRecv()
				closeAndRecvResult <- StreamMsg[Res]{Data: rsp, Error: err}
			case <-ctx.Done():
				return
			}
		}
	}()
	return Sender[Req, Res]{
		ctx:                ctx,
		sendChan:           sendChan,
		sendErrChan:        sendErrChan,
		closeAndRecvChan:   closeAndRecvChan,
		closeAndRecvResult: closeAndRecvResult,
	}
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

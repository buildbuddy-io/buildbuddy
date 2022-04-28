package build_event_publisher

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

const (
	// numStreamRetries is the max number of retries to allow for publishing
	// a build event stream.
	numStreamRetries = 5
)

// Publisher publishes Bazel build events for a single build event stream.
// It retries the stream if it gets disconnected.
type Publisher struct {
	streamID   *bepb.StreamId
	apiKey     string
	besBackend string

	events *EventBuffer
	// errCh streams the stream publishing error (if any) after all retry attempts
	// are exhausted.
	errCh chan error
}

func New(besBackend, apiKey, invocationID string) (*Publisher, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	streamID := &bepb.StreamId{
		InvocationId: invocationID,
		BuildId:      id.String(),
	}
	return &Publisher{
		streamID:   streamID,
		apiKey:     apiKey,
		besBackend: besBackend,
		events:     NewEventBuffer(streamID),
		errCh:      make(chan error, 1),
	}, nil
}

// Start the event publishing loop in the background. Stops handling new events
// as soon as the first call to Finish() occurs.
func (p *Publisher) Start(ctx context.Context) {
	go func() {
		defer close(p.errCh)
		// Use a count-based max number of retries (rather than timeout based),
		// to be consistent with Bazel.
		r := retry.New(ctx, &retry.Options{
			MaxRetries:     numStreamRetries,
			InitialBackoff: 300 * time.Millisecond,
			MaxBackoff:     1 * time.Second,
			Multiplier:     2,
		})
		var err error
		for r.Next() {
			err = p.run(ctx)
			if err == nil {
				return
			}
			log.Debugf("Retrying build event stream due to error: %s", err)
		}
		p.errCh <- err
	}()
}

// run performs a single attempt to stream any currently buffered events
// as well as all subsequently published events to the BES backend.
func (p *Publisher) run(ctx context.Context) error {
	conn, err := grpc_client.DialTarget(p.besBackend)
	if err != nil {
		return status.WrapError(err, "error dialing bes_backend")
	}
	defer conn.Close()
	besClient := pepb.NewPublishBuildEventClient(conn)
	if p.apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, auth.APIKeyHeader, p.apiKey)
	}
	stream, err := besClient.PublishBuildToolEventStream(ctx)
	if err != nil {
		return status.WrapError(err, "error dialing bes_backend")
	}

	doneReceiving := make(chan error, 1)
	go func() {
		defer close(doneReceiving)
		for {
			_, err := stream.Recv()
			if err == nil {
				continue
			}
			if err == io.EOF {
				return
			}
			doneReceiving <- status.UnavailableErrorf("recv failed: %s", err)
			return
		}
	}()

	events, cancel := p.events.Subscribe()
	defer cancel()

	for obe := range events {
		req := &pepb.PublishBuildToolEventStreamRequest{OrderedBuildEvent: obe}
		if err := stream.Send(req); err != nil {
			return status.UnavailableErrorf("send failed: %s", err)
		}
	}
	// After successfully transmitting all events, close our side of the stream
	// and wait for server ACKs before closing the connection.
	if err := stream.CloseSend(); err != nil {
		return status.UnavailableErrorf("close send failed: %s", err)
	}
	// Return any error from receiving ACKs
	return <-doneReceiving
}

func (p *Publisher) Publish(bazelEvent *bespb.BuildEvent) error {
	bazelEventAny, err := anypb.New(bazelEvent)
	if err != nil {
		return err
	}
	be := &bepb.BuildEvent{
		EventTime: timestamppb.Now(),
		Event:     &bepb.BuildEvent_BazelEvent{BazelEvent: bazelEventAny},
	}
	p.events.Add(be)
	return nil
}

// Finish publishes the final stream event and then waits for the server to ACK
// all published events.
func (p *Publisher) Finish() error {
	be := &bepb.BuildEvent{
		EventTime: timestamppb.Now(),
		Event: &bepb.BuildEvent_ComponentStreamFinished{
			ComponentStreamFinished: &bepb.BuildEvent_BuildComponentStreamFinished{
				Type: bepb.BuildEvent_BuildComponentStreamFinished_FINISHED,
			},
		},
	}
	p.events.Add(be)
	if err := <-p.errCh; err != nil {
		return status.UnavailableErrorf("failed to publish build event stream after multiple attempts: %s", err)
	}
	return nil
}

// EventBuffer holds build events that have not been ack'd by the server.
//
// It supports a single consumer and multiple producers: multiple goroutines can
// call Add(), and a single goroutine can call Subscribe() to get the currently
// buffered events and subsequently added events.
type EventBuffer struct {
	streamID *bepb.StreamId

	mu sync.Mutex
	// TODO(bduffany): Store these on disk since the buffer can get quite large if
	// there is a lot of log output.
	items []*bepb.BuildEvent
	// notify is used to signal that an event has been added to the buffer.
	notify chan struct{}
}

func NewEventBuffer(streamID *bepb.StreamId) *EventBuffer {
	return &EventBuffer{
		streamID: streamID,
		notify:   make(chan struct{}, 1),
	}
}

// Subscribe returns a channel that streams the currently buffered events as
// well as any subsequently added events, up until the ComponentStreamFinished
// event.
//
// The returned callback function cancels the subscription. It must be called,
// or else a goroutine leak may occur. Once cancel is called, it is safe for
// another goroutine to call Subscribe() again.
func (b *EventBuffer) Subscribe() (<-chan *pepb.OrderedBuildEvent, func()) {
	events := make(chan *pepb.OrderedBuildEvent)
	cancel := make(chan struct{})
	go func() {
		defer close(events)
		i := 0
		for {
			b.mu.Lock()
			buffer := b.items
			b.mu.Unlock()

			// Send all events which haven't been sent yet
			for i < len(buffer) {
				buildEvent := buffer[i]
				obe := &pepb.OrderedBuildEvent{
					StreamId:       b.streamID,
					SequenceNumber: int64(i) + 1,
					Event:          buildEvent,
				}
				select {
				case <-cancel:
					return
				case events <- obe:
				}
				if _, ok := buildEvent.Event.(*bepb.BuildEvent_ComponentStreamFinished); ok {
					return
				}
				i++
			}
			// Wait until another event has been added to the buffer, then rinse and
			// repeat.
			select {
			case <-cancel:
				return
			case <-b.notify:
			}
		}
	}()
	return events, func() { close(cancel) }
}

// Add adds an item to the event buffer, making it available to the subscriber.
func (b *EventBuffer) Add(event *bepb.BuildEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.items = append(b.items, event)
	// Set a notification if one is not already set
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

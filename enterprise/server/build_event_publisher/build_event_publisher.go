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
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"

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
	done   chan error
}

func New(besBackend, apiKey, invocationID string) (*Publisher, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	return &Publisher{
		streamID: &bepb.StreamId{
			InvocationId: invocationID,
			BuildId:      id.String(),
		},
		apiKey:     apiKey,
		besBackend: besBackend,
		events:     NewEventBuffer(),
		done:       make(chan error, 1),
	}, nil
}

// Start the event publishing loop in the background. Stops handling new events
// as soon as the first call to `Wait()` occurs.
func (p *Publisher) Start(ctx context.Context) {
	go func() {
		defer close(p.done)
		r := retry.New(ctx, &retry.Options{
			MaxRetries:     numStreamRetries,
			InitialBackoff: 300 * time.Millisecond,
			MaxBackoff:     1 * time.Second,
			Multiplier:     2,
		})
		var err error
		for r.Next() {
			if err != nil {
				log.Debugf("Retrying build event stream due to error: %s", err)
			}
			err = p.run(ctx)
			if err == nil {
				return
			}
		}
		p.done <- err
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

	i := int64(0)
	finished := false
	for {
		buffer := p.events.Get()
		// Send all events which haven't been sent yet
		for i < int64(len(buffer)) {
			event := buffer[i]
			req := &pepb.PublishBuildToolEventStreamRequest{
				OrderedBuildEvent: &pepb.OrderedBuildEvent{
					StreamId:       p.streamID,
					SequenceNumber: i + 1,
					Event:          event,
				},
			}
			i++
			if err := stream.Send(req); err != nil {
				return status.UnavailableErrorf("send failed: %s", err)
			}
			// After successfully transmitting all events, close our side of the stream
			// and wait for server ACKs before closing the connection.
			if _, ok := event.Event.(*bepb.BuildEvent_ComponentStreamFinished); ok {
				if err := stream.CloseSend(); err != nil {
					return status.UnavailableErrorf("close send failed: %s", err)
				}
				finished = true
			}
		}
		if finished {
			break
		}
		// Wait until another event has been added to the buffer since we called
		// Get(), then rinse and repeat.
		p.events.Wait()
	}

	// Return any error from receiving ACKs
	return <-doneReceiving
}

func (p *Publisher) Publish(event *bespb.BuildEvent) error {
	be := &bepb.BuildEvent{EventTime: ptypes.TimestampNow()}
	if event == nil {
		be.Event = &bepb.BuildEvent_ComponentStreamFinished{ComponentStreamFinished: &bepb.BuildEvent_BuildComponentStreamFinished{
			Type: bepb.BuildEvent_BuildComponentStreamFinished_FINISHED,
		}}
	} else {
		bazelEvent, err := ptypes.MarshalAny(event)
		if err != nil {
			return err
		}
		be.Event = &bepb.BuildEvent_BazelEvent{BazelEvent: bazelEvent}
	}

	p.events.Add(be)
	return nil
}

// Wait publishes the Finished event and then waits for the server to ACK
// all published events.
func (p *Publisher) Wait() error {
	if err := p.Publish(nil); err != nil {
		return err
	}
	if err := <-p.done; err != nil {
		return status.UnavailableErrorf("failed to publish build event stream after multiple attempts: %s", err)
	}
	return nil
}

// EventBuffer holds build events that have not been ack'd by the server.
//
// It supports a single consumer and multiple producers: multiple goroutines
// can call Add(), and a single goroutine can call Get() and Wait() in a loop
// to observe added events. The consumer keeps track of which events it has
// already processed.
type EventBuffer struct {
	mu sync.Mutex
	// TODO(bduffany): Store these on disk since the buffer can get quite large if
	// there is a lot of log output.
	items []*bepb.BuildEvent
	// notify is used to signal that an event has been added to the buffer.
	notify chan struct{}
}

func NewEventBuffer() *EventBuffer {
	return &EventBuffer{notify: make(chan struct{}, 1)}
}

// Get returns the currently buffered items.
func (b *EventBuffer) Get() []*bepb.BuildEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.items
}

// Add adds an item to the event buffer and unblocks a Wait() caller.
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

// Wait waits until the next call to Add().
func (b *EventBuffer) Wait() {
	<-b.notify
}

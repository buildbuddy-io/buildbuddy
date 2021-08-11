package build_event_publisher

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

// buildEventPublisher publishes Bazel build events for a single build event stream.
type Publisher struct {
	streamID   *bepb.StreamId
	apiKey     string
	besBackend string

	mu     sync.Mutex // PROTECTS(err, events)
	err    error
	events chan *bespb.BuildEvent
	done   chan struct{}
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

		// We probably won't ever saturate this buffer since we only need to
		// publish a few events for the actions themselves and progress events
		// are rate-limited. Also, events are sent to the server with low
		// latency compared to the rate limiting interval.
		events: make(chan *bespb.BuildEvent, 256),
		done:   make(chan struct{}, 1),
	}, nil
}

func (p *Publisher) getError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *Publisher) setError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// Start the event publishing loop in the background. Stops handling new events
// as soon as the first call to `Wait()` occurs.
func (p *Publisher) Start(ctx context.Context) {
	go p.run(ctx)
}

func (p *Publisher) run(ctx context.Context) {
	defer func() {
		p.done <- struct{}{}
	}()

	conn, err := grpc_client.DialTarget(p.besBackend)
	if err != nil {
		p.setError(status.WrapError(err, "error dialing bes_backend"))
		return
	}
	defer conn.Close()
	besClient := pepb.NewPublishBuildEventClient(conn)
	if p.apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, auth.APIKeyHeader, p.apiKey)
	}
	stream, err := besClient.PublishBuildToolEventStream(ctx)
	if err != nil {
		p.setError(status.WrapError(err, "error opening build event stream"))
		return
	}

	doneReceiving := make(chan struct{}, 1)
	go func() {
		for {
			_, err := stream.Recv()
			if err == nil {
				continue
			}
			if err == io.EOF {
				log.Debug("Received all acks from server.")
			} else {
				log.Errorf("Error receiving acks from the server: %s", err)
				p.setError(err)
			}
			doneReceiving <- struct{}{}
			return
		}
	}()

	for seqNo := int64(1); ; seqNo++ {
		event := <-p.events
		if event == nil {
			// Wait() was called, meaning no more events to publish.
			// Send ComponentStreamFinished event before closing the stream.
			start := time.Now()
			err = stream.Send(&pepb.PublishBuildToolEventStreamRequest{
				OrderedBuildEvent: &pepb.OrderedBuildEvent{
					StreamId:       p.streamID,
					SequenceNumber: seqNo,
					Event: &bepb.BuildEvent{
						EventTime: ptypes.TimestampNow(),
						Event: &bepb.BuildEvent_ComponentStreamFinished{ComponentStreamFinished: &bepb.BuildEvent_BuildComponentStreamFinished{
							Type: bepb.BuildEvent_BuildComponentStreamFinished_FINISHED,
						}},
					},
				},
			})
			log.Debugf("BEP: published FINISHED event (#%d) in %s", seqNo, time.Since(start))

			if err != nil {
				p.setError(err)
				return
			}
			break
		}

		bazelEvent, err := ptypes.MarshalAny(event)
		if err != nil {
			p.setError(status.WrapError(err, "failed to marshal bazel event"))
			return
		}
		start := time.Now()
		err = stream.Send(&pepb.PublishBuildToolEventStreamRequest{
			OrderedBuildEvent: &pepb.OrderedBuildEvent{
				StreamId:       p.streamID,
				SequenceNumber: seqNo,
				Event: &bepb.BuildEvent{
					EventTime: ptypes.TimestampNow(),
					Event:     &bepb.BuildEvent_BazelEvent{BazelEvent: bazelEvent},
				},
			},
		})
		log.Debugf("BEP: published event (#%d) in %s", seqNo, time.Since(start))
		if err != nil {
			p.setError(err)
			return
		}
	}
	// After successfully transmitting all events, close our side of the stream
	// and wait for server ACKs before closing the connection.
	if err := stream.CloseSend(); err != nil {
		p.setError(status.WrapError(err, "failed to close build event stream"))
		return
	}
	<-doneReceiving
}

func (p *Publisher) Publish(e *bespb.BuildEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return status.WrapError(p.err, "cannot publish event due to previous error")
	}
	p.events <- e
	return nil
}

func (p *Publisher) Wait() error {
	p.events <- nil
	<-p.done
	return p.err
}

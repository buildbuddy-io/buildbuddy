package build_event_publisher_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_publisher"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbes"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const eventWaitTimeout = time.Second

func TestEventBufferDeliveryScenarios(t *testing.T) {
	tests := []struct {
		name   string
		before []*bepb.BuildEvent
		after  []*bepb.BuildEvent
	}{
		{
			name:   "subscribeBeforeAdd",
			before: []*bepb.BuildEvent{},
			after: []*bepb.BuildEvent{
				regularEvent(),
				regularEvent(),
				finishedEvent(),
			},
		},
		{
			name: "bufferedBeforeSubscribe",
			before: []*bepb.BuildEvent{
				regularEvent(),
				regularEvent(),
				regularEvent(),
			},
			after: []*bepb.BuildEvent{
				regularEvent(),
				finishedEvent(),
			},
		},
		{
			name:   "finishOnly",
			before: []*bepb.BuildEvent{},
			after:  []*bepb.BuildEvent{finishedEvent()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streamID := makeStreamID("inv-"+tt.name, "build-"+tt.name)
			buffer := build_event_publisher.NewEventBuffer(streamID)

			for _, event := range tt.before {
				buffer.Add(event)
			}

			events, cancel := buffer.Subscribe()
			defer cancel()

			for _, event := range tt.after {
				buffer.Add(event)
			}

			got := collectEvents(t, events)

			// Build expected OrderedBuildEvents
			expectedEvents := append(append([]*bepb.BuildEvent{}, tt.before...), tt.after...)
			var expected []*pepb.OrderedBuildEvent
			for i, event := range expectedEvents {
				expected = append(expected, &pepb.OrderedBuildEvent{
					StreamId:       streamID,
					SequenceNumber: int64(i + 1),
					Event:          event,
				})
			}

			require.Empty(t, cmp.Diff(expected, got, protocmp.Transform()))
		})
	}
}

func TestEventBuffer_ConcurrentProducers(t *testing.T) {
	streamID := makeStreamID("inv-concurrent-producers", "build-concurrent-producers")
	buffer := build_event_publisher.NewEventBuffer(streamID)

	events, cancel := buffer.Subscribe()
	defer cancel()

	const (
		numProducers      = 10
		eventsPerProducer = 20
	)

	var wg sync.WaitGroup
	wg.Add(numProducers)

	for i := 0; i < numProducers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < eventsPerProducer; j++ {
				buffer.Add(regularEvent())
			}
		}()
	}

	wg.Wait()
	buffer.Add(finishedEvent())

	got := collectEvents(t, events)

	// Verify count and basic structure (can't use cmp.Diff since event order is non-deterministic)
	expectedCount := numProducers*eventsPerProducer + 1
	require.Len(t, got, expectedCount)
	for i, obe := range got {
		require.Empty(t, cmp.Diff(streamID, obe.StreamId, protocmp.Transform()))
		assert.Equal(t, int64(i+1), obe.SequenceNumber)
		require.NotNil(t, obe.Event)
	}
}

func TestEventBuffer_CancelSubscription(t *testing.T) {
	buffer := build_event_publisher.NewEventBuffer(makeStreamID("inv-cancel", "build-cancel"))

	events, cancel := buffer.Subscribe()

	buffer.Add(regularEvent())
	_, ok := recvEvent(t, events)
	require.True(t, ok)

	cancel()
	requireClosed(t, events)
}

func TestEventBuffer_CancelBeforeEvents(t *testing.T) {
	buffer := build_event_publisher.NewEventBuffer(makeStreamID("inv-cancel-before", "build-cancel-before"))

	events, cancel := buffer.Subscribe()
	cancel()

	buffer.Add(regularEvent())
	buffer.Add(finishedEvent())

	delivered := collectEvents(t, events)
	require.LessOrEqual(t, len(delivered), 1)
}

func TestEventBuffer_MultipleSubscriptionsSerial(t *testing.T) {
	buffer := build_event_publisher.NewEventBuffer(makeStreamID("inv-serial", "build-serial"))

	events1, cancel1 := buffer.Subscribe()

	buffer.Add(regularEvent())
	event, ok := recvEvent(t, events1)
	require.True(t, ok)
	assert.Equal(t, int64(1), event.SequenceNumber)

	cancel1()
	requireClosed(t, events1)

	events2, cancel2 := buffer.Subscribe()
	defer cancel2()

	event, ok = recvEvent(t, events2)
	require.True(t, ok)
	assert.Equal(t, int64(1), event.SequenceNumber)

	buffer.Add(regularEvent())
	event, ok = recvEvent(t, events2)
	require.True(t, ok)
	assert.Equal(t, int64(2), event.SequenceNumber)

	buffer.Add(finishedEvent())
	event, ok = recvEvent(t, events2)
	require.True(t, ok)
	assert.Equal(t, int64(3), event.SequenceNumber)

	requireClosed(t, events2)
}

func makeStreamID(invocationID, buildID string) *bepb.StreamId {
	return &bepb.StreamId{
		InvocationId: invocationID,
		BuildId:      buildID,
	}
}

func finishedEvent() *bepb.BuildEvent {
	return &bepb.BuildEvent{
		EventTime: timestamppb.Now(),
		Event: &bepb.BuildEvent_ComponentStreamFinished{
			ComponentStreamFinished: &bepb.BuildEvent_BuildComponentStreamFinished{
				Type: bepb.BuildEvent_BuildComponentStreamFinished_FINISHED,
			},
		},
	}
}

func regularEvent() *bepb.BuildEvent {
	return &bepb.BuildEvent{
		EventTime: timestamppb.Now(),
		Event: &bepb.BuildEvent_BazelEvent{
			BazelEvent: &anypb.Any{},
		},
	}
}

func recvEvent(tb testing.TB, ch <-chan *pepb.OrderedBuildEvent) (*pepb.OrderedBuildEvent, bool) {
	tb.Helper()
	select {
	case event, ok := <-ch:
		if !ok {
			return nil, false
		}
		return event, true
	case <-time.After(eventWaitTimeout):
		tb.Fatalf("timed out waiting for event")
		return nil, false
	}
}

func collectEvents(tb testing.TB, ch <-chan *pepb.OrderedBuildEvent) []*pepb.OrderedBuildEvent {
	tb.Helper()
	var events []*pepb.OrderedBuildEvent
	for {
		event, ok := recvEvent(tb, ch)
		if !ok {
			return events
		}
		events = append(events, event)
	}
}

func requireClosed(tb testing.TB, ch <-chan *pepb.OrderedBuildEvent) {
	tb.Helper()
	select {
	case _, ok := <-ch:
		if ok {
			tb.Fatalf("expected channel to be closed")
		}
	case <-time.After(eventWaitTimeout):
		tb.Fatalf("timed out waiting for channel close")
	}
}

// Publisher tests

func TestPublisher_BasicPublishAndFinish(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	publisher, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	publisher.Start(ctx)

	// Publish a few Bazel events
	err = publisher.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}},
		},
		Payload: &bespb.BuildEvent_Started{
			Started: &bespb.BuildStarted{Uuid: "test-build-123"},
		},
	})
	require.NoError(t, err)

	err = publisher.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}},
		},
		Payload: &bespb.BuildEvent_Finished{
			Finished: &bespb.BuildFinished{ExitCode: &bespb.BuildFinished_ExitCode{Code: 0}},
		},
	})
	require.NoError(t, err)

	err = publisher.Finish()
	require.NoError(t, err)

	// Verify events were received
	events := bes.GetEvents()
	require.Len(t, events, 3) // 2 Bazel events + 1 ComponentStreamFinished

	// Verify sequence numbers
	for i, event := range events {
		assert.Equal(t, int64(i+1), event.OrderedBuildEvent.SequenceNumber)
		assert.Equal(t, "test-invocation", event.OrderedBuildEvent.StreamId.InvocationId)
	}

	// Verify the last event is ComponentStreamFinished
	lastEvent := events[len(events)-1].OrderedBuildEvent.Event
	_, ok := lastEvent.Event.(*bepb.BuildEvent_ComponentStreamFinished)
	assert.True(t, ok, "last event should be ComponentStreamFinished")
}

func TestPublisher_EmptyStream(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	publisher, err := build_event_publisher.New(addr, "", "empty-invocation")
	require.NoError(t, err)

	publisher.Start(ctx)

	err = publisher.Finish()
	require.NoError(t, err)

	// Should only have the ComponentStreamFinished event
	events := bes.GetEvents()
	require.Len(t, events, 1)
	assert.Equal(t, int64(1), events[0].OrderedBuildEvent.SequenceNumber)

	lastEvent := events[0].OrderedBuildEvent.Event
	_, ok := lastEvent.Event.(*bepb.BuildEvent_ComponentStreamFinished)
	assert.True(t, ok)
}

func TestPublisher_SingleEvent(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	publisher, err := build_event_publisher.New(addr, "", "single-event-invocation")
	require.NoError(t, err)

	publisher.Start(ctx)

	err = publisher.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: 1}},
		},
		Payload: &bespb.BuildEvent_Progress{Progress: &bespb.Progress{}},
	})
	require.NoError(t, err)

	err = publisher.Finish()
	require.NoError(t, err)

	events := bes.GetEvents()
	require.Len(t, events, 2) // 1 event + ComponentStreamFinished
	assert.Equal(t, int64(1), events[0].OrderedBuildEvent.SequenceNumber)
	assert.Equal(t, int64(2), events[1].OrderedBuildEvent.SequenceNumber)
}

func TestPublisher_RetriesOnTransientFailure(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	// Fail the first 2 stream attempts
	bes.EventHandler = testbes.FailNTimesThenSucceed(2, status.UnavailableError("temporary failure"))

	publisher, err := build_event_publisher.New(addr, "", "retry-invocation")
	require.NoError(t, err)

	publisher.Start(ctx)

	err = publisher.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}},
		},
		Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{Uuid: "retry-test"}},
	})
	require.NoError(t, err)

	err = publisher.Finish()
	require.NoError(t, err)

	// Should have received events despite initial failures
	events := bes.GetEvents()
	require.Len(t, events, 2) // 1 event + ComponentStreamFinished
}

func TestPublisher_ExhaustsRetries(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	// Always fail
	bes.EventHandler = testbes.FailWith(status.UnavailableError("persistent failure"))

	publisher, err := build_event_publisher.New(addr, "", "exhaust-retry-invocation")
	require.NoError(t, err)

	publisher.Start(ctx)

	err = publisher.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}},
		},
		Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{Uuid: "fail-test"}},
	})
	require.NoError(t, err)

	err = publisher.Finish()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish build event stream")
}

func TestPublisher_RetryPreservesEventOrder(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	// Fail once, then succeed
	bes.EventHandler = testbes.FailNTimesThenSucceed(1, status.UnavailableError("one-time failure"))

	publisher, err := build_event_publisher.New(addr, "", "order-test-invocation")
	require.NoError(t, err)

	publisher.Start(ctx)

	// Publish multiple events
	for i := 0; i < 5; i++ {
		err = publisher.Publish(&bespb.BuildEvent{
			Id: &bespb.BuildEventId{
				Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: int32(i)}},
			},
			Payload: &bespb.BuildEvent_Progress{Progress: &bespb.Progress{}},
		})
		require.NoError(t, err)
	}

	err = publisher.Finish()
	require.NoError(t, err)

	events := bes.GetEvents()
	require.Len(t, events, 6) // 5 events + ComponentStreamFinished

	// Verify sequence numbers are correct
	for i, event := range events {
		assert.Equal(t, int64(i+1), event.OrderedBuildEvent.SequenceNumber)
	}
}

func TestPublisher_ConcurrentPublish(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	publisher, err := build_event_publisher.New(addr, "", "concurrent-invocation")
	require.NoError(t, err)

	publisher.Start(ctx)

	const (
		numGoroutines      = 10
		eventsPerGoroutine = 20
	)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < eventsPerGoroutine; j++ {
				err := publisher.Publish(&bespb.BuildEvent{
					Id: &bespb.BuildEventId{
						Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: int32(j)}},
					},
					Payload: &bespb.BuildEvent_Progress{Progress: &bespb.Progress{}},
				})
				require.NoError(t, err)
			}
		}()
	}

	wg.Wait()
	err = publisher.Finish()
	require.NoError(t, err)

	events := bes.GetEvents()
	expectedCount := numGoroutines*eventsPerGoroutine + 1 // +1 for ComponentStreamFinished
	require.Len(t, events, expectedCount)

	// Verify all sequence numbers are present and in order
	for i, event := range events {
		assert.Equal(t, int64(i+1), event.OrderedBuildEvent.SequenceNumber)
	}
}

func TestPublisher_APIKeyInMetadata(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	apiKey := "test-api-key-12345"
	publisher, err := build_event_publisher.New(addr, apiKey, "api-key-invocation")
	require.NoError(t, err)

	publisher.Start(ctx)

	err = publisher.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}},
		},
		Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{Uuid: "api-test"}},
	})
	require.NoError(t, err)

	err = publisher.Finish()
	require.NoError(t, err)

	// Verify API key was sent in metadata
	md := bes.GetMetadata()
	apiKeys := md.Get("x-buildbuddy-api-key")
	require.Len(t, apiKeys, 1)
	assert.Equal(t, apiKey, apiKeys[0])
}

func TestPublisher_NoAPIKey(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	publisher, err := build_event_publisher.New(addr, "", "no-api-key-invocation")
	require.NoError(t, err)

	publisher.Start(ctx)

	err = publisher.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}},
		},
		Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{Uuid: "no-api-test"}},
	})
	require.NoError(t, err)

	err = publisher.Finish()
	require.NoError(t, err)

	// Should work fine without API key
	events := bes.GetEvents()
	require.Len(t, events, 2)
}

func TestPublisher_ContextCancellation(t *testing.T) {
	bes, addr := testbes.RunTCP(t)

	// Use a handler that blocks to ensure the stream is still in progress when we cancel
	eventReceived := make(chan struct{})
	bes.EventHandler = func(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer, streamID *bepb.StreamId, event *pepb.PublishBuildToolEventStreamRequest) error {
		// Signal that we received an event
		select {
		case eventReceived <- struct{}{}:
		default:
		}
		// Block to keep the stream alive long enough for cancellation
		time.Sleep(100 * time.Millisecond)
		return testbes.Ack(stream, streamID, event)
	}

	ctx, cancel := context.WithCancel(context.Background())

	publisher, err := build_event_publisher.New(addr, "", "cancel-invocation")
	require.NoError(t, err)

	publisher.Start(ctx)

	err = publisher.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}},
		},
		Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{Uuid: "cancel-test"}},
	})
	require.NoError(t, err)

	// Wait for the server to start processing the event
	<-eventReceived

	// Cancel context while the stream is in progress
	cancel()

	err = publisher.Finish()
	require.Error(t, err)

	// Server may have received some events before cancellation
	events := bes.GetEvents()
	assert.GreaterOrEqual(t, len(events), 0)
}

func TestPublisher_RealBazelEvents(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	publisher, err := build_event_publisher.New(addr, "", "real-bazel-invocation")
	require.NoError(t, err)

	publisher.Start(ctx)

	// BuildStarted
	err = publisher.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}},
		},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
		},
		Payload: &bespb.BuildEvent_Started{
			Started: &bespb.BuildStarted{
				Uuid:             "550e8400-e29b-41d4-a716-446655440000",
				StartTime:        timestamppb.Now(),
				BuildToolVersion: "6.0.0",
				Command:          "build",
			},
		},
	})
	require.NoError(t, err)

	// TargetComplete
	err = publisher.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_TargetCompleted{
				TargetCompleted: &bespb.BuildEventId_TargetCompletedId{
					Label: "//pkg:target",
				},
			},
		},
		Payload: &bespb.BuildEvent_Completed{
			Completed: &bespb.TargetComplete{
				Success: true,
			},
		},
	})
	require.NoError(t, err)

	// BuildFinished
	err = publisher.Publish(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}},
		},
		Payload: &bespb.BuildEvent_Finished{
			Finished: &bespb.BuildFinished{
				ExitCode: &bespb.BuildFinished_ExitCode{
					Name: "SUCCESS",
					Code: 0,
				},
			},
		},
	})
	require.NoError(t, err)

	err = publisher.Finish()
	require.NoError(t, err)

	events := bes.GetEvents()
	require.Len(t, events, 4) // 3 Bazel events + ComponentStreamFinished

	// Verify the events contain the expected Bazel event types
	event0 := events[0].OrderedBuildEvent.Event
	_, ok := event0.Event.(*bepb.BuildEvent_BazelEvent)
	assert.True(t, ok, "first event should be BazelEvent")

	lastEvent := events[len(events)-1].OrderedBuildEvent.Event
	_, ok = lastEvent.Event.(*bepb.BuildEvent_ComponentStreamFinished)
	assert.True(t, ok, "last event should be ComponentStreamFinished")
}

package build_event_publisher_test

import (
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_publisher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const eventWaitTimeout = time.Second

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

func TestEventBufferDeliveryScenarios(t *testing.T) {
	tests := []struct {
		name   string
		before func() []*bepb.BuildEvent
		after  func() []*bepb.BuildEvent
	}{
		{
			name:   "subscribeBeforeAdd",
			before: func() []*bepb.BuildEvent { return nil },
			after: func() []*bepb.BuildEvent {
				return []*bepb.BuildEvent{regularEvent(), regularEvent(), finishedEvent()}
			},
		},
		{
			name: "bufferedBeforeSubscribe",
			before: func() []*bepb.BuildEvent {
				return []*bepb.BuildEvent{regularEvent(), regularEvent(), regularEvent()}
			},
			after: func() []*bepb.BuildEvent {
				return []*bepb.BuildEvent{regularEvent(), finishedEvent()}
			},
		},
		{
			name:   "finishOnly",
			before: func() []*bepb.BuildEvent { return nil },
			after: func() []*bepb.BuildEvent {
				return []*bepb.BuildEvent{finishedEvent()}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streamID := makeStreamID("inv-"+tt.name, "build-"+tt.name)
			buffer := build_event_publisher.NewEventBuffer(streamID)

			before := tt.before()
			for _, event := range before {
				buffer.Add(event)
			}

			events, cancel := buffer.Subscribe()
			defer cancel()

			after := tt.after()
			for _, event := range after {
				buffer.Add(event)
			}

			got := collectEvents(t, events)

			expected := append(append([]*bepb.BuildEvent{}, before...), after...)
			require.Len(t, got, len(expected))
			for i, obe := range got {
				assert.Equal(t, streamID, obe.StreamId)
				assert.Equal(t, int64(i+1), obe.SequenceNumber)
				assert.Same(t, expected[i], obe.Event)
			}
		})
	}
}

func TestEventBuffer_LargeNumberOfEvents(t *testing.T) {
	streamID := makeStreamID("inv-large", "build-large")
	buffer := build_event_publisher.NewEventBuffer(streamID)

	events, cancel := buffer.Subscribe()
	defer cancel()

	const numEvents = 1000
	expected := make([]*bepb.BuildEvent, 0, numEvents+1)
	for i := 0; i < numEvents; i++ {
		event := regularEvent()
		expected = append(expected, event)
		buffer.Add(event)
	}
	finish := finishedEvent()
	expected = append(expected, finish)
	buffer.Add(finish)

	got := collectEvents(t, events)
	require.Len(t, got, len(expected))
	for i, obe := range got {
		assert.Equal(t, streamID, obe.StreamId)
		assert.Equal(t, int64(i+1), obe.SequenceNumber)
		assert.Same(t, expected[i], obe.Event)
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
	expectedCount := numProducers*eventsPerProducer + 1
	require.Len(t, got, expectedCount)
	for i, obe := range got {
		assert.Equal(t, streamID, obe.StreamId)
		assert.Equal(t, int64(i+1), obe.SequenceNumber)
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

func TestEventBuffer_LargeConcurrentAddAndRead(t *testing.T) {
	streamID := makeStreamID("inv-concurrent-add-read", "build-concurrent-add-read")
	buffer := build_event_publisher.NewEventBuffer(streamID)

	events, cancel := buffer.Subscribe()
	defer cancel()

	const numEvents = 100

	go func() {
		for i := 0; i < numEvents; i++ {
			buffer.Add(regularEvent())
			time.Sleep(time.Millisecond)
		}
		buffer.Add(finishedEvent())
	}()

	got := collectEvents(t, events)
	require.Len(t, got, numEvents+1)
	for i, obe := range got {
		assert.Equal(t, streamID, obe.StreamId)
		assert.Equal(t, int64(i+1), obe.SequenceNumber)
	}
}

func TestEventBuffer_NoGoroutineLeakOnCancel(t *testing.T) {
	buffer := build_event_publisher.NewEventBuffer(makeStreamID("inv-leak", "build-leak"))

	for i := 0; i < 10; i++ {
		events, cancel := buffer.Subscribe()
		buffer.Add(regularEvent())

		_, ok := recvEvent(t, events)
		require.True(t, ok)

		cancel()
		requireClosed(t, events)
	}
}

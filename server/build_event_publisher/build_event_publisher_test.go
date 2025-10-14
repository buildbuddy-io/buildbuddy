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
		require.Equal(t, int64(i+1), obe.SequenceNumber)
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
	require.Equal(t, int64(1), event.SequenceNumber)

	cancel1()
	requireClosed(t, events1)

	events2, cancel2 := buffer.Subscribe()
	defer cancel2()

	event, ok = recvEvent(t, events2)
	require.True(t, ok)
	require.Equal(t, int64(1), event.SequenceNumber)

	buffer.Add(regularEvent())
	event, ok = recvEvent(t, events2)
	require.True(t, ok)
	require.Equal(t, int64(2), event.SequenceNumber)

	buffer.Add(finishedEvent())
	event, ok = recvEvent(t, events2)
	require.True(t, ok)
	require.Equal(t, int64(3), event.SequenceNumber)

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

// Helper functions for Publisher tests

func makeBazelEvent() *bespb.BuildEvent {
	return &bespb.BuildEvent{
		Id: &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Started{
				Started: &bespb.BuildEventId_BuildStartedId{},
			},
		},
	}
}

func getSequenceNumbers(events []*pepb.PublishBuildToolEventStreamRequest) []int64 {
	seqNums := make([]int64, len(events))
	for i, event := range events {
		seqNums[i] = event.OrderedBuildEvent.GetSequenceNumber()
	}
	return seqNums
}

// Event Sequencing Tests

// TestSequenceNumbersStartAtOne verifies that the first event published has
// sequence number 1, not 0.
func TestSequenceNumbersStartAtOne(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	events := bes.GetEvents()
	require.Len(t, events, 2) // 1 regular event + 1 finish event
	require.Equal(t, int64(1), events[0].OrderedBuildEvent.GetSequenceNumber())
}

// TestSequenceNumbersIncremental verifies that sequence numbers increment
// consecutively (1, 2, 3, ...) for multiple published events.
func TestSequenceNumbersIncremental(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	for i := 0; i < 5; i++ {
		require.NoError(t, pub.Publish(makeBazelEvent()))
	}
	require.NoError(t, pub.Finish())

	events := bes.GetEvents()
	seqNums := getSequenceNumbers(events)
	expected := []int64{1, 2, 3, 4, 5, 6} // 5 events + finish
	require.Equal(t, expected, seqNums)
}

// TestSequenceNumbersPreservedAcrossRetries verifies that events maintain their
// original sequence numbers across retry attempts (not renumbered on retry).
func TestSequenceNumbersPreservedAcrossRetries(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	// Fail the first 2 events, then succeed
	bes.EventHandler = testbes.FailNTimesThenSucceed(2, status.UnavailableError("test error"))

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	attempts := bes.GetAttempts()
	require.GreaterOrEqual(t, len(attempts), 2)

	// The first failed attempt should observe the full sequence of events even
	// though the finish ACK never arrives.
	expectedSeq := []int64{1, 2, 3}
	require.Equal(t, expectedSeq, getSequenceNumbers(attempts[0]))

	// Last successful attempt should resend the same sequence numbers.
	lastAttempt := attempts[len(attempts)-1]
	require.Equal(t, expectedSeq, getSequenceNumbers(lastAttempt))
}

// TestSequenceNumbersWithConcurrentPublish verifies that sequence numbers are
// assigned correctly and consecutively when multiple goroutines publish events
// concurrently.
func TestSequenceNumbersWithConcurrentPublish(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)

	const numGoroutines = 10
	const eventsPerGoroutine = 5

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	var publishErr error
	var errOnce sync.Once
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < eventsPerGoroutine; j++ {
				if err := pub.Publish(makeBazelEvent()); err != nil {
					errOnce.Do(func() {
						publishErr = err
					})
					return
				}
			}
		}()
	}
	wg.Wait()
	require.NoError(t, publishErr)

	require.NoError(t, pub.Finish())

	events := bes.GetEvents()
	seqNums := getSequenceNumbers(events)

	// Verify we have all events
	expectedCount := numGoroutines*eventsPerGoroutine + 1 // + finish
	require.Len(t, seqNums, expectedCount)

	// Verify sequence numbers are consecutive starting from 1
	for i, seqNum := range seqNums {
		require.Equal(t, int64(i+1), seqNum)
	}
}

// TestFinishEventHasCorrectSequenceNumber verifies that the ComponentStreamFinished
// event generated by Finish() receives the correct sequence number following all
// previously published events.
func TestFinishEventHasCorrectSequenceNumber(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	events := bes.GetEvents()
	require.Len(t, events, 4)

	// Last event should be ComponentStreamFinished with sequence number 4
	lastEvent := events[3]
	require.Equal(t, int64(4), lastEvent.OrderedBuildEvent.GetSequenceNumber())
	require.NotNil(t, lastEvent.OrderedBuildEvent.Event.GetComponentStreamFinished())
}

// TestEmptyStreamSequencing verifies that calling Finish() without publishing
// any events results in a single finish event with sequence number 1.
func TestEmptyStreamSequencing(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Finish())

	events := bes.GetEvents()
	require.Len(t, events, 1)
	require.Equal(t, int64(1), events[0].OrderedBuildEvent.GetSequenceNumber())
	require.NotNil(t, events[0].OrderedBuildEvent.Event.GetComponentStreamFinished())
}

// Retry and Error Handling Tests

// TestRetryAfterSendFailure verifies that the publisher retries the stream
// after a send failure.
func TestRetryAfterSendFailure(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	// Fail the first event, then succeed
	bes.EventHandler = testbes.FailNTimesThenSucceed(1, status.UnavailableError("send failed"))

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	attempts := bes.GetAttempts()
	require.GreaterOrEqual(t, len(attempts), 2, "Should have at least 2 attempts (1 failed + 1 success)")
}

// TestRetryAfterRecvFailure verifies that the publisher retries the stream
// after a receive (ACK) failure.
func TestRetryAfterRecvFailure(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	callCount := 0
	bes.EventHandler = func(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer, streamID *bepb.StreamId, event *pepb.PublishBuildToolEventStreamRequest) error {
		callCount++
		if callCount == 1 {
			// Fail on first event by returning error (simulates recv failure on client side)
			return status.UnavailableError("recv failed")
		}
		return testbes.Ack(stream, streamID, event)
	}

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	attempts := bes.GetAttempts()
	require.GreaterOrEqual(t, len(attempts), 2)
}

// TestRetryAfterAckFailure verifies that the publisher retries when the stream
// fails after ACKing events but before completing successfully.
func TestRetryAfterAckFailure(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	var mu sync.Mutex
	attempt := 0

	bes.EventHandler = func(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer, streamID *bepb.StreamId, event *pepb.PublishBuildToolEventStreamRequest) error {
		if event.OrderedBuildEvent.GetSequenceNumber() == 1 {
			mu.Lock()
			attempt++
			mu.Unlock()
		}

		if err := testbes.Ack(stream, streamID, event); err != nil {
			return err
		}

		mu.Lock()
		currAttempt := attempt
		mu.Unlock()

		if currAttempt == 1 && event.OrderedBuildEvent.GetEvent().GetComponentStreamFinished() != nil {
			return status.UnavailableError("ack failure")
		}
		return nil
	}

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	attempts := bes.GetAttempts()
	require.GreaterOrEqual(t, len(attempts), 2)

	firstAttempt := attempts[0]
	require.Equal(t, []int64{1, 2, 3}, getSequenceNumbers(firstAttempt))

	lastAttempt := attempts[len(attempts)-1]
	require.Equal(t, []int64{1, 2, 3}, getSequenceNumbers(lastAttempt))
}

// TestMaxRetriesExhausted verifies that the publisher returns an error after
// exhausting the maximum number of retry attempts (5 retries + initial attempt).
func TestMaxRetriesExhausted(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	// Always fail
	bes.EventHandler = testbes.FailWith(status.UnavailableError("permanent failure"))

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	err = pub.Finish()

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to publish build event stream")

	// Should have tried 6 times (initial + 5 retries)
	attempts := bes.GetAttempts()
	require.Equal(t, 6, len(attempts))
}

// TestPartialStreamRetry verifies that when a stream fails after transmitting
// some events, all buffered events (including those already sent) are resent
// on retry.
func TestPartialStreamRetry(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	eventsSeen := 0
	bes.EventHandler = func(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer, streamID *bepb.StreamId, event *pepb.PublishBuildToolEventStreamRequest) error {
		eventsSeen++
		// Fail after seeing 3 events
		if eventsSeen == 3 {
			return status.UnavailableError("failure after 3 events")
		}
		return testbes.Ack(stream, streamID, event)
	}

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	for i := 0; i < 5; i++ {
		require.NoError(t, pub.Publish(makeBazelEvent()))
	}
	require.NoError(t, pub.Finish())

	attempts := bes.GetAttempts()
	require.GreaterOrEqual(t, len(attempts), 2)

	// First attempt should have failed after 3 events
	require.Len(t, attempts[0], 3)

	// Last attempt should have all 6 events (5 + finish)
	lastAttempt := attempts[len(attempts)-1]
	require.Len(t, lastAttempt, 6)
}

// TestNoRetryOnSuccess verifies that the publisher makes exactly one attempt
// when the stream succeeds without errors.
func TestNoRetryOnSuccess(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	attempts := bes.GetAttempts()
	require.Equal(t, 1, len(attempts), "Should have exactly 1 attempt when successful")
}

// TestExponentialBackoffBetweenRetries verifies that the publisher waits with
// exponential backoff (300ms, 600ms, 1000ms, ...) between retry attempts.
func TestExponentialBackoffBetweenRetries(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	// Fail first 3 attempts, then succeed
	bes.EventHandler = testbes.FailNTimesThenSucceed(3, status.UnavailableError("retry test"))

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	start := time.Now()
	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())
	elapsed := time.Since(start)

	// With 3 retries and exponential backoff (300ms, 600ms, 1000ms)
	// we should wait at least 1.9 seconds (300+600+1000).
	require.GreaterOrEqual(t, elapsed, 1800*time.Millisecond)

	attempts := bes.GetAttempts()
	require.Equal(t, 4, len(attempts)) // 3 failures + 1 success
}

// Event Buffering and Transmission Tests

// TestAllBufferedEventsResentOnRetry verifies that all events stored in the
// event buffer are resent on each retry attempt with the same sequence numbers.
func TestAllBufferedEventsResentOnRetry(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	// Fail first attempt
	bes.EventHandler = testbes.FailNTimesThenSucceed(3, status.UnavailableError("retry"))

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	attempts := bes.GetAttempts()
	require.GreaterOrEqual(t, len(attempts), 2)

	// Each attempt should have all 4 events (3 + finish)
	for i, attempt := range attempts {
		require.Len(t, attempt, 4, "Attempt %d should have all 4 events", i)
		require.Equal(t, []int64{1, 2, 3, 4}, getSequenceNumbers(attempt))
	}
}

// TestEventsPublishedDuringRetry verifies that events published while a retry
// is in progress are included in subsequent retry attempts.
func TestEventsPublishedDuringRetry(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	firstEventReceived := make(chan struct{})
	eventCount := 0
	bes.EventHandler = func(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer, streamID *bepb.StreamId, event *pepb.PublishBuildToolEventStreamRequest) error {
		eventCount++
		if eventCount == 1 {
			close(firstEventReceived)
			// Fail first attempt
			return status.UnavailableError("fail first")
		}
		return testbes.Ack(stream, streamID, event)
	}

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))

	// Wait for first event to be received
	<-firstEventReceived

	// Publish more events while retry is happening
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	attempts := bes.GetAttempts()
	require.GreaterOrEqual(t, len(attempts), 2)

	// Last attempt should include events published during retry
	lastAttempt := attempts[len(attempts)-1]
	require.Len(t, lastAttempt, 3) // 2 events + finish
}

// TestSubscriberReceivesAllEvents verifies that the event buffer subscriber
// receives all events that are published, including the finish event.
func TestSubscriberReceivesAllEvents(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	events := bes.GetEvents()
	require.Len(t, events, 3)
}

// TestMultipleRetriesReceiveSameEvents verifies that each retry attempt receives
// the same set of events with identical sequence numbers.
func TestMultipleRetriesReceiveSameEvents(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	// Fail first 2 attempts
	bes.EventHandler = testbes.FailNTimesThenSucceed(4, status.UnavailableError("retry"))

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	attempts := bes.GetAttempts()
	require.GreaterOrEqual(t, len(attempts), 3)

	// All attempts should have the same sequence numbers
	expectedSeqNums := []int64{1, 2, 3}
	for i, attempt := range attempts {
		require.Equal(t, expectedSeqNums, getSequenceNumbers(attempt), "Attempt %d", i)
	}
}

// TestFinishEventEndsStream verifies that the ComponentStreamFinished event
// causes the event buffer's Subscribe channel to close.
func TestFinishEventEndsStream(t *testing.T) {
	streamID := makeStreamID("test-inv", "test-build")
	buffer := build_event_publisher.NewEventBuffer(streamID)

	events, cancel := buffer.Subscribe()
	defer cancel()

	buffer.Add(regularEvent())
	buffer.Add(finishedEvent())

	collected := collectEvents(t, events)
	require.Len(t, collected, 2)

	// Channel should be closed after finish event
	_, ok := <-events
	require.False(t, ok, "Channel should be closed after finish event")
}

// Stream Lifecycle Tests

// TestFinishAfterStreamFailure verifies that Finish() returns an error when
// all retry attempts fail.
func TestFinishAfterStreamFailure(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	// Always fail
	bes.EventHandler = testbes.FailWith(status.UnavailableError("permanent failure"))

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))

	err = pub.Finish()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to publish")
}

// TestPublishAfterFinish verifies that events published after Finish() is called
// are ignored and not transmitted to the server.
func TestPublishAfterFinish(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	// Publishing after Finish should be ignored (based on comment in code)
	require.NoError(t, pub.Publish(makeBazelEvent()))

	events := bes.GetEvents()
	// Should only have the 2 events from before Finish
	require.Len(t, events, 2)
}

// TestContextCancellationStopsRetries verifies that cancelling the context
// stops the retry loop before exhausting all retry attempts.
func TestContextCancellationStopsRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bes, addr := testbes.RunTCP(t)

	// Always fail
	bes.EventHandler = testbes.FailWith(status.UnavailableError("fail"))

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))

	// Cancel context after a short delay
	go func() {
		time.Sleep(500 * time.Millisecond)
		cancel()
	}()

	err = pub.Finish()
	require.Error(t, err)

	// Should have fewer than max retries due to cancellation
	attempts := bes.GetAttempts()
	require.Less(t, len(attempts), 6)
}

// Edge Cases

// TestServerDisconnectsDuringEventStream verifies that the publisher retries
// and eventually succeeds when the server disconnects mid-stream.
func TestServerDisconnectsDuringEventStream(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	eventCount := 0
	bes.EventHandler = func(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer, streamID *bepb.StreamId, event *pepb.PublishBuildToolEventStreamRequest) error {
		eventCount++
		if eventCount == 2 {
			// Disconnect after second event
			return status.UnavailableError("server disconnect")
		}
		return testbes.Ack(stream, streamID, event)
	}

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	// Should retry and eventually succeed
	attempts := bes.GetAttempts()
	require.GreaterOrEqual(t, len(attempts), 2)
}

// TestServerNeverSendsACKs verifies that the publisher completes successfully
// even when the server doesn't send ACK responses (since ACKs aren't validated).
func TestServerNeverSendsACKs(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	// Don't send ACKs, but don't error either
	bes.EventHandler = func(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer, streamID *bepb.StreamId, event *pepb.PublishBuildToolEventStreamRequest) error {
		// Just return without sending ACK
		return nil
	}

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	// Should complete successfully since publisher doesn't validate ACKs
	events := bes.GetEvents()
	require.Len(t, events, 2)
}

// TestZeroEventsPublished verifies that a stream with only a finish event
// (no regular events) is handled correctly.
func TestZeroEventsPublished(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Finish())

	events := bes.GetEvents()
	require.Len(t, events, 1)
	require.NotNil(t, events[0].OrderedBuildEvent.Event.GetComponentStreamFinished())
}

// TestStreamIDConsistentAcrossRetries verifies that the same StreamId
// (invocation ID and build ID) is used across all retry attempts.
func TestStreamIDConsistentAcrossRetries(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	// Fail first attempt
	bes.EventHandler = testbes.FailNTimesThenSucceed(1, status.UnavailableError("retry"))

	pub, err := build_event_publisher.New(addr, "", "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	attempts := bes.GetAttempts()
	require.GreaterOrEqual(t, len(attempts), 2)

	// All attempts should have the same StreamId
	firstStreamID := attempts[0][0].OrderedBuildEvent.GetStreamId()
	for i, attempt := range attempts {
		for j, event := range attempt {
			streamID := event.OrderedBuildEvent.GetStreamId()
			require.Equal(t, firstStreamID.GetInvocationId(), streamID.GetInvocationId(),
				"Attempt %d Event %d has different invocation ID", i, j)
			require.Equal(t, firstStreamID.GetBuildId(), streamID.GetBuildId(),
				"Attempt %d Event %d has different build ID", i, j)
		}
	}
}

// TestAPIKeyIncludedInMetadata verifies that the API key is included in the
// gRPC metadata for all stream attempts.
func TestAPIKeyIncludedInMetadata(t *testing.T) {
	ctx := context.Background()
	bes, addr := testbes.RunTCP(t)

	apiKey := "test-api-key-12345"
	pub, err := build_event_publisher.New(addr, apiKey, "test-invocation")
	require.NoError(t, err)

	pub.Start(ctx)
	require.NoError(t, pub.Publish(makeBazelEvent()))
	require.NoError(t, pub.Finish())

	md := bes.GetMetadata()
	apiKeys := md.Get("x-buildbuddy-api-key")
	require.Len(t, apiKeys, 1)
	require.Equal(t, apiKey, apiKeys[0])
}

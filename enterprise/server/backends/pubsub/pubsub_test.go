package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	channel1Name = "testChannelName"
	message1     = "msg1"
	message2     = "msg2"
	message3     = "msg3"
)

func TestLossyPubSub(t *testing.T) {
	pubSub := NewPubSub(testredis.Start(t).Client())

	ctx := context.Background()
	subscriber := pubSub.Subscribe(ctx, "test")
	ch := subscriber.Chan()

	err := pubSub.Publish(ctx, "test", "hello")
	require.NoError(t, err)

	msg := <-ch
	require.Equal(t, "hello", msg)

	err = subscriber.Close()
	require.NoError(t, err)

	// Channel should be closed
	_, ok := <-ch
	require.False(t, ok)
}

func TestStreamPubSub(t *testing.T) {
	redisHandle := testredis.Start(t)
	pubSub := NewStreamPubSub(redis.NewClient(redisutil.TargetToOptions(redisHandle.Target)))

	ctx := context.Background()

	channel1 := pubSub.UnmonitoredChannel(channel1Name)

	subscriber := pubSub.SubscribeHead(ctx, channel1)
	defer subscriber.Close()
	requireNoMessages(t, subscriber)

	// Publish a message and it should be immediately available to the subscriber.
	err := pubSub.Publish(ctx, channel1, message1)
	require.NoError(t, err)
	requireMessages(t, subscriber, message1)

	// Subscriber should not receive any other messages.
	requireNoMessages(t, subscriber)

	// Publish a second message and verify subscriber receives it.
	err = pubSub.Publish(ctx, channel1, message2)
	require.NoError(t, err)
	requireMessages(t, subscriber, message2)

	// Create a new "head" subscriber which should see both previously published messages.
	subscriber2 := pubSub.SubscribeHead(ctx, channel1)
	requireMessages(t, subscriber2, message1, message2)

	// Create a "tail" subscriber which should only see the last message.
	tailSubscriber := pubSub.SubscribeTail(ctx, channel1)
	requireMessages(t, tailSubscriber, message2)

	// Publish another message which should be seen by all subscribers,
	err = pubSub.Publish(ctx, channel1, message3)
	require.NoError(t, err)
	requireMessages(t, subscriber, message3)
	requireMessages(t, subscriber2, message3)
	requireMessages(t, tailSubscriber, message3)
}

func TestMonitoredPubSub(t *testing.T) {
	flags.Set(t, "remote_execution.pubsub_monitored_stream_check_interval", 50*time.Millisecond)
	// The stream can be checked once per subscription or once per interval
	// for all subscriptions; a lost stream must surface either way.
	for _, batched := range []bool{false, true} {
		t.Run(fmt.Sprintf("batched=%t", batched), func(t *testing.T) {
			flags.Set(t, "remote_execution.pubsub_batch_monitored_stream_checks", batched)
			redisHandle := testredis.Start(t)
			rdb := redis.NewClient(redisutil.TargetToOptions(redisHandle.Target))
			pubSub := NewStreamPubSub(rdb)

			ctx := t.Context()

			err := pubSub.CreateMonitoredChannel(ctx, channel1Name)
			require.NoError(t, err)
			channel1 := pubSub.MonitoredChannel(channel1Name)

			subscriber := pubSub.SubscribeHead(ctx, channel1)
			defer subscriber.Close()
			requireNoMessages(t, subscriber)

			// Publish a message and it should be immediately available to the subscriber.
			err = pubSub.Publish(ctx, channel1, message1)
			require.NoError(t, err)
			requireMessages(t, subscriber, message1)

			// Verify the intact stream passes a check, and consume any premature
			// subscription error before restarting Redis and losing the stream.
			require.NoError(t, pubSub.checkMonitoredChannelExists(ctx, channel1))
			requireNoMessages(t, subscriber)
			redisHandle.Restart()

			err = requireError(t, subscriber)
			require.True(t, status.IsUnavailableError(err), "expected UNAVAILABLE error but got %s", err)
			require.Contains(t, err.Error(), "disappeared")
		})
	}
}

func TestDeleteMonitoredChannel(t *testing.T) {
	flags.Set(t, "remote_execution.pubsub_monitored_stream_check_interval", 50*time.Millisecond)
	// A deleted stream must surface whether it is checked per subscription
	// or in the shared batch.
	for _, batched := range []bool{false, true} {
		t.Run(fmt.Sprintf("batched=%t", batched), func(t *testing.T) {
			flags.Set(t, "remote_execution.pubsub_batch_monitored_stream_checks", batched)
			redisHandle := testredis.Start(t)
			pubSub := NewStreamPubSub(redis.NewClient(redisutil.TargetToOptions(redisHandle.Target)))

			ctx := t.Context()

			err := pubSub.CreateMonitoredChannel(ctx, channel1Name)
			require.NoError(t, err)
			channel1 := pubSub.MonitoredChannel(channel1Name)

			subscriber := pubSub.SubscribeHead(ctx, channel1)
			defer subscriber.Close()

			err = pubSub.Publish(ctx, channel1, message1)
			require.NoError(t, err)
			requireMessages(t, subscriber, message1)

			// Verify the intact stream passes a check, and consume any premature
			// subscription error before deleting the stream. Connections remain
			// intact, so the existence check must detect the loss.
			require.NoError(t, pubSub.checkMonitoredChannelExists(ctx, channel1))
			requireNoMessages(t, subscriber)

			err = pubSub.DeleteMonitoredChannel(ctx, channel1Name)
			require.NoError(t, err)

			err = requireError(t, subscriber)
			require.True(t, status.IsUnavailableError(err), "expected UNAVAILABLE error but got %s", err)
			require.Contains(t, err.Error(), "disappeared")

			// Deleting a non-existent channel should be a no-op.
			err = pubSub.DeleteMonitoredChannel(ctx, channel1Name)
			require.NoError(t, err)
		})
	}
}

func TestStreamExistenceChecker(t *testing.T) {
	// A short interval keeps the checks quick; each check waits for the
	// next pipeline execution.
	flags.Set(t, "remote_execution.pubsub_monitored_stream_check_interval", 50*time.Millisecond)
	redisHandle := testredis.Start(t)
	rdb := redis.NewClient(redisutil.TargetToOptions(redisHandle.Target))
	pubSub := NewStreamPubSub(rdb)
	ctx := t.Context()

	checker := pubSub.checker

	// An intact stream checks clean, and a stream that was never created
	// reports as disappeared on the first pipeline that reads it.
	err := pubSub.CreateMonitoredChannel(ctx, channel1Name)
	require.NoError(t, err)
	err = checker.check(ctx, pubSub.MonitoredChannel(channel1Name))
	require.NoError(t, err)
	err = checker.check(ctx, pubSub.MonitoredChannel("never-created"))
	require.True(t, status.IsUnavailableError(err), "expected UNAVAILABLE error but got %s", err)
	require.Contains(t, err.Error(), "disappeared")

	// With Redis gone, reads go unanswered. The check queues on later
	// pipelines and only gives up after batchedCheckAttempts in a row, which
	// its message records.
	redisHandle.Shutdown()
	err = checker.check(ctx, pubSub.MonitoredChannel(channel1Name))
	require.True(t, status.IsUnavailableError(err), "expected UNAVAILABLE error but got %s", err)
	require.Contains(t, err.Error(), fmt.Sprintf("after %d attempts", batchedCheckAttempts))
}

func TestStreamExistenceChecker_CanceledContext(t *testing.T) {
	flags.Set(t, "remote_execution.pubsub_monitored_stream_check_interval", 50*time.Millisecond)
	rdb := testredis.Start(t).Client()
	pubSub := NewStreamPubSub(rdb)
	checker := pubSub.checker
	channel := pubSub.MonitoredChannel(channel1Name)

	// Hold the batch open so the caller can only return through cancellation.
	checker.pipe = rdb.Pipeline()
	checker.executed = make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := checker.checkOnce(ctx, channel.name)
	require.ErrorIs(t, err, context.Canceled)

	// Complete the canceled caller's batch, then verify a live caller can check.
	checker.execute()
	require.ErrorContains(t, checker.check(t.Context(), channel), "disappeared")
}

func TestStreamExistenceChecker_ShardFailure(t *testing.T) {
	redisHandle := testredis.StartSharded(t, 2)
	addrs := make(map[string]string)
	for i, shard := range redisHandle.Shards {
		addrs[fmt.Sprintf("shard%d", i)] = redisutil.TargetToOptions(shard.Target).Addr
	}
	rdb := redis.NewRing(&redis.RingOptions{
		Addrs: addrs,
		// Keep the keys on their original shards while testing failed reads.
		HeartbeatFrequency: time.Hour,
		Dialer: func(ctx context.Context, _, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		},
	})
	t.Cleanup(func() { require.NoError(t, rdb.Close()) })
	pubSub := NewStreamPubSub(rdb)
	ctx := t.Context()
	shardClient := redisHandle.Shards[0].Client()
	var unavailable, healthy *Channel
	// Find streams on both shards without depending on the Ring's hash function.
	for i := 0; i < 100 && (unavailable == nil || healthy == nil); i++ {
		name := fmt.Sprintf("stream%d", i)
		require.NoError(t, pubSub.CreateMonitoredChannel(ctx, name))
		channel := pubSub.MonitoredChannel(name)
		exists, err := shardClient.Exists(ctx, channel.name).Result()
		require.NoError(t, err)
		if exists == 1 {
			unavailable = channel
		} else {
			healthy = channel
		}
	}
	require.NotNil(t, unavailable)
	require.NotNil(t, healthy)

	// Put both reads in the same pipeline, then take only the first shard down.
	checker := pubSub.checker
	checker.pipe = rdb.Pipeline()
	checker.executed = make(chan struct{})
	bad := make(chan error, 1)
	go func() {
		_, err := checker.checkOnce(ctx, unavailable.name)
		bad <- err
	}()
	good := make(chan error, 1)
	go func() {
		result, err := checker.checkOnce(ctx, healthy.name)
		good <- checkMonitoredStreamResult(healthy, result, err)
	}()
	require.Eventually(t, func() bool {
		checker.mu.Lock()
		defer checker.mu.Unlock()
		return checker.pipe.Len() == 2
	}, time.Second, time.Millisecond)
	redisHandle.Shards[0].Shutdown()
	checker.execute()

	// The failed shard's transport error must not invalidate the other shard's
	// successful reply, even though Exec returns an error for the whole pipeline.
	err := <-bad
	require.True(t, retryable(err), "expected a transport error, got %v", err)
	require.NoError(t, <-good)
}

// disconnectAfterStreamReplyConn closes a wrapped connection after delivering
// the pubsub start marker message, leaving subsequent replies in the pipeline unread.
type disconnectAfterStreamReplyConn struct {
	net.Conn
	disconnect *atomic.Bool
	reply      []byte
}

func (c *disconnectAfterStreamReplyConn) Read(b []byte) (int, error) {
	if !c.disconnect.Load() {
		return c.Conn.Read(b)
	}
	// Prevent go-redis from buffering later replies before the connection is
	// closed. The marker is the final value in the stream's first entry.
	n, err := c.Conn.Read(b[:min(1, len(b))])
	c.reply = append(c.reply, b[:n]...)
	if bytes.HasSuffix(c.reply, []byte(streamStartMarkerMessage+"\r\n")) && c.disconnect.CompareAndSwap(true, false) {
		_ = c.Conn.Close()
	}
	return n, err
}

func TestStreamExistenceChecker_DroppedConnection(t *testing.T) {
	flags.Set(t, "remote_execution.pubsub_monitored_stream_check_interval", 50*time.Millisecond)
	redisHandle := testredis.Start(t)
	var disconnect atomic.Bool
	// Exercise Ring's pipeline path, which disables retries in shard clients.
	// Recovery must come from the checker's retries on subsequent pipelines.
	rdb := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{"shard": redisutil.TargetToOptions(redisHandle.Target).Addr},
		Dialer: func(ctx context.Context, _, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "unix", addr)
			if err != nil {
				return nil, err
			}
			return &disconnectAfterStreamReplyConn{Conn: conn, disconnect: &disconnect}, nil
		},
	})
	t.Cleanup(func() { require.NoError(t, rdb.Close()) })
	counter := testredis.NewCommandCounter("XREAD")
	rdb.AddHook(counter)
	pubSub := NewStreamPubSub(rdb)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, pubSub.CreateMonitoredChannel(ctx, "answered"))
	require.NoError(t, pubSub.CreateMonitoredChannel(ctx, "unanswered"))

	// Hold the first batch open until all checks are queued, then execute it
	// explicitly. Later batches use the checker's normal timer.
	checker := pubSub.checker
	checker.pipe = rdb.Pipeline()
	checker.executed = make(chan struct{})
	var results []chan error
	for i, name := range []string{"missing", "answered", "unanswered"} {
		result := make(chan error, 1)
		results = append(results, result)
		go func() { result <- checker.check(ctx, pubSub.MonitoredChannel(name)) }()
		require.Eventually(t, func() bool {
			checker.mu.Lock()
			defer checker.mu.Unlock()
			return checker.pipe.Len() == i+1
		}, time.Second, time.Millisecond)
	}

	// Redis returns the missing-stream reply and the first intact stream,
	// then the connection drops before the final reply can be read.
	disconnect.Store(true)
	checker.execute()
	require.False(t, disconnect.Load(), "the connection should have dropped after a stream reply")

	// A genuine missing stream is reported immediately. Both intact streams
	// recover, including the read whose successful reply go-redis invalidates
	// when the connection drops later in the pipeline.
	err := <-results[0]
	require.True(t, status.IsUnavailableError(err), "expected UNAVAILABLE error but got %s", err)
	require.Contains(t, err.Error(), "disappeared")
	require.NoError(t, <-results[1])
	require.NoError(t, <-results[2])
	require.Equal(t, int64(5), counter.Count(), "only the two intact streams should be retried")
}

func requireNoMessages(t *testing.T, subscriber *StreamSubscription) {
	select {
	case msg, ok := <-subscriber.Chan():
		if !ok {
			assert.FailNow(t, "subscriber channel closed prematurely")
		}
		assert.FailNow(t, "received PubSub message but none were expected", "message: %q", msg)
	case <-time.After(500 * time.Millisecond):
		return
	}
}

func requireMessages(t *testing.T, subscriber *StreamSubscription, expectedMessages ...string) {
	done := false
	var receivedMsgs []string
	for !done {
		select {
		case msg, ok := <-subscriber.Chan():
			if !ok {
				assert.FailNow(t, "subscriber channel closed prematurely")
			}
			require.NoError(t, msg.Err, "expected message, but got error")
			receivedMsgs = append(receivedMsgs, msg.Data)
			if len(receivedMsgs) == len(expectedMessages) {
				done = true
			}
		case <-time.After(2 * time.Second):
			done = true
		}
	}

	if len(receivedMsgs) == 0 {
		assert.FailNow(t, "expected PubSub messages to be available, but none received")
	}

	require.Equal(t, expectedMessages, receivedMsgs, "received PubSub messages did not match expected messages")
}

func requireError(t *testing.T, subscriber *StreamSubscription) error {
	select {
	case msg, ok := <-subscriber.Chan():
		if !ok {
			assert.FailNow(t, "subscriber channel closed prematurely")
		}
		require.Error(t, msg.Err, "subscriber should have returned an error")
		return msg.Err
	case <-time.After(2 * time.Second):
		assert.FailNow(t, "expected to receive an error but none received")
	}
	return nil
}

package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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

func TestPubSub(t *testing.T) {
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
	requireMessages(t, subscriber, message3)
	requireMessages(t, subscriber2, message3)
	requireMessages(t, tailSubscriber, message3)
}

func TestMonitoredPubSub(t *testing.T) {
	redisHandle := testredis.Start(t)
	pubSub := NewStreamPubSub(redis.NewClient(redisutil.TargetToOptions(redisHandle.Target)))

	ctx := context.Background()

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

	redisHandle.Restart()

	err = requireError(t, subscriber)
	require.True(t, status.IsUnavailableError(err), "expected UNAVAILABLE error but got %s", err)
	require.Contains(t, err.Error(), "disappeared")
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

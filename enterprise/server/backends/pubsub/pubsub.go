package pubsub

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/go-redis/redis/v8"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

type PubSub struct {
	rdb *redis.Client
}

// NewPubSub creates a PubSub client based on the built-in Redis pubsub commands.
// Note that this mechanism is "lossy" in the sense that published messages are lost if there are no listeners.
// See NewListPubSub for a Redis list-based implementation that retains messages even if there are no subscribers.
func NewPubSub(redisClient *redis.Client) *PubSub {
	return &PubSub{
		rdb: redisClient,
	}
}

func (p *PubSub) Publish(ctx context.Context, channelName string, message string) error {
	return p.rdb.Publish(ctx, channelName, message).Err()
}

// To prevent resource leakage, you should close the subscriber when done.
// For example:
//  subscriber := ps.Subscribe(ctx, channelName)
//  defer subscriber.Close()
//  for m := range subscriber.Chan() {
//    // GOT CALLBACK!
//  }
func (p *PubSub) Subscribe(ctx context.Context, channelName string) interfaces.Subscriber {
	return &Subscriber{
		ps:  p.rdb.Subscribe(ctx, channelName),
		ctx: ctx,
	}
}

type Subscriber struct {
	ps  *redis.PubSub
	ctx context.Context
}

func (s *Subscriber) Close() error {
	return s.ps.Close()
}

func (s *Subscriber) Chan() <-chan string {
	internalChannel := s.ps.Channel()
	externalChannel := make(chan string)
	go func() {
		for m := range internalChannel {
			select {
			case externalChannel <- m.Payload:
			case <-s.ctx.Done():
				return
			}
		}
	}()
	return externalChannel
}

const (
	listTTL = 15 * time.Minute
)

type ListPubSub struct {
	rdb *redis.Client
}

// NewListPubSub creates a PubSub client based on a Redis-list.
// Publishes are done via RPUSH and subscribes are performed via BLPOP.
// The underlying list TTL is automatically extended to 15 minutes whenever a new value is published.
//
// CAUTION: Only a single consumer will receive a given published message.
// Think carefully about whether you may have multiple concurrent consumers and the behavior you expect.
func NewListPubSub(redisClient *redis.Client) interfaces.PubSub {
	return &ListPubSub{
		rdb: redisClient,
	}
}

func (p *ListPubSub) Publish(ctx context.Context, channelName string, message string) error {
	pipe := p.rdb.TxPipeline()
	pipe.RPush(ctx, channelName, message)
	pipe.Expire(ctx, channelName, listTTL)
	_, err := pipe.Exec(ctx)
	return err
}

type channelSubscriber struct {
	cancel context.CancelFunc
	ch     <-chan string
}

func (s *channelSubscriber) Close() error {
	s.cancel()
	return nil
}

func (s *channelSubscriber) Chan() <-chan string {
	return s.ch
}

func (p *ListPubSub) Subscribe(ctx context.Context, channelName string) interfaces.Subscriber {
	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan string)
	go func() {
		defer close(ch)
		for {
			vals, err := p.rdb.BLPop(ctx, 0*time.Second, channelName).Result()
			if err != nil {
				if err != context.Canceled {
					log.Errorf("Error executing BLPop: %v", err)
				}
				return
			}

			// Should not happen.
			if vals == nil || len(vals) != 2 {
				log.Errorf("Wrong number of elemenents returned from BLPop: %v", vals)
				return
			}
			// ... and neither should this.
			if vals[0] != channelName {
				log.Errorf("Returned key %q did not match expected key %q", vals[0], channelName)
				return
			}
			select {
			case ch <- vals[1]:
			case <-ctx.Done():
				return
			}
		}
	}()
	return &channelSubscriber{
		cancel: cancel,
		ch:     ch,
	}
}

// To keep things simple for now, we maintain streams with a single value per element stored under the following key.
const streamDataField = "data"

type StreamPubSub struct {
	rdb *redis.Client
}

// NewStreamPubSub creates a PubSub client based on a Redis-stream.
func NewStreamPubSub(redisClient *redis.Client) *StreamPubSub {
	return &StreamPubSub{rdb: redisClient}
}

func (p *StreamPubSub) subscribe(ctx context.Context, channelName string, startFromTail bool) interfaces.Subscriber {
	ch := make(chan string)
	ctx, cancel := context.WithCancel(ctx)

	deliverMsg := func(msg *redis.XMessage) bool {
		data, ok := msg.Values[streamDataField]
		if !ok {
			log.Errorf("Message %q on stream %q missing data field", msg.ID, channelName)
			return false
		}
		str, ok := data.(string)
		if !ok {
			log.Errorf("Message %q on stream %q is of type %T, wanted string", msg.ID, channelName, data)
			return false
		}

		select {
		case ch <- str:
			return true
		case <-ctx.Done():
			return false
		}
	}

	go func() {
		defer close(ch)

		// Start from beginning of stream.
		streamID := "0"

		// If starting from tail, check if the stream has any elements.
		// If it does then publish the last element and subscribe to messages following that element.
		if startFromTail {
			msgs, err := p.rdb.XRevRangeN(ctx, channelName, "+", "-", 1).Result()
			if err != nil {
				log.Errorf("Unable to retrieve last element of stream %q: %s", channelName, err)
				return
			}
			if len(msgs) == 1 {
				msg := msgs[0]
				streamID = msg.ID
				if !deliverMsg(&msg) {
					return
				}
			}
		}

		for {
			result, err := p.rdb.XRead(ctx, &redis.XReadArgs{
				Streams: []string{channelName, streamID},
				Block:   0, // Block indefinitely.
			}).Result()
			if err != nil {
				if err != context.Canceled {
					log.Errorf("Error reading from stream %q: %s", channelName, err)
				}
				return
			}

			// We are subscribing to a single stream so there should be exactly one response.
			if len(result) != 1 {
				log.Errorf("Did not receive exactly one result for channel %q, got %d", channelName, len(result))
				return
			}

			for _, msg := range result[0].Messages {
				if !deliverMsg(&msg) {
					return
				}
				streamID = msg.ID
			}
		}
	}()
	return &channelSubscriber{
		cancel: cancel,
		ch:     ch,
	}
}

// SubscribeHead returns a subscription for all previous and future message on the stream.
func (p *StreamPubSub) SubscribeHead(ctx context.Context, channelName string) interfaces.Subscriber {
	// Subscribe from the beginning of the stream.
	return p.subscribe(ctx, channelName, false /*startFromTail=*/)
}

// SubscribeTail returns a subscription for messages starting from the last message already on the stream, if any.
func (p *StreamPubSub) SubscribeTail(ctx context.Context, channelName string) interfaces.Subscriber {
	// Subscribe from the last elements of the stream, if any.
	return p.subscribe(ctx, channelName, true /*startFromTail=*/)
}

func (p *StreamPubSub) Publish(ctx context.Context, channelName string, message string) error {
	pipe := p.rdb.TxPipeline()
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: channelName,
		Values: map[string]interface{}{streamDataField: message},
	})
	pipe.Expire(ctx, channelName, listTTL)
	_, err := pipe.Exec(ctx)
	return err
}

package pubsub

import (
	"context"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

type PubSub struct {
	rdb redis.UniversalClient
}

// NewPubSub creates a PubSub client based on the built-in Redis pubsub commands.
// Note that this mechanism is "lossy" in the sense that published messages are lost if there are no listeners.
// See NewListPubSub for a Redis list-based implementation that retains messages even if there are no subscribers.
func NewPubSub(redisClient redis.UniversalClient) *PubSub {
	return &PubSub{
		rdb: redisClient,
	}
}

func (p *PubSub) Publish(ctx context.Context, channelName string, message string) error {
	return p.rdb.Publish(ctx, channelName, message).Err()
}

// To prevent resource leakage, you should close the subscriber when done.
// For example:
//
//	subscriber := ps.Subscribe(ctx, channelName)
//	defer subscriber.Close()
//	for m := range subscriber.Chan() {
//	  // GOT CALLBACK!
//	}
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
	listTTL = 8 * time.Hour
)

type ListPubSub struct {
	rdb redis.UniversalClient
}

// NewListPubSub creates a PubSub client based on a Redis-list.
// Publishes are done via RPUSH and subscribes are performed via BLPOP.
// The underlying list TTL is automatically extended to 15 minutes whenever a new value is published.
//
// CAUTION: Only a single consumer will receive a given published message.
// Think carefully about whether you may have multiple concurrent consumers and the behavior you expect.
func NewListPubSub(redisClient redis.UniversalClient) interfaces.PubSub {
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
				log.Errorf("Wrong number of elements returned from BLPop: %v", vals)
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

const (
	// To keep things simple for now, we maintain streams with a single value per element stored under the following key.
	streamDataField = "data"
	// Key prefix to identify monitored channels.
	monitoredKeyPrefix = "monitoredPubSub/"
	// For "monitored" channels, we publish a dummy message at channel creation time so we have a means to verify
	// whether the object still exists in Redis.
	streamStartMarkerMessage = "this is a dummy message to verify existence of stream"
	// Frequency at which we check whether the PubSub stream still exists in Redis.
	monitoredChannelExistenceCheckInterval = 1 * time.Second
)

type StreamPubSub struct {
	rdb redis.UniversalClient
}

// NewStreamPubSub creates a PubSub client based on a Redis-stream.
func NewStreamPubSub(redisClient redis.UniversalClient) *StreamPubSub {
	return &StreamPubSub{rdb: redisClient}
}

type Channel struct {
	name string
}

func (c *Channel) String() string {
	return c.name
}

func (p *StreamPubSub) CreateMonitoredChannel(ctx context.Context, name string) error {
	channelName := monitoredKeyPrefix + name
	channel := &Channel{name: channelName}
	if err := p.Publish(ctx, channel, streamStartMarkerMessage); err != nil {
		return err
	}
	return nil
}

func (p *StreamPubSub) MonitoredChannel(name string) *Channel {
	return &Channel{name: monitoredKeyPrefix + name}
}

func (p *StreamPubSub) UnmonitoredChannel(name string) *Channel {
	return &Channel{name: name}
}

type Message struct {
	Err  error
	Data string
}

type StreamSubscription struct {
	cancel context.CancelFunc
	ch     <-chan *Message
}

func (s *StreamSubscription) Close() error {
	s.cancel()
	return nil
}

func (s *StreamSubscription) Chan() <-chan *Message {
	return s.ch
}

func (p *StreamPubSub) extractMsgData(channel *Channel, msg *redis.XMessage) (string, error) {
	data, ok := msg.Values[streamDataField]
	if !ok {
		return "", status.FailedPreconditionErrorf("Message %q on stream %q missing data field", msg.ID, channel.name)
	}
	str, ok := data.(string)
	if !ok {
		return "", status.FailedPreconditionErrorf("Message %q on stream %q is of type %T, wanted string", msg.ID, channel.name, data)
	}

	return str, nil
}

func (p *StreamPubSub) deliverMsg(ctx context.Context, psChannel *Channel, outCh chan *Message, msg *redis.XMessage) bool {
	data, err := p.extractMsgData(psChannel, msg)
	if err != nil {
		alert.UnexpectedEvent("could not extract PubSub message contents", "error: %s", err)
		return false
	}
	if data == streamStartMarkerMessage {
		return true
	}

	select {
	case outCh <- &Message{Data: data}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *StreamPubSub) checkMonitoredChannelExists(ctx context.Context, channel *Channel) error {
	result, err := p.rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{channel.name, "0"},
		Block:   -1, // No blocking.
	}).Result()
	if err == redis.Nil {
		return status.UnavailableErrorf("PubSub channel %q disappeared", channel.name)
	}
	if err != nil {
		return status.UnavailableErrorf("unable to check existence of PubSub channel %q: %s", channel.name, err)
	}
	if len(result) != 1 {
		return status.UnknownErrorf("invalid xread return for channel %q", channel.name)
	}
	stream := result[0]
	if len(stream.Messages) == 0 {
		return status.UnavailableErrorf("PubSub channel %q disappeared", channel.name)
	}
	data, err := p.extractMsgData(channel, &stream.Messages[0])
	if err != nil {
		return status.UnavailableErrorf("unable to check existence of PubSub channel %q", channel.name)
	}
	if data != streamStartMarkerMessage {
		return status.UnavailableErrorf("PubSub channel %q does not start with verification message", channel.name)
	}
	return nil
}

func (p *StreamPubSub) subscribe(ctx context.Context, psChannel *Channel, startFromTail bool) *StreamSubscription {
	ctx, cancel := context.WithCancel(ctx)

	// If this is a monitored channel, start a goroutine that will periodically check that the stream still exists or
	// publish an error if it does not.
	monChan := make(chan *Message)
	if strings.HasPrefix(psChannel.name, monitoredKeyPrefix) {
		go func() {
			defer close(monChan)
			for {
				if err := p.checkMonitoredChannelExists(ctx, psChannel); err != nil {
					monChan <- &Message{Err: err}
					return
				}
				select {
				case <-time.After(monitoredChannelExistenceCheckInterval):
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	msgChan := make(chan *Message)
	go func() {
		defer close(msgChan)

		// Start from beginning of stream.
		streamID := "0"

		// If starting from tail, check if the stream has any elements.
		// If it does then publish the last element and subscribe to messages following that element.
		if startFromTail {
			msgs, err := p.rdb.XRevRangeN(ctx, psChannel.name, "+", "-", 1).Result()
			if err != nil {
				log.CtxErrorf(ctx, "Unable to retrieve last element of stream!!! %q: %s", psChannel.name, err)
				msgChan <- &Message{Err: err}
				return
			}
			if len(msgs) == 1 {
				msg := msgs[0]
				streamID = msg.ID
				if !p.deliverMsg(ctx, psChannel, msgChan, &msg) {
					return
				}
			}
		}

		for {
			result, err := p.rdb.XRead(ctx, &redis.XReadArgs{
				Streams: []string{psChannel.name, streamID},
				Block:   0, // Block indefinitely
			}).Result()
			if err != nil {
				if err != context.Canceled {
					log.CtxErrorf(ctx, "Error reading from stream %q: %s", psChannel.name, err)
					msgChan <- &Message{Err: err}
				}
				return
			}
			// We are subscribing to a single stream so there should be exactly one response.
			if len(result) != 1 {
				log.CtxErrorf(ctx, "Did not receive exactly one result for channel %q, got %d", psChannel.name, len(result))
				return
			}
			for _, msg := range result[0].Messages {
				if !p.deliverMsg(ctx, psChannel, msgChan, &msg) {
					return
				}
				streamID = msg.ID
			}
		}
	}()

	ch := make(chan *Message)
	go func() {
		defer cancel()
		defer close(ch)
		for {
			select {
			case msg, ok := <-monChan:
				if !ok {
					return
				}
				ch <- msg
			case msg, ok := <-msgChan:
				if !ok {
					return
				}
				ch <- msg
			case <-ctx.Done():
				return
			}
		}
	}()

	return &StreamSubscription{
		cancel: cancel,
		ch:     ch,
	}
}

// SubscribeHead returns a subscription for all previous and future message on the stream.
func (p *StreamPubSub) SubscribeHead(ctx context.Context, channel *Channel) *StreamSubscription {
	// Subscribe from the beginning of the stream.
	return p.subscribe(ctx, channel, false /*startFromTail=*/)
}

// SubscribeTail returns a subscription for messages starting from the last message already on the stream, if any.
func (p *StreamPubSub) SubscribeTail(ctx context.Context, channel *Channel) *StreamSubscription {
	// Subscribe from the last elements of the stream, if any.
	return p.subscribe(ctx, channel, true /*startFromTail=*/)
}

func (p *StreamPubSub) Publish(ctx context.Context, channel *Channel, message string) error {
	pipe := p.rdb.TxPipeline()
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: channel.name,
		Values: map[string]interface{}{streamDataField: message},
	})
	pipe.Expire(ctx, channel.name, listTTL)
	_, err := pipe.Exec(ctx)
	return err
}

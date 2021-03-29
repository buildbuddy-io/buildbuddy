package pubsub

import (
	"context"
	"log"
	"time"

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
		ps: p.rdb.Subscribe(ctx, channelName),
	}
}

type Subscriber struct {
	ps *redis.PubSub
}

func (s *Subscriber) Close() error {
	return s.ps.Close()
}

func (s *Subscriber) Chan() <-chan string {
	internalChannel := s.ps.Channel()
	externalChannel := make(chan string)
	go func() {
		for m := range internalChannel {
			externalChannel <- m.Payload
		}
	}()
	return externalChannel
}

const (
	listTTL = 15 * time.Minute
	// Active subscribe will refresh the list TTL periodically at this interval.
	ttlRefreshInterval = 5 * time.Minute
)

type ListPubSub struct {
	rdb *redis.Client
}

// NewListPubSub creates a PubSub client based on a Redis-list.
// Publishes are done via RPUSH and subscribes are performed via BLPOP.
// The underlying list TTL is automatically extended to 15 minutes whenever a new value is published and periodically
// by active subscribers.
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

type listSubscriber struct {
	cancel context.CancelFunc
	ch     <-chan string
}

func (s *listSubscriber) Close() error {
	s.cancel()
	return nil
}

func (s *listSubscriber) Chan() <-chan string {
	return s.ch
}

func (p *ListPubSub) Subscribe(ctx context.Context, channelName string) interfaces.Subscriber {
	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan string)
	go func() {
		defer close(ch)
		for {
			vals, err := p.rdb.BLPop(ctx, ttlRefreshInterval, channelName).Result()
			// Timeout.
			if err == redis.Nil {
				if err := p.rdb.Expire(ctx, channelName, listTTL).Err(); err != nil {
					log.Printf("Was not able to refresh TTL for %q: %v", channelName, err)
				}
				continue
			}
			if err != nil {
				if err != context.Canceled {
					log.Printf("Error executing BLPop: %v", err)
				}
				return
			}

			// Should not happen.
			if vals == nil || len(vals) != 2 {
				log.Printf("Wrong number of elemenents returned from BLPop: %v", vals)
				return
			}
			// ... and neither should this.
			if vals[0] != channelName {
				log.Printf("Returned key %q did not match expected key %q", vals[0], channelName)
				return
			}
			ch <- vals[1]
		}
	}()
	return &listSubscriber{
		cancel: cancel,
		ch:     ch,
	}
}

package pubsub

import (
	"context"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

type TestSubscriber struct {
	ch chan string
}

func (s *TestSubscriber) Close() error {
	return nil
}

func (s *TestSubscriber) Chan() <-chan string {
	return s.ch
}

type TestPubSub struct {
	subs   map[string][]chan string
	mu     sync.RWMutex
	closed bool
}

func NewTestPubSub() interfaces.PubSub {
	return &TestPubSub{
		mu:     sync.RWMutex{},
		subs:   make(map[string][]chan string, 0),
		closed: false,
	}
}

func (ps *TestPubSub) Publish(ctx context.Context, channelName string, message string) error {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if ps.closed {
		return nil
	}

	for _, ch := range ps.subs[channelName] {
		ch <- message
	}

	return nil
}

func (ps *TestPubSub) Subscribe(ctx context.Context, channelName string) interfaces.Subscriber {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ch := make(chan string, 1)
	ps.subs[channelName] = append(ps.subs[channelName], ch)

	return &TestSubscriber{
		ch: ch,
	}
}

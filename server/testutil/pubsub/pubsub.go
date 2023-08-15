package pubsub

import (
	"context"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

type TestSubscriber struct {
	name string
	ch   chan string

	mu     sync.Mutex
	closed bool
}

func (s *TestSubscriber) publish(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		// In practice, we wouldn't be alerted to this error since the
		// subscriber wouldn't exist once closed. But in tests, publishing a
		// message to a topic with no subscribers might be indicative of a bug,
		// so we log a warning here for easier debugging.
		log.Warningf("TestSubscriber on channel %q is closed; dropping message %q", s.name, message)
		return
	}

	select {
	case s.ch <- message:
	default:
		log.Warningf("TestSubscriber on channel %q dropped message %q", s.name, message)
	}
}

func (s *TestSubscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.ch)
	s.closed = true
	return nil
}

func (s *TestSubscriber) Chan() <-chan string {
	return s.ch
}

type TestPubSub struct {
	subs   map[string][]*TestSubscriber
	mu     sync.RWMutex
	closed bool
}

func NewTestPubSub() interfaces.PubSub {
	return &TestPubSub{
		mu:     sync.RWMutex{},
		subs:   make(map[string][]*TestSubscriber, 0),
		closed: false,
	}
}

func (ps *TestPubSub) Publish(ctx context.Context, channelName string, message string) error {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if ps.closed {
		return nil
	}

	for _, sub := range ps.subs[channelName] {
		sub.publish(message)
	}

	return nil
}

func (ps *TestPubSub) Subscribe(ctx context.Context, channelName string) interfaces.Subscriber {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	sub := &TestSubscriber{
		name: channelName,
		ch:   make(chan string, 1),
	}
	ps.subs[channelName] = append(ps.subs[channelName], sub)

	return sub
}

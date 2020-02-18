package build_event_handler

import (
	"fmt"
	"sync"

	"github.com/tryflame/buildbuddy/server/blobstore"
	"proto/build_event_stream"
)

const (
	eventChannelBufferSize = 25
)

type EventChannel struct {
	done chan struct{}
	in   chan *build_event_stream.BuildEvent
}

func (c *EventChannel) WriteEvent(e *build_event_stream.BuildEvent, lastEvent bool) {
	if e != nil {
		c.in <- e
	}
	if lastEvent {
		close(c.done)
	}
}

type BuildEventHandler struct {
	bs                 blobstore.Blobstore
	activeChannels     map[string]*EventChannel
	activeChannelsLock sync.RWMutex
}

func NewBuildEventHandler(bs blobstore.Blobstore) *BuildEventHandler {
	return &BuildEventHandler{
		bs:                 bs,
		activeChannels:     make(map[string]*EventChannel),
		activeChannelsLock: sync.RWMutex{},
	}
}

func (b *BuildEventHandler) startChannelProcessor(ec *EventChannel) {
	go func() {
		events := make([]*build_event_stream.BuildEvent, 0)
		receivedDoneSignal := false
		for event := range ec.in {
			if receivedDoneSignal {
				break
			}
			events = append(events, event)
			select {
			case <-ec.done:
				fmt.Printf("got done signal!\n")
				receivedDoneSignal = true
			}
		}
		fmt.Printf("received %d events in total\n", len(events))
		// sort events
		// write to storage and elsewhere
	}()
}

func (b *BuildEventHandler) GetEventChannel(eventID string) *EventChannel {
	b.activeChannelsLock.RLock()
	defer b.activeChannelsLock.RUnlock()

	if _, ok := b.activeChannels[eventID]; ok {
		return b.activeChannels[eventID]
	}
	ec := EventChannel{
		done: make(chan struct{}),
		in:   make(chan *build_event_stream.BuildEvent, eventChannelBufferSize),
	}
	b.activeChannels[eventID] = &ec
	b.startChannelProcessor(&ec)
	return &ec
}

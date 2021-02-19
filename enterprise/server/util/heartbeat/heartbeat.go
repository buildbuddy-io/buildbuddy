package hearbeat

import (
	"context"
	"log"
	"sort"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

const (
	// This node will send heartbeats every this often.
	heartbeatPeriod = 1 * time.Second

	// After this timeout, a node will be removed from the set of active
	// nodes.
	heartbeatTimeout = 3 * heartbeatPeriod

	// How often this node will check if heartbeats are still valid.
	heartbeatCheckPeriod = 100 * time.Millisecond
)

type PeersUpdateFn func(peerSet ...string)

type HeartbeatChannel struct {
	groupName string
	myAddr    string
	peers     map[string]time.Time
	ps        interfaces.PubSub
	updateFn  PeersUpdateFn
}

func NewHeartbeatChannel(ps interfaces.PubSub, myAddr, groupName string, updateFn PeersUpdateFn) *HeartbeatChannel {
	hac := &HeartbeatChannel{
		groupName: groupName,
		myAddr:    myAddr,
		peers:     make(map[string]time.Time, 0),
		ps:        ps,
		updateFn:  updateFn,
	}
	ctx := context.Background()
	go hac.sendHeartbeat(ctx)
	go hac.watchPeers(ctx)
	return hac
}

func (c *HeartbeatChannel) sendHeartbeat(ctx context.Context) {
	for {
		err := c.ps.Publish(ctx, c.groupName, c.myAddr)
		if err != nil {
			log.Printf("HeartbeatChannel(%s): error publishing: %s", c.groupName, err.Error())
		}
		time.Sleep(heartbeatPeriod)
	}
}

func (c *HeartbeatChannel) notifySetChanged() {
	nodes := make([]string, 0, len(c.peers))
	for peer, _ := range c.peers {
		nodes = append(nodes, peer)
	}
	sort.Strings(nodes)
	log.Printf("HeartbeatChannel(%s): peerset changed: %s", c.groupName, nodes)
	c.updateFn(nodes...)
}

func (c *HeartbeatChannel) watchPeers(ctx context.Context) {
	subscriber := c.ps.Subscribe(ctx, c.groupName)
	defer subscriber.Close()
	pubsubChan := subscriber.Chan()
	for {
		select {
		case peer := <-pubsubChan:
			_, ok := c.peers[peer]
			c.peers[peer] = time.Now()
			if !ok {
				c.notifySetChanged()
			}
		case <-time.After(heartbeatCheckPeriod):
			updated := false
			for peer, lastBeat := range c.peers {
				if time.Since(lastBeat) > heartbeatTimeout {
					delete(c.peers, peer)
					updated = true
				}
			}
			if updated {
				c.notifySetChanged()
			}
		}
	}
}

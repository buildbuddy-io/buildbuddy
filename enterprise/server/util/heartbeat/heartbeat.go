package heartbeat

import (
	"context"
	"log"
	"sort"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

const (
	// This node will send heartbeats every this often.
	defaultHeartbeatPeriod = 1 * time.Second

	// After this timeout, a node will be removed from the set of active
	// nodes.
	defaultHeartbeatTimeout = 30 * defaultHeartbeatPeriod

	// How often this node will check if heartbeats are still valid.
	defaultHeartbeatCheckPeriod = 100 * time.Millisecond
)

type PeersUpdateFn func(peerSet ...string)

type HeartbeatChannel struct {
	// This node will send heartbeats every this often.
	Period time.Duration

	// After this timeout, a node will be removed from the set of active
	// nodes.
	Timeout time.Duration

	// How often this node will check if heartbeats are still valid.
	CheckPeriod time.Duration

	groupName        string
	myAddr           string
	peers            map[string]time.Time
	ps               interfaces.PubSub
	updateFn         PeersUpdateFn
	enablePeerExpiry bool
	quit             chan struct{}
}

type Config struct {
	// The address of this node to broadcast to peers.
	MyPublicAddr string
	// The name of the group to broadcast in.
	GroupName string
	// A func(peerSet ...string) that will be called on peerset updates.
	UpdateFn PeersUpdateFn
	// If true, enable peers to be dropped after defaultHeartbeatTimeout.
	EnablePeerExpiry bool
}

func NewHeartbeatChannel(ps interfaces.PubSub, config *Config) *HeartbeatChannel {
	hac := &HeartbeatChannel{
		groupName:        config.GroupName,
		myAddr:           config.MyPublicAddr,
		peers:            make(map[string]time.Time, 0),
		ps:               ps,
		updateFn:         config.UpdateFn,
		quit:             make(chan struct{}),
		enablePeerExpiry: config.EnablePeerExpiry,
		Period:           defaultHeartbeatPeriod,
		Timeout:          defaultHeartbeatTimeout,
		CheckPeriod:      defaultHeartbeatCheckPeriod,
	}
	ctx := context.Background()
	go hac.watchPeers(ctx)
	return hac
}

func (c *HeartbeatChannel) StartAdvertising() {
	close(c.quit)
	c.quit = make(chan struct{})
	go func() {
		for {
			select {
			case <-c.quit:
				return
			case <-time.After(c.Period):
				c.sendHeartbeat(context.Background())
			}
		}
	}()
}

func (c *HeartbeatChannel) StopAdvertising() {
	close(c.quit)
}

func (c *HeartbeatChannel) sendHeartbeat(ctx context.Context) {
	err := c.ps.Publish(ctx, c.groupName, c.myAddr)
	if err != nil {
		log.Printf("HeartbeatChannel(%s): error publishing: %s", c.groupName, err)
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
		case <-time.After(c.CheckPeriod):
			updated := false
			for peer, lastBeat := range c.peers {
				if time.Since(lastBeat) > c.Timeout {
					if c.enablePeerExpiry {
						log.Printf("Peer %q has timed out. LastBeat: %s, timeout: %s", peer, lastBeat, c.Timeout)
						delete(c.peers, peer)
						updated = true
					}
				}
			}
			if updated {
				c.notifySetChanged()
			}
		}
	}
}

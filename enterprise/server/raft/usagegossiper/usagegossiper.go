package usagegossiper

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/hashicorp/serf/serf"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
)

const (
	// Broadcast this nodes set of active replicas after this period.
	defaultBroadcastPeriod = 30 * time.Second
)

type UsageObserver interface {
	OnNodeUsage(usage *rfpb.NodeUsage)
}

type Opts struct {
	// BroadcastPeriod is how often this node will broadcast its set of
	// active replicas.
	BroadcastPeriod time.Duration
}

func DefaultOpts() Opts {
	return Opts{
		BroadcastPeriod: defaultBroadcastPeriod,
	}
}

func TestingOpts() Opts {
	return Opts{
		BroadcastPeriod: 2 * time.Second,
	}
}

type Gossiper struct {
	opts          Opts
	store         rfspb.ApiServer
	gossipManager *gossip.GossipManager
	mu            *sync.Mutex
	started       bool
	broadcastQuit chan struct{}
	observers     []UsageObserver
}

func New(store rfspb.ApiServer, gossipManager *gossip.GossipManager, opts Opts) *Gossiper {
	d := &Gossiper{
		opts:          opts,
		store:         store,
		gossipManager: gossipManager,
		mu:            &sync.Mutex{},
		started:       false,
	}
	// Register the node registry as a gossip listener so that it receives
	// gossip callbacks.
	gossipManager.AddListener(d)
	return d
}

func (d *Gossiper) AddObserver(o UsageObserver) {
	d.observers = append(d.observers, o)
}

func (d *Gossiper) Start() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started {
		return nil
	}
	d.broadcastQuit = make(chan struct{})
	go d.broadcastLoop()

	d.started = true
	log.Debugf("Gossiper started")
	return nil
}

func (d *Gossiper) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.started {
		return nil
	}
	close(d.broadcastQuit)
	d.started = false
	log.Debugf("Gossiper stopped")
	return nil
}

// broadcastLoop does not return; call it from a goroutine.
func (d *Gossiper) broadcastLoop() {
	for {
		select {
		case <-d.broadcastQuit:
			return
		case <-time.After(d.opts.BroadcastPeriod):
			err := d.broadcast()
			if err != nil {
				log.Errorf("Broadcast error: %s", err)
			}
		}
	}
}

// broadcasts a proto containing usage information about this store's
// replicas.
func (d *Gossiper) broadcast() error {
	ctx := context.Background()
	rsp, err := d.store.ListCluster(ctx, &rfpb.ListClusterRequest{})
	if err != nil {
		return err
	}

	nu := &rfpb.NodeUsage{Node: rsp.GetNode()}
	nu.PartitionUsage = rsp.PartitionUsage
	buf, err := proto.Marshal(nu)
	if err != nil {
		return err
	}
	if err := d.gossipManager.SendUserEvent(constants.NodeUsageEvent, buf, false /*=coalesce*/); err != nil {
		return err
	}

	// Need to be very careful about what is broadcast here because the max
	// allowed UserEvent size is 9K, and broadcasting too much could cause
	// slow rebalancing etc.

	// A max ReplicaUsage should be around 8 bytes * 3 = 24 bytes. So 375
	// replica usages should fit in a single gossip message. Use 350 as the
	// target size so there is room for the NHID and a small margin of
	// safety.
	batchSize := 350
	numReplicas := len(rsp.GetRangeReplicas())
	gossiped := false
	for start := 0; start < numReplicas; start += batchSize {
		nu := &rfpb.NodeUsage{Node: rsp.GetNode()}
		end := start + batchSize
		if end > numReplicas {
			end = numReplicas
		}
		for _, rr := range rsp.GetRangeReplicas()[start:end] {
			nu.ReplicaUsage = append(nu.ReplicaUsage, rr.GetReplicaUsage())
		}
		buf, err := proto.Marshal(nu)
		if err != nil {
			return err
		}
		if err := d.gossipManager.SendUserEvent(constants.NodeUsageEvent, buf, false /*=coalesce*/); err != nil {
			return err
		}
		gossiped = true
	}

	// If a node does not yet have any replicas, the loop above will not
	// have gossiped anything. In that case, gossip an "empty" usage event
	// now.
	if !gossiped {
		nu := &rfpb.NodeUsage{Node: rsp.GetNode()}
		buf, err := proto.Marshal(nu)
		if err != nil {
			return err
		}
		if err := d.gossipManager.SendUserEvent(constants.NodeUsageEvent, buf, false /*=coalesce*/); err != nil {
			return err
		}
	}
	return nil
}

// OnEvent listens for other nodes' gossip events and handles them.
func (d *Gossiper) OnEvent(updateType serf.EventType, event serf.Event) {
	switch updateType {
	case serf.EventUser:
		userEvent, _ := event.(serf.UserEvent)
		d.handleEvent(&userEvent)
	default:
		break
	}
}

// handleEvent parses and ingests the data from other nodes' gossip events.
func (d *Gossiper) handleEvent(event *serf.UserEvent) {
	if event.Name != constants.NodeUsageEvent {
		return
	}
	nu := &rfpb.NodeUsage{}
	if err := proto.Unmarshal(event.Payload, nu); err != nil {
		return
	}
	if nu.GetNode() == nil {
		log.Warningf("Ignoring malformed node usage: %+v", nu)
		return
	}
	for _, o := range d.observers {
		o.OnNodeUsage(nu)
	}
}

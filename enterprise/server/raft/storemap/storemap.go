package storemap

import (
	"encoding/base64"
	"flag"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/hashicorp/serf/serf"
	"github.com/jonboulle/clockwork"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

var (
	deadStoreTimeout     = flag.Duration("cache.raft.dead_store_timeout", 5*time.Minute, "The amount of time after which we didn't receive alive status for a node, consider a store dead")
	suspectStoreDuration = flag.Duration("cache.raft.suspect_store_duration", 30*time.Second, "The amount of time we consider a node suspect after it becomes unavailable")
)

type storeStatus int

const (
	storeStatusUnknown storeStatus = iota
	storeStatusDead
	storeStatusAvailable
	storeStatusSuspect
)

type StoreDetail struct {
	usage             *rfpb.StoreUsage
	lastUnavailableAt time.Time
}

type StoreMap struct {
	gossipManager interfaces.GossipService

	mu *sync.RWMutex
	// NHID to StoreDetail
	storeDetails map[string]*StoreDetail

	startTime time.Time
	clock     clockwork.Clock
}

func New(gossipManager interfaces.GossipService, clock clockwork.Clock) *StoreMap {
	sm := &StoreMap{
		mu:            &sync.RWMutex{},
		startTime:     time.Now(),
		storeDetails:  make(map[string]*StoreDetail),
		gossipManager: gossipManager,
		clock:         clock,
	}
	gossipManager.AddListener(sm)
	return sm
}

type ReplicasByStatus struct {
	LiveReplicas    []*rfpb.ReplicaDescriptor
	DeadReplicas    []*rfpb.ReplicaDescriptor
	SuspectReplicas []*rfpb.ReplicaDescriptor
}

func (sm *StoreMap) DivideByStatus(repls []*rfpb.ReplicaDescriptor) *ReplicasByStatus {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	res := &ReplicasByStatus{}
	for _, repl := range repls {
		detail := sm.getDetail(repl.GetNhid())
		status := detail.status(sm.clock)
		switch status {
		case storeStatusAvailable:
			res.LiveReplicas = append(res.LiveReplicas, repl)
		case storeStatusDead:
			res.DeadReplicas = append(res.DeadReplicas, repl)
		case storeStatusSuspect:
			res.SuspectReplicas = append(res.SuspectReplicas, repl)
		case storeStatusUnknown:
			// do nothing
		}
	}
	return res
}

func (sm *StoreMap) getDetail(nhid string) *StoreDetail {
	detail, ok := sm.storeDetails[nhid]
	if !ok {
		detail = &StoreDetail{}
		sm.storeDetails[nhid] = detail
	}
	return detail

}

func (sd *StoreDetail) status(clock clockwork.Clock) storeStatus {
	if !sd.lastUnavailableAt.IsZero() && clock.Since(sd.lastUnavailableAt) > *deadStoreTimeout {
		return storeStatusDead
	}

	if sd.usage == nil {
		return storeStatusUnknown
	}

	if time.Since(sd.lastUnavailableAt) <= *suspectStoreDuration {
		return storeStatusSuspect
	}

	return storeStatusAvailable
}

func (sm *StoreMap) updateStoreDetail(nhid string, usage *rfpb.StoreUsage, nodeStatus serf.MemberStatus) error {
	if nhid == "" {
		return status.FailedPreconditionError("empty nodehost ID")
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	detail := sm.getDetail(nhid)
	if usage != nil {
		detail.usage = usage
	}
	now := sm.clock.Now()
	if nodeStatus != serf.StatusAlive {
		detail.lastUnavailableAt = now
	} else {
		detail.lastUnavailableAt = time.Time{}
	}
	return nil
}

// OnEvent listens for other nodes' gossip events and handles them.
func (sm *StoreMap) OnEvent(updateType serf.EventType, event serf.Event) {
	memberEvent, ok := event.(serf.MemberEvent)
	if !ok {
		return
	}
	for _, member := range memberEvent.Members {
		var usage *rfpb.StoreUsage
		usageTag := member.Tags[constants.StoreUsageTag]
		if len(usageTag) == 0 {
			log.Debugf("member %q did not have usage tag yet", member.Name)
		}
		usageBuf, err := base64.StdEncoding.DecodeString(usageTag)
		if err != nil {
			log.Warningf("error b64 decoding usage tag: %s", err)
		}
		usage = &rfpb.StoreUsage{}
		if err := proto.Unmarshal(usageBuf, usage); err != nil {
			log.Warningf("error unmarshaling usage buf: %s", err)
			usage = nil
		}
		if err := sm.updateStoreDetail(usage.GetNode().GetNhid(), usage, member.Status); err != nil {
			log.Errorf("Error observing cluster change for node %+v: %s", usage.GetNode(), err)
		}
		log.Debugf("Successfuly observed cluster change for node %+v", usage.GetNode())
	}
}

type Stat struct {
	n    int
	Mean float64
}

func (s *Stat) update(x int64) {
	s.n++
	s.Mean += (float64(x) - s.Mean) / float64(s.n)
}

type StoresWithStats struct {
	Usages []*rfpb.StoreUsage

	ReplicaCount   Stat
	LeaseCount     Stat
	ReadQPS        Stat
	RaftProposeQPS Stat
	TotalBytesUsed Stat
}

func createStoresWithStats(usages []*rfpb.StoreUsage) *StoresWithStats {
	res := &StoresWithStats{}
	for _, usage := range usages {
		res.Usages = append(res.Usages, usage.CloneVT())
		res.ReplicaCount.update(usage.GetReplicaCount())
		res.LeaseCount.update(usage.GetLeaseCount())
		res.ReadQPS.update(usage.GetReadQps())
		res.RaftProposeQPS.update(usage.GetRaftProposeQps())
		res.TotalBytesUsed.update(usage.GetTotalBytesUsed())
	}
	return res
}

// Returns stores with stats that are alive
func (sm *StoreMap) GetStoresWithStats() *StoresWithStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	alive := make([]*rfpb.StoreUsage, 0, len(sm.storeDetails))
	for _, sd := range sm.storeDetails {
		status := sd.status(sm.clock)
		if status == storeStatusAvailable {
			alive = append(alive, sd.usage)
		}
	}
	return createStoresWithStats(alive)
}

func (sm *StoreMap) GetStoresWithStatsFromIDs(nhids []string) *StoresWithStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	alive := make([]*rfpb.StoreUsage, 0, len(nhids))
	for _, nhid := range nhids {
		sd, ok := sm.storeDetails[nhid]
		if !ok {
			continue
		}
		status := sd.status(sm.clock)
		if status == storeStatusAvailable || status == storeStatusSuspect {
			alive = append(alive, sd.usage)
		}
	}
	return createStoresWithStats(alive)
}

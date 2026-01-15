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
	// The store is never seen OR the store is currently down, but for
	// less than deadStoreTimeout.
	storeStatusUnknown storeStatus = iota
	// The store is continuously unavailable > deadStoreTimeout
	storeStatusDead
	// The store is consistently alive for at lease suspectStoreDuration.
	storeStatusAvailable
	// The store is currently alive but recently recovered from being unavailable.
	// We don't fully trust it yet. After suspectStoreDuration passes without the
	// store becoming unavailable again, it becomes Available.
	storeStatusSuspect
)

type IStoreMap interface {
	GetStoresWithStats() *StoresWithStats
	GetStoresWithStatsFromIDs(nhids []string) *StoresWithStats
	DivideByStatus(repls []*rfpb.ReplicaDescriptor) *ReplicasByStatus
	AllStoresAvailableAndReady() bool
}

type StoreDetail struct {
	usage *rfpb.StoreUsage

	nhid string

	// lastBecameUnavailableAt is the time when the store mostly recently
	// transitioned from alive to not-alive. This is RESET when the store becomes
	// available again.
	lastBecameUnavailableAt time.Time

	// lastBecameAvailableAt is the time when the store most recently
	// transitioned from not-alive to alive. Used for the suspect window.
	lastBecameAvailableAt time.Time

	// wasAlive is the last known status.
	wasAlive bool
}

type StoreMap struct {
	gossipManager interfaces.GossipService

	mu *sync.RWMutex
	// NHID to StoreDetail
	storeDetails map[string]*StoreDetail

	startTime time.Time
	clock     clockwork.Clock
	log       log.Logger
}

func create(gossipManager interfaces.GossipService, clock clockwork.Clock, nhLogger log.Logger) *StoreMap {
	sm := &StoreMap{
		mu:            &sync.RWMutex{},
		startTime:     time.Now(),
		storeDetails:  make(map[string]*StoreDetail),
		gossipManager: gossipManager,
		clock:         clock,
		log:           nhLogger,
	}
	gossipManager.AddListener(sm)
	return sm
}

func New(gossipManager interfaces.GossipService, clock clockwork.Clock, nhLogger log.Logger) IStoreMap {
	return create(gossipManager, clock, nhLogger)
}

type ReplicasByStatus struct {
	LiveReplicas    []*rfpb.ReplicaDescriptor
	DeadReplicas    []*rfpb.ReplicaDescriptor
	SuspectReplicas []*rfpb.ReplicaDescriptor
}

func (sm *StoreMap) getStoreUsage(m serf.Member) *rfpb.StoreUsage {
	usageTag := m.Tags[constants.StoreUsageTag]
	if len(usageTag) == 0 {
		return nil
	}
	usageBuf, err := base64.StdEncoding.DecodeString(usageTag)
	if err != nil {
		sm.log.Warningf("error b64 decoding usage tag: %s", err)
		return nil
	}
	usage := &rfpb.StoreUsage{}
	if err := proto.Unmarshal(usageBuf, usage); err != nil {
		sm.log.Warningf("error unmarshaling usage buf: %s", err)
		return nil
	}
	return usage
}

// getMemberStatus queries serf for the current status of all members and returns
// a map from NHID to whether the member is alive
func (sm *StoreMap) getMemberStatus() map[string]bool {
	members := sm.gossipManager.Members()
	result := make(map[string]bool, len(members))
	for _, m := range members {
		usage := sm.getStoreUsage(m)
		if usage == nil {
			log.Infof("skip because of nil usage")
			continue
		}
		nhid := usage.GetNode().GetNhid()
		if nhid != "" {
			result[nhid] = (m.Status == serf.StatusAlive)
		} else {
			log.Infof("skip because of empty nhid")
		}
	}

	log.Infof("result:%+v", result)
	return result
}

func (sm *StoreMap) DivideByStatus(repls []*rfpb.ReplicaDescriptor) *ReplicasByStatus {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	memberStatus := sm.getMemberStatus()
	now := sm.clock.Now()

	res := &ReplicasByStatus{}
	for _, repl := range repls {
		detail := sm.getDetailLocked(repl.GetNhid())
		status := detail.refreshAndComputeStatus(sm.clock, memberStatus, now)

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

func (sm *StoreMap) getDetailLocked(nhid string) *StoreDetail {
	detail, ok := sm.storeDetails[nhid]
	if !ok {
		detail = &StoreDetail{nhid: nhid}
		sm.storeDetails[nhid] = detail
	}
	return detail
}

func (sm *StoreMap) getDetails(nhid string) *StoreDetail {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.getDetailLocked(nhid)
}

func (sd *StoreDetail) refreshAndComputeStatus(clock clockwork.Clock, liveMemberStatus map[string]bool, now time.Time) storeStatus {
	currentlyAlive := liveMemberStatus[sd.nhid]
	log.Infof("refresh: currentlyAlive = %t", currentlyAlive)
	sd.updateTransitionsLocked(currentlyAlive, now)

	if !currentlyAlive {
		if !sd.lastBecameUnavailableAt.IsZero() && now.Sub(sd.lastBecameUnavailableAt) > *deadStoreTimeout {
			return storeStatusDead
		}
		return storeStatusUnknown
	}

	// The store is currently alive
	if !sd.lastBecameAvailableAt.IsZero() && now.Sub(sd.lastBecameAvailableAt) <= *suspectStoreDuration {
		// The store just became available recently
		return storeStatusSuspect
	}

	// The store has been consistently available for longer than suspectDuration.
	return storeStatusAvailable
}

// updateTransitionsLocked updates the transition timestaps based on state changes.
// Must be called with sm.mu held
func (sd *StoreDetail) updateTransitionsLocked(currentlyAlive bool, now time.Time) {
	wasAlive := sd.wasAlive
	if currentlyAlive && !wasAlive {
		// unavailable -> available
		sd.lastBecameAvailableAt = now
		sd.lastBecameUnavailableAt = time.Time{}
	} else if !currentlyAlive && wasAlive {
		// available -> unavailable
		sd.lastBecameUnavailableAt = now
	} else if !currentlyAlive && !wasAlive && sd.lastBecameUnavailableAt.IsZero() {
		sd.lastBecameUnavailableAt = now
	}

	sd.wasAlive = currentlyAlive
}

func (sm *StoreMap) updateStoreDetail(nhid string, usage *rfpb.StoreUsage, nodeStatus serf.MemberStatus) error {
	if nhid == "" {
		return status.FailedPreconditionError("empty nodehost ID")
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	detail := sm.getDetailLocked(nhid)
	if usage != nil {
		detail.usage = usage
	}
	now := sm.clock.Now()
	currentlyAlive := nodeStatus == serf.StatusAlive
	log.Infof("update to currently Alive=%t", currentlyAlive)
	detail.updateTransitionsLocked(currentlyAlive, now)
	return nil
}

// OnEvent listens for other nodes' gossip events and handles them.
func (sm *StoreMap) OnEvent(updateType serf.EventType, event serf.Event) {
	memberEvent, ok := event.(serf.MemberEvent)
	if !ok {
		return
	}
	for _, member := range memberEvent.Members {
		usage := sm.getStoreUsage(member)
		if err := sm.updateStoreDetail(usage.GetNode().GetNhid(), usage, member.Status); err != nil {
			sm.log.Errorf("Error observing cluster change for node %+v: %s", usage.GetNode(), err)
		}
		sm.log.Debugf("Successfuly observed cluster change for node %+v", usage.GetNode())
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

func CreateStoresWithStats(usages []*rfpb.StoreUsage) *StoresWithStats {
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

	// Get current member status from serf
	memberStatus := sm.getMemberStatus()
	now := sm.clock.Now()

	alive := make([]*rfpb.StoreUsage, 0, len(sm.storeDetails))
	for _, sd := range sm.storeDetails {
		status := sd.refreshAndComputeStatus(sm.clock, memberStatus, now)
		if status == storeStatusAvailable {
			alive = append(alive, sd.usage)
		}
	}
	return CreateStoresWithStats(alive)
}

// Returns stores with stats that are not dead (include both alive and suspect).
func (sm *StoreMap) GetStoresWithStatsFromIDs(nhids []string) *StoresWithStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Get current member status from serf
	memberStatus := sm.getMemberStatus()
	now := sm.clock.Now()

	alive := make([]*rfpb.StoreUsage, 0, len(nhids))
	for _, nhid := range nhids {
		sd, ok := sm.storeDetails[nhid]
		if !ok {
			continue
		}
		status := sd.refreshAndComputeStatus(sm.clock, memberStatus, now)

		if status == storeStatusAvailable || status == storeStatusSuspect {
			alive = append(alive, sd.usage)
		}
	}
	return CreateStoresWithStats(alive)
}

func (sm *StoreMap) AllStoresAvailableAndReady() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	memberStatus := sm.getMemberStatus()
	now := sm.clock.Now()

	for _, sd := range sm.storeDetails {
		status := sd.refreshAndComputeStatus(sm.clock, memberStatus, now)
		if status != storeStatusAvailable {
			return false
		}
		if !sd.usage.GetIsReady() {
			return false
		}
	}
	return true
}

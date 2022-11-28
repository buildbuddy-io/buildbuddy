package rangecache

import (
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/hashicorp/serf/serf"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

type RangeCache struct {
	rangeMu sync.RWMutex

	// Keep a rangemap that maps from ranges to descriptors.
	rangeMap *rangemap.RangeMap
}

func New() *RangeCache {
	return &RangeCache{
		rangeMu:  sync.RWMutex{},
		rangeMap: rangemap.New(),
	}
}

// A lockingRangeDescriptor pointer is stored in the rangeMap to prevent
// data races when mutating the order of replicas elsewhere.
type lockingRangeDescriptor struct {
	mu sync.Mutex
	rd *rfpb.RangeDescriptor
}

func newLockingRangeDescriptor(rd *rfpb.RangeDescriptor) *lockingRangeDescriptor {
	return &lockingRangeDescriptor{
		mu: sync.Mutex{},
		rd: rd,
	}
}
func (lr *lockingRangeDescriptor) Get() *rfpb.RangeDescriptor {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	return lr.rd
}
func (lr *lockingRangeDescriptor) Update(rd *rfpb.RangeDescriptor) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	lr.rd = rd
}

func (rc *RangeCache) updateRange(rangeDescriptor *rfpb.RangeDescriptor) error {
	rc.rangeMu.Lock()
	defer rc.rangeMu.Unlock()

	metrics.RaftRangeCacheLookups.With(prometheus.Labels{
		metrics.RaftRangeCacheEventTypeLabel: "update",
	}).Inc()

	left := rangeDescriptor.GetLeft()
	right := rangeDescriptor.GetRight()
	newDescriptor := proto.Clone(rangeDescriptor).(*rfpb.RangeDescriptor)

	r := rc.rangeMap.Get(left, right)
	if r == nil {
		// If this range overlaps, we'll check it against the overlapping
		// ranges and possibly delete them or if they are newer, then we'll
		// ignore this update.
		for _, overlappingRange := range rc.rangeMap.GetOverlapping(left, right) {
			lr, ok := overlappingRange.Val.(*lockingRangeDescriptor)
			if !ok {
				continue
			}
			v := lr.Get()
			if v.GetGeneration() >= newDescriptor.GetGeneration() {
				log.Debugf("Ignoring rangeDescriptor %+v, because current has same or later generation: %+v", newDescriptor, v)
				return nil
			}
		}

		// If we got here, all overlapping ranges are older, so we're gonna
		// delete them and replace with the new one.
		for _, overlappingRange := range rc.rangeMap.GetOverlapping(left, right) {
			log.Debugf("Removing (outdated) overlapping range: [%q, %q)", overlappingRange.Left, overlappingRange.Right)
			rc.rangeMap.Remove(overlappingRange.Left, overlappingRange.Right)
		}
		log.Debugf("Adding new range: %d [%q, %q)", newDescriptor.GetRangeId(), left, right)
		_, err := rc.rangeMap.Add(left, right, newLockingRangeDescriptor(newDescriptor))
		return err
	} else {
		lr, ok := r.Val.(*lockingRangeDescriptor)
		if !ok {
			return status.FailedPreconditionError("Val was not a rangeVal")
		}
		v := lr.Get()
		if newDescriptor.GetGeneration() > v.GetGeneration() {
			lr.Update(newDescriptor)
			log.Debugf("Updated rangelease generation: [%q, %q) (%d -> %d)", left, right, v.GetGeneration(), newDescriptor.GetGeneration())
		}
	}
	return nil
}

// OnEvent is called when a node joins, leaves, or is updated.
func (rc *RangeCache) OnEvent(updateType serf.EventType, event serf.Event) {
	switch updateType {
	case serf.EventMemberJoin, serf.EventMemberUpdate:
		memberEvent, _ := event.(serf.MemberEvent)
		for _, member := range memberEvent.Members {
			if tagData, ok := member.Tags[constants.MetaRangeTag]; ok {
				rd := &rfpb.RangeDescriptor{}
				if err := proto.Unmarshal([]byte(tagData), rd); err != nil {
					log.Errorf("unparsable rangeset: %s", err)
					return
				}
				if err := rc.updateRange(rd); err != nil {
					log.Errorf("Error updating ranges: %s", err)
				}
			}
		}
	default:
		break
	}
}

func (rc *RangeCache) UpdateRange(rangeDescriptor *rfpb.RangeDescriptor) error {
	return rc.updateRange(rangeDescriptor)
}

func (rc *RangeCache) SetPreferredReplica(rep *rfpb.ReplicaDescriptor, rng *rfpb.RangeDescriptor) {
	rc.rangeMu.RLock()
	defer rc.rangeMu.RUnlock()

	r := rc.rangeMap.Get(rng.GetLeft(), rng.GetRight())
	if r == nil {
		log.Errorf("SetPreferredReplica called but range %+v not in cache", rng)
		return
	}

	lr, ok := r.Val.(*lockingRangeDescriptor)
	if !ok {
		return
	}
	rd := lr.Get()
	newDescriptor := proto.Clone(rd).(*rfpb.RangeDescriptor)

	leadReplicaIndex := -1
	for i, replica := range newDescriptor.GetReplicas() {
		if replica.GetClusterId() == rep.GetClusterId() &&
			replica.GetNodeId() == rep.GetNodeId() {
			leadReplicaIndex = i
			break
		}
	}
	if leadReplicaIndex == -1 {
		log.Errorf("SetPreferredReplica called but range %+v does not contain preferred %+v", rng, rep)
		return
	}
	newDescriptor.Replicas[0], newDescriptor.Replicas[leadReplicaIndex] = newDescriptor.Replicas[leadReplicaIndex], newDescriptor.Replicas[0]
	lr.Update(newDescriptor)
}

func (rc *RangeCache) Get(key []byte) *rfpb.RangeDescriptor {
	rc.rangeMu.RLock()
	defer rc.rangeMu.RUnlock()

	val := rc.rangeMap.Lookup(key)

	var rd *rfpb.RangeDescriptor
	label := "miss"

	if lr, ok := val.(*lockingRangeDescriptor); ok {
		rd = lr.Get()
		label = "hit"
	}

	metrics.RaftRangeCacheLookups.With(prometheus.Labels{
		metrics.RaftRangeCacheEventTypeLabel: label,
	}).Inc()

	return rd
}

func (rc *RangeCache) String() string {
	return rc.rangeMap.String()
}

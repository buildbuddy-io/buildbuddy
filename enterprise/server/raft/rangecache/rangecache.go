package rangecache

import (
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

// RangeCache is a synchronous map from ranges to their descriptors.
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

	start := rangeDescriptor.GetStart()
	end := rangeDescriptor.GetEnd()
	newDescriptor := rangeDescriptor.CloneVT()

	r := rc.rangeMap.Get(start, end)
	if r == nil {
		overlappingRanges := rc.rangeMap.GetOverlapping(start, end)
		// If this range overlaps, we'll check it against the overlapping
		// ranges and possibly delete them or if they are newer, then we'll
		// ignore this update.
		for _, overlappingRange := range overlappingRanges {
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
		for _, overlappingRange := range overlappingRanges {
			log.Debugf("Removing (outdated) overlapping range: [%q, %q)", overlappingRange.Start, overlappingRange.End)
			rc.rangeMap.Remove(overlappingRange.Start, overlappingRange.End)
		}
		log.Debugf("Adding new range: %d [%q, %q)", newDescriptor.GetRangeId(), start, end)
		_, err := rc.rangeMap.Add(start, end, newLockingRangeDescriptor(newDescriptor))
		return err
	} else {
		lr, ok := r.Val.(*lockingRangeDescriptor)
		if !ok {
			return status.FailedPreconditionError("Val was not a rangeVal")
		}
		v := lr.Get()
		if newDescriptor.GetGeneration() > v.GetGeneration() {
			lr.Update(newDescriptor)
			log.Debugf("Updated rangelease generation: [%q, %q) (%d -> %d)", start, end, v.GetGeneration(), newDescriptor.GetGeneration())
		}
	}
	return nil
}

// UpdateRange updates the rangeDescriptor if its generation is later. Note that
// it deletes all older overlapping ranges.
func (rc *RangeCache) UpdateRange(rangeDescriptor *rfpb.RangeDescriptor) error {
	return rc.updateRange(rangeDescriptor)
}

// SetPreferredReplica moves the given replica to the front for the given range
// descriptor. It's an no-op when the given range is not in the cache or the given
// replica is not included in the range descriptor's replica list.
func (rc *RangeCache) SetPreferredReplica(rep *rfpb.ReplicaDescriptor, rng *rfpb.RangeDescriptor) {
	rc.rangeMu.RLock()
	defer rc.rangeMu.RUnlock()

	r := rc.rangeMap.Get(rng.GetStart(), rng.GetEnd())
	if r == nil {
		log.Errorf("SetPreferredReplica called but range %+v not in cache", rng)
		return
	}

	lr, ok := r.Val.(*lockingRangeDescriptor)
	if !ok {
		return
	}
	rd := lr.Get()
	newDescriptor := rd.CloneVT()

	leadReplicaIndex := -1
	for i, replica := range newDescriptor.GetReplicas() {
		if replica.GetRangeId() == rep.GetRangeId() &&
			replica.GetReplicaId() == rep.GetReplicaId() {
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

// Get returns a RangeDescriptor that includes the given key within its range.
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

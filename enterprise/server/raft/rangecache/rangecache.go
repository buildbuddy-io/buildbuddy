package rangecache

import (
	"context"
	"fmt"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

// RangeCache is a synchronous map from ranges to their descriptors.
type RangeCache struct {
	rangeMu sync.RWMutex

	// Keep a rangemap that maps from ranges to descriptors.
	rangeMap *rangemap.RangeMap[*lockingRangeDescriptor]
}

func New() *RangeCache {
	return &RangeCache{
		rangeMu:  sync.RWMutex{},
		rangeMap: rangemap.New[*lockingRangeDescriptor](),
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
		checkFn := func(r *lockingRangeDescriptor) bool {
			v := r.Get()
			return v.GetGeneration() < newDescriptor.GetGeneration()
		}
		added, err := rc.rangeMap.AddAndRemoveOverlapping(start, end, newLockingRangeDescriptor(newDescriptor), checkFn)
		if added {
			log.Debugf("Adding new range: %d [%q, %q)", newDescriptor.GetRangeId(), start, end)
		} else {
			log.Debugf("Ignoring rangeDescriptor %+v, because current has same or later generation", newDescriptor)
		}
		return err
	} else {
		lr := r.Val
		if lr == nil {
			return status.FailedPreconditionError("Val was nil")
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
func (rc *RangeCache) SetPreferredReplica(ctx context.Context, rep *rfpb.ReplicaDescriptor, rng *rfpb.RangeDescriptor) {
	ctx, spn := tracing.StartSpan(ctx) // nolint:SA4006
	rangeIDAttr := attribute.Int64("range_id", int64(rep.GetRangeId()))
	spn.SetAttributes(rangeIDAttr)
	defer spn.End()

	rc.rangeMu.RLock()
	defer rc.rangeMu.RUnlock()

	r := rc.rangeMap.Get(rng.GetStart(), rng.GetEnd())
	if r == nil {
		err := fmt.Errorf("SetPreferredReplica called but range %+v not in cache", rng)
		log.Error(err.Error())
		tracing.RecordErrorToSpan(spn, err)
		return
	}

	lr := r.Val
	if lr == nil {
		err := fmt.Errorf("locking range descriptor value for range [%q, %q) is nil", r.Start, r.End)
		log.Error(err.Error())
		tracing.RecordErrorToSpan(spn, err)
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
		err := fmt.Errorf("SetPreferredReplica called but range %+v does not contain preferred %+v", rng, rep)
		log.Error(err.Error())
		tracing.RecordErrorToSpan(spn, err)
		return
	}
	newDescriptor.Replicas[0], newDescriptor.Replicas[leadReplicaIndex] = newDescriptor.Replicas[leadReplicaIndex], newDescriptor.Replicas[0]

	lr.Update(newDescriptor)

	replica_ids := []int64{}
	for _, repl := range newDescriptor.GetReplicas() {
		replica_ids = append(replica_ids, int64(repl.GetReplicaId()))
	}
	replicaIDAttr := attribute.Int64Slice("replicas", replica_ids)
	spn.SetAttributes(replicaIDAttr)
}

// Get returns a RangeDescriptor that includes the given key within its range.
func (rc *RangeCache) Get(key []byte) *rfpb.RangeDescriptor {
	rc.rangeMu.RLock()
	defer rc.rangeMu.RUnlock()

	lr, found := rc.rangeMap.Lookup(key)

	var rd *rfpb.RangeDescriptor
	label := "miss"

	if found {
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

package rangecache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

// RangeCache is a synchronous map from ranges to their descriptors.
//
// Descriptors are stored behind atomic pointers so SetPreferredReplica can
// swap in a new descriptor without holding the cache-wide write lock. The
// stored *rfpb.RangeDescriptor is treated as immutable; mutators must clone,
// modify, then Store a new pointer.
type RangeCache struct {
	rangeMu sync.RWMutex

	rangeMap *rangemap.RangeMap[*atomic.Pointer[rfpb.RangeDescriptor]]
}

func New() *RangeCache {
	return &RangeCache{
		rangeMap: rangemap.New[*atomic.Pointer[rfpb.RangeDescriptor]](),
	}
}

func newAtomicDescriptor(rd *rfpb.RangeDescriptor) *atomic.Pointer[rfpb.RangeDescriptor] {
	p := &atomic.Pointer[rfpb.RangeDescriptor]{}
	p.Store(rd)
	return p
}

func (rc *RangeCache) updateRange(rangeDescriptor *rfpb.RangeDescriptor) error {
	metrics.RaftRangeCacheLookups.WithLabelValues("update").Inc()
	start := rangeDescriptor.GetStart()
	end := rangeDescriptor.GetEnd()
	newDescriptor := rangeDescriptor.CloneVT()

	rc.rangeMu.Lock()
	defer rc.rangeMu.Unlock()

	r := rc.rangeMap.Get(start, end)
	if r == nil {
		checkFn := func(p *atomic.Pointer[rfpb.RangeDescriptor]) bool {
			return p.Load().GetGeneration() < newDescriptor.GetGeneration()
		}
		added, err := rc.rangeMap.AddAndRemoveOverlapping(start, end, newAtomicDescriptor(newDescriptor), checkFn)
		if added {
			log.Debugf("Adding new range: %d [%q, %q)", newDescriptor.GetRangeId(), start, end)
		} else {
			log.Debugf("Ignoring rangeDescriptor %+v, because current has same or later generation", newDescriptor)
		}
		return err
	}
	v := r.Val.Load()
	if newDescriptor.GetGeneration() > v.GetGeneration() {
		r.Val.Store(newDescriptor)
		log.Debugf("Updated rangelease generation: [%q, %q) (%d -> %d)", start, end, v.GetGeneration(), newDescriptor.GetGeneration())
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
	if spn.IsRecording() {
		spn.SetAttributes(attribute.Int64("range_id", int64(rep.GetRangeId())))
	}
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

	rd := r.Val.Load()
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

	r.Val.Store(newDescriptor)

	replica_ids := make([]int64, 0, len(newDescriptor.GetReplicas()))
	for _, repl := range newDescriptor.GetReplicas() {
		replica_ids = append(replica_ids, int64(repl.GetReplicaId()))
	}
	if spn.IsRecording() {
		spn.SetAttributes(attribute.Int64Slice("replicas", replica_ids))
	}
}

var raftRangeCacheLookupCounter = map[bool]prometheus.Counter{
	true:  metrics.RaftRangeCacheLookups.With(prometheus.Labels{metrics.RaftRangeCacheEventTypeLabel: "hit"}),
	false: metrics.RaftRangeCacheLookups.With(prometheus.Labels{metrics.RaftRangeCacheEventTypeLabel: "miss"}),
}

// Get returns a RangeDescriptor that includes the given key within its range.
func (rc *RangeCache) Get(key []byte) *rfpb.RangeDescriptor {
	rc.rangeMu.RLock()
	lr, found := rc.rangeMap.Lookup(key)
	rc.rangeMu.RUnlock()

	raftRangeCacheLookupCounter[found].Inc()
	if found {
		return lr.Load()
	}
	return nil
}

func (rc *RangeCache) String() string {
	rc.rangeMu.RLock()
	defer rc.rangeMu.RUnlock()
	return rc.rangeMap.String()
}

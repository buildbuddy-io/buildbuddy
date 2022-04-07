package rangecache

import (
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"

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

func (rc *RangeCache) updateRange(rangeDescriptor *rfpb.RangeDescriptor) error {
	rc.rangeMu.Lock()
	defer rc.rangeMu.Unlock()

	left := rangeDescriptor.GetLeft()
	right := rangeDescriptor.GetRight()
	newDescriptor := proto.Clone(rangeDescriptor).(*rfpb.RangeDescriptor)

	r := rc.rangeMap.Get(left, right)
	if r == nil {
		// If this range overlaps, we'll check it against the overlapping
		// ranges and possibly delete them or if they are newer, then we'll
		// ignore this update.
		for _, overlappingRange := range rc.rangeMap.GetOverlapping(left, right) {
			v, ok := overlappingRange.Val.(*rfpb.RangeDescriptor)
			if !ok {
				continue
			}
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
		_, err := rc.rangeMap.Add(left, right, newDescriptor)
		return err
	} else {
		v, ok := r.Val.(*rfpb.RangeDescriptor)
		if !ok {
			return status.FailedPreconditionError("Val was not a rangeVal")
		}
		if newDescriptor.GetGeneration() > v.GetGeneration() {
			r.Val = newDescriptor
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

	rd, ok := r.Val.(*rfpb.RangeDescriptor)
	if !ok {
		return
	}

	leadReplicaIndex := -1
	for i, replica := range rd.GetReplicas() {
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
	rd.Replicas[0], rd.Replicas[leadReplicaIndex] = rd.Replicas[leadReplicaIndex], rd.Replicas[0]
}

func (rc *RangeCache) Get(key []byte) *rfpb.RangeDescriptor {
	rc.rangeMu.RLock()
	defer rc.rangeMu.RUnlock()

	val := rc.rangeMap.Lookup(key)
	if rv, ok := val.(*rfpb.RangeDescriptor); ok {
		return rv
	}
	return nil
}

func (rc *RangeCache) String() string {
	return rc.rangeMap.String()
}

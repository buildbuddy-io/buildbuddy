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

	// Keep a rangemap that maps from ranges to rangeValues.
	rangeMap *rangemap.RangeMap

	// Keep a reverse map that lets us easily remove a node from the
	// rangeMap when a node leaves the cluster. This list may contain
	// dupes.
	nodeRanges map[string][]*rangemap.Range
}

func New() *RangeCache {
	return &RangeCache{
		rangeMu:    sync.RWMutex{},
		rangeMap:   rangemap.New(),
		nodeRanges: make(map[string][]*rangemap.Range, 0),
	}
}

// This value is stored by rangeMap.
type rangeValue struct {
	// An easily indexable set of the hosts in this range.
	members map[string]string
}

func (rc *RangeCache) removeRanges(grpcAddress string) error {
	rc.rangeMu.Lock()
	defer rc.rangeMu.Unlock()

	log.Debugf("removing %q from ranges", grpcAddress)
	for _, r := range rc.nodeRanges[grpcAddress] {
		rv, ok := r.Val.(*rangeValue)
		if !ok {
			log.Debugf("skipping !ok rval")
			continue
		}
		delete(rv.members, grpcAddress)
		log.Debugf("rv.members is now: %+v", rv.members)
		if len(rv.members) == 0 {
			// ignore error, if this range isn't in the map anymore
			// that's ok.
			log.Debugf("removing from rangemap b/c no members left.")
			rc.rangeMap.Remove(r.Left, r.Right)
		}
	}
	delete(rc.nodeRanges, grpcAddress)
	return nil
}

func (rc *RangeCache) updateRange(grpcAddress string, rangeDescriptor *rfpb.RangeDescriptor) error {
	rc.rangeMu.Lock()
	defer rc.rangeMu.Unlock()

	left := rangeDescriptor.GetLeft()
	right := rangeDescriptor.GetRight()

	log.Debugf("Adding %q to rangemap with descriptor: %+v", grpcAddress, rangeDescriptor)
	r := rc.rangeMap.Get(left, right)
	if r == nil {
		err := rc.rangeMap.Add(left, right, &rangeValue{map[string]string{
			grpcAddress: grpcAddress,
		}})
		if err != nil {
			return err
		}
		rc.nodeRanges[grpcAddress] = append(rc.nodeRanges[grpcAddress], rc.rangeMap.Get(left, right))
	} else {
		rv, ok := r.Val.(*rangeValue)
		if !ok {
			return status.FailedPreconditionError("Val was not a rangeVal")
		}
		rv.members[grpcAddress] = grpcAddress
	}
	return nil
}

func (rc *RangeCache) updateRanges(grpcAddress string, rangeset *rfpb.RangeSet) error {
	for _, rangeDescriptor := range rangeset.GetRanges() {
		if err := rc.updateRange(grpcAddress, rangeDescriptor); err != nil {
			return err
		}
	}
	return nil
}

// RegisterTagProviderFn gets a callback function that can  be used to set tags.
func (rc *RangeCache) RegisterTagProviderFn(setTagFn func(tagName, tagValue string) error) {
	// no-op, we only consume tags, we don't send them.
}

// MemberEvent is called when a node joins, leaves, or is updated.
func (rc *RangeCache) MemberEvent(updateType serf.EventType, member *serf.Member) {
	grpcAddress, ok := member.Tags[constants.GRPCAddressTag]
	if !ok {
		return
	}

	switch updateType {
	case serf.EventMemberJoin, serf.EventMemberUpdate:
		if tagData, ok := member.Tags[constants.StoredRangesTag]; ok {
			rangeset := &rfpb.RangeSet{}
			if err := proto.Unmarshal([]byte(tagData), rangeset); err != nil {
				log.Errorf("unparsable rangeset: %s", err)
				return
			}
			rc.removeRanges(grpcAddress)
			rc.updateRanges(grpcAddress, rangeset)
		}
	case serf.EventMemberLeave:
		rc.removeRanges(grpcAddress)
	default:
		break
	}
}

func (rc *RangeCache) UpdateStaleRange(grpcAddress string, rangeDescriptor *rfpb.RangeDescriptor) error {
	rc.removeRanges(grpcAddress)
	return rc.updateRange(grpcAddress, rangeDescriptor)
}

func (rc *RangeCache) Get(key []byte) string {
	rc.rangeMu.RLock()
	defer rc.rangeMu.RUnlock()

	val := rc.rangeMap.Lookup(key)
	if rv, ok := val.(*rangeValue); ok {
		for _, member := range rv.members {
			return member
		}
	}
	return ""
}

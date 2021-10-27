package rangecache_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

// Range flow looks like this:
// grpcAddr := rangeCache.Get(key)
// for {
// 	if grpcAddr == nil {
// 		grpcAddr = readRangeFromMeta1(key)
// 		rangeCache.Update(key, grpcAddr)
// 	}
// 	rsp, err := sendRequest(grpcAddr &req{})
// 	if status.IsOutOfRangeError(err) {
// 		// cache gave us a stale node
// 		grpcAddr = nil
// 	}
// 	// handle rsp and err
// }

func init() {
	log.Configure(log.Opts{
		Level: "debug",
	})
}

func rangeLeaveEvent(nhid string) (serf.EventType, *serf.Member) {
	member := &serf.Member{
		Tags: map[string]string{
			constants.NodeHostIDTag: nhid,
		},
	}
	return serf.EventMemberLeave, member
}

func rangeUpdateEvent(t *testing.T, nhid string, rangeDescriptor *rfpb.RangeDescriptor) (serf.EventType, *serf.Member) {
	buf, err := proto.Marshal(&rfpb.RangeSet{
		Ranges: []*rfpb.RangeDescriptor{rangeDescriptor},
	})
	if err != nil {
		t.Fatalf("error marshaling proto: %s", err)
	}
	compressedBuf, err := compression.CompressFlate(buf, 9 /*=max compression*/)
	if err != nil {
		t.Fatalf("error compressing proto: %s", err)
	}
	member := &serf.Member{
		Tags: map[string]string{
			constants.StoredRangesTag: string(compressedBuf),
			constants.NodeHostIDTag:   nhid,
		},
	}

	return serf.EventMemberUpdate, member
}

func TestUpdateAndLeave(t *testing.T) {
	rc := rangecache.New()

	// Advertise a (fake) range advertisement.
	rc.MemberEvent(rangeUpdateEvent(t, "nhid-11", &rfpb.RangeDescriptor{
		Left:       []byte("a"),
		Right:      []byte("b"),
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
		},
	}))

	// Make sure we get back that range descriptor.
	rd := rc.Get([]byte("a"))
	require.NotNil(t, rd)
	require.Equal(t, uint64(1), rd.GetReplicas()[0].GetClusterId())
}

func TestMultipleNodesInRange(t *testing.T) {
	rc := rangecache.New()
	nodes := []string{
		"nhid-22",
		"nhid-33",
		"nhid-44",
	}

	rd := &rfpb.RangeDescriptor{
		Left:       []byte("a"),
		Right:      []byte("z"),
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}

	// Advertise a few (fake) range advertisements.
	rc.MemberEvent(rangeUpdateEvent(t, nodes[0], rd))
	rc.MemberEvent(rangeUpdateEvent(t, nodes[1], rd))
	rc.MemberEvent(rangeUpdateEvent(t, nodes[2], rd))

	rr := rc.Get([]byte("m"))
	require.NotNil(t, rr)
	require.Equal(t, uint64(1), rr.GetReplicas()[0].GetNodeId())
	require.Equal(t, uint64(2), rr.GetReplicas()[1].GetNodeId())
	require.Equal(t, uint64(3), rr.GetReplicas()[2].GetNodeId())
}

func TestUpdateRange(t *testing.T) {
	rc := rangecache.New()

	rd1 := &rfpb.RangeDescriptor{
		Left:       []byte("a"),
		Right:      []byte("b"),
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}

	// Advertise a (fake) range advertisement.
	rc.MemberEvent(rangeUpdateEvent(t, "nhid-11", rd1))

	rd2 := &rfpb.RangeDescriptor{
		Left:       []byte("a"),
		Right:      []byte("z"),
		Generation: 2,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 2, NodeId: 3},
			{ClusterId: 2, NodeId: 4},
			{ClusterId: 2, NodeId: 5},
		},
	}

	// Now advertise again, with a higher generation this time.
	rc.MemberEvent(rangeUpdateEvent(t, "nhid-11", rd2))

	rr := rc.Get([]byte("m"))
	require.NotNil(t, rr)
	require.Equal(t, uint64(3), rr.GetReplicas()[0].GetNodeId())
	require.Equal(t, uint64(4), rr.GetReplicas()[1].GetNodeId())
	require.Equal(t, uint64(5), rr.GetReplicas()[2].GetNodeId())
}

func TestStaleUpdate(t *testing.T) {
	rc := rangecache.New()

	rd1 := &rfpb.RangeDescriptor{
		Left:       []byte("a"),
		Right:      []byte("b"),
		Generation: 2,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}

	// Advertise a (fake) range advertisement.
	rc.MemberEvent(rangeUpdateEvent(t, "nhid-11", rd1))

	rd2 := &rfpb.RangeDescriptor{
		Left:       []byte("a"),
		Right:      []byte("z"),
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 2, NodeId: 3},
			{ClusterId: 2, NodeId: 4},
			{ClusterId: 2, NodeId: 5},
		},
	}

	// Send another member event with the stale advertisement.
	rc.MemberEvent(rangeUpdateEvent(t, "nhid-11", rd2))

	// Expect that the range update was not accepted.
	require.Nil(t, rc.Get([]byte("m")))
}

package rangecache_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

// Range flow looks like this:
// grpcAddr := rangeCache.Get(key)
// for {
// 	if grpcAddr == nil {
// 		grpcAddr = readRangeFromMeta(key)
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
	*log.LogLevel = "debug"
	log.Configure()
}

func metaRangeEvent(t *testing.T, nhid string, rangeDescriptor *rfpb.RangeDescriptor) (serf.EventType, serf.Event) {
	buf, err := proto.Marshal(rangeDescriptor)
	if err != nil {
		t.Fatalf("error marshaling proto: %s", err)
	}

	event := serf.MemberEvent{
		Type: serf.EventMemberUpdate,
		Members: []serf.Member{serf.Member{
			Tags: map[string]string{
				constants.MetaRangeTag: string(buf),
			},
		}},
	}

	return event.Type, event
}

func TestMemberEvent(t *testing.T) {
	rc := rangecache.New()

	// Advertise a (fake) range advertisement.
	rc.OnEvent(metaRangeEvent(t, "nhid-11", &rfpb.RangeDescriptor{
		Left:       keys.MinByte,
		Right:      keys.MaxByte,
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
		Left:       keys.MinByte,
		Right:      keys.MaxByte,
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}

	// Advertise a few (fake) range advertisements.
	rc.OnEvent(metaRangeEvent(t, nodes[0], rd))
	rc.OnEvent(metaRangeEvent(t, nodes[1], rd))
	rc.OnEvent(metaRangeEvent(t, nodes[2], rd))

	rr := rc.Get([]byte("m"))
	require.NotNil(t, rr)
	require.Equal(t, uint64(1), rr.GetReplicas()[0].GetNodeId())
	require.Equal(t, uint64(2), rr.GetReplicas()[1].GetNodeId())
	require.Equal(t, uint64(3), rr.GetReplicas()[2].GetNodeId())
}

func TestRangeUpdatedMemberEvent(t *testing.T) {
	rc := rangecache.New()

	rd1 := &rfpb.RangeDescriptor{
		Left:       keys.MinByte,
		Right:      keys.MaxByte,
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}

	// Advertise a (fake) range advertisement.
	rc.OnEvent(metaRangeEvent(t, "nhid-11", rd1))

	rd2 := &rfpb.RangeDescriptor{
		Left:       keys.MinByte,
		Right:      []byte("z"),
		Generation: 2,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 2, NodeId: 3},
			{ClusterId: 2, NodeId: 4},
			{ClusterId: 2, NodeId: 5},
		},
	}

	// Now advertise again, with a higher generation this time.
	rc.OnEvent(metaRangeEvent(t, "nhid-11", rd2))

	rr := rc.Get([]byte("m"))
	require.NotNil(t, rr)
	require.Equal(t, uint64(3), rr.GetReplicas()[0].GetNodeId())
	require.Equal(t, uint64(4), rr.GetReplicas()[1].GetNodeId())
	require.Equal(t, uint64(5), rr.GetReplicas()[2].GetNodeId())
}

func TestRangeStaleMemberEvent(t *testing.T) {
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
	rc.OnEvent(metaRangeEvent(t, "nhid-11", rd1))

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
	rc.OnEvent(metaRangeEvent(t, "nhid-11", rd2))

	// Expect that the range update was not accepted.
	require.Nil(t, rc.Get([]byte("m")))
}

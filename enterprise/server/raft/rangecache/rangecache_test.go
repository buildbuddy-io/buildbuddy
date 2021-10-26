package rangecache_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
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

func rangeLeaveEvent(grpcAddress string) (serf.EventType, *serf.Member) {
	member := &serf.Member{
		Tags: map[string]string{
			constants.GRPCAddressTag: grpcAddress,
		},
	}
	return serf.EventMemberLeave, member
}

func rangeUpdateEvent(t *testing.T, grpcAddress string, rangeDescriptor *rfpb.RangeDescriptor) (serf.EventType, *serf.Member) {
	buf, err := proto.Marshal(&rfpb.RangeSet{
		Ranges: []*rfpb.RangeDescriptor{rangeDescriptor},
	})
	if err != nil {
		t.Fatalf("error marshaling proto: %s", err)
	}
	member := &serf.Member{
		Tags: map[string]string{
			constants.StoredRangesTag: string(buf),
			constants.GRPCAddressTag:  grpcAddress,
		},
	}
	return serf.EventMemberUpdate, member
}

func TestUpdateAndLeave(t *testing.T) {
	rc := rangecache.New()

	// Advertise a (fake) range advertisement.
	rc.MemberEvent(rangeUpdateEvent(t, "localhost:2000", &rfpb.RangeDescriptor{
		Left:  []byte("a"),
		Right: []byte("b"),
	}))

	require.Equal(t, "localhost:2000", rc.Get([]byte("a")))
	rc.MemberEvent(rangeLeaveEvent("localhost:2000"))
	require.Equal(t, "", rc.Get([]byte("a")))
}

func TestMultipleNodesInRange(t *testing.T) {
	rc := rangecache.New()
	nodes := []string{
		"localhost:2000",
		"localhost:3000",
		"localhost:4000",
	}

	// Advertise a few (fake) range advertisements.
	rc.MemberEvent(rangeUpdateEvent(t, nodes[0], &rfpb.RangeDescriptor{
		Left:  []byte("a"),
		Right: []byte("b"),
	}))
	rc.MemberEvent(rangeUpdateEvent(t, nodes[1], &rfpb.RangeDescriptor{
		Left:  []byte("b"),
		Right: []byte("c"),
	}))
	rc.MemberEvent(rangeUpdateEvent(t, nodes[2], &rfpb.RangeDescriptor{
		Left:  []byte("c"),
		Right: []byte("d"),
	}))

	require.Contains(t, nodes, rc.Get([]byte("a")))
	require.Contains(t, nodes, rc.Get([]byte("b")))
	require.Contains(t, nodes, rc.Get([]byte("c")))
}

func TestOverrideStaleResponse(t *testing.T) {
	rc := rangecache.New()

	// Advertise a (fake) range advertisement.
	rc.MemberEvent(rangeUpdateEvent(t, "localhost:2000", &rfpb.RangeDescriptor{
		Left:  []byte("a"),
		Right: []byte("z"),
	}))
	require.Equal(t, "localhost:2000", rc.Get([]byte("a")))

	err := rc.UpdateStaleRange("localhost:2000", &rfpb.RangeDescriptor{
		Left:  []byte("m"),
		Right: []byte("z"),
	})
	require.Nil(t, err)
	require.Equal(t, "", rc.Get([]byte("a")))
}

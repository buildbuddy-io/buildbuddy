package rangecache_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"

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

func TestMemberEvent(t *testing.T) {
	rc := rangecache.New()

	rc.UpdateRange(&rfpb.RangeDescriptor{
		Start:      keys.MinByte,
		End:        keys.MaxByte,
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ShardId: 1, ReplicaId: 1},
		},
	})

	// Make sure we get back that range descriptor.
	rd := rc.Get([]byte("a"))
	require.NotNil(t, rd)
	require.Equal(t, uint64(1), rd.GetReplicas()[0].GetShardId())
}

func TestSimple(t *testing.T) {
	rc := rangecache.New()

	rd := &rfpb.RangeDescriptor{
		Start:      keys.MinByte,
		End:        keys.MaxByte,
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ShardId: 1, ReplicaId: 1},
			{ShardId: 1, ReplicaId: 2},
			{ShardId: 1, ReplicaId: 3},
		},
	}

	rc.UpdateRange(rd)

	rr := rc.Get([]byte("m"))
	require.NotNil(t, rr)
	require.Equal(t, uint64(1), rr.GetReplicas()[0].GetReplicaId())
	require.Equal(t, uint64(2), rr.GetReplicas()[1].GetReplicaId())
	require.Equal(t, uint64(3), rr.GetReplicas()[2].GetReplicaId())
}

func TestRangeUpdatedMemberEvent(t *testing.T) {
	rc := rangecache.New()

	rd1 := &rfpb.RangeDescriptor{
		Start:      keys.MinByte,
		End:        keys.MaxByte,
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ShardId: 1, ReplicaId: 1},
			{ShardId: 1, ReplicaId: 2},
			{ShardId: 1, ReplicaId: 3},
		},
	}

	// Advertise a (fake) range advertisement.
	rc.UpdateRange(rd1)

	rd2 := &rfpb.RangeDescriptor{
		Start:      keys.MinByte,
		End:        []byte("z"),
		Generation: 2,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ShardId: 2, ReplicaId: 3},
			{ShardId: 2, ReplicaId: 4},
			{ShardId: 2, ReplicaId: 5},
		},
	}

	// Now advertise again, with a higher generation this time.
	rc.UpdateRange(rd2)

	rr := rc.Get([]byte("m"))
	require.NotNil(t, rr)
	require.Equal(t, uint64(3), rr.GetReplicas()[0].GetReplicaId())
	require.Equal(t, uint64(4), rr.GetReplicas()[1].GetReplicaId())
	require.Equal(t, uint64(5), rr.GetReplicas()[2].GetReplicaId())
}

func TestRangeStaleMemberEvent(t *testing.T) {
	rc := rangecache.New()

	rd1 := &rfpb.RangeDescriptor{
		Start:      []byte("a"),
		End:        []byte("b"),
		Generation: 2,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ShardId: 1, ReplicaId: 1},
			{ShardId: 1, ReplicaId: 2},
			{ShardId: 1, ReplicaId: 3},
		},
	}

	// Advertise a (fake) range advertisement.
	rc.UpdateRange(rd1)

	rd2 := &rfpb.RangeDescriptor{
		Start:      []byte("a"),
		End:        []byte("z"),
		Generation: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ShardId: 2, ReplicaId: 3},
			{ShardId: 2, ReplicaId: 4},
			{ShardId: 2, ReplicaId: 5},
		},
	}

	// Send another member event with the stale advertisement.
	rc.UpdateRange(rd2)

	// Expect that the range update was not accepted.
	require.Nil(t, rc.Get([]byte("m")))
}

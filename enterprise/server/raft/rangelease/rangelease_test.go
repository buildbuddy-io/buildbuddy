package rangelease_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/lni/goutils/random"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dbcl "github.com/lni/dragonboat/v4/client"
	sm "github.com/lni/dragonboat/v4/statemachine"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

const shardID = 1

type testingProposer struct {
	t    testing.TB
	Data map[string]string
}

var _ sender.ISender = &testingSender{}

func statusProto(err error) *statuspb.Status {
	s, _ := gstatus.FromError(err)
	return s.Proto()
}

func (tp *testingProposer) cmdResponse(kv *rfpb.KV, err error) sm.Result {
	p := &rfpb.BatchCmdResponse{
		Union: []*rfpb.ResponseUnion{{
			Status: statusProto(err),
			Value: &rfpb.ResponseUnion_Cas{
				Cas: &rfpb.CASResponse{
					Kv: kv,
				},
			},
		}},
	}
	buf, err := proto.Marshal(p)
	require.NoError(tp.t, err)
	return sm.Result{Data: buf}
}

func (tp *testingProposer) GetNoOPSession(shardID uint64) *dbcl.Session {
	return dbcl.NewNoOPSession(shardID, random.LockGuardedRand)
}

func (tp *testingProposer) SyncPropose(ctx context.Context, session *dbcl.Session, cmd []byte) (sm.Result, error) {
	batch := &rfpb.BatchCmdRequest{}
	if err := proto.Unmarshal(cmd, batch); err != nil {
		return sm.Result{}, err
	}
	// This is "fake" sender that only supports CAS values and stores them in a local map for ease of testing.
	if len(batch.GetUnion()) != 1 {
		tp.t.Fatal("Only one cmd at a time is allowed.")
	}
	for _, req := range batch.GetUnion() {
		switch value := req.Value.(type) {
		case *rfpb.RequestUnion_Cas:
			kv := value.Cas.GetKv()
			key := string(kv.GetKey())
			expected := string(value.Cas.GetExpectedValue())
			existing := tp.Data[key]
			if expected != existing {
				currentKV := &rfpb.KV{
					Key:   kv.Key,
					Value: []byte(existing),
				}
				return tp.cmdResponse(currentKV, status.FailedPreconditionError(constants.CASErrorMessage)), nil
			}
			tp.Data[key] = string(kv.GetValue())
			return tp.cmdResponse(kv, nil), nil
		default:
			break
		}
	}
	tp.t.Fatal("unsupported batch cmd value was provided.")
	return sm.Result{}, nil
}

func (tp *testingProposer) SyncRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}

// testingSender forwards all requests directly to the underlying proposer.
type testingSender struct {
	tp *testingProposer
}

func (t *testingSender) SyncPropose(ctx context.Context, key []byte, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}

	sess := t.tp.GetNoOPSession(shardID)

	res, err := t.tp.SyncPropose(ctx, sess, buf)
	if err != nil {
		return nil, err
	}

	resp := &rfpb.BatchCmdResponse{}
	if err := proto.Unmarshal(res.Data, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *testingSender) SyncRead(ctx context.Context, key []byte, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}

	res, err := t.tp.SyncRead(ctx, shardID, buf)
	if err != nil {
		return nil, err
	}

	resp := &rfpb.BatchCmdResponse{}
	if err := proto.Unmarshal(res.([]byte), resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func newTestingProposerAndSender(t testing.TB) (*testingProposer, *testingSender) {
	p := &testingProposer{
		t:    t,
		Data: make(map[string]string),
	}
	return p, &testingSender{p}
}

func TestAcquireAndRelease(t *testing.T) {
	proposer, sender := newTestingProposerAndSender(t)
	liveness := nodeliveness.New("replicaID-1", sender)

	rd := &rfpb.RangeDescriptor{
		Start:   []byte("a"),
		End:     []byte("z"),
		RangeId: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ShardId: 1, ReplicaId: 1},
			{ShardId: 1, ReplicaId: 2},
			{ShardId: 1, ReplicaId: 3},
		},
	}
	l := rangelease.New(proposer, shardID, liveness, rd)

	// Should be able to get a rangelease.
	err := l.Lease()
	require.NoError(t, err)

	// Rangelease should be valid.
	valid := l.Valid()
	require.True(t, valid)

	log.Printf("RangeLease: %s", l)

	// Should be able to release a rangelease.
	err = l.Release()
	require.NoError(t, err)

	// Rangelease should be invalid after release.
	valid = l.Valid()
	require.False(t, valid)
}

func TestAcquireAndReleaseMetaRange(t *testing.T) {
	proposer, sender := newTestingProposerAndSender(t)
	liveness := nodeliveness.New("replicaID-2", sender)

	rd := &rfpb.RangeDescriptor{
		Start:   keys.MinByte,
		End:     []byte("z"),
		RangeId: 2,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ShardId: 1, ReplicaId: 1},
			{ShardId: 1, ReplicaId: 2},
			{ShardId: 1, ReplicaId: 3},
		},
	}
	l := rangelease.New(proposer, shardID, liveness, rd)

	// Should be able to get a rangelease.
	err := l.Lease()
	require.NoError(t, err)

	// Rangelease should be valid.
	valid := l.Valid()
	require.True(t, valid)

	log.Printf("RangeLease: %s", l)

	// Should be able to release a rangelease.
	err = l.Release()
	require.NoError(t, err)

	// Rangelease should be invalid after release.
	valid = l.Valid()
	require.False(t, valid)
}

func TestMetaRangeLeaseKeepalive(t *testing.T) {
	proposer, sender := newTestingProposerAndSender(t)
	liveness := nodeliveness.New("replicaID-3", sender)

	rd := &rfpb.RangeDescriptor{
		Start:   keys.MinByte,
		End:     []byte("z"),
		RangeId: 3,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ShardId: 1, ReplicaId: 1},
			{ShardId: 1, ReplicaId: 2},
			{ShardId: 1, ReplicaId: 3},
		},
	}
	leaseDuration := 100 * time.Millisecond
	gracePeriod := 50 * time.Millisecond
	l := rangelease.New(proposer, shardID, liveness, rd).WithTimeouts(leaseDuration, gracePeriod)

	// Should be able to get a rangelease.
	err := l.Lease()
	require.NoError(t, err)

	// Rangelease should be valid.
	valid := l.Valid()
	require.True(t, valid)

	log.Printf("RangeLease: %s", l)
	time.Sleep(2 * leaseDuration)
	log.Printf("RangeLease: %s", l)

	// Rangelease should have auto-renewed itself and should still be valid.
	valid = l.Valid()
	require.True(t, valid)

	// Should be able to release a rangelease.
	err = l.Release()
	require.NoError(t, err)

	// Rangelease should be invalid after release.
	valid = l.Valid()
	require.False(t, valid)
}

func TestNodeEpochInvalidation(t *testing.T) {
	proposer, sender := newTestingProposerAndSender(t)
	liveness := nodeliveness.New("replicaID-4", sender)

	rd := &rfpb.RangeDescriptor{
		Start:   []byte("a"),
		End:     []byte("z"),
		RangeId: 4,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ShardId: 1, ReplicaId: 1},
			{ShardId: 1, ReplicaId: 2},
			{ShardId: 1, ReplicaId: 3},
		},
	}
	l := rangelease.New(proposer, shardID, liveness, rd)

	// Should be able to get a rangelease.
	err := l.Lease()
	require.NoError(t, err)

	// Rangelease should be valid.
	valid := l.Valid()
	require.True(t, valid)

	log.Printf("RangeLease: %s", l)

	err = liveness.Release()
	require.NoError(t, err)

	// Rangelease should be invalid after the nodeliveness record it's
	// dependent on is released.
	valid = l.Valid()
	require.False(t, valid)

	// Should be able to re-lease it again.
	err = l.Lease()
	require.NoError(t, err)

	// Rangelease should be valid again.
	valid = l.Valid()
	require.True(t, valid)
}

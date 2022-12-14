package rangelease_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

type testingProposer struct {
	t    testing.TB
	Data map[string]string
}

func newTestingProposer(t testing.TB) *testingProposer {
	return &testingProposer{
		t:    t,
		Data: make(map[string]string),
	}
}

func statusProto(err error) *statuspb.Status {
	s, _ := gstatus.FromError(err)
	return s.Proto()
}

func (tp *testingProposer) cmdResponse(kv *rfpb.KV, err error) *rfpb.BatchCmdResponse {
	return &rfpb.BatchCmdResponse{
		Union: []*rfpb.ResponseUnion{{
			Status: statusProto(err),
			Value: &rfpb.ResponseUnion_Cas{
				Cas: &rfpb.CASResponse{
					Kv: kv,
				},
			},
		}},
	}
}

func (tp *testingProposer) SyncPropose(ctx context.Context, _ []byte, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
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
	return nil, nil
}

func (tp *testingProposer) SyncRead(ctx context.Context, _ []byte, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}

func TestAcquireAndRelease(t *testing.T) {
	proposer := newTestingProposer(t)
	liveness := nodeliveness.New("nodeID-1", proposer)

	rd := &rfpb.RangeDescriptor{
		Left:    []byte("a"),
		Right:   []byte("z"),
		RangeId: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}
	l := rangelease.New(proposer, liveness, rd)

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
	proposer := newTestingProposer(t)
	liveness := nodeliveness.New("nodeID-2", proposer)

	rd := &rfpb.RangeDescriptor{
		Left:    keys.MinByte,
		Right:   []byte("z"),
		RangeId: 2,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}
	l := rangelease.New(proposer, liveness, rd)

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
	proposer := newTestingProposer(t)
	liveness := nodeliveness.New("nodeID-3", proposer)

	rd := &rfpb.RangeDescriptor{
		Left:    keys.MinByte,
		Right:   []byte("z"),
		RangeId: 3,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}
	leaseDuration := 100 * time.Millisecond
	gracePeriod := 50 * time.Millisecond
	l := rangelease.New(proposer, liveness, rd).WithTimeouts(leaseDuration, gracePeriod)

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
	proposer := newTestingProposer(t)
	liveness := nodeliveness.New("nodeID-4", proposer)

	rd := &rfpb.RangeDescriptor{
		Left:    []byte("a"),
		Right:   []byte("z"),
		RangeId: 4,
		Replicas: []*rfpb.ReplicaDescriptor{
			{ClusterId: 1, NodeId: 1},
			{ClusterId: 1, NodeId: 2},
			{ClusterId: 1, NodeId: 3},
		},
	}
	l := rangelease.New(proposer, liveness, rd)

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

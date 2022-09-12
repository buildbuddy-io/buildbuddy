package nodeliveness_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
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

	// Should be able to lease a liveness record.
	err := liveness.Lease()
	require.NoError(t, err)

	// Liveness record should be valid.
	valid := liveness.Valid()
	require.True(t, valid)

	// Should be able to release a liveness record.
	err = liveness.Release()
	require.NoError(t, err)

	// Liveness record should not be valid.
	valid = liveness.Valid()
	require.False(t, valid)
}

func TestKeepalive(t *testing.T) {
	proposer := newTestingProposer(t)
	leaseDuration := 100 * time.Millisecond
	gracePeriod := 50 * time.Millisecond
	liveness := nodeliveness.New("nodeID-2", proposer).WithTimeouts(leaseDuration, gracePeriod)

	// Should be able to lease a liveness record.
	err := liveness.Lease()
	require.NoError(t, err)

	// Liveness record should be valid.
	valid := liveness.Valid()
	require.True(t, valid)

	time.Sleep(leaseDuration * 2)

	// Liveness record hould have been kept alive.
	valid = liveness.Valid()
	require.True(t, valid)

}

func TestEpochChangeOnLease(t *testing.T) {
	proposer := newTestingProposer(t)
	liveness := nodeliveness.New("nodeID-3", proposer)

	// Should be able to lease a liveness record.
	err := liveness.Lease()
	require.NoError(t, err)

	// Liveness record should be valid.
	valid := liveness.Valid()
	require.True(t, valid)

	// Get the epoch of the liveness record.
	nl, err := liveness.BlockingGetCurrentNodeLiveness()
	require.NoError(t, err)
	require.Equal(t, int64(0), nl.GetEpoch())

	// Release the liveness record.
	err = liveness.Release()
	require.NoError(t, err)

	// Re-acquire it, using a new nodeliveness object, but
	// the same stored data.
	liveness2 := nodeliveness.New("nodeID-3", proposer)

	err = liveness2.Lease()
	require.NoError(t, err)

	valid = liveness2.Valid()
	require.True(t, valid)

	// Ensure that epoch has been incremented.
	nl, err = liveness2.BlockingGetCurrentNodeLiveness()
	require.NoError(t, err)
	require.Equal(t, int64(1), nl.GetEpoch())
}

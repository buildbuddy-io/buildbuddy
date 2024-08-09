package rangelease_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

const rangeID = 1

var _ sender.ISender = &testingSender{}

// testingSender forwards all requests directly to the underlying proposer.
type testingSender struct {
	tp *testutil.TestingProposer
}

func (t *testingSender) SyncPropose(ctx context.Context, key []byte, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}

	sess := t.tp.GetNoOPSession(rangeID)

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

func (t *testingSender) SyncRead(ctx context.Context, key []byte, batch *rfpb.BatchCmdRequest, mods ...sender.Option) (*rfpb.BatchCmdResponse, error) {
	return nil, status.UnimplementedError("not implemented in testingSender")
}

func newTestingProposerAndSenderAndReplica(t testing.TB) (*testutil.TestingProposer, *testingSender, *replica.Replica) {
	r := testutil.NewTestingReplica(t, 1, 1)
	require.NotNil(t, r)

	randID, err := random.RandomString(10)
	require.NoError(t, err)
	p := testutil.NewTestingProposer(t, randID, r.Replica)
	return p, &testingSender{p}, r.Replica
}

func TestAcquireAndRelease(t *testing.T) {
	bgCtx := context.Background()
	ctx, cancel := context.WithTimeout(bgCtx, 3*time.Second)
	defer cancel()

	proposer, sender, rep := newTestingProposerAndSenderAndReplica(t)
	liveness := nodeliveness.New(bgCtx, "replicaID-1", sender)
	session := client.NewSession()

	rd := &rfpb.RangeDescriptor{
		Start:   []byte("a"),
		End:     []byte("z"),
		RangeId: 1,
		Replicas: []*rfpb.ReplicaDescriptor{
			{RangeId: 1, ReplicaId: 1},
			{RangeId: 1, ReplicaId: 2},
			{RangeId: 1, ReplicaId: 3},
		},
	}
	l := rangelease.New(proposer, session, log.NamedSubLogger("test"), liveness, rd, rep)

	// Should be able to get a rangelease.
	err := l.Lease(ctx)
	require.NoError(t, err)

	// Rangelease should be valid.
	valid := l.Valid(ctx)
	require.True(t, valid)

	log.Printf("RangeLease: %s", l.Desc(ctx))

	// Should be able to release a rangelease.
	err = l.Release(ctx)
	require.NoError(t, err)

	// Rangelease should be invalid after release.
	valid = l.Valid(ctx)
	require.False(t, valid)
}

func TestAcquireAndReleaseMetaRange(t *testing.T) {
	bgCtx := context.Background()
	ctx, cancel := context.WithTimeout(bgCtx, 3*time.Second)
	defer cancel()

	proposer, sender, rep := newTestingProposerAndSenderAndReplica(t)
	liveness := nodeliveness.New(bgCtx, "replicaID-2", sender)
	session := client.NewSession()

	rd := &rfpb.RangeDescriptor{
		Start:   keys.MinByte,
		End:     []byte("z"),
		RangeId: 2,
		Replicas: []*rfpb.ReplicaDescriptor{
			{RangeId: 1, ReplicaId: 1},
			{RangeId: 1, ReplicaId: 2},
			{RangeId: 1, ReplicaId: 3},
		},
	}
	l := rangelease.New(proposer, session, log.NamedSubLogger("test"), liveness, rd, rep)

	// Should be able to get a rangelease.
	err := l.Lease(ctx)
	require.NoError(t, err)

	// Rangelease should be valid.
	valid := l.Valid(ctx)
	require.True(t, valid)

	log.Printf("RangeLease: %s", l.Desc(ctx))

	// Should be able to release a rangelease.
	err = l.Release(ctx)
	require.NoError(t, err)

	// Rangelease should be invalid after release.
	valid = l.Valid(ctx)
	require.False(t, valid)
}

func TestMetaRangeLeaseKeepalive(t *testing.T) {
	bgCtx := context.Background()
	ctx, cancel := context.WithTimeout(bgCtx, 3*time.Second)
	defer cancel()

	proposer, sender, rep := newTestingProposerAndSenderAndReplica(t)
	liveness := nodeliveness.New(bgCtx, "replicaID-3", sender)
	session := client.NewSession()

	rd := &rfpb.RangeDescriptor{
		Start:   keys.MinByte,
		End:     []byte("z"),
		RangeId: 3,
		Replicas: []*rfpb.ReplicaDescriptor{
			{RangeId: 1, ReplicaId: 1},
			{RangeId: 1, ReplicaId: 2},
			{RangeId: 1, ReplicaId: 3},
		},
	}
	leaseDuration := 100 * time.Millisecond
	gracePeriod := 50 * time.Millisecond
	l := rangelease.New(proposer, session, log.NamedSubLogger("test"), liveness, rd, rep).WithTimeouts(leaseDuration, gracePeriod)

	// Should be able to get a rangelease.
	err := l.Lease(ctx)
	require.NoError(t, err)

	// Rangelease should be valid.
	valid := l.Valid(ctx)
	require.True(t, valid)

	log.Printf("RangeLease: %s", l.Desc(ctx))
	time.Sleep(2 * leaseDuration)
	log.Printf("RangeLease: %s", l.Desc(ctx))

	// Rangelease should have auto-renewed itself and should still be valid.
	valid = l.Valid(ctx)
	require.True(t, valid)

	// Should be able to release a rangelease.
	err = l.Release(ctx)
	require.NoError(t, err)

	// Rangelease should be invalid after release.
	valid = l.Valid(ctx)
	require.False(t, valid)
}

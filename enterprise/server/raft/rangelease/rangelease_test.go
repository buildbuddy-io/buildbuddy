package rangelease_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/pebble"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/lni/dragonboat/v4"
	"github.com/stretchr/testify/require"

	dbcl "github.com/lni/dragonboat/v4/client"
	dbsm "github.com/lni/dragonboat/v4/statemachine"
	dbrd "github.com/lni/goutils/random"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

type fakeStore struct{}

func (fs *fakeStore) AddRange(rd *rfpb.RangeDescriptor, r *replica.Replica)    {}
func (fs *fakeStore) RemoveRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {}
func (fs *fakeStore) Sender() *sender.Sender {
	return nil
}
func (fs *fakeStore) AddPeer(ctx context.Context, sourceShardID, newShardID uint64) error {
	return nil
}
func (fs *fakeStore) SnapshotCluster(ctx context.Context, shardID uint64) error {
	return nil
}
func newTestReplica(t testing.TB, rootDir string, shardID, replicaID uint64, store replica.IStore) *replica.Replica {
	db, err := pebble.Open(rootDir, "test", &pebble.Options{})
	require.NoError(t, err)

	leaser := pebble.NewDBLeaser(db)
	t.Cleanup(func() {
		leaser.Close()
		db.Close()
	})

	return replica.New(leaser, shardID, replicaID, store, nil /*=usageUpdates=*/)
}

const shardID = 1

type testingProposer struct {
	t   testing.TB
	id  string
	r   *replica.Replica
	idx uint64
}

func (tp *testingProposer) ID() string {
	return tp.id
}

func (tp *testingProposer) GetNoOPSession(shardID uint64) *dbcl.Session {
	return dbcl.NewNoOPSession(shardID, dbrd.LockGuardedRand)
}

func (tp *testingProposer) makeEntry(cmd []byte) dbsm.Entry {
	tp.idx += 1
	return dbsm.Entry{Cmd: cmd, Index: tp.idx}
}

func (tp *testingProposer) SyncPropose(ctx context.Context, session *dbcl.Session, cmd []byte) (dbsm.Result, error) {
	entries, err := tp.r.Update([]dbsm.Entry{tp.makeEntry(cmd)})
	if err != nil {
		return dbsm.Result{}, err
	}
	return entries[0].Result, nil
}

func (tp *testingProposer) SyncRead(ctx context.Context, shardID uint64, query interface{}) (interface{}, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}
func (tp *testingProposer) ReadIndex(shardID uint64, timeout time.Duration) (*dragonboat.RequestState, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}
func (tp *testingProposer) ReadLocalNode(rs *dragonboat.RequestState, query interface{}) (interface{}, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}
func (tp *testingProposer) StaleRead(shardID uint64, query interface{}) (interface{}, error) {
	return nil, status.UnimplementedError("not implemented in testingProposer")
}

var _ sender.ISender = &testingSender{}

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

func (t *testingSender) SyncRead(ctx context.Context, key []byte, batch *rfpb.BatchCmdRequest, mods ...sender.Option) (*rfpb.BatchCmdResponse, error) {
	return nil, status.UnimplementedError("not implemented in testingSender")
}

func newTestingProposerAndSenderAndReplica(t testing.TB) (*testingProposer, *testingSender, *replica.Replica) {
	rootDir := testfs.MakeTempDir(t)
	store := &fakeStore{}
	r := newTestReplica(t, rootDir, 1, 1, store)
	require.NotNil(t, r)

	randID, err := random.RandomString(10)
	require.NoError(t, err)
	p := &testingProposer{
		t:  t,
		id: randID,
		r:  r,
	}
	return p, &testingSender{p}, r
}

func TestAcquireAndRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
	defer cancel()

	proposer, sender, rep := newTestingProposerAndSenderAndReplica(t)
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
	l := rangelease.New(proposer, log.NamedSubLogger("test"), liveness, rd, rep)

	// Should be able to get a rangelease.
	err := l.Lease(ctx)
	require.NoError(t, err)

	// Rangelease should be valid.
	valid := l.Valid()
	require.True(t, valid)

	log.Printf("RangeLease: %s", l)

	// Should be able to release a rangelease.
	err = l.Release(ctx)
	require.NoError(t, err)

	// Rangelease should be invalid after release.
	valid = l.Valid()
	require.False(t, valid)
}

func TestAcquireAndReleaseMetaRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
	defer cancel()

	proposer, sender, rep := newTestingProposerAndSenderAndReplica(t)
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
	l := rangelease.New(proposer, log.NamedSubLogger("test"), liveness, rd, rep)

	// Should be able to get a rangelease.
	err := l.Lease(ctx)
	require.NoError(t, err)

	// Rangelease should be valid.
	valid := l.Valid()
	require.True(t, valid)

	log.Printf("RangeLease: %s", l)

	// Should be able to release a rangelease.
	err = l.Release(ctx)
	require.NoError(t, err)

	// Rangelease should be invalid after release.
	valid = l.Valid()
	require.False(t, valid)
}

func TestMetaRangeLeaseKeepalive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
	defer cancel()

	proposer, sender, rep := newTestingProposerAndSenderAndReplica(t)
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
	l := rangelease.New(proposer, log.NamedSubLogger("test"), liveness, rd, rep).WithTimeouts(leaseDuration, gracePeriod)

	// Should be able to get a rangelease.
	err := l.Lease(ctx)
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
	err = l.Release(ctx)
	require.NoError(t, err)

	// Rangelease should be invalid after release.
	valid = l.Valid()
	require.False(t, valid)
}

func TestNodeEpochInvalidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
	defer cancel()

	proposer, sender, rep := newTestingProposerAndSenderAndReplica(t)
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
	l := rangelease.New(proposer, log.NamedSubLogger("test"), liveness, rd, rep)

	// Should be able to get a rangelease.
	err := l.Lease(ctx)
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
	err = l.Lease(ctx)
	require.NoError(t, err)

	// Rangelease should be valid again.
	valid = l.Valid()
	require.True(t, valid)
}

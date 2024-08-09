package client_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/testutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

func newTestingProposal(t testing.TB, rangeID uint64) *testutil.TestingProposer {
	r := testutil.NewTestingReplica(t, rangeID, 1)
	require.NotNil(t, r)
	randID, err := random.RandomString(10)
	require.NoError(t, err)
	p := testutil.NewTestingProposer(t, randID, r.Replica)
	require.NotNil(t, p)
	return p
}

func increment(t testing.TB, ctx context.Context, rangeID uint64, p *testutil.TestingProposer, session *client.Session, expectedValue int64) {
	req, err := rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{
		Key:   []byte(fmt.Sprintf("range%d", rangeID)),
		Delta: 1,
	}).ToProto()
	require.NoError(t, err)
	rsp, err := session.SyncProposeLocal(ctx, p, rangeID, req)
	require.NoError(t, err)
	incrBatch := rbuilder.NewBatchResponseFromProto(rsp)
	incrRsp, err := incrBatch.IncrementResponse(0)
	require.NoError(t, err)
	require.EqualValues(t, expectedValue, incrRsp.GetValue())
}

func TestSession(t *testing.T) {
	tp1 := newTestingProposal(t, 1)
	tp2 := newTestingProposal(t, 2)
	ctx := context.Background()

	session := client.NewSession()

	increment(t, ctx, 1, tp1, session, 1)
	increment(t, ctx, 2, tp2, session, 1)
	increment(t, ctx, 1, tp1, session, 2)
}

func TestSessionInParallel(t *testing.T) {
	proposers := make([]*testutil.TestingProposer, 0, 5)
	for i := 1; i <= 5; i++ {
		tp := newTestingProposal(t, uint64(i))
		proposers = append(proposers, tp)
	}

	session := client.NewSession()

	ctx := context.Background()
	eg, egCtx := errgroup.WithContext(ctx)
	for i := 1; i <= 5; i++ {
		i := i
		eg.Go(func() error {
			increment(t, egCtx, uint64(i), proposers[i-1], session, 1)
			increment(t, egCtx, uint64(i), proposers[i-1], session, 2)
			return nil
		})
	}
	err := eg.Wait()
	require.NoError(t, err)
}

func TestRefreshSession(t *testing.T) {
	clock := clockwork.NewFakeClock()
	proposers := make([]*testutil.TestingProposer, 0, 3)
	for i := 1; i <= 3; i++ {
		tp := newTestingProposal(t, uint64(i))
		proposers = append(proposers, tp)
	}

	session := client.NewSessionWithClock(clock)

	// advance the clock to trigger a refresh
	clock.Advance(90 * time.Minute)

	ctx := context.Background()
	eg, egCtx := errgroup.WithContext(ctx)
	for i := 1; i <= 3; i++ {
		i := i
		eg.Go(func() error {
			increment(t, egCtx, uint64(i), proposers[i-1], session, 1)
			increment(t, egCtx, uint64(i), proposers[i-1], session, 2)
			return nil
		})
	}
	err := eg.Wait()

	require.NoError(t, err)
}

func BenchmarkSession(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		proposers := make([]*testutil.TestingProposer, 0, 5)
		for i := 1; i <= 5; i++ {
			tp := newTestingProposal(b, uint64(i))
			proposers = append(proposers, tp)
		}
		session := client.NewSession()
		ctx := context.Background()
		eg, egCtx := errgroup.WithContext(ctx)
		b.StartTimer()
		for i := 1; i <= 5; i++ {
			i := i
			eg.Go(func() error {
				increment(b, egCtx, uint64(i), proposers[i-1], session, 1)
				increment(b, egCtx, uint64(i), proposers[i-1], session, 2)
				return nil
			})
		}
		eg.Wait()
	}
}

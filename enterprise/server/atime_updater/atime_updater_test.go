package atime_updater

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	user1  = "USER1"
	user2  = "USER2"
	anon   = "ANON"
	group1 = "GROUP1"
	group2 = "GROUP2"
)

var (
	aDigest = digestProto(strings.Repeat("a", 64), 3)
	bDigest = digestProto(strings.Repeat("b", 64), 3)
	cDigest = digestProto(strings.Repeat("c", 64), 3)
	dDigest = digestProto(strings.Repeat("d", 64), 3)
	eDigest = digestProto(strings.Repeat("e", 64), 3)
	fDigest = digestProto(strings.Repeat("f", 64), 3)
	gDigest = digestProto(strings.Repeat("g", 64), 3)
	hDigest = digestProto(strings.Repeat("h", 64), 3)
	iDigest = digestProto(strings.Repeat("i", 64), 3)
	jDigest = digestProto(strings.Repeat("j", 64), 3)
	kDigest = digestProto(strings.Repeat("k", 64), 3)
)

func digestProto(hash string, sizeBytes int64) *repb.Digest {
	return &repb.Digest{Hash: hash, SizeBytes: sizeBytes}
}

type update struct {
	groupID        string
	instanceName   string
	key            digest.Key
	digestFunction repb.DigestFunction_Value
}

func updateOf(groupID string, instanceName string, d *repb.Digest, digestFunction repb.DigestFunction_Value) update {
	return update{
		groupID:        groupID,
		instanceName:   instanceName,
		key:            digest.NewKey(d),
		digestFunction: digestFunction,
	}
}

func (u update) String() string {
	return fmt.Sprintf("groupID:'%s' instanceName:'%s', key:'%s/%d', digestFunction:%s", u.groupID, u.instanceName, u.key.Hash, u.key.SizeBytes, u.digestFunction)
}

type fakeCAS struct {
	t *testing.T

	authenticator interfaces.Authenticator

	mu      sync.Mutex
	updates []update
}

func (f *fakeCAS) getUpdates() []update {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.updates
}

func (f *fakeCAS) clearUpdates() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = []update{}
}

func (f *fakeCAS) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	log.Debugf("FindMissingBlobs: %s", req)

	groupID := interfaces.AuthAnonymousUser
	user, err := f.authenticator.AuthenticatedUser(ctx)
	if err == nil {
		groupID = user.GetGroupID()
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	for _, d := range req.BlobDigests {
		f.updates = append(f.updates, updateOf(groupID, req.InstanceName, d, req.DigestFunction))
	}
	return &repb.FindMissingBlobsResponse{}, nil
}

func (f *fakeCAS) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	f.t.Fatal("Unexpected call to BatchUpdateBlobs")
	return nil, status.InternalError("Unexpected call to BatchUpdateBlobs")
}

func (f *fakeCAS) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	f.t.Fatal("Unexpected call to BatchReadBlobs")
	return nil, status.InternalError("Unexpected call to BatchReadBlobs")
}

func (f *fakeCAS) GetTree(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	f.t.Fatal("Unexpected call to GetTree")
	return status.InternalError("Unexpected call to GetTree")
}

func runFakeCAS(ctx context.Context, env *testenv.TestEnv, t *testing.T) (*fakeCAS, repb.ContentAddressableStorageClient) {
	cas := fakeCAS{t: t, authenticator: env.GetAuthenticator(), updates: []update{}}
	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterContentAddressableStorageServer(grpcServer, &cas)
	go runFunc()
	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return &cas, repb.NewContentAddressableStorageClient(conn)
}

func waitExpectingNUpdates(t *testing.T, n int, cas *fakeCAS) {
	wait := time.Millisecond
	for i := 0; i < 6; i++ {
		time.Sleep(wait)
		wait = wait * 2
		actual := cas.getUpdates()
		if len(actual) == n {
			return
		}
	}
	t.Fatalf("Timed out waiting for expected number of digests. Expected %d, found %d", n, len(cas.getUpdates()))
}

func waitExpectingUpdates(t *testing.T, expected []update, cas *fakeCAS) {
	waitExpectingNUpdates(t, len(expected), cas)
	require.ElementsMatch(t, expected, cas.getUpdates())
}

func tickAlot(clock clockwork.FakeClock) {
	for i := 0; i < 25; i++ {
		clock.Advance(time.Second)
		time.Sleep(5 * time.Millisecond)
	}
}

func expectNoMoreUpdates(t *testing.T, clock clockwork.FakeClock, cas *fakeCAS) {
	cas.clearUpdates()
	tickAlot(clock)
	require.Equal(t, 0, len(cas.updates))
}

func setup(t *testing.T) (interfaces.Authenticator, interfaces.AtimeUpdater, *fakeCAS, clockwork.FakeClock) {
	env := testenv.GetTestEnv(t)
	authenticator := testauth.NewTestAuthenticator(testauth.TestUsers(user1, group1, user2, group2))
	env.SetAuthenticator(authenticator)
	cas, casClient := runFakeCAS(context.Background(), env, t)
	env.SetContentAddressableStorageClient(casClient)
	flags.Set(t, "cache_proxy.remote_atime_max_digests_per_update", 5)
	flags.Set(t, "cache_proxy.remote_atime_max_updates_per_group", 3)
	flags.Set(t, "cache_proxy.remote_atime_update_interval", 995*time.Millisecond)
	fakeClock := clockwork.NewFakeClock()
	env.SetClock(fakeClock)
	require.NoError(t, Register(env))
	updater := env.GetAtimeUpdater()
	require.NotNil(t, updater)
	return authenticator, updater, cas, fakeClock
}

func TestEnqueue_SimpleBatching(t *testing.T) {
	_, updater, cas, clock := setup(t)
	ctx := context.Background()

	updater.Enqueue(ctx, "instance-1", []*repb.Digest{aDigest, bDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(ctx, "instance-1", []*repb.Digest{cDigest}, repb.DigestFunction_SHA256)
	require.Equal(t, 0, len(cas.getUpdates()))
	clock.Advance(time.Minute)
	waitExpectingUpdates(t,
		[]update{
			updateOf(anon, "instance-1", aDigest, repb.DigestFunction_SHA256),
			updateOf(anon, "instance-1", bDigest, repb.DigestFunction_SHA256),
			updateOf(anon, "instance-1", cDigest, repb.DigestFunction_SHA256)},
		cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_Deduping(t *testing.T) {
	_, updater, cas, clock := setup(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		updater.Enqueue(ctx, "instance-1", []*repb.Digest{aDigest}, repb.DigestFunction_SHA256)
	}
	require.Equal(t, 0, len(cas.getUpdates()))
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-1", aDigest, repb.DigestFunction_SHA256)}, cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_InstanceNamesIsolated(t *testing.T) {
	_, updater, cas, clock := setup(t)
	ctx := context.Background()

	updater.Enqueue(ctx, "instance-1", []*repb.Digest{aDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(ctx, "instance-2", []*repb.Digest{aDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(ctx, "instance-3", []*repb.Digest{aDigest}, repb.DigestFunction_SHA256)
	require.Equal(t, 0, len(cas.getUpdates()))
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-1", aDigest, repb.DigestFunction_SHA256)}, cas)
	cas.clearUpdates()
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-2", aDigest, repb.DigestFunction_SHA256)}, cas)
	cas.clearUpdates()
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-3", aDigest, repb.DigestFunction_SHA256)}, cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_DigestFunctionsIsolated(t *testing.T) {
	_, updater, cas, clock := setup(t)
	ctx := context.Background()

	updater.Enqueue(ctx, "instance-1", []*repb.Digest{aDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(ctx, "instance-1", []*repb.Digest{aDigest}, repb.DigestFunction_MD5)
	require.Equal(t, 0, len(cas.getUpdates()))
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-1", aDigest, repb.DigestFunction_SHA256)}, cas)
	cas.clearUpdates()
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-1", aDigest, repb.DigestFunction_MD5)}, cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_GroupsIsolated(t *testing.T) {
	authenticator, updater, cas, clock := setup(t)
	anonCtx := context.Background()
	group1Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user1)
	group2Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user2)

	updater.Enqueue(anonCtx, "instance-1", []*repb.Digest{aDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(group1Ctx, "instance-1", []*repb.Digest{aDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(group2Ctx, "instance-1", []*repb.Digest{aDigest}, repb.DigestFunction_SHA256)
	clock.Advance(time.Second)
	waitExpectingUpdates(t,
		[]update{
			updateOf(anon, "instance-1", aDigest, repb.DigestFunction_SHA256),
			updateOf(group1, "instance-1", aDigest, repb.DigestFunction_SHA256),
			updateOf(group2, "instance-1", aDigest, repb.DigestFunction_SHA256)},
		cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_DedupesToFrontOfLine(t *testing.T) {
	_, updater, cas, clock := setup(t)
	ctx := context.Background()

	updater.Enqueue(ctx, "instance-1", []*repb.Digest{aDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(ctx, "instance-2", []*repb.Digest{bDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(ctx, "instance-1", []*repb.Digest{cDigest}, repb.DigestFunction_SHA256)
	require.Equal(t, 0, len(cas.getUpdates()))
	clock.Advance(time.Second)
	waitExpectingUpdates(t,
		[]update{
			updateOf(anon, "instance-1", aDigest, repb.DigestFunction_SHA256),
			updateOf(anon, "instance-1", cDigest, repb.DigestFunction_SHA256)},
		cas)
	cas.clearUpdates()
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-2", bDigest, repb.DigestFunction_SHA256)}, cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_DigestsDropped(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_atime_max_digests_per_group", 7)
	_, updater, cas, clock := setup(t)
	ctx := context.Background()
	digests := []*repb.Digest{aDigest, bDigest, cDigest, dDigest, eDigest, fDigest, gDigest, hDigest, iDigest, jDigest, kDigest}

	// If the updater accumulates too many digests in a group, it drops them.
	// This threshold is set to 7 above, so expect that many to be sent, though
	// order is not guaranteed so we don't know which 7 will be sent.
	updater.Enqueue(ctx, "instance-1", digests, repb.DigestFunction_SHA256)
	tickAlot(clock)
	waitExpectingNUpdates(t, 7, cas)
	expectNoMoreUpdates(t, clock, cas)

	// Make sure there are no more updates sent.
	cas.clearUpdates()
	expectNoMoreUpdates(t, clock, cas)

	// Expect the same behavior if they're sent one-by-one.
	for _, digest := range digests {
		updater.Enqueue(ctx, "instance-1", []*repb.Digest{digest}, repb.DigestFunction_SHA256)
	}
	tickAlot(clock)
	waitExpectingNUpdates(t, 7, cas)
	cas.clearUpdates()
	expectNoMoreUpdates(t, clock, cas)

	// It shouldn't matter what instance name / digest function they're using.
	for i, digest := range digests {
		updater.Enqueue(ctx, fmt.Sprintf("instance-%d", i%3), []*repb.Digest{digest}, repb.DigestFunction_SHA256)
	}
	tickAlot(clock)
	waitExpectingNUpdates(t, 7, cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_UpdatesDropped(t *testing.T) {
	_, updater, cas, clock := setup(t)
	ctx := context.Background()

	// If the updater accumulates too many updates, it should start to drop
	// them. setup() sets the value of remote_atime_max_updates_per_group to
	// 3, so expect the first 3 to be sent and the others to be dropped.
	for i := 1; i <= 10; i++ {
		updater.Enqueue(ctx, fmt.Sprintf("instance-%d", i), []*repb.Digest{aDigest}, repb.DigestFunction_SHA256)
	}
	tickAlot(clock)
	waitExpectingUpdates(t,
		[]update{
			updateOf(anon, "instance-1", aDigest, repb.DigestFunction_SHA256),
			updateOf(anon, "instance-2", aDigest, repb.DigestFunction_SHA256),
			updateOf(anon, "instance-3", aDigest, repb.DigestFunction_SHA256)},
		cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_Fairness(t *testing.T) {
	authenticator, updater, cas, clock := setup(t)
	anonCtx := context.Background()
	group1Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user1)
	group2Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user2)

	// Updates (in order of first update in the shard):
	//   Anon/1/SHA: A, B, C, D, E
	//   Grp1/1/SHA: A, F, C, G, H, I
	//   Anon/1/MD5: A, B, C, D, E
	//   Anon/2/SHA: F, G, H, I, J
	//   Grp1/2/SHA: A, F
	//   Grp2/1/SHA: A, G, B, G
	//   Anon/1/SHA: A, B, I, G, I, A, B, I, C
	//   Anon/1/SHA: A, A, A, G, B, A, B, C, C
	updater.Enqueue(anonCtx, "instance-1", []*repb.Digest{aDigest, bDigest, cDigest, dDigest, eDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(group1Ctx, "instance-1", []*repb.Digest{aDigest, fDigest, cDigest, gDigest, hDigest, iDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(anonCtx, "instance-1", []*repb.Digest{aDigest, bDigest, cDigest, dDigest, eDigest}, repb.DigestFunction_MD5)
	updater.Enqueue(anonCtx, "instance-2", []*repb.Digest{fDigest, gDigest, hDigest, iDigest, jDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(group1Ctx, "instance-2", []*repb.Digest{aDigest, fDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(group2Ctx, "instance-1", []*repb.Digest{aDigest, gDigest, bDigest, gDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(anonCtx, "instance-1", []*repb.Digest{aDigest, bDigest, iDigest, gDigest, iDigest, aDigest, bDigest, iDigest, cDigest}, repb.DigestFunction_SHA256)
	updater.Enqueue(anonCtx, "instance-1", []*repb.Digest{aDigest, aDigest, aDigest, gDigest, bDigest, aDigest, bDigest, cDigest, cDigest}, repb.DigestFunction_SHA256)
	clock.Advance(time.Second)

	// Expect 5 updates from anon, 5 from group1, and 3 from group2.
	waitExpectingNUpdates(t, 13, cas)
	updateCount := map[string]int{anon: 0, group1: 0, group2: 0}
	for _, update := range cas.getUpdates() {
		updateCount[update.groupID]++
	}
	require.Equal(t, map[string]int{anon: 5, group1: 5, group2: 3}, updateCount)

	// Expect 5 more updates from anon, 2 from group1, and 0 from group2.
	cas.clearUpdates()
	clock.Advance(time.Second)
	waitExpectingNUpdates(t, 7, cas)
	updateCount = map[string]int{anon: 0, group1: 0, group2: 0}
	for _, update := range cas.getUpdates() {
		updateCount[update.groupID]++
	}
	require.Equal(t, map[string]int{anon: 5, group1: 2, group2: 0}, updateCount)

	// Expect 5 more for anon and 1 more for group1.
	cas.clearUpdates()
	clock.Advance(time.Second)
	waitExpectingNUpdates(t, 6, cas)
	updateCount = map[string]int{anon: 0, group1: 0, group2: 0}
	for _, update := range cas.getUpdates() {
		updateCount[update.groupID]++
	}
	require.Equal(t, map[string]int{anon: 5, group1: 1, group2: 0}, updateCount)

	// Finally, expect 2 more for anon.
	cas.clearUpdates()
	clock.Advance(time.Second)
	waitExpectingNUpdates(t, 2, cas)
	updateCount = map[string]int{anon: 0, group1: 0, group2: 0}
	for _, update := range cas.getUpdates() {
		updateCount[update.groupID]++
	}
	require.Equal(t, map[string]int{anon: 2, group1: 0, group2: 0}, updateCount)
}

// The atimeUpdater code does a lot of stuff with mutexes... This test tries to
// trip the race detector. Make sure you run with --config=race for debugging.
func TestEnqueue_Raciness(t *testing.T) {
	authenticator, updater, _, clock := setup(t)
	anonCtx := context.Background()
	group1Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user1)
	group2Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user2)

	contexts := []context.Context{anonCtx, group1Ctx, group2Ctx}
	instances := []string{"instance-1", "instance-2", "instance-3"}
	batches := [][]*repb.Digest{
		[]*repb.Digest{aDigest, bDigest, cDigest, dDigest, eDigest},
		[]*repb.Digest{aDigest, fDigest, cDigest, gDigest, hDigest, iDigest},
		[]*repb.Digest{aDigest, bDigest, cDigest, dDigest, eDigest},
		[]*repb.Digest{eDigest, fDigest, aDigest},
		[]*repb.Digest{aDigest, aDigest, aDigest, aDigest, aDigest, aDigest},
		[]*repb.Digest{aDigest, bDigest, iDigest, gDigest, iDigest, aDigest, bDigest, iDigest, cDigest},
		[]*repb.Digest{aDigest},
	}
	digestFunctions := []repb.DigestFunction_Value{repb.DigestFunction_SHA256, repb.DigestFunction_MD5}

	for i := 0; i < 3000; i++ {
		updater.Enqueue(
			contexts[rand.IntN(len(contexts))],
			instances[rand.IntN(len(instances))],
			batches[rand.IntN(len(batches))],
			digestFunctions[rand.IntN(len(digestFunctions))])
		if rand.IntN(25) == 0 {
			clock.Advance(time.Second)
		}
	}
}

func TestEnqueueByResourceName_ActionCache(t *testing.T) {
	_, updater, cas, ticker := setup(t)
	ctx := context.Background()

	rn, err := digest.NewResourceName(aDigest, "instance-1", rspb.CacheType_AC, repb.DigestFunction_SHA256).ActionCacheString()
	require.NoError(t, err)
	updater.EnqueueByResourceName(ctx, rn)
	expectNoMoreUpdates(t, ticker, cas)
}

func casResourceName(t *testing.T, d *repb.Digest, instanceName string) string {
	rn, err := digest.NewResourceName(d, instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256).DownloadString()
	require.NoError(t, err)
	return rn
}

func TestEnqueueByResourceName_CAS(t *testing.T) {
	authenticator, updater, cas, clock := setup(t)
	anonCtx := context.Background()
	group1Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user1)
	group2Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user2)

	rnA1 := casResourceName(t, aDigest, "instance-1")
	rnA2 := casResourceName(t, aDigest, "instance-2")
	rnB1 := casResourceName(t, bDigest, "instance-1")
	rnB2 := casResourceName(t, bDigest, "instance-2")
	rnC1 := casResourceName(t, cDigest, "instance-1")
	rnC2 := casResourceName(t, cDigest, "instance-2")
	rnD1 := casResourceName(t, dDigest, "instance-1")
	rnD2 := casResourceName(t, dDigest, "instance-2")
	rnE1 := casResourceName(t, eDigest, "instance-1")
	rnF1 := casResourceName(t, fDigest, "instance-1")

	// Updates (in order of first update in the shard):
	//   Anon/2: B, A, C, D
	//   Anon/1: A, C, D, B, E, F
	//   Grp1/1: A
	//   Grp2/1: C
	updater.EnqueueByResourceName(anonCtx, rnB2)
	updater.EnqueueByResourceName(anonCtx, rnA1)
	updater.EnqueueByResourceName(anonCtx, rnC1)
	updater.EnqueueByResourceName(anonCtx, rnA2)
	updater.EnqueueByResourceName(anonCtx, rnB2)
	updater.EnqueueByResourceName(anonCtx, rnC2)
	updater.EnqueueByResourceName(anonCtx, rnA1)
	updater.EnqueueByResourceName(anonCtx, rnD1)
	updater.EnqueueByResourceName(anonCtx, rnA1)
	updater.EnqueueByResourceName(group1Ctx, rnA1)
	updater.EnqueueByResourceName(anonCtx, rnB1)
	updater.EnqueueByResourceName(anonCtx, rnD2)
	updater.EnqueueByResourceName(anonCtx, rnE1)
	updater.EnqueueByResourceName(group2Ctx, rnC1)
	updater.EnqueueByResourceName(anonCtx, rnF1)

	// First update should be 3 for Anon/2, 1 for Grp1/1, and 1 for Grp2/1
	clock.Advance(time.Second)
	waitExpectingNUpdates(t, 6, cas)
	updateCount := map[string]int{anon: 0, group1: 0, group2: 0}
	for _, update := range cas.getUpdates() {
		updateCount[update.groupID]++
	}
	require.Equal(t, map[string]int{anon: 4, group1: 1, group2: 1}, updateCount)

	// Second update should be 5 for Anon/1
	cas.clearUpdates()
	clock.Advance(time.Second)
	waitExpectingNUpdates(t, 5, cas)
	updateCount = map[string]int{anon: 0, group1: 0, group2: 0}
	for _, update := range cas.getUpdates() {
		updateCount[update.groupID]++
	}
	require.Equal(t, map[string]int{anon: 5, group1: 0, group2: 0}, updateCount)

	// Expect that final Anon/1 update to come through.
	cas.clearUpdates()
	clock.Advance(time.Second)
	waitExpectingNUpdates(t, 1, cas)
	updateCount = map[string]int{anon: 0, group1: 0, group2: 0}
	for _, update := range cas.getUpdates() {
		updateCount[update.groupID]++
	}
	require.Equal(t, map[string]int{anon: 1, group1: 0, group2: 0}, updateCount)
}

func TestEnqueueByFindMissing(t *testing.T) {
	_, updater, cas, clock := setup(t)
	ctx := context.Background()

	req := repb.FindMissingBlobsRequest{
		InstanceName:   "instance-1",
		BlobDigests:    []*repb.Digest{aDigest, bDigest, cDigest, dDigest},
		DigestFunction: repb.DigestFunction_SHA256,
	}
	for i := 0; i < 10; i++ {
		updater.EnqueueByFindMissingRequest(ctx, &req)
	}
	require.Equal(t, 0, len(cas.getUpdates()))
	clock.Advance(time.Second)
	waitExpectingUpdates(t,
		[]update{
			updateOf(anon, "instance-1", aDigest, repb.DigestFunction_SHA256),
			updateOf(anon, "instance-1", bDigest, repb.DigestFunction_SHA256),
			updateOf(anon, "instance-1", cDigest, repb.DigestFunction_SHA256),
			updateOf(anon, "instance-1", dDigest, repb.DigestFunction_SHA256)},
		cas)
	cas.clearUpdates()
	for i := 0; i < 100; i++ {
		clock.Advance(time.Second)
	}
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, 0, len(cas.getUpdates()))
}

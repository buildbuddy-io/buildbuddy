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
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	user1  = "USER1"
	user2  = "USER2"
	anon   = "ANON"
	group1 = "GROUP1"
	group2 = "GROUP2"
)

var (
	digest0 = digestProto(strings.Repeat("0", 64), 3)
	digest1 = digestProto(strings.Repeat("1", 64), 3)
	digest2 = digestProto(strings.Repeat("2", 64), 3)
	digest3 = digestProto(strings.Repeat("3", 64), 3)
	digest4 = digestProto(strings.Repeat("4", 64), 3)
	digest5 = digestProto(strings.Repeat("5", 64), 3)
	digest6 = digestProto(strings.Repeat("6", 64), 3)
	digest7 = digestProto(strings.Repeat("7", 64), 3)
	digest8 = digestProto(strings.Repeat("8", 64), 3)
	digest9 = digestProto(strings.Repeat("9", 64), 3)
	digestA = digestProto(strings.Repeat("a", 64), 3)
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
	t testing.TB

	authenticator interfaces.Authenticator

	lastCtx context.Context

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
	f.lastCtx = ctx
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

func runFakeCAS(ctx context.Context, env *testenv.TestEnv, t testing.TB) (*fakeCAS, repb.ContentAddressableStorageClient) {
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

func tickAlot(clock clockwork.FakeClock, interval time.Duration) {
	for i := 0; i < 25; i++ {
		clock.Advance(interval)
		time.Sleep(5 * time.Millisecond)
	}
}

func expectNoMoreUpdates(t *testing.T, clock clockwork.FakeClock, cas *fakeCAS) {
	cas.clearUpdates()
	tickAlot(clock, time.Second)
	require.Equal(t, 0, len(cas.updates))
}

func setup(t testing.TB) (interfaces.Authenticator, interfaces.AtimeUpdater, *fakeCAS, clockwork.FakeClock) {
	env := testenv.GetTestEnv(t)
	authenticator := testauth.NewTestAuthenticator(testauth.TestUsers(user1, group1, user2, group2))
	env.SetAuthenticator(authenticator)
	cas, casClient := runFakeCAS(context.Background(), env, t)
	env.SetContentAddressableStorageClient(casClient)
	fakeClock := clockwork.NewFakeClock()
	env.SetClock(fakeClock)
	require.NoError(t, Register(env))
	updater := env.GetAtimeUpdater()
	require.NotNil(t, updater)
	return authenticator, updater, cas, fakeClock
}

func ctxWithClientIdentity() context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(authutil.ClientIdentityHeaderName, "fakeheader"))
}

func TestAuth(t *testing.T) {
	authenticator, updater, cas, clock := setup(t)

	// No auth, no updates
	ctx := context.Background()
	updater.Enqueue(ctx, "instance-1", []*repb.Digest{digest0}, repb.DigestFunction_SHA256)
	clock.Advance(time.Minute)
	expectNoMoreUpdates(t, clock, cas)

	// Anon updates with client-identity header
	ctx = ctxWithClientIdentity()
	updater.Enqueue(ctx, "instance-1", []*repb.Digest{digest0}, repb.DigestFunction_SHA256)
	clock.Advance(time.Minute)
	waitExpectingUpdates(t,
		[]update{updateOf(anon, "instance-1", digest0, repb.DigestFunction_SHA256)},
		cas)

	cas.mu.Lock()
	clientIdentity := metadata.ValueFromIncomingContext(cas.lastCtx, authutil.ClientIdentityHeaderName)
	require.Equal(t, []string{"fakeheader"}, clientIdentity)
	cas.mu.Unlock()

	cas.clearUpdates()

	// Group updates with JWT and client-identity header
	group1Ctx := authenticator.AuthContextFromAPIKey(ctxWithClientIdentity(), user1)
	updater.Enqueue(group1Ctx, "instance-1", []*repb.Digest{digest0}, repb.DigestFunction_SHA256)
	clock.Advance(time.Minute)
	waitExpectingUpdates(t,
		[]update{updateOf(group1, "instance-1", digest0, repb.DigestFunction_SHA256)},
		cas)

	cas.mu.Lock()
	clientIdentity = metadata.ValueFromIncomingContext(cas.lastCtx, authutil.ClientIdentityHeaderName)
	require.Equal(t, []string{"fakeheader"}, clientIdentity)
	cas.mu.Unlock()
}

func TestEnqueue_SimpleBatching(t *testing.T) {
	_, updater, cas, clock := setup(t)
	ctx := ctxWithClientIdentity()

	updater.Enqueue(ctx, "instance-1", []*repb.Digest{digest0, digest1}, repb.DigestFunction_SHA256)
	updater.Enqueue(ctx, "instance-1", []*repb.Digest{digest2}, repb.DigestFunction_SHA256)
	require.Equal(t, 0, len(cas.getUpdates()))
	clock.Advance(time.Minute)
	waitExpectingUpdates(t,
		[]update{
			updateOf(anon, "instance-1", digest0, repb.DigestFunction_SHA256),
			updateOf(anon, "instance-1", digest1, repb.DigestFunction_SHA256),
			updateOf(anon, "instance-1", digest2, repb.DigestFunction_SHA256)},
		cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_Deduping(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_atime_update_interval", time.Millisecond)
	_, updater, cas, clock := setup(t)
	ctx := ctxWithClientIdentity()

	for i := 0; i < 10; i++ {
		updater.Enqueue(ctx, "instance-1", []*repb.Digest{digest0}, repb.DigestFunction_SHA256)
	}
	require.Equal(t, 0, len(cas.getUpdates()))
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-1", digest0, repb.DigestFunction_SHA256)}, cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_InstanceNamesIsolated(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_atime_update_interval", 999*time.Millisecond)
	_, updater, cas, clock := setup(t)
	ctx := ctxWithClientIdentity()

	updater.Enqueue(ctx, "instance-1", []*repb.Digest{digest0}, repb.DigestFunction_SHA256)
	updater.Enqueue(ctx, "instance-2", []*repb.Digest{digest0}, repb.DigestFunction_SHA256)
	updater.Enqueue(ctx, "instance-3", []*repb.Digest{digest0}, repb.DigestFunction_SHA256)
	require.Equal(t, 0, len(cas.getUpdates()))
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-1", digest0, repb.DigestFunction_SHA256)}, cas)
	cas.clearUpdates()
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-2", digest0, repb.DigestFunction_SHA256)}, cas)
	cas.clearUpdates()
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-3", digest0, repb.DigestFunction_SHA256)}, cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_DigestFunctionsIsolated(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_atime_update_interval", 999*time.Millisecond)
	_, updater, cas, clock := setup(t)
	ctx := ctxWithClientIdentity()

	updater.Enqueue(ctx, "instance-1", []*repb.Digest{digest0}, repb.DigestFunction_SHA256)
	updater.Enqueue(ctx, "instance-1", []*repb.Digest{digest0}, repb.DigestFunction_BLAKE3)
	require.Equal(t, 0, len(cas.getUpdates()))
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-1", digest0, repb.DigestFunction_SHA256)}, cas)
	cas.clearUpdates()
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-1", digest0, repb.DigestFunction_BLAKE3)}, cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_GroupsIsolated(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_atime_update_interval", time.Millisecond)
	authenticator, updater, cas, clock := setup(t)
	anonCtx := ctxWithClientIdentity()
	group1Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user1)
	group2Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user2)

	updater.Enqueue(anonCtx, "instance-1", []*repb.Digest{digest0}, repb.DigestFunction_SHA256)
	updater.Enqueue(group1Ctx, "instance-1", []*repb.Digest{digest0}, repb.DigestFunction_SHA256)
	updater.Enqueue(group2Ctx, "instance-1", []*repb.Digest{digest0}, repb.DigestFunction_SHA256)
	clock.Advance(time.Second)
	waitExpectingUpdates(t,
		[]update{
			updateOf(anon, "instance-1", digest0, repb.DigestFunction_SHA256),
			updateOf(group1, "instance-1", digest0, repb.DigestFunction_SHA256),
			updateOf(group2, "instance-1", digest0, repb.DigestFunction_SHA256)},
		cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_DedupesToFrontOfLine(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_atime_update_interval", 999*time.Millisecond)
	_, updater, cas, clock := setup(t)
	ctx := ctxWithClientIdentity()

	updater.Enqueue(ctx, "instance-1", []*repb.Digest{digest0}, repb.DigestFunction_SHA256)
	updater.Enqueue(ctx, "instance-2", []*repb.Digest{digest1}, repb.DigestFunction_SHA256)
	updater.Enqueue(ctx, "instance-1", []*repb.Digest{digest2}, repb.DigestFunction_SHA256)
	require.Equal(t, 0, len(cas.getUpdates()))
	clock.Advance(time.Second)
	waitExpectingUpdates(t,
		[]update{
			updateOf(anon, "instance-1", digest0, repb.DigestFunction_SHA256),
			updateOf(anon, "instance-1", digest2, repb.DigestFunction_SHA256)},
		cas)
	cas.clearUpdates()
	clock.Advance(time.Second)
	waitExpectingUpdates(t, []update{updateOf(anon, "instance-2", digest1, repb.DigestFunction_SHA256)}, cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_DigestsDropped(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_atime_max_digests_per_group", 7)
	_, updater, cas, clock := setup(t)
	ctx := ctxWithClientIdentity()
	digests := []*repb.Digest{digest0, digest1, digest2, digest3, digest4, digest5, digest6, digest7, digest8, digest9, digestA}

	// If the updater accumulates too many digests in a group, it drops them.
	// This threshold is set to 7 above, so expect that many to be sent, though
	// order is not guaranteed so we don't know which 7 will be sent.
	updater.Enqueue(ctx, "instance-1", digests, repb.DigestFunction_SHA256)
	tickAlot(clock, time.Second)
	waitExpectingNUpdates(t, 7, cas)
	expectNoMoreUpdates(t, clock, cas)

	// Make sure there are no more updates sent.
	cas.clearUpdates()
	expectNoMoreUpdates(t, clock, cas)

	// Expect the same behavior if they're sent one-by-one.
	for _, digest := range digests {
		updater.Enqueue(ctx, "instance-1", []*repb.Digest{digest}, repb.DigestFunction_SHA256)
	}
	tickAlot(clock, time.Second)
	waitExpectingNUpdates(t, 7, cas)
	cas.clearUpdates()
	expectNoMoreUpdates(t, clock, cas)

	// It shouldn't matter what instance name / digest function they're using.
	for i, digest := range digests {
		updater.Enqueue(ctx, fmt.Sprintf("instance-%d", i%3), []*repb.Digest{digest}, repb.DigestFunction_SHA256)
	}
	tickAlot(clock, time.Second)
	waitExpectingNUpdates(t, 7, cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_UpdatesDropped(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_atime_max_digests_per_update", 5)
	flags.Set(t, "cache_proxy.remote_atime_max_updates_per_group", 3)
	flags.Set(t, "cache_proxy.remote_atime_update_interval", 999*time.Millisecond)
	_, updater, cas, clock := setup(t)
	ctx := ctxWithClientIdentity()

	// If the updater accumulates too many updates, it should start to drop
	// them. setupTest() sets the value of remote_atime_max_updates_per_group to
	// 3, so expect the first 3 to be sent and the others to be dropped.
	for i := 1; i <= 10; i++ {
		updater.Enqueue(ctx, fmt.Sprintf("instance-%d", i), []*repb.Digest{digest0}, repb.DigestFunction_SHA256)
	}
	tickAlot(clock, time.Second)
	waitExpectingUpdates(t,
		[]update{
			updateOf(anon, "instance-1", digest0, repb.DigestFunction_SHA256),
			updateOf(anon, "instance-2", digest0, repb.DigestFunction_SHA256),
			updateOf(anon, "instance-3", digest0, repb.DigestFunction_SHA256)},
		cas)
	expectNoMoreUpdates(t, clock, cas)
}

func TestEnqueue_Fairness(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_atime_max_digests_per_update", 5)
	flags.Set(t, "cache_proxy.remote_atime_max_updates_per_group", 3)
	flags.Set(t, "cache_proxy.remote_atime_update_interval", 999*time.Millisecond)
	authenticator, updater, cas, clock := setup(t)
	anonCtx := ctxWithClientIdentity()
	group1Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user1)
	group2Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user2)

	// Updates (in order of first update in the shard):
	//   Anon/1/SHA: 0, 1, 2, 3, 4
	//   Grp1/1/SHA: 0, 5, 2, 6, 7, 8
	//   Anon/1/MD5: 0, 1, 2, 3, 4
	//   Anon/2/SHA: 5, 6, 8, 9, A
	//   Grp1/2/SHA: 0, 5
	//   Grp2/1/SHA: 0, 6, 1, 6
	//   Anon/1/SHA: 0, 1, 9, 6, 9, 0, 1, 9, 2
	//   Anon/1/SHA: 0, 0, 0, 6, 1, 0, 1, 2, 2
	updater.Enqueue(anonCtx, "instance-1", []*repb.Digest{digest0, digest1, digest2, digest3, digest4}, repb.DigestFunction_SHA256)
	updater.Enqueue(group1Ctx, "instance-1", []*repb.Digest{digest0, digest5, digest2, digest6, digest7, digest8}, repb.DigestFunction_SHA256)
	updater.Enqueue(anonCtx, "instance-1", []*repb.Digest{digest0, digest1, digest2, digest3, digest4}, repb.DigestFunction_MD5)
	updater.Enqueue(anonCtx, "instance-2", []*repb.Digest{digest5, digest6, digest8, digest9, digestA}, repb.DigestFunction_SHA256)
	updater.Enqueue(group1Ctx, "instance-2", []*repb.Digest{digest0, digest5}, repb.DigestFunction_SHA256)
	updater.Enqueue(group2Ctx, "instance-1", []*repb.Digest{digest0, digest6, digest1, digest6}, repb.DigestFunction_SHA256)
	updater.Enqueue(anonCtx, "instance-1", []*repb.Digest{digest0, digest1, digest9, digest6, digest9, digest0, digest1, digest9, digest2}, repb.DigestFunction_SHA256)
	updater.Enqueue(anonCtx, "instance-1", []*repb.Digest{digest0, digest0, digest0, digest6, digest1, digest0, digest1, digest2, digest2}, repb.DigestFunction_SHA256)
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
	flags.Set(t, "cache_proxy.remote_atime_max_digests_per_update", 1_000)
	flags.Set(t, "cache_proxy.remote_atime_max_updates_per_group", 1_000)
	flags.Set(t, "cache_proxy.remote_atime_update_interval", time.Millisecond)
	authenticator, updater, _, clock := setup(t)
	anonCtx := ctxWithClientIdentity()
	group1Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user1)
	group2Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user2)

	contexts := []context.Context{anonCtx, group1Ctx, group2Ctx}
	instances := []string{"instance-1", "instance-2", "instance-3"}
	batches := [][]*repb.Digest{
		[]*repb.Digest{digest0, digest1, digest2, digest3, digest4},
		[]*repb.Digest{digest0, digest5, digest2, digest6, digest7, digest8},
		[]*repb.Digest{digest0, digest1, digest2, digest3, digest4},
		[]*repb.Digest{digest4, digest5, digest0},
		[]*repb.Digest{digest0, digest0, digest0, digest0, digest0, digest0},
		[]*repb.Digest{digest0, digest1, digest8, digest6, digest8, digest0, digest1, digest8, digest2},
		[]*repb.Digest{digest0},
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
	ctx := ctxWithClientIdentity()

	rn := digest.NewACResourceName(digest0, "instance-1", repb.DigestFunction_SHA256).ActionCacheString()
	updater.EnqueueByResourceName(ctx, rn)
	expectNoMoreUpdates(t, ticker, cas)
}

func casResourceName(t *testing.T, d *repb.Digest, instanceName string) string {
	return digest.NewCASResourceName(d, instanceName, repb.DigestFunction_SHA256).DownloadString()
}

func TestEnqueueByResourceName_CAS(t *testing.T) {
	flags.Set(t, "cache_proxy.remote_atime_max_digests_per_update", 5)
	flags.Set(t, "cache_proxy.remote_atime_max_updates_per_group", 3)
	flags.Set(t, "cache_proxy.remote_atime_update_interval", 999*time.Millisecond)
	authenticator, updater, cas, clock := setup(t)
	anonCtx := ctxWithClientIdentity()
	group1Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user1)
	group2Ctx := authenticator.AuthContextFromAPIKey(context.Background(), user2)

	rn01 := casResourceName(t, digest0, "instance-1")
	rn02 := casResourceName(t, digest0, "instance-2")
	rn11 := casResourceName(t, digest1, "instance-1")
	rn12 := casResourceName(t, digest1, "instance-2")
	rn21 := casResourceName(t, digest2, "instance-1")
	rn22 := casResourceName(t, digest2, "instance-2")
	rn31 := casResourceName(t, digest3, "instance-1")
	rn32 := casResourceName(t, digest3, "instance-2")
	rn41 := casResourceName(t, digest4, "instance-1")
	rn51 := casResourceName(t, digest5, "instance-1")

	// Updates (in order of first update in the shard):
	//   Anon/2: B, A, C, D
	//   Anon/1: A, C, D, B, E, F
	//   Grp1/1: A
	//   Grp2/1: C
	updater.EnqueueByResourceName(anonCtx, rn12)
	updater.EnqueueByResourceName(anonCtx, rn01)
	updater.EnqueueByResourceName(anonCtx, rn21)
	updater.EnqueueByResourceName(anonCtx, rn02)
	updater.EnqueueByResourceName(anonCtx, rn12)
	updater.EnqueueByResourceName(anonCtx, rn22)
	updater.EnqueueByResourceName(anonCtx, rn01)
	updater.EnqueueByResourceName(anonCtx, rn31)
	updater.EnqueueByResourceName(anonCtx, rn01)
	updater.EnqueueByResourceName(group1Ctx, rn01)
	updater.EnqueueByResourceName(anonCtx, rn11)
	updater.EnqueueByResourceName(anonCtx, rn32)
	updater.EnqueueByResourceName(anonCtx, rn41)
	updater.EnqueueByResourceName(group2Ctx, rn21)
	updater.EnqueueByResourceName(anonCtx, rn51)

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

func BenchmarkEnqueue(b *testing.B) {
	numToEnqueue := 10_000
	instances := []string{"instance-1", "instance-2", "instance-3"}
	digests := []*repb.Digest{digest0, digest1, digest2, digest3, digest4, digest5, digest6, digest7, digest8, digest9}

	flags.Set(b, "cache_proxy.remote_atime_max_digests_per_update", b.N*len(instances)*len(digests)*numToEnqueue)
	flags.Set(b, "cache_proxy.remote_atime_max_updates_per_group", b.N*len(instances)*len(digests)*numToEnqueue)

	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		_, updater, _, _ := setup(b)
		b.StartTimer()
		wg := sync.WaitGroup{}
		for i := 0; i < 10_000; i++ {
			for _, instance := range instances {
				for _, digest := range digests {
					wg.Add(1)
					go func() {
						updater.Enqueue(ctxWithClientIdentity(), instance, []*repb.Digest{digest}, repb.DigestFunction_SHA256)
						wg.Done()
					}()
				}
			}
		}
		wg.Wait()
	}
}

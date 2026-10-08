package distributed

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

// rollingRestartScenario describes a simulated StatefulSet rollout of a
// distributed cache cluster while clients keep writing to it.
type rollingRestartScenario struct {
	name string
	// Number of cache nodes in the cluster.
	nodes int
	// Number of nodes restarted at the same time, like a StatefulSet's
	// rollingUpdate.maxUnavailable.
	maxUnavailable int
	// Number of full rollouts to run back to back.
	rollouts int
	// How long each node stays down during a restart.
	downtime time.Duration
	// Pause after a batch of nodes is back before restarting the next batch.
	// Kubernetes moves on as soon as the restarted pods are ready, so this is
	// usually zero.
	pauseBetweenBatches time.Duration
	// Size of each node's hinted handoff queue per peer.
	maxHintedHandoffsPerPeer int64
	// Delay between writes from the writer goroutine.
	writeInterval time.Duration
}

type rollingRestartNode struct {
	addr   string
	config Options
	// local stands in for the pod's persistent disk: it survives restarts.
	local interfaces.Cache
	dc    *Cache
	up    bool
}

// TestRollingRestart writes to a distributed cache while its nodes are
// restarted like a StatefulSet rollout, then checks that every blob that was
// written successfully is still readable and stored on all of its replicas.
//
// A restart creates a new Cache on the same local cache instead of calling
// Shutdown and StartListening on the same Cache, because a real restart loses
// the in-memory hinted handoff queues.
func TestRollingRestart(t *testing.T) {
	// testenv logs at debug level, which is far too much with this many
	// writes. Warnings still include dropped hinted handoffs.
	*log.LogLevel = "warn"
	require.NoError(t, log.Configure())
	for _, s := range []rollingRestartScenario{
		{
			name:                     "OneAtATime_OneRollout",
			nodes:                    8,
			maxUnavailable:           1,
			rollouts:                 1,
			downtime:                 2 * time.Second,
			maxHintedHandoffsPerPeer: 100_000,
			writeInterval:            2 * time.Millisecond,
		},
		{
			name:                     "OneAtATime_ThreeRollouts",
			nodes:                    8,
			maxUnavailable:           1,
			rollouts:                 3,
			downtime:                 2 * time.Second,
			maxHintedHandoffsPerPeer: 100_000,
			writeInterval:            2 * time.Millisecond,
		},
		{
			name:                     "TwoAtATime_OneRollout",
			nodes:                    8,
			maxUnavailable:           2,
			rollouts:                 1,
			downtime:                 2 * time.Second,
			maxHintedHandoffsPerPeer: 100_000,
			writeInterval:            2 * time.Millisecond,
		},
		{
			name:                     "TwoAtATime_ThreeRollouts",
			nodes:                    8,
			maxUnavailable:           2,
			rollouts:                 3,
			downtime:                 2 * time.Second,
			maxHintedHandoffsPerPeer: 100_000,
			writeInterval:            2 * time.Millisecond,
		},
		{
			// A small hinted handoff queue stands in for prod write rates,
			// which can fill the real 100k queue while a node is down.
			name:                     "OneAtATime_OneRollout_SmallHintQueue",
			nodes:                    8,
			maxUnavailable:           1,
			rollouts:                 1,
			downtime:                 2 * time.Second,
			maxHintedHandoffsPerPeer: 20,
			writeInterval:            2 * time.Millisecond,
		},
	} {
		t.Run(s.name, func(t *testing.T) {
			runRollingRestartScenario(t, s)
		})
	}
}

func runRollingRestartScenario(t *testing.T, s rollingRestartScenario) {
	flags.Set(t, "cache.distributed_cache.max_hinted_handoffs_per_peer", s.maxHintedHandoffsPerPeer)
	env := testenv.GetTestEnv(t)
	authenticator := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	env.SetAuthenticator(authenticator)
	ctx, err := authenticator.WithAuthenticatedUser(context.Background(), "user1")
	require.NoError(t, err)
	ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)

	addrs := make([]string, s.nodes)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("localhost:%d", testport.FindFree(t))
	}
	baseConfig := Options{
		ReplicationFactor:  3,
		Nodes:              addrs,
		DisableLocalLookup: true,
		// Real nodes heartbeat every second and are down for tens of
		// seconds. Scale both down by about the same factor.
		RPCHeartbeatInterval: 100 * time.Millisecond,
	}

	// mu guards the nodes' dc and up fields, which the writer reads while
	// the rollout changes them.
	var mu sync.Mutex
	nodes := make([]*rollingRestartNode, s.nodes)
	for i, addr := range addrs {
		config := baseConfig
		// NewDistributedCache sorts Nodes in place, so each node needs its
		// own copy.
		config.Nodes = slices.Clone(addrs)
		config.ListenAddr = addr
		local, err := memory_cache.NewMemoryCache(1_000_000_000)
		require.NoError(t, err)
		nodes[i] = &rollingRestartNode{
			addr:   addr,
			config: config,
			local:  local,
			dc:     startRollingRestartNode(t, env, config, local),
			up:     true,
		}
	}
	for _, n := range nodes {
		waitForRollingRestartNode(t, n.addr)
	}

	// Write through random live nodes until the rollouts finish, like
	// clients talking to whichever app pods are up.
	var written []*rspb.ResourceName
	writeErrors := 0
	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			mu.Lock()
			var live []*Cache
			for _, n := range nodes {
				if n.up {
					live = append(live, n.dc)
				}
			}
			mu.Unlock()
			rn, buf := testdigest.RandomCASResourceBuf(t, 100)
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := live[rand.IntN(len(live))].Set(writeCtx, rn, buf)
			cancel()
			if err != nil {
				writeErrors++
			} else {
				written = append(written, rn)
			}
			time.Sleep(s.writeInterval)
		}
	}()

	hintsLostOnRestart := 0
	// Let some writes land before the first restart.
	time.Sleep(500 * time.Millisecond)
	for range s.rollouts {
		for start := 0; start < len(nodes); start += s.maxUnavailable {
			batch := nodes[start:min(start+s.maxUnavailable, len(nodes))]
			for _, n := range batch {
				mu.Lock()
				n.up = false
				dc := n.dc
				mu.Unlock()
				shutdownRollingRestartNode(dc)
				// Shutdown stops delivering hinted handoffs, and a real
				// restart loses whatever is still queued.
				dc.hintedHandoffsMu.RLock()
				for _, ch := range dc.hintedHandoffsByPeer {
					hintsLostOnRestart += len(ch)
				}
				dc.hintedHandoffsMu.RUnlock()
			}
			time.Sleep(s.downtime)
			for _, n := range batch {
				dc := startRollingRestartNode(t, env, n.config, n.local)
				waitForRollingRestartNode(t, n.addr)
				mu.Lock()
				n.dc = dc
				n.up = true
				mu.Unlock()
			}
			time.Sleep(s.pauseBetweenBatches)
		}
	}
	// Keep writing briefly with every node up, then let queued hinted
	// handoffs drain.
	time.Sleep(500 * time.Millisecond)
	close(stop)
	<-writerDone
	time.Sleep(2 * time.Second)

	locals := make(map[string]interfaces.Cache, len(nodes))
	for _, n := range nodes {
		locals[n.addr] = n.local
	}
	underReplicated := 0
	missingFromAllReplicas := 0
	unreadable := 0
	for _, rn := range written {
		replicas := nodes[0].dc.consistentHash.GetAllReplicas(rn.GetDigest().GetHash())[:baseConfig.ReplicationFactor]
		copies := 0
		for _, addr := range replicas {
			exists, err := locals[addr].Contains(ctx, rn)
			require.NoError(t, err)
			if exists {
				copies++
			}
		}
		if copies < len(replicas) {
			underReplicated++
		}
		if copies == 0 {
			missingFromAllReplicas++
		}
		reader := nodes[rand.IntN(len(nodes))].dc
		exists, err := reader.Contains(ctx, rn)
		require.NoError(t, err)
		if !exists {
			unreadable++
		}
	}

	t.Logf("%d writes succeeded, %d failed", len(written), writeErrors)
	t.Logf("%d hinted handoffs were still queued on nodes when they restarted", hintsLostOnRestart)
	t.Logf("%d blobs (%.2f%%) are on fewer than %d replicas, %d on none of them, %d unreadable through the cache",
		underReplicated, 100*float64(underReplicated)/float64(len(written)), baseConfig.ReplicationFactor, missingFromAllReplicas, unreadable)
	assert.Zero(t, underReplicated, "blobs on fewer than %d replicas", baseConfig.ReplicationFactor)
	assert.Zero(t, unreadable, "blobs that were written successfully but can't be read")
}

func startRollingRestartNode(t *testing.T, env environment.Env, config Options, local interfaces.Cache) *Cache {
	dc, err := NewDistributedCache(env, local, config, env.GetHealthChecker())
	require.NoError(t, err)
	require.NoError(t, dc.StartListening())
	t.Cleanup(func() { shutdownRollingRestartNode(dc) })
	return dc
}

func shutdownRollingRestartNode(dc *Cache) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	dc.Shutdown(ctx)
}

func waitForRollingRestartNode(t *testing.T, addr string) {
	conn, err := grpc_client.DialSimple("grpc://"+addr, grpc.WithBlock(), grpc.WithTimeout(3*time.Second))
	require.NoError(t, err)
	conn.Close()
}

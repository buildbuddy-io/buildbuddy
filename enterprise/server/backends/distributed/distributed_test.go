package distributed

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/heartbeat"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/pubsub"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const heartbeatGroupName = "testGroup"

var (
	emptyUserMap = testauth.TestUsers()
)

func getTestEnv(t *testing.T, users map[string]interfaces.UserInfo) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(users))
	return te
}

func getAnonContext(t *testing.T) context.Context {
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	te := getTestEnv(t, emptyUserMap)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	return ctx
}

func lowerTimeoutsForTesting(hbc *heartbeat.HeartbeatChannel) {
	hbc.Period = 100 * time.Millisecond
	hbc.Timeout = 3 * hbc.Period
	hbc.CheckPeriod = 10 * time.Millisecond
}

func newDistributedCache(t *testing.T, te environment.Env, ps interfaces.PubSub, peer string, replicationFactor int, maxSizeBytes int64) *Cache {
	mc, err := memory_cache.NewMemoryCache(maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	dcc := CacheConfig{
		ListenAddr:        peer,
		GroupName:         heartbeatGroupName,
		ReplicationFactor: replicationFactor,
	}
	c, err := NewDistributedCache(te, ps, mc, dcc, te.GetHealthChecker())
	if err != nil {
		t.Fatal(err)
	}
	lowerTimeoutsForTesting(c.heartbeatChannel)
	c.StartListening()
	return c
}

func TestDroppedNode(t *testing.T) {
	return
	te := getTestEnv(t, emptyUserMap)
	ps := pubsub.NewTestPubSub()
	ctx := getAnonContext(t)

	var liveNodes map[string]struct{}
	liveNodesLock := sync.RWMutex{}
	hbc := heartbeat.NewHeartbeatChannel(ps, "", heartbeatGroupName, func(nodes ...string) {
		liveNodesLock.Lock()
		liveNodes = make(map[string]struct{}, 0)
		for _, n := range nodes {
			liveNodes[n] = struct{}{}
		}
		liveNodesLock.Unlock()
	})
	lowerTimeoutsForTesting(hbc)

	waitForNodes := func(peers []string) {
		for {
			liveNodesLock.RLock()
			numAlive := len(liveNodes)
			anyMissing := false
			for _, p := range peers {
				if _, ok := liveNodes[p]; !ok {
					anyMissing = true
					break
				}
			}
			liveNodesLock.RUnlock()

			if len(peers) == numAlive && !anyMissing {
				// Hack: wait a little longer for all nodes to catch up.
				time.Sleep(100 * time.Millisecond)
				log.Printf("Finished waiting for nodes: %s", peers)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	tests := []struct {
		replicas          int
		replicationFactor int
		replicasToFail    int
	}{
		{1, 1, 0},
		{2, 2, 1},
		{3, 2, 1},
		{5, 3, 2},
	}

	for _, testStruct := range tests {
		maxSizeBytes := int64(10000000) // 10MB
		peers := make([]string, 0, testStruct.replicas)
		caches := make(map[string]*Cache, testStruct.replicas)

		log.Printf("TestStruct; %+v", testStruct)
		for i := 0; i < testStruct.replicas; i++ {
			peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
			peers = append(peers, peer)
			caches[peer] = newDistributedCache(t, te, ps, peer, testStruct.replicationFactor, maxSizeBytes)
		}

		waitForNodes(peers)

		digests := make([]*repb.Digest, 0, 100)
		for i := 0; i < 10; i += 1 {
			d, buf := testdigest.NewRandomDigestBuf(t, 1500)
			digests = append(digests, d)

			randomPeer := peers[rand.Intn(len(peers))]
			c := caches[randomPeer]
			err := c.Set(ctx, d, buf)
			if err != nil {
				t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
			}

			randomPeer = peers[rand.Intn(len(peers))]
			c = caches[randomPeer]
			rbuf, err := c.Get(ctx, d)
			if err != nil {
				t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
			}

			// Compute a digest for the bytes returned.
			d2, err := digest.Compute(bytes.NewReader(rbuf))
			if err != nil {
				t.Fatalf("Error computing digest: %s", err.Error())
			}
			if d.GetHash() != d2.GetHash() {
				t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
			}
		}

		// Now shutdown the requested number of nodes.
		for i := 0; i < testStruct.replicasToFail; i++ {
			randPeerIdx := rand.Intn(len(peers))
			randomPeer := peers[randPeerIdx]
			caches[randomPeer].Shutdown()
			delete(caches, randomPeer)
			peers = append(peers[:randPeerIdx], peers[randPeerIdx+1:]...)
		}

		waitForNodes(peers)
		log.Printf("Reduced peer set %s running.", peers)

		for _, d := range digests {
			randomPeer := peers[rand.Intn(len(peers))]
			c := caches[randomPeer]
			rbuf, err := c.Get(ctx, d)
			if err != nil {
				t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
			}

			// Compute a digest for the bytes returned.
			d2, err := digest.Compute(bytes.NewReader(rbuf))
			if err != nil {
				t.Fatalf("Error computing digest: %s", err.Error())
			}
			if d.GetHash() != d2.GetHash() {
				t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
			}
		}

		for _, cache := range caches {
			cache.Shutdown()
		}

		waitForNodes([]string{})
	}
}

func TestEventualConsistency(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ps := pubsub.NewTestPubSub()
	ctx := getAnonContext(t)

	var liveNodes map[string]struct{}
	liveNodesLock := sync.RWMutex{}
	hbc := heartbeat.NewHeartbeatChannel(ps, "", heartbeatGroupName, func(nodes ...string) {
		liveNodesLock.Lock()
		liveNodes = make(map[string]struct{}, 0)
		for _, n := range nodes {
			liveNodes[n] = struct{}{}
		}
		liveNodesLock.Unlock()
	})
	lowerTimeoutsForTesting(hbc)

	waitForNodes := func(peers []string) {
		for {
			liveNodesLock.RLock()
			numAlive := len(liveNodes)
			anyMissing := false
			for _, p := range peers {
				if _, ok := liveNodes[p]; !ok {
					anyMissing = true
					break
				}
			}
			liveNodesLock.RUnlock()

			if len(peers) == numAlive && !anyMissing {
				// Hack: wait a little longer for everyone else to catch up.
				time.Sleep(100 * time.Millisecond)
				log.Printf("Finished waiting for nodes: %s", peers)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	tests := []struct {
		replicas          int
		replicationFactor int
		numFailures       int
	}{
		{2, 2, 1},
		{3, 2, 1},
		{5, 3, 1},
	}

	for _, testStruct := range tests {
		maxSizeBytes := int64(10000000) // 10MB
		peers := make([]string, 0, testStruct.replicas)
		caches := make(map[string]*Cache, testStruct.replicas)

		mu := sync.Mutex{}
		randomPeer := func() (int, string) {
			i := rand.Intn(len(peers))
			return i, peers[i]
		}
		useRandomCache := func(fn func(c *Cache)) {
			mu.Lock()
			defer mu.Unlock()
			_, p := randomPeer()
			fn(caches[p])
		}
		shutdownRandomCache := func() string {
			i, p := randomPeer()
			log.Printf("Shutting down %q...", p)
			mu.Lock()
			defer mu.Unlock()
			c := caches[p]
			delete(caches, p)
			peers = append(peers[:i], peers[i+1:]...)
			c.Shutdown()
			waitForNodes(peers)
			log.Printf("Finished shutting down %q", p)
			return p
		}
		restartCache := func(peer string) {
			log.Printf("Restarting %q...", peer)
			mu.Lock()
			defer mu.Unlock()
			peers = append(peers, peer)
			caches[peer] = newDistributedCache(t, te, ps, peer, testStruct.replicationFactor, maxSizeBytes)
			waitForNodes(peers)
			log.Printf("Finished restarting %q", peer)
		}

		log.Printf("TestStruct; %+v", testStruct)
		for i := 0; i < testStruct.replicas; i++ {
			peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
			peers = append(peers, peer)
			caches[peer] = newDistributedCache(t, te, ps, peer, testStruct.replicationFactor, maxSizeBytes)
		}

		// wait until all nodes have advertised.
		waitForNodes(peers)

		written := make([]*repb.Digest, 0)

		quit := make(chan struct{}, 0)
		done := make(chan bool, 1)
		go func() {
			numReads := 0
			defer func() {
				log.Printf("Writing true to done!")
				done <- true
			}()
			for {
				select {
				case <-quit:
					log.Printf("watcher succesfully ran %d reads and %d writes.", numReads, len(written))
					return
				default:
					useRandomCache(func(c *Cache) {
						d, buf := testdigest.NewRandomDigestBuf(t, 1500)
						if err := c.Set(ctx, d, buf); err != nil {
							t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
						}
						written = append(written, d)
					})

					useRandomCache(func(c *Cache) {
						d := written[rand.Intn(len(written))]
						_, err := c.Get(ctx, d)
						if err != nil {
							t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
						}
						numReads += 1
					})
				}
			}
		}()

		for i := 0; i < testStruct.numFailures; i += 1 {
			p := shutdownRandomCache()
			time.Sleep(10 * time.Millisecond)
			restartCache(p)
			time.Sleep(10 * time.Millisecond)
		}

		// Close our background goroutine & wait for it.
		log.Printf("Closing quit channel!")
		close(quit)
		log.Printf("Waiting for done...")
		<-done
		log.Printf("Got done signal!")

		// Cleanup.
		for _, cache := range caches {
			cache.Shutdown()
		}
		waitForNodes([]string{})
	}
}

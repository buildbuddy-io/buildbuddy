package distributed_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/heartbeat"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/pubsub"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	testauth "github.com/buildbuddy-io/buildbuddy/server/testutil/auth"
	testdigest "github.com/buildbuddy-io/buildbuddy/server/testutil/digest"
	testenv "github.com/buildbuddy-io/buildbuddy/server/testutil/environment"
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

func waitUntilServerIsAlive(addr string) {
	for {
		_, err := net.DialTimeout("tcp", addr, 10*time.Millisecond)
		if err == nil {
			return
		}
	}
}

func waitUntilServerIsDead(addr string) {
	for {
		_, err := net.DialTimeout("tcp", addr, 10*time.Millisecond)
		if err != nil {
			return
		}
	}
}

func newDistributedCache(t *testing.T, te environment.Env, peer string, replicationFactor int, maxSizeBytes int64) *distributed.Cache {
	mc, err := memory_cache.NewMemoryCache(maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	c, err := distributed.NewDistributedCache(te, mc, peer, heartbeatGroupName, replicationFactor)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestDroppedNode(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ps := pubsub.NewTestPubSub()
	te.SetPubSub(ps)
	ctx := getAnonContext(t)

	var liveNodes map[string]struct{}
	liveNodesLock := sync.RWMutex{}
	hbc := heartbeat.NewHeartbeatChannel(te.GetPubSub(), "", heartbeatGroupName, func(nodes ...string) {
		liveNodesLock.Lock()
		liveNodes = make(map[string]struct{}, 0)
		for _, n := range nodes {
			liveNodes[n] = struct{}{}
		}
		liveNodesLock.Unlock()
	})
	_ = hbc // keep hbc around to update liveNodes

	waitForNodes := func(numDesired int) {
		for {
			liveNodesLock.RLock()
			count := len(liveNodes)
			liveNodesLock.RUnlock()
			if count == numDesired {
				return
			}
			time.Sleep(100 * time.Millisecond)
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
		caches := make(map[string]*distributed.Cache, testStruct.replicas)

		log.Printf("TestStruct; %+v", testStruct)
		for i := 0; i < testStruct.replicas; i++ {
			peer := fmt.Sprintf("localhost:%d", app.FreePort(t))
			peers = append(peers, peer)
			caches[peer] = newDistributedCache(t, te, peer, testStruct.replicationFactor, maxSizeBytes)
		}

		// wait for the come up
		for peer, _ := range caches {
			waitUntilServerIsAlive(peer)
		}

		// wait until all nodes have advertised.
		waitForNodes(testStruct.replicas)

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
			waitUntilServerIsDead(randomPeer)
		}

		// Wait until we've reached the desired number of failed nodes.
		desiredNumberRunning := testStruct.replicas - testStruct.replicasToFail
		waitForNodes(desiredNumberRunning)

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
	}
}

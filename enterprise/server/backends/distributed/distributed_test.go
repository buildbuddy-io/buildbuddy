package distributed_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
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
			log.Printf("Got err %+v, server %q is dead", err, addr)
			return
		}
	}
}

func newDistributedCache(t *testing.T, te environment.Env, peer string, maxSizeBytes int64) *distributed.Cache {
	mc, err := memory_cache.NewMemoryCache(maxSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	c, err := distributed.NewDistributedCache(te, mc, peer, heartbeatGroupName)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestDroppedNode(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	ps := pubsub.NewTestPubSub()
	te.SetPubSub(ps)
	maxSizeBytes := int64(10000000) // 10MB

	caches := make([]*distributed.Cache, 0)
	peer1 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	caches = append(caches, newDistributedCache(t, te, peer1, maxSizeBytes))
	waitUntilServerIsAlive(peer1)

	peer2 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	caches = append(caches, newDistributedCache(t, te, peer2, maxSizeBytes))
	waitUntilServerIsAlive(peer2)

	peer3 := fmt.Sprintf("localhost:%d", app.FreePort(t))
	caches = append(caches, newDistributedCache(t, te, peer3, maxSizeBytes))
	waitUntilServerIsAlive(peer3)

	liveNodes := make(map[string]struct{}, 0)
	hbc := heartbeat.NewHeartbeatChannel(te.GetPubSub(), "", heartbeatGroupName, func(nodes ...string) {
		for _, n := range nodes {
			liveNodes[n] = struct{}{}
		}
	})
	// wait until all nodes are up.
	for {
		if len(liveNodes) == 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = hbc

	ctx := getAnonContext(t)
	digests := make([]*repb.Digest, 0, 100)
	for i := 0; i < 100; i += 1 {
		d, buf := testdigest.NewRandomDigestBuf(t, 150)
		digests = append(digests, d)

		c := caches[rand.Intn(len(caches))]
		err := c.Set(ctx, d, buf)
		if err != nil {
			t.Fatalf("Error setting %q in cache: %s", d.GetHash(), err.Error())
		}

		c = caches[rand.Intn(len(caches))]
		rbuf, err := c.Get(ctx, d)
		if err != nil {
			t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
		}

		// Compute a digest for the bytes returned.
		d2, err := digest.Compute(bytes.NewReader(rbuf))
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}

	// Now drop a node.
	caches[0].Shutdown()
	caches = caches[1:]
	waitUntilServerIsDead(peer1)

	// Wait until this node has failed the healthcheck.
	liveNodes = make(map[string]struct{}, 0)
	for {
		if len(liveNodes) == 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	for _, d := range digests {
		c := caches[rand.Intn(len(caches))]
		rbuf, err := c.Get(ctx, d)
		if err != nil {
			t.Fatalf("Error getting %q from cache: %s", d.GetHash(), err.Error())
		}

		// Compute a digest for the bytes returned.
		d2, err := digest.Compute(bytes.NewReader(rbuf))
		if d.GetHash() != d2.GetHash() {
			t.Fatalf("Returned digest %q did not match set value: %q", d2.GetHash(), d.GetHash())
		}
	}
}

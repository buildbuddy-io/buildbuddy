package cache_proxy_peers_test

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/cache_proxy_peers"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

const (
	firstQuarterStart  = "0000000000000000000000000000000000000000000000000000000000000000"
	firstQuarterEnd    = "3fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	secondQuarterStart = "40000000000000000000000000000000000000000000000000000000000000000"
	secondQuarterEnd   = "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	thirdQuarterStart  = "8000000000000000000000000000000000000000000000000000000000000000"
	thirdQuarterEnd    = "bfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	fourthQuarterStart = "c000000000000000000000000000000000000000000000000000000000000000"
	fourthQuarterEnd   = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

	firstHalfStart  = firstQuarterStart
	firstHalfEnd    = secondQuarterEnd
	secondHalfStart = thirdQuarterStart
	secondHalfEnd   = fourthQuarterEnd

	minHash = firstQuarterStart
	maxHash = fourthQuarterEnd
)

func TestNew(t *testing.T) {
	env := testenv.GetTestEnv(t)

	// No peers
	peers, err := cache_proxy_peers.New(env)
	require.NoError(t, err)
	require.True(t, getPeer(t, peers, minHash).Local)
	require.True(t, getPeer(t, peers, maxHash).Local)

	// One local peer
	flags.Set(t, "cache_proxy.peers.local_address", "localhost:1")
	flags.Set(t, "cache_proxy.peers.addresses", []string{"localhost:1"})
	peers, err = cache_proxy_peers.New(env)
	require.NoError(t, err)
	require.True(t, getPeer(t, peers, minHash).Local)
	require.True(t, getPeer(t, peers, maxHash).Local)

	// One remote peer
	flags.Set(t, "cache_proxy.peers.local_address", "localhost:1")
	flags.Set(t, "cache_proxy.peers.addresses", []string{"localhost:2"})
	peers, err = cache_proxy_peers.New(env)
	require.NoError(t, err)
	require.False(t, getPeer(t, peers, minHash).Local)
	require.False(t, getPeer(t, peers, maxHash).Local)

	// Two peers
	flags.Set(t, "cache_proxy.peers.local_address", "localhost:1")
	flags.Set(t, "cache_proxy.peers.addresses", generatePeers(2))
	peers, err = cache_proxy_peers.New(env)
	require.NoError(t, err)
	require.True(t, getPeer(t, peers, firstHalfStart).Local)
	require.True(t, getPeer(t, peers, firstHalfEnd).Local)
	requireSamePeer(t, peers, firstHalfStart, firstHalfEnd)
	requireDifferentPeer(t, peers, firstHalfEnd, secondHalfStart)
	require.False(t, getPeer(t, peers, secondHalfStart).Local)
	require.False(t, getPeer(t, peers, secondHalfEnd).Local)
	requireSamePeer(t, peers, secondHalfStart, secondHalfEnd)

	// Four peers
	flags.Set(t, "cache_proxy.peers.local_address", "localhost:1")
	flags.Set(t, "cache_proxy.peers.addresses", generatePeers(4))
	peers, err = cache_proxy_peers.New(env)
	require.NoError(t, err)
	require.True(t, getPeer(t, peers, firstQuarterStart).Local)
	require.True(t, getPeer(t, peers, firstQuarterEnd).Local)
	require.False(t, getPeer(t, peers, secondQuarterStart).Local)
	require.False(t, getPeer(t, peers, fourthQuarterEnd).Local)
	requireSamePeer(t, peers, firstQuarterStart, firstQuarterEnd)
	requireDifferentPeer(t, peers, firstQuarterEnd, secondQuarterStart)
	requireSamePeer(t, peers, secondQuarterStart, secondQuarterEnd)
	requireDifferentPeer(t, peers, secondQuarterEnd, thirdQuarterStart)
	requireSamePeer(t, peers, thirdQuarterStart, thirdQuarterEnd)
	requireDifferentPeer(t, peers, thirdQuarterEnd, fourthQuarterStart)
	requireSamePeer(t, peers, fourthQuarterStart, fourthQuarterEnd)

	// Try prime-number peer counts to ensure the full hash range is covered.
	for _, i := range []int{7, 13, 43, 97, 1009} {
		flags.Set(t, "cache_proxy.peers.addresses", generatePeers(i))
		requireDifferentPeer(t, peers, minHash, maxHash)
	}
}

func generatePeers(count int) []string {
	peers := []string{}
	for i := range count {
		peers = append(peers, fmt.Sprintf("localhost:%d", i+1))
	}
	return peers
}

func requireSamePeer(t *testing.T, peers cache_proxy_peers.Peers, first, second string) {
	require.Equal(t, getPeer(t, peers, first), getPeer(t, peers, second))
}

func requireDifferentPeer(t *testing.T, peers cache_proxy_peers.Peers, first, second string) {
	require.NotEqual(t, getPeer(t, peers, first), getPeer(t, peers, second))
}

func getPeer(t *testing.T, peers cache_proxy_peers.Peers, key string) cache_proxy_peers.Peer {
	peer, err := peers.Get(digest.Key{Hash: key})
	require.NoError(t, err)
	return peer
}

package registry_test

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"
)

func init() {
	*log.LogLevel = "debug"
	*log.IncludeShortFileName = true
	log.Configure()
}

func localAddr(t *testing.T) string {
	return fmt.Sprintf("127.0.0.1:%d", testport.FindFree(t))
}

func newGossipManager(t testing.TB, nodeAddr string, seeds []string) *gossip.GossipManager {
	node, err := gossip.NewGossipManager("name-"+nodeAddr, nodeAddr, seeds)
	require.NoError(t, err)
	t.Cleanup(func() {
		node.Shutdown()
	})
	return node
}

func requireResolves(t testing.TB, dnr registry.NodeRegistry, clusterID, nodeID uint64, raftAddr, grpcAddr string) {
	addr, key, err := dnr.Resolve(clusterID, nodeID)
	require.NoError(t, err)
	require.Equal(t, raftAddr, addr, dnr.String())
	require.NotNil(t, key)

	addr, key, err = dnr.ResolveGRPC(clusterID, nodeID)
	require.NoError(t, err)
	require.Equal(t, grpcAddr, addr, dnr.String())
	require.NotNil(t, key)
}

func requireError(t testing.TB, dnr registry.NodeRegistry, clusterID, nodeID uint64, expectedErr error) {
	addr, key, err := dnr.Resolve(clusterID, nodeID)
	require.Equal(t, "", addr)
	require.Equal(t, "", key)
	require.NotNil(t, err)
	require.Equal(t, err.Error(), expectedErr.Error())

	addr, key, err = dnr.ResolveGRPC(clusterID, nodeID)
	require.Equal(t, "", addr)
	require.Equal(t, "", key)
	require.NotNil(t, err)
	require.Equal(t, err.Error(), expectedErr.Error())
}

func TestStaticRegistryAdd(t *testing.T) {
	nr := registry.NewStaticNodeRegistry(1, nil)
	nr.Add(1, 1, "nhid-1")
	nr.AddNode("nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, nr, 1, 1, "raftaddress:1", "grpcaddress:1")
}

func TestDynamicRegistryAdd(t *testing.T) {
	nodeAddr := localAddr(t)
	gm := newGossipManager(t, nodeAddr, nil)
	dnr := registry.NewDynamicNodeRegistry(gm, 1, nil)
	dnr.Add(1, 1, "nhid-1")
	dnr.AddNode("nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr, 1, 1, "raftaddress:1", "grpcaddress:1")
}

func TestDynamicRegistryResolution(t *testing.T) {
	node1Addr := localAddr(t)
	node2Addr := localAddr(t)
	node3Addr := localAddr(t)
	seeds := []string{node1Addr, node2Addr}

	gm1 := newGossipManager(t, node1Addr, seeds)
	gm2 := newGossipManager(t, node2Addr, seeds)
	gm3 := newGossipManager(t, node3Addr, seeds)

	dnr1 := registry.NewDynamicNodeRegistry(gm1, 1, nil)
	dnr2 := registry.NewDynamicNodeRegistry(gm2, 1, nil)
	dnr3 := registry.NewDynamicNodeRegistry(gm3, 1, nil)

	dnr1.Add(1, 1, "nhid-1")
	dnr1.AddNode("nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr1, 1, 1, "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr2, 1, 1, "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr3, 1, 1, "raftaddress:1", "grpcaddress:1")

	dnr2.Add(1, 2, "nhid-2")
	dnr2.AddNode("nhid-2", "raftaddress:2", "grpcaddress:2")
	requireResolves(t, dnr1, 1, 2, "raftaddress:2", "grpcaddress:2")
	requireResolves(t, dnr2, 1, 2, "raftaddress:2", "grpcaddress:2")
	requireResolves(t, dnr3, 1, 2, "raftaddress:2", "grpcaddress:2")

	dnr3.Add(1, 3, "nhid-3")
	dnr3.AddNode("nhid-3", "raftaddress:3", "grpcaddress:3")
	requireResolves(t, dnr1, 1, 3, "raftaddress:3", "grpcaddress:3")
	requireResolves(t, dnr2, 1, 3, "raftaddress:3", "grpcaddress:3")
	requireResolves(t, dnr3, 1, 3, "raftaddress:3", "grpcaddress:3")
}

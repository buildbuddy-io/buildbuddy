package registry_test

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"
)

func init() {
	log.Configure(log.Opts{Level: "debug", EnableShortFileName: true})
}

func localAddr(t *testing.T) string {
	return fmt.Sprintf("127.0.0.1:%d", app.FreePort(t))
}

func newGossipManager(t testing.TB, nodeAddr string, seeds []string) *gossip.GossipManager {
	node, err := gossip.NewGossipManager(nodeAddr, seeds)
	require.Nil(t, err)
	//	t.Cleanup(func() {
	//		node.Shutdown()
	//	})
	return node
}

func newDynamicNodeRegistry(t testing.TB, nhid, raftAddress, grpcAddress string, gossipManager *gossip.GossipManager) *registry.DynamicNodeRegistry {
	dnr, err := registry.NewDynamicNodeRegistry(nhid, raftAddress, grpcAddress, gossipManager, 1, nil)
	require.Nil(t, err)
	return dnr
}

func requireResolves(t testing.TB, dnr *registry.DynamicNodeRegistry, clusterID, nodeID uint64, raftAddr, grpcAddr string) {
	addr, key, err := dnr.Resolve(clusterID, nodeID)
	require.Nil(t, err, err)
	require.Equal(t, raftAddr, addr)
	require.NotNil(t, key)

	addr, key, err = dnr.ResolveGRPC(clusterID, nodeID)
	require.Nil(t, err, err)
	require.Equal(t, grpcAddr, addr)
	require.NotNil(t, key)
}

func requireError(t testing.TB, dnr *registry.DynamicNodeRegistry, clusterID, nodeID uint64, expectedErr error) {
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

func TestRegistryAdd(t *testing.T) {
	node1Addr := localAddr(t)
	gm1 := newGossipManager(t, node1Addr, nil)
	dnr1 := newDynamicNodeRegistry(t, "nhid-1", "raftaddress:1", "grpcaddress:1", gm1)
	dnr1.Add(1, 1, "nhid-1")
	requireResolves(t, dnr1, 1, 1, "raftaddress:1", "grpcaddress:1")
}

func TestGossippedRegistryAdd(t *testing.T) {
	node1Addr := localAddr(t)
	node2Addr := localAddr(t)
	node3Addr := localAddr(t)
	seeds := []string{node1Addr, node2Addr}

	gm1 := newGossipManager(t, node1Addr, seeds)
	gm2 := newGossipManager(t, node2Addr, seeds)
	gm3 := newGossipManager(t, node3Addr, seeds)

	dnr1 := newDynamicNodeRegistry(t, "nhid-1", "raftaddress:1", "grpcaddress:1", gm1)
	dnr2 := newDynamicNodeRegistry(t, "nhid-2", "raftaddress:2", "grpcaddress:2", gm2)
	dnr3 := newDynamicNodeRegistry(t, "nhid-3", "raftaddress:3", "grpcaddress:3", gm3)

	dnr1.Add(1, 1, "nhid-1")
	requireResolves(t, dnr1, 1, 1, "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr2, 1, 1, "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr3, 1, 1, "raftaddress:1", "grpcaddress:1")

	dnr2.Add(1, 2, "nhid-2")
	requireResolves(t, dnr1, 1, 2, "raftaddress:2", "grpcaddress:2")
	requireResolves(t, dnr2, 1, 2, "raftaddress:2", "grpcaddress:2")
	requireResolves(t, dnr3, 1, 2, "raftaddress:2", "grpcaddress:2")

	dnr3.Add(1, 3, "nhid-3")
	requireResolves(t, dnr1, 1, 3, "raftaddress:3", "grpcaddress:3")
	requireResolves(t, dnr2, 1, 3, "raftaddress:3", "grpcaddress:3")
	requireResolves(t, dnr3, 1, 3, "raftaddress:3", "grpcaddress:3")
}

func TestGossippedRegistryRemove(t *testing.T) {
	node1Addr := localAddr(t)
	node2Addr := localAddr(t)
	node3Addr := localAddr(t)
	seeds := []string{node1Addr, node2Addr}

	gm1 := newGossipManager(t, node1Addr, seeds)
	gm2 := newGossipManager(t, node2Addr, seeds)
	gm3 := newGossipManager(t, node3Addr, seeds)

	dnr1 := newDynamicNodeRegistry(t, "nhid-1", "raftaddress:1", "grpcaddress:1", gm1)
	dnr2 := newDynamicNodeRegistry(t, "nhid-2", "raftaddress:2", "grpcaddress:2", gm2)
	dnr3 := newDynamicNodeRegistry(t, "nhid-3", "raftaddress:3", "grpcaddress:3", gm3)

	dnr1.Add(1, 1, "nhid-1")
	requireResolves(t, dnr1, 1, 1, "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr2, 1, 1, "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr3, 1, 1, "raftaddress:1", "grpcaddress:1")

	dnr1.Remove(1, 1)
	requireError(t, dnr1, 1, 2, registry.TargetAddressUnknownError)
	//	requireError(t, dnr2, 1, 2, registry.TargetAddressUnknownError)
	//	requireError(t, dnr3, 1, 2, registry.TargetAddressUnknownError)
}

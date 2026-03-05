package registry_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/kuberesolver"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	node, err := gossip.NewWithArgs("name-"+nodeAddr, nodeAddr, seeds)
	require.NoError(t, err)
	t.Cleanup(func() {
		node.Shutdown()
	})
	return node
}

func requireResolves(t testing.TB, dnr registry.NodeRegistry, rangeID, replicaID uint64, nhid, raftAddr, grpcAddr string) {
	addr, key, err := dnr.Resolve(rangeID, replicaID)
	require.NoError(t, err)
	require.Equal(t, raftAddr, addr, dnr.String())
	require.NotNil(t, key)

	ctx := context.Background()
	addr, err = dnr.ResolveGRPC(ctx, nhid)
	require.NoError(t, err)
	require.Equal(t, grpcAddr, addr, dnr.String())
}

func TestStaticRegistryAdd(t *testing.T) {
	nr := registry.NewStaticNodeRegistry(1, nil, log.Logger{})
	nr.Add(1, 1, "nhid-1")
	nr.AddNode("nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, nr, 1, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
}

func TestStaticRegistryRemove(t *testing.T) {
	nr := registry.NewStaticNodeRegistry(1, nil, log.Logger{})
	nr.Add(1, 1, "nhid-1")
	nr.Add(2, 1, "nhid-1")
	nr.Add(1, 2, "nhid-2")
	nr.AddNode("nhid-1", "raftaddress:1", "grpcaddress:1")
	nr.AddNode("nhid-2", "raftaddress:2", "grpcaddress:2")
	requireResolves(t, nr, 1, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, nr, 2, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, nr, 1, 2, "nhid-2", "raftaddress:2", "grpcaddress:2")

	nr.Remove(1, 1)
	requireResolves(t, nr, 2, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, nr, 1, 2, "nhid-2", "raftaddress:2", "grpcaddress:2")
	_, _, err := nr.Resolve(1, 1)
	require.True(t, status.IsNotFoundError(err))
}

func TestStaticRegistryRemoveShard(t *testing.T) {
	nr := registry.NewStaticNodeRegistry(1, nil, log.Logger{})
	nr.Add(1, 1, "nhid-1")
	nr.Add(2, 1, "nhid-1")
	nr.Add(1, 2, "nhid-2")
	nr.AddNode("nhid-1", "raftaddress:1", "grpcaddress:1")
	nr.AddNode("nhid-2", "raftaddress:2", "grpcaddress:2")
	requireResolves(t, nr, 1, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, nr, 2, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, nr, 1, 2, "nhid-2", "raftaddress:2", "grpcaddress:2")

	nr.RemoveShard(1)
	requireResolves(t, nr, 2, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	_, _, err := nr.Resolve(1, 1)
	require.True(t, status.IsNotFoundError(err))
	_, _, err = nr.Resolve(1, 2)
	require.True(t, status.IsNotFoundError(err))
}

func TestDynamicRegistryAdd(t *testing.T) {
	nodeAddr := localAddr(t)
	gm := newGossipManager(t, nodeAddr, nil)
	dnr := registry.NewDynamicNodeRegistry(gm, 1, nil, log.Logger{})
	dnr.Add(1, 1, "nhid-1")
	dnr.AddNode("nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr, 1, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")

	// When the target changes addresses, the registry should resolve the target
	// to the new address.
	dnr.AddNode("nhid-1", "raftaddress:2", "grpcaddress:2")
	requireResolves(t, dnr, 1, 1, "nhid-1", "raftaddress:2", "grpcaddress:2")
}

func TestDynamicRegistryRemove(t *testing.T) {
	nodeAddr := localAddr(t)
	gm := newGossipManager(t, nodeAddr, nil)
	dnr := registry.NewDynamicNodeRegistry(gm, 1, nil, log.Logger{})
	dnr.Add(1, 1, "nhid-1")
	dnr.Add(2, 1, "nhid-1")
	dnr.Add(1, 2, "nhid-2")
	dnr.AddNode("nhid-1", "raftaddress:1", "grpcaddress:1")
	dnr.AddNode("nhid-2", "raftaddress:2", "grpcaddress:2")
	requireResolves(t, dnr, 1, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr, 2, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr, 1, 2, "nhid-2", "raftaddress:2", "grpcaddress:2")

	dnr.Remove(1, 1)
	requireResolves(t, dnr, 2, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr, 1, 2, "nhid-2", "raftaddress:2", "grpcaddress:2")
	_, _, err := dnr.Resolve(1, 1)
	require.True(t, status.IsNotFoundError(err))
}

func TestDynamicRegistryRemoveShard(t *testing.T) {
	nodeAddr := localAddr(t)
	gm := newGossipManager(t, nodeAddr, nil)
	dnr := registry.NewDynamicNodeRegistry(gm, 1, nil, log.Logger{})
	dnr.Add(1, 1, "nhid-1")
	dnr.Add(2, 1, "nhid-1")
	dnr.Add(1, 2, "nhid-2")
	dnr.AddNode("nhid-1", "raftaddress:1", "grpcaddress:1")
	dnr.AddNode("nhid-2", "raftaddress:2", "grpcaddress:2")
	requireResolves(t, dnr, 1, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr, 2, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr, 1, 2, "nhid-2", "raftaddress:2", "grpcaddress:2")

	dnr.RemoveShard(1)
	requireResolves(t, dnr, 2, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	_, _, err := dnr.Resolve(1, 1)
	require.True(t, status.IsNotFoundError(err))
	_, _, err = dnr.Resolve(1, 2)
	require.True(t, status.IsNotFoundError(err))
}

func TestStaticRegistryResolveWithPodWatcher(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "server-0",
			Namespace: "ns",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}
	k8sClient := fake.NewClientset(pod)
	m := kuberesolver.NewPodWatcherManager(k8sClient)

	nr := registry.NewStaticNodeRegistry(1, nil, log.Logger{})
	nr.SetPodWatcherManager(m)

	raftAddr := "server-0.headless.ns.svc.cluster.local:7238"
	nr.Add(1, 1, "nhid-1")
	nr.AddNode("nhid-1", raftAddr, "grpcaddress:1")

	// Resolve should return the resolved pod IP instead of the hostname.
	require.Eventually(t, func() bool {
		addr, _, err := nr.Resolve(1, 1)
		return err == nil && addr == "10.0.0.1:7238"
	}, 5*time.Second, 10*time.Millisecond, "expected Resolve to return resolved IP")

	// Connection key should use the resolved IP, not the hostname.
	addr, key1, err := nr.Resolve(1, 1)
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:7238", addr)
	require.Contains(t, key1, "10.0.0.1:7238")
	require.NotContains(t, key1, "server-0.headless")

	// ResolveGRPC is unaffected by pod watcher (resolves by NHID).
	grpcAddr, err := nr.ResolveGRPC(context.Background(), "nhid-1")
	require.NoError(t, err)
	require.Equal(t, "grpcaddress:1", grpcAddr)

	// Wait for the watch to be established before updating.
	require.Eventually(t, func() bool {
		for _, a := range k8sClient.Actions() {
			if a.GetVerb() == "watch" {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "timed out waiting for watch")

	// Simulate a pod restart with a new IP.
	pod.Status.PodIP = "10.0.0.2"
	_, err = k8sClient.CoreV1().Pods("ns").Update(
		context.Background(), pod, metav1.UpdateOptions{},
	)
	require.NoError(t, err)

	// Resolve should return the new IP and a different connection key.
	require.Eventually(t, func() bool {
		addr, _, err := nr.Resolve(1, 1)
		return err == nil && addr == "10.0.0.2:7238"
	}, 5*time.Second, 10*time.Millisecond, "expected Resolve to return updated IP")

	_, key2, err := nr.Resolve(1, 1)
	require.NoError(t, err)
	require.NotEqual(t, key1, key2, "connection key should change when IP changes")
}

func TestDynamicRegistryResolution(t *testing.T) {
	node1Addr := localAddr(t)
	node2Addr := localAddr(t)
	node3Addr := localAddr(t)
	seeds := []string{node1Addr, node2Addr}

	gm1 := newGossipManager(t, node1Addr, seeds)
	gm2 := newGossipManager(t, node2Addr, seeds)
	gm3 := newGossipManager(t, node3Addr, seeds)

	dnr1 := registry.NewDynamicNodeRegistry(gm1, 1, nil, log.Logger{})
	dnr2 := registry.NewDynamicNodeRegistry(gm2, 1, nil, log.Logger{})
	dnr3 := registry.NewDynamicNodeRegistry(gm3, 1, nil, log.Logger{})

	dnr1.Add(1, 1, "nhid-1")
	dnr1.AddNode("nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr1, 1, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr2, 1, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")
	requireResolves(t, dnr3, 1, 1, "nhid-1", "raftaddress:1", "grpcaddress:1")

	dnr2.Add(1, 2, "nhid-2")
	dnr2.AddNode("nhid-2", "raftaddress:2", "grpcaddress:2")
	requireResolves(t, dnr1, 1, 2, "nhid-2", "raftaddress:2", "grpcaddress:2")
	requireResolves(t, dnr2, 1, 2, "nhid-2", "raftaddress:2", "grpcaddress:2")
	requireResolves(t, dnr3, 1, 2, "nhid-2", "raftaddress:2", "grpcaddress:2")

	dnr3.Add(1, 3, "nhid-3")
	dnr3.AddNode("nhid-3", "raftaddress:3", "grpcaddress:3")
	requireResolves(t, dnr1, 1, 3, "nhid-3", "raftaddress:3", "grpcaddress:3")
	requireResolves(t, dnr2, 1, 3, "nhid-3", "raftaddress:3", "grpcaddress:3")
	requireResolves(t, dnr3, 1, 3, "nhid-3", "raftaddress:3", "grpcaddress:3")
}

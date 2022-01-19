package driver_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/driver"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

// testingPlacer is a fake map-driven placer that allows for easy testing of
// the placer driver.
type testingPlacer struct {
	nodeUsage    map[string]*rfpb.NodeUsage
	nodeReplicas map[string][]*rfpb.ReplicaDescriptor
}

func NewTestingPlacer() *testingPlacer {
	return &testingPlacer{
		nodeUsage:    make(map[string]*rfpb.NodeUsage),
		nodeReplicas: make(map[string][]*rfpb.ReplicaDescriptor),
	}
}

func (tp *testingPlacer) AddNode(u *rfpb.NodeUsage) {
	tp.nodeUsage[u.GetNhid()] = u
	tp.nodeReplicas[u.GetNhid()] = make([]*rfpb.ReplicaDescriptor, 0)
}

func (tp *testingPlacer) FailNode(nhid string) {
	delete(tp.nodeUsage, nhid)
}

func (tp *testingPlacer) AddReplica(nhid string, rd *rfpb.ReplicaDescriptor) {
	tp.nodeReplicas[nhid] = append(tp.nodeReplicas[nhid], rd)
}

func (tp *testingPlacer) GetRangeDescriptor(ctx context.Context, clusterID uint64) (*rfpb.RangeDescriptor, error) {
	rangeDescriptor := &rfpb.RangeDescriptor{}
	for _, replicas := range tp.nodeReplicas {
		for _, r := range replicas {
			if r.GetClusterId() == clusterID {
				rangeDescriptor.Replicas = append(rangeDescriptor.Replicas, r)
			}
		}
	}
	return rangeDescriptor, nil
}

func (tp *testingPlacer) GetNodeUsage(ctx context.Context, replica *rfpb.ReplicaDescriptor) (*rfpb.NodeUsage, error) {
	for nhid, replicas := range tp.nodeReplicas {
		for _, r := range replicas {
			if proto.Equal(r, replica) {
				if usage, ok := tp.nodeUsage[nhid]; ok {
					usage.NumReplicas = int64(len(tp.nodeReplicas[nhid]))
					return usage, nil
				}
			}
		}
	}
	return nil, status.NotFoundErrorf("Usage not found for c%dn%d", replica.GetClusterId(), replica.GetNodeId())
}

func (tp *testingPlacer) FindNodes(ctx context.Context, query *rfpb.PlacementQuery) ([]*rfpb.NodeDescriptor, error) {
	availableNodes := make([]*rfpb.NodeDescriptor, 0)
	for nhid, usage := range tp.nodeUsage {
		if float64(usage.GetDiskBytesUsed())/float64(usage.GetDiskBytesTotal()) > .95 {
			continue
		}
		found := false
		for _, replica := range tp.nodeReplicas[nhid] {
			if replica.GetClusterId() == query.GetTargetClusterId() {
				found = true
			}
		}
		if !found {
			availableNodes = append(availableNodes, &rfpb.NodeDescriptor{Nhid: nhid})
		}
	}
	return availableNodes, nil
}

func (tp *testingPlacer) SeenNodes() int64 {
	return int64(len(tp.nodeUsage))
}

func (tp *testingPlacer) GetManagedClusters(ctx context.Context) []uint64 {
	clusterSet := make(map[uint64]struct{})
	for _, replicas := range tp.nodeReplicas {
		for _, r := range replicas {
			clusterSet[r.GetClusterId()] = struct{}{}
		}
	}
	clusterIDs := make([]uint64, len(clusterSet))
	for clusterID, _ := range clusterSet {
		clusterIDs = append(clusterIDs, clusterID)
	}
	return clusterIDs
}

func (tp *testingPlacer) SplitRange(ctx context.Context, clusterID uint64) error {
	var maxClusterID uint64
	for _, replicas := range tp.nodeReplicas {
		for _, r := range replicas {
			if r.GetClusterId() > maxClusterID {
				maxClusterID = r.GetClusterId()
			}
		}
	}
	newClusterID := maxClusterID + 1
	nextNodeID := uint64(1)
	for nhid, replicas := range tp.nodeReplicas {
		for _, r := range replicas {
			if r.GetClusterId() == clusterID {
				tp.nodeReplicas[nhid] = append(tp.nodeReplicas[nhid], &rfpb.ReplicaDescriptor{
					ClusterId: newClusterID,
					NodeId: nextNodeID,
				})
				nextNodeID += 1
				break
			}
		}
	}
	return nil
}

func (tp *testingPlacer) AddNodeToCluster(ctx context.Context, rangeDescriptor *rfpb.RangeDescriptor, node *rfpb.NodeDescriptor) error {
	var clusterID, maxNodeID uint64
	for _, replica := range rangeDescriptor.GetReplicas() {
		clusterID = replica.GetClusterId()
		if replica.GetNodeId() > maxNodeID {
			maxNodeID = replica.GetNodeId()
		}
	}
	tp.nodeReplicas[node.GetNhid()] = append(tp.nodeReplicas[node.GetNhid()], &rfpb.ReplicaDescriptor{
		ClusterId: clusterID,
		NodeId:    maxNodeID + 1,
	})
	return nil
}

func (tp *testingPlacer) RemoveNodeFromCluster(ctx context.Context, rangeDescriptor *rfpb.RangeDescriptor, targetNodeID uint64) error {
	clusterID := rangeDescriptor.GetReplicas()[0].GetClusterId()
	for nhid, replicas := range tp.nodeReplicas {
		for i, r := range replicas {
			if r.GetClusterId() == clusterID && r.GetNodeId() == targetNodeID {
				tp.nodeReplicas[nhid] = append(replicas[:i], replicas[i+1:]...)
				return nil
			}
		}
	}
	return status.FailedPreconditionErrorf("targetNodeID %d not in cluster", targetNodeID)
}

func TestSteadyState(t *testing.T) {
	ctx := context.Background()
	placer := NewTestingPlacer()
	// Replicas are evenly distributed; nothing should be moved.
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-1", DiskBytesTotal: 1e3})
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-2", DiskBytesTotal: 1e3})
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-3", DiskBytesTotal: 1e3})
	placer.AddReplica("nhid-1", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 1})
	placer.AddReplica("nhid-2", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 2})
	placer.AddReplica("nhid-3", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 3})
	d := driver.New(placer, driver.Opts{ReplicaTimeoutDuration: time.Millisecond})
	err := d.LoopOnce(ctx)
	require.Nil(t, err)
}

func TestDeadReplica(t *testing.T) {
	ctx := context.Background()
	placer := NewTestingPlacer()

	// Replicas are evenly distributed; nothing should be moved.
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-1", DiskBytesTotal: 1e3})
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-2", DiskBytesTotal: 1e3})
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-3", DiskBytesTotal: 1e3})
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-4", DiskBytesTotal: 1e3})
	placer.AddReplica("nhid-1", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 1})
	placer.AddReplica("nhid-2", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 2})
	placer.AddReplica("nhid-3", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 3})

	d := driver.New(placer, driver.Opts{ReplicaTimeoutDuration: time.Millisecond})
	err := d.LoopOnce(ctx)
	require.Nil(t, err)
	
	// Remove a node (simulating failure).
	placer.FailNode("nhid-1")

	time.Sleep(2 * time.Millisecond)
	err = d.LoopOnce(ctx)
	require.Nil(t, err, err)

	// Ensure that the replica on nhid-1 has been removed
	// and that a new replica has been added on nhid-4.
	require.Contains(t, placer.nodeReplicas, "nhid-4")
	require.Len(t, placer.nodeReplicas["nhid-4"], 1)

	newReplica := placer.nodeReplicas["nhid-4"][0]
	require.Equal(t, uint64(1), newReplica.GetClusterId())
}

func TestRebalance(t *testing.T) {
	ctx := context.Background()
	placer := NewTestingPlacer()

	// Replicas are evenly distributed; nothing should be moved.
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-1", DiskBytesTotal: 1e3})
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-2", DiskBytesTotal: 1e3})
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-3", DiskBytesTotal: 1e3})

	placer.AddReplica("nhid-1", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 1})
	placer.AddReplica("nhid-1", &rfpb.ReplicaDescriptor{ClusterId: 2, NodeId: 1})

	placer.AddReplica("nhid-2", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 2})
	placer.AddReplica("nhid-2", &rfpb.ReplicaDescriptor{ClusterId: 2, NodeId: 2})

	placer.AddReplica("nhid-3", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 3})
	placer.AddReplica("nhid-3", &rfpb.ReplicaDescriptor{ClusterId: 3, NodeId: 3})

	d := driver.New(placer, driver.Opts{ReplicaTimeoutDuration: time.Millisecond})
	err := d.LoopOnce(ctx)
	require.Nil(t, err)

	// Add a node, and expect a replica to move onto it.
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-4", DiskBytesTotal: 1e3})

	err = d.LoopOnce(ctx)
	require.Nil(t, err)

	require.Contains(t, placer.nodeReplicas, "nhid-4")
	require.Len(t, placer.nodeReplicas["nhid-4"], 1)
}

func TestRebalanceNoGlobalImprovement(t *testing.T) {
	ctx := context.Background()
	placer := NewTestingPlacer()

	// Replicas are not evenly distributed but no even distribution exists.
	// Nothing should be moved.
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-1", DiskBytesTotal: 1e3})
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-2", DiskBytesTotal: 1e3})
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-3", DiskBytesTotal: 1e3})
	placer.AddReplica("nhid-1", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 1})
	placer.AddReplica("nhid-1", &rfpb.ReplicaDescriptor{ClusterId: 2, NodeId: 1})
	placer.AddReplica("nhid-2", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 2})
	placer.AddReplica("nhid-2", &rfpb.ReplicaDescriptor{ClusterId: 2, NodeId: 2})
	placer.AddReplica("nhid-3", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 3})

	d := driver.New(placer, driver.Opts{ReplicaTimeoutDuration: time.Millisecond})
	err := d.LoopOnce(ctx)
	require.Nil(t, err)
	
	require.Len(t, placer.nodeReplicas["nhid-1"], 2)
	require.Len(t, placer.nodeReplicas["nhid-2"], 2)
	require.Len(t, placer.nodeReplicas["nhid-3"], 1)
}

func TestSplit(t *testing.T) {
	ctx := context.Background()
	placer := NewTestingPlacer()

	// Replicas evenly distributed.
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-1", DiskBytesTotal: 1e3})
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-2", DiskBytesTotal: 1e3})
	placer.AddNode(&rfpb.NodeUsage{Nhid: "nhid-3", DiskBytesTotal: 1e3})
	placer.AddReplica("nhid-1", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 1})
	placer.AddReplica("nhid-2", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 2})
	placer.AddReplica("nhid-3", &rfpb.ReplicaDescriptor{ClusterId: 1, NodeId: 3})

	d := driver.New(placer, driver.Opts{ReplicaTimeoutDuration: time.Millisecond})
	err := d.LoopOnce(ctx)
	require.Nil(t, err)
}

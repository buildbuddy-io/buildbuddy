package store

import (
	"context"
	"sort"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"

	raftConfig "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

type Store struct {
	rootDir string
	fileDir string

	nodeHost      *dragonboat.NodeHost
	gossipManager *gossip.GossipManager
	sender        *sender.Sender
	apiClient     *client.APIClient
	heartbeat     *nodelease.Heartbeat

	rangeMu       sync.RWMutex
	activeRanges  map[uint64]struct{}
	clusterRanges map[uint64]*rfpb.RangeDescriptor

	leaseMu sync.RWMutex
	leases  map[uint64]*rangelease.Lease
}

func (s *Store) Initialize(rootDir, fileDir string, nodeHost *dragonboat.NodeHost, gossipManager *gossip.GossipManager, sender *sender.Sender, apiClient *client.APIClient) {
	s.rootDir = rootDir
	s.fileDir = fileDir
	s.nodeHost = nodeHost
	s.gossipManager = gossipManager
	s.sender = sender
	s.apiClient = apiClient
	s.heartbeat = nodelease.NewHeartbeat([]byte(nodeHost.ID()), sender)
	s.rangeMu = sync.RWMutex{}
	s.activeRanges = make(map[uint64]struct{})
	s.clusterRanges = make(map[uint64]*rfpb.RangeDescriptor)

	s.leaseMu = sync.RWMutex{}
	s.leases = make(map[uint64]*rangelease.Lease)
	s.heartbeat.Start()
}

func (s *Store) LeaderUpdated(info raftio.LeaderInfo) {
	if s.nodeHost == nil {
		// not initialized yet
		return
	}
	leader := s.isLeader(info.ClusterID)
	log.Printf("LeaderUpdated: %+v: am leader: %t", info, leader)
	if leader {
		go s.leaseRange(info.ClusterID)
	} else {
		go s.releaseRange(info.ClusterID)
	}
}

func (s *Store) leaseRange(clusterID uint64) {
	s.rangeMu.RLock()
	rd, ok := s.clusterRanges[clusterID]
	s.rangeMu.RUnlock()
	if !ok {
		return
	}

	rl := rangelease.New(s.nodeHost, s.heartbeat, rd)
	if err := rl.Acquire(); err != nil {
		log.Errorf("error acquiring rl: %s", err)
		return
	}
	s.leaseMu.Lock()
	s.leases[rd.GetRangeId()] = rl
	s.leaseMu.Unlock()
}

func (s *Store) releaseRange(clusterID uint64) {
	s.leaseMu.Lock()
	if rl, ok := s.leases[clusterID]; ok {
		rl.Release()
	}
	s.leaseMu.Unlock()
}

func containsMetaRange(rd *rfpb.RangeDescriptor) bool {
	r := rangemap.Range{Left: rd.Left, Right: rd.Right}
	return r.Contains([]byte{constants.MinByte}) && r.Contains([]byte{constants.UnsplittableMaxByte - 1})
}

// We need to implement the RangeTracker interface so that stores opened and
// closed on this node will notify us when their range appears and disappears.
// We'll use this information to drive the range tags we broadcast.
func (s *Store) AddRange(rd *rfpb.RangeDescriptor) {
	log.Printf("AddRange: %+v", rd)
	if len(rd.GetReplicas()) == 0 {
		return
	}
	if containsMetaRange(rd) {
		// If we own the metarange, use gossip to notify other nodes
		// of that fact.
		buf, err := proto.Marshal(rd)
		if err != nil {
			log.Errorf("Error marshaling metarange descriptor: %s", err)
			return
		}
		s.gossipManager.SetTag(constants.MetaRangeTag, string(buf))
	}
	clusterID := rd.GetReplicas()[0].GetClusterId()

	s.rangeMu.Lock()
	s.activeRanges[rd.GetRangeId()] = struct{}{}
	s.clusterRanges[clusterID] = rd
	s.rangeMu.Unlock()

	leader := s.isLeader(clusterID)
	log.Printf("Node %s, leader for cluster %d: %t", s.nodeHost.ID(), clusterID, leader)
	if leader {
		log.Printf("I am the leader for cluster %d, attempting to get range lease!", clusterID)

		log.Printf("Got range lease!")
	}
}

func (s *Store) RemoveRange(rd *rfpb.RangeDescriptor) {
	log.Printf("RemoveRange: %+v", rd)
	clusterID := rd.GetReplicas()[0].GetClusterId()
	s.rangeMu.Lock()
	delete(s.activeRanges, rd.GetRangeId())
	delete(s.clusterRanges, clusterID)
	s.rangeMu.Unlock()
}

func (s *Store) RangeIsActive(rangeID uint64) bool {
	s.rangeMu.RLock()
	defer s.rangeMu.RUnlock()
	_, ok := s.activeRanges[rangeID]
	return ok
}

func (s *Store) ReplicaFactoryFn(clusterID, nodeID uint64) dbsm.IOnDiskStateMachine {
	return replica.New(s.rootDir, s.fileDir, clusterID, nodeID, s, s.sender, s.apiClient)
}

// TODO(check cluster has been added / dont care if it's published)
func (s *Store) syncProposeLocal(ctx context.Context, clusterID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	sesh := s.nodeHost.GetNoOPSession(clusterID)
	buf, err := proto.Marshal(batch)
	if err != nil {
		return nil, err
	}
	var raftResponse dbsm.Result
	retrier := retry.DefaultWithContext(ctx)
	for retrier.Next() {
		raftResponse, err = s.nodeHost.SyncPropose(ctx, sesh, buf)
		if err != nil {
			if err == dragonboat.ErrClusterNotReady {
				log.Errorf("continuing, got cluster not ready err...")
				continue
			}
			log.Errorf("Got unretriable SyncPropose err: %s", err)
			return nil, err
		}
		break
	}
	batchResponse := &rfpb.BatchCmdResponse{}
	if err := proto.Unmarshal(raftResponse.Data, batchResponse); err != nil {
		return nil, err
	}
	return batchResponse, err
}

func (s *Store) isLeader(clusterID uint64) bool {
	nodeHostInfo := s.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	if nodeHostInfo == nil {
		return false
	}
	for _, clusterInfo := range nodeHostInfo.ClusterInfoList {
		if clusterInfo.ClusterID == clusterID {
			return clusterInfo.IsLeader
		}
	}
	return false
}

func (s *Store) StartCluster(ctx context.Context, req *rfpb.StartClusterRequest) (*rfpb.StartClusterResponse, error) {
	rc := raftConfig.GetRaftConfig(req.GetClusterId(), req.GetNodeId())

	err := s.nodeHost.StartOnDiskCluster(req.GetInitialMember(), false /*=join*/, s.ReplicaFactoryFn, rc)
	if err != nil {
		if err == dragonboat.ErrClusterAlreadyExist {
			err = status.AlreadyExistsError(err.Error())
		}
		return nil, err
	}
	rsp := &rfpb.StartClusterResponse{}

	// If we are the first member in the cluster, we'll do the syncPropose.
	nodeIDs := make([]uint64, 0, len(req.GetInitialMember()))
	for nodeID, _ := range req.GetInitialMember() {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })
	if req.GetNodeId() == nodeIDs[0] {
		log.Printf("I am the first node! running sync propose")
		batchResponse, err := s.syncProposeLocal(ctx, req.GetClusterId(), req.GetBatch())
		log.Printf("first node syncpropose response: %+v, err: %s", batchResponse, err)
		if err != nil {
			return nil, err
		}
		rsp.Batch = batchResponse
	}
	return rsp, nil
}

func (s *Store) SyncPropose(ctx context.Context, req *rfpb.SyncProposeRequest) (*rfpb.SyncProposeResponse, error) {
	if !s.RangeIsActive(req.GetHeader().GetRangeId()) {
		return nil, status.OutOfRangeErrorf("Range %d not present", req.GetHeader().GetRangeId())
	}
	batchResponse, err := s.syncProposeLocal(ctx, req.GetHeader().GetReplica().GetClusterId(), req.GetBatch())
	if err != nil {
		return nil, err
	}
	return &rfpb.SyncProposeResponse{
		Batch: batchResponse,
	}, nil
}

func (s *Store) SyncRead(ctx context.Context, req *rfpb.SyncReadRequest) (*rfpb.SyncReadResponse, error) {
	if !s.RangeIsActive(req.GetHeader().GetRangeId()) {
		return nil, status.OutOfRangeErrorf("Range %d not present", req.GetHeader().GetRangeId())
	}
	buf, err := proto.Marshal(req.GetBatch())
	if err != nil {
		return nil, err
	}
	raftResponseIface, err := s.nodeHost.SyncRead(ctx, req.GetHeader().GetReplica().GetClusterId(), buf)
	if err != nil {
		return nil, err
	}

	buf, ok := raftResponseIface.([]byte)
	if !ok {
		return nil, status.FailedPreconditionError("SyncRead returned a non-[]byte response.")
	}

	batchResponse := &rfpb.BatchCmdResponse{}
	if err := proto.Unmarshal(buf, batchResponse); err != nil {
		return nil, err
	}

	return &rfpb.SyncReadResponse{
		Batch: batchResponse,
	}, nil
}

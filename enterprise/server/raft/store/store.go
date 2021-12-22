package store

import (
	"context"
	"sort"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
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

	liveness     *nodeliveness.Liveness

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
	s.liveness = nodeliveness.New(nodeHost.ID(), &client.NodeHostSender{NodeHost: nodeHost})
	s.rangeMu = sync.RWMutex{}
	s.activeRanges = make(map[uint64]struct{})
	s.clusterRanges = make(map[uint64]*rfpb.RangeDescriptor)

	s.leaseMu = sync.RWMutex{}
	s.leases = make(map[uint64]*rangelease.Lease)
}

func (s *Store) LeaderUpdated(info raftio.LeaderInfo) {
	if s.nodeHost == nil {
		// not initialized yet
		return
	}
	clusterID := info.ClusterID
	leader := s.isLeader(clusterID)
	if leader {
		s.leaseRange(clusterID)
	} else {
		s.releaseRange(clusterID)
	}
}

func containsMetaRange(rd *rfpb.RangeDescriptor) bool {
	r := rangemap.Range{Left: rd.Left, Right: rd.Right}
	return r.Contains([]byte{constants.MinByte}) && r.Contains([]byte{constants.UnsplittableMaxByte - 1})
}

func (s *Store) lookupRange(clusterID uint64) *rfpb.RangeDescriptor {
	s.rangeMu.RLock()
	defer s.rangeMu.RUnlock()

	for rangeClusterID, rangeDescriptor := range s.clusterRanges {
		if clusterID == rangeClusterID {
			return rangeDescriptor
		}
	}
	return nil
}

func (s *Store) leaseRange(clusterID uint64) {
	rd := s.lookupRange(clusterID)
	if rd == nil {
		log.Warningf("No range descriptor found for cluster: %d", clusterID)
		return
	}
	rl := rangelease.New(&client.NodeHostSender{NodeHost: s.nodeHost}, s.liveness, rd)
	s.leaseMu.Lock()
	s.leases[rd.GetRangeId()] = rl
	s.leaseMu.Unlock()

	go func() {
		for {
			err := rl.Lease()
			if err == nil {
				break
			}
			log.Warningf("Error leasing range: %s", err)
		}
		log.Printf("Succesfully leased range: %+v", rl)
	}()
}

func (s *Store) releaseRange(clusterID uint64) {
	rd := s.lookupRange(clusterID)
	if rd == nil {
		log.Warningf("No range descriptor found for cluster: %d", clusterID)
		return
	}

	s.leaseMu.Lock()
	rl, ok := s.leases[rd.GetRangeId()]
	if ok {
		delete(s.leases, rd.GetRangeId())
	}
	s.leaseMu.Unlock()
	if !ok {
		log.Warningf("Attempted to release range that is not held")
		return
	}
	go func() {
		rl.Release()
	}()
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
	if leader {
		s.leaseRange(clusterID)
	} else {
		s.releaseRange(clusterID)
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
	s.leaseMu.RLock()
	rl, ok := s.leases[rangeID]
	s.leaseMu.RUnlock()

	if !ok {
		log.Printf("%q did not have rangelease for range: %d", s.nodeHost.ID(), rangeID)
		return false
	}
	valid := rl.Valid()
	return valid
}

func (s *Store) ReplicaFactoryFn(clusterID, nodeID uint64) dbsm.IOnDiskStateMachine {
	return replica.New(s.rootDir, s.fileDir, clusterID, nodeID, s, s.sender, s.apiClient)
}

// TODO(check cluster has been added / dont care if it's published)
func (s *Store) syncProposeLocal(ctx context.Context, clusterID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	rsp, err := client.SyncProposeLocal(ctx, s.nodeHost, clusterID, batch)
	return rsp, err
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
		err := status.OutOfRangeErrorf("Range %d not present", req.GetHeader().GetRangeId())
		log.Errorf("Range not active, returning err: %s", err)
		return nil, err
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
		err := status.OutOfRangeErrorf("Range %d not present", req.GetHeader().GetRangeId())
		log.Errorf("Range not active, returning err: %s", err)
		return nil, err
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

package store

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
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

	liveness *nodeliveness.Liveness

	rangeMu       sync.RWMutex
	clusterRanges map[uint64]*rfpb.RangeDescriptor

	leaseMu sync.RWMutex
	leases  map[uint64]*rangelease.Lease

	replicaMu sync.RWMutex
	replicas  map[uint64]*replica.Replica
}

func New(rootDir, fileDir string, nodeHost *dragonboat.NodeHost, gossipManager *gossip.GossipManager, sender *sender.Sender, apiClient *client.APIClient) *Store {
	s := &Store{
		rootDir:       rootDir,
		fileDir:       fileDir,
		nodeHost:      nodeHost,
		gossipManager: gossipManager,
		sender:        sender,
		apiClient:     apiClient,
		liveness:      nodeliveness.New(nodeHost.ID(), &client.NodeHostSender{NodeHost: nodeHost}),

		rangeMu:       sync.RWMutex{},
		clusterRanges: make(map[uint64]*rfpb.RangeDescriptor),

		leaseMu: sync.RWMutex{},
		leases:  make(map[uint64]*rangelease.Lease),

		replicaMu: sync.RWMutex{},
		replicas:  make(map[uint64]*replica.Replica),
	}

	cb := listener.LeaderCB(s.LeaderUpdated)
	listener.DefaultListener().RegisterLeaderUpdatedCB(&cb)
	return s
}

func (s *Store) LeaderUpdated(info raftio.LeaderInfo) {
	if s.nodeHost == nil {
		// not initialized yet
		return
	}
	clusterID := info.ClusterID
	leader := s.isLeader(clusterID)
	if leader {
		s.acquireRangeLease(clusterID)
	} else {
		s.releaseRangeLease(clusterID)
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

func (s *Store) acquireRangeLease(clusterID uint64) {
	rd := s.lookupRange(clusterID)
	if rd == nil {
		return
	}
	var rl *rangelease.Lease

	s.leaseMu.Lock()
	if existingLease, ok := s.leases[rd.GetRangeId()]; ok {
		rl = existingLease
	} else {
		rl = rangelease.New(&client.NodeHostSender{NodeHost: s.nodeHost}, s.liveness, rd)
		s.leases[rd.GetRangeId()] = rl
	}
	s.leaseMu.Unlock()

	go func() {
		for {
			if rl.Valid() {
				return
			}
			err := rl.Lease()
			if err == nil {
				log.Printf("Succesfully leased range: %+v", rl)
				return
			}
			log.Warningf("Error leasing range: %s", err)
		}
	}()
}

func (s *Store) releaseRangeLease(clusterID uint64) {
	rd := s.lookupRange(clusterID)
	if rd == nil {
		return
	}

	s.leaseMu.Lock()
	rl, ok := s.leases[rd.GetRangeId()]
	if ok {
		delete(s.leases, rd.GetRangeId())
	}
	s.leaseMu.Unlock()
	if !ok {
		return
	}
	go func() {
		rl.Release()
	}()
}

func (s *Store) gossipUsage() {
	usage := &rfpb.NodeUsage{
		Nhid: s.nodeHost.ID(),
	}

	s.replicaMu.RLock()
	usage.NumRanges = int64(len(s.replicas))
	s.replicaMu.RUnlock()

	du, err := disk.GetDirUsage(s.fileDir)
	if err != nil {
		log.Errorf("error getting fs usage: %s", err)
		return
	}
	usage.DiskBytesTotal = int64(du.TotalBytes)
	usage.DiskBytesUsed = int64(du.UsedBytes)

	s.gossipManager.SetTag(constants.NodeUsageTag, proto.MarshalTextString(usage))
}

// We need to implement the RangeTracker interface so that stores opened and
// closed on this node will notify us when their range appears and disappears.
// We'll use this information to drive the range tags we broadcast.
func (s *Store) AddRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {
	log.Printf("AddRange: %+v", rd)
	if len(rd.GetReplicas()) == 0 {
		log.Error("range descriptor had no replicas; this should not happen")
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

	s.replicaMu.Lock()
	s.replicas[rd.GetRangeId()] = r
	s.replicaMu.Unlock()

	s.rangeMu.Lock()
	s.clusterRanges[clusterID] = rd
	s.rangeMu.Unlock()

	leader := s.isLeader(clusterID)
	if leader {
		s.acquireRangeLease(clusterID)
	} else {
		s.releaseRangeLease(clusterID)
	}
	s.gossipUsage()
}

func (s *Store) RemoveRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {
	log.Printf("RemoveRange: %+v", rd)
	clusterID := rd.GetReplicas()[0].GetClusterId()

	s.replicaMu.Lock()
	delete(s.replicas, rd.GetRangeId())
	s.replicaMu.Unlock()

	s.rangeMu.Lock()
	delete(s.clusterRanges, clusterID)
	s.rangeMu.Unlock()
	s.gossipUsage()
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

func (s *Store) AddNode(ctx context.Context, req *rfpb.AddNodeRequest) (*rfpb.AddNodeResponse, error) {
	err := s.nodeHost.SyncRequestAddNode(ctx, req.GetClusterId(), req.GetNodeId(), s.nodeHost.ID(), req.GetConfigChangeIndex())
	if err != nil {
		return nil, err
	}
	return &rfpb.AddNodeResponse{}, nil
}

func (s *Store) StartCluster(ctx context.Context, req *rfpb.StartClusterRequest) (*rfpb.StartClusterResponse, error) {
	rc := raftConfig.GetRaftConfig(req.GetClusterId(), req.GetNodeId())

	waitErr := make(chan error, 1)
	// Wait for the notification that the cluster node is ready on the local
	// nodehost.
	go func() {
		err := listener.DefaultListener().WaitForClusterReady(ctx, req.GetClusterId())
		waitErr <- err
		close(waitErr)
	}()

	err := s.nodeHost.StartOnDiskCluster(req.GetInitialMember(), false /*=join*/, s.ReplicaFactoryFn, rc)
	if err != nil {
		if err == dragonboat.ErrClusterAlreadyExist {
			err = status.AlreadyExistsError(err.Error())
		}
		return nil, err
	}

	err, ok := <-waitErr
	if ok && err != nil {
		log.Errorf("Got a WaitForClusterReady error: %s", err)
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
		time.Sleep(100 * time.Millisecond)
		batchResponse, err := s.syncProposeLocal(ctx, req.GetClusterId(), req.GetBatch())
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
		log.Infof("Rangelease(%d) not held on %q, returning err: %s", req.GetHeader().GetRangeId(), s.nodeHost.ID(), err)
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
		log.Infof("Rangelease(%d) not held on %q, returning err: %s", req.GetHeader().GetRangeId(), s.nodeHost.ID(), err)
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

func (s *Store) FindMissing(ctx context.Context, req *rfpb.FindMissingRequest) (*rfpb.FindMissingResponse, error) {
	s.replicaMu.RLock()
	defer s.replicaMu.RUnlock()
	if !s.RangeIsActive(req.GetHeader().GetRangeId()) {
		err := status.OutOfRangeErrorf("Range %d not present", req.GetHeader().GetRangeId())
		log.Errorf("Range not active, returning err: %s", err)
		return nil, err
	}
	r, ok := s.replicas[req.GetHeader().GetRangeId()]
	if !ok {
		return nil, status.FailedPreconditionError("Range was active but replica not found; this should not happen")
	}
	db, err := r.DB()
	if err != nil {
		return nil, err
	}

	rsp := &rfpb.FindMissingResponse{}
	iter := db.NewIter(nil)
	defer iter.Close()

	for _, fileRecord := range req.GetFileRecord() {
		fileKey, err := constants.FileKey(fileRecord)
		if err != nil {
			return nil, err
		}
		if !iter.SeekGE(fileKey) || bytes.Compare(iter.Key(), fileKey) != 0 {
			rsp.FileRecord = append(rsp.FileRecord, fileRecord)
		}
	}
	return rsp, nil
}

package store

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"

	raftConfig "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

const nodeLeaseDuration = 10 * time.Second

type Store struct {
	rootDir string
	fileDir string

	nodeHost      *dragonboat.NodeHost
	gossipManager *gossip.GossipManager
	sender        *sender.Sender
	apiClient     *client.APIClient
	heartbeat     *nodelease.Heartbeat

	rangeMu      sync.RWMutex
	activeRanges map[uint64]*rfpb.RangeDescriptor
}

func New(rootDir, fileDir string, nodeHost *dragonboat.NodeHost, gossipManager *gossip.GossipManager, sender *sender.Sender, apiClient *client.APIClient) *Store {
	store := &Store{
		rootDir:       rootDir,
		fileDir:       fileDir,
		nodeHost:      nodeHost,
		gossipManager: gossipManager,
		sender:        sender,
		apiClient:     apiClient,

		rangeMu:      sync.RWMutex{},
		activeRanges: make(map[uint64]*rfpb.RangeDescriptor),

		heartbeat: nodelease.NewHeartbeat([]byte(nodeHost.ID()), sender),
	}
	store.heartbeat.Start()
	return store
}

func containsMetaRange(rd *rfpb.RangeDescriptor) bool {
	r := rangemap.Range{Left: rd.Left, Right: rd.Right}
	return r.Contains([]byte{constants.MinByte}) && r.Contains([]byte{constants.UnsplittableMaxByte - 1})
}

// We need to implement the RangeTracker interface so that stores opened and
// closed on this node will notify us when their range appears and disappears.
// We'll use this information to drive the range tags we broadcast.
func (s *Store) AddRange(rd *rfpb.RangeDescriptor) {
	s.rangeMu.Lock()
	defer s.rangeMu.Unlock()
	log.Printf("AddRange: %+v", rd)
	s.activeRanges[rd.GetRangeId()] = rd
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
}

func (s *Store) RemoveRange(rd *rfpb.RangeDescriptor) {
	s.rangeMu.Lock()
	defer s.rangeMu.Unlock()
	log.Printf("RemoveRange: %+v", rd)
	delete(s.activeRanges, rd.GetRangeId())
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
			log.Errorf("SyncPropose err: %s", err)
			if err == dragonboat.ErrClusterNotReady {
				log.Errorf("continuing, got cluster not ready err...")
				continue
			}
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

func (s *Store) StartCluster(ctx context.Context, req *rfpb.StartClusterRequest) (*rfpb.StartClusterResponse, error) {
	rc := raftConfig.GetRaftConfig(req.GetClusterId(), req.GetNodeId())

	err := s.nodeHost.StartOnDiskCluster(req.GetInitialMember(), false /*=join*/, s.ReplicaFactoryFn, rc)
	if err != nil {
		if err == dragonboat.ErrClusterAlreadyExist {
			err = status.AlreadyExistsError(err.Error())
		}
		return nil, err
	}
	amLeader := false
	nodeHostInfo := s.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	for _, clusterInfo := range nodeHostInfo.ClusterInfoList {
		if clusterInfo.ClusterID == req.GetClusterId() && clusterInfo.IsLeader {
			amLeader = true
			break
		}
	}
	rsp := &rfpb.StartClusterResponse{}
	if amLeader {
		batchResponse, err := s.syncProposeLocal(ctx, req.GetClusterId(), req.GetBatch())
		if err != nil {
			return nil, err
		}
		rsp.Batch = batchResponse
	}
	return rsp, nil
}

func (s *Store) SyncPropose(ctx context.Context, req *rfpb.SyncProposeRequest) (*rfpb.SyncProposeResponse, error) {
	log.Debugf("SyncPropose (server) got request: %+v", req)
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
	log.Debugf("SyncRead (server) got request: %+v", req)
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

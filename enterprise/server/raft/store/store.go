package store

import (
	"bytes"
	"context"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"

	raftConfig "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

const readBufSizeBytes = 1000000 // 1MB

type Store struct {
	rootDir string
	fileDir string

	nodeHost      *dragonboat.NodeHost
	gossipManager *gossip.GossipManager
	sender        *sender.Sender
	registry      *registry.DynamicNodeRegistry
	apiClient     *client.APIClient
	liveness      *nodeliveness.Liveness

	rangeMu       sync.RWMutex
	clusterRanges map[uint64]*rfpb.RangeDescriptor

	leaseMu sync.RWMutex
	leases  map[uint64]*rangelease.Lease

	replicaMu sync.RWMutex
	replicas  map[uint64]*replica.Replica
}

func New(rootDir, fileDir string, nodeHost *dragonboat.NodeHost, gossipManager *gossip.GossipManager, sender *sender.Sender, registry *registry.DynamicNodeRegistry, apiClient *client.APIClient) *Store {
	s := &Store{
		rootDir:       rootDir,
		fileDir:       fileDir,
		nodeHost:      nodeHost,
		gossipManager: gossipManager,
		sender:        sender,
		registry:      registry,
		apiClient:     apiClient,
		liveness:      nodeliveness.New(nodeHost.ID(), &client.NodeHostSender{NodeHost: nodeHost}),

		rangeMu:       sync.RWMutex{},
		clusterRanges: make(map[uint64]*rfpb.RangeDescriptor),

		leaseMu: sync.RWMutex{},
		leases:  make(map[uint64]*rangelease.Lease),

		replicaMu: sync.RWMutex{},
		replicas:  make(map[uint64]*replica.Replica),
	}

	cb := listener.LeaderCB(s.leaderUpdated)
	listener.DefaultListener().RegisterLeaderUpdatedCB(&cb)
	s.gossipUsage()
	return s
}

func (s *Store) Stop() {
	log.Debugf("Stop")
}

func (s *Store) leaderUpdated(info raftio.LeaderInfo) {
	clusterID := info.ClusterID
	leader := s.isLeader(clusterID)
	if leader {
		s.acquireRangeLease(clusterID)
	} else {
		s.releaseRangeLease(clusterID)
	}
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

func (s *Store) GetRange(clusterID uint64) *rfpb.RangeDescriptor {
	return s.lookupRange(clusterID)
}

func (s *Store) GetRangeLease(clusterID uint64) *rangelease.Lease {
	rd := s.lookupRange(clusterID)
	if rd == nil {
		return nil
	}

	s.leaseMu.RLock()
	defer s.leaseMu.RUnlock()
	rl, ok := s.leases[rd.GetRangeId()]
	if !ok {
		return nil
	}
	return rl
}

func (s *Store) gossipUsage() {
	usage := &rfpb.NodeUsage{
		Nhid: s.nodeHost.ID(),
	}

	s.replicaMu.RLock()
	usage.NumReplicas = int64(len(s.replicas))
	for _, r := range s.replicas {
		ru, err := r.Usage()
		if err != nil {
			log.Warningf("error getting replica usage: %s", err)
			continue
		}
		usage.ReplicaUsage = append(usage.ReplicaUsage, ru)
	}
	s.replicaMu.RUnlock()

	du, err := disk.GetDirUsage(s.fileDir)
	if err != nil {
		log.Errorf("error getting fs usage: %s", err)
		return
	}
	usage.DiskBytesTotal = int64(du.TotalBytes)
	usage.DiskBytesUsed = int64(du.UsedBytes)
	s.gossipManager.SetTag(constants.NodeHostIDTag, string(s.nodeHost.ID()))
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
	clusterID := rd.GetReplicas()[0].GetClusterId()

	if rangelease.ContainsMetaRange(rd) {
		// If we own the metarange, use gossip to notify other nodes
		// of that fact.
		buf, err := proto.Marshal(rd)
		if err != nil {
			log.Errorf("Error marshaling metarange descriptor: %s", err)
			return
		}
		s.gossipManager.SetTag(constants.MetaRangeTag, string(buf))
	}

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
		return false
	}
	valid := rl.Valid()
	return valid
}

func (s *Store) ReplicaFactoryFn(clusterID, nodeID uint64) dbsm.IOnDiskStateMachine {
	return replica.New(s.rootDir, s.fileDir, clusterID, nodeID, s)
}

func (s *Store) ReadFileFromPeer(ctx context.Context, except *rfpb.ReplicaDescriptor, fileRecord *rfpb.FileRecord) (io.ReadCloser, error) {
	fileKey, err := constants.FileKey(fileRecord)
	if err != nil {
		return nil, err
	}
	var rc io.ReadCloser
	err = s.sender.Run(ctx, fileKey, func(c rfspb.ApiClient, h *rfpb.Header) error {
		log.Printf("in sender.Run, filekey: %q", fileKey)
		if h.GetReplica().GetClusterId() == except.GetClusterId() &&
			h.GetReplica().GetNodeId() == except.GetNodeId() {
			log.Printf("NOT ATTEMPTING TO READ FROM SELF: +%v", h.GetReplica())
			return status.OutOfRangeError("except node")
		}
		req := &rfpb.ReadRequest{
			Header:     h,
			FileRecord: fileRecord,
			Offset:     0,
		}
		r, err := s.apiClient.RemoteReader(ctx, c, req)
		if err != nil {
			log.Printf("RemoteReader %+v err: %s", fileRecord, err)
			return err
		}
		log.Printf("in sender.Run, filekey: %q, remoteReader returned r: %+v", fileKey, r)
		rc = r
		return nil
	})
	return rc, err
}

// TODO(check cluster has been added / dont care if it's published)
func (s *Store) syncProposeLocal(ctx context.Context, clusterID uint64, batch *rfpb.BatchCmdRequest) (*rfpb.BatchCmdResponse, error) {
	rsp, err := client.SyncProposeLocal(ctx, s.nodeHost, clusterID, batch)
	return rsp, err
}

func (s *Store) isLeader(clusterID uint64) bool {
	nodeHostInfo := s.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{
		SkipLogInfo: true,
	})
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

	waitErr := make(chan error, 1)
	// Wait for the notification that the cluster node is ready on the local
	// nodehost.
	go func() {
		err := listener.DefaultListener().WaitForClusterReady(ctx, req.GetClusterId())
		waitErr <- err
		close(waitErr)
	}()

	err := s.nodeHost.StartOnDiskCluster(req.GetInitialMember(), req.GetJoin(), s.ReplicaFactoryFn, rc)
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
	if req.GetBatch() == nil || len(req.GetInitialMember()) == 0 {
		return rsp, nil
	}

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

func (s *Store) RemoveData(ctx context.Context, req *rfpb.RemoveDataRequest) (*rfpb.RemoveDataResponse, error) {
	log.Printf("RemoveData req: %+v", req)
	s.registry.Add(req.GetClusterId(), req.GetNodeId(), s.nodeHost.ID())
	if err := s.nodeHost.StopNode(req.GetClusterId(), req.GetNodeId()); err != nil {
		return nil, err
	}
	if err := s.nodeHost.SyncRemoveData(ctx, req.GetClusterId(), req.GetNodeId()); err != nil {
		return nil, err
	}
	return &rfpb.RemoveDataResponse{}, nil
}

func (s *Store) SyncPropose(ctx context.Context, req *rfpb.SyncProposeRequest) (*rfpb.SyncProposeResponse, error) {
	if !s.RangeIsActive(req.GetHeader().GetRangeId()) {
		err := status.OutOfRangeErrorf("Range %d not present", req.GetHeader().GetRangeId())
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
		return nil, err
	}
	buf, err := proto.Marshal(req.GetBatch())
	if err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		c, cancel := context.WithTimeout(ctx, client.DefaultContextTimeout)
		defer cancel()
		ctx = c
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
		return nil, err
	}
	r, ok := s.replicas[req.GetHeader().GetRangeId()]
	if !ok {
		return nil, status.FailedPreconditionError("Range was active but replica not found; this should not happen")
	}
	iter, err := r.Iterator()
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	rsp := &rfpb.FindMissingResponse{}
	for _, fileRecord := range req.GetFileRecord() {
		fileMetadaKey, err := constants.FileMetadataKey(fileRecord)
		if err != nil {
			return nil, err
		}
		if !iter.SeekGE(fileMetadaKey) || bytes.Compare(iter.Key(), fileMetadaKey) != 0 {
			rsp.FileRecord = append(rsp.FileRecord, fileRecord)
		}
	}
	return rsp, nil
}

type streamWriter struct {
	stream rfspb.Api_ReadServer
}

func (w *streamWriter) Write(buf []byte) (int, error) {
	err := w.stream.Send(&rfpb.ReadResponse{
		Data: buf,
	})
	return len(buf), err
}

func (s *Store) Read(req *rfpb.ReadRequest, stream rfspb.Api_ReadServer) error {
	s.replicaMu.RLock()
	defer s.replicaMu.RUnlock()
	if !s.RangeIsActive(req.GetHeader().GetRangeId()) {
		err := status.OutOfRangeErrorf("Range %d not present", req.GetHeader().GetRangeId())
		return err
	}
	r, ok := s.replicas[req.GetHeader().GetRangeId()]
	if !ok {
		return status.FailedPreconditionError("Range was active but replica not found; this should not happen")
	}
	iter, err := r.Iterator()
	if err != nil {
		return err
	}
	defer iter.Close()

	fileKey, err := constants.FileKey(req.GetFileRecord())
	if err != nil {
		return err
	}
	fileMetadataKey, err := constants.FileMetadataKey(req.GetFileRecord())
	if err != nil {
		return err
	}

	// First, lookup the FileRecord. If it's not found, we don't have the file.
	found := iter.SeekGE(fileMetadataKey)
	if !found || bytes.Compare(fileMetadataKey, iter.Key()) != 0 {
		return status.NotFoundErrorf("file %q not found", fileKey)
	}
	fileMetadata := &rfpb.FileMetadata{}
	if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
		return status.InternalErrorf("error reading file %q metadata", fileKey)
	}
	readCloser, err := filestore.NewReader(stream.Context(), s.fileDir, iter, fileMetadata.GetStorageMetadata())
	if err != nil {
		return err
	}
	defer readCloser.Close()

	bufSize := int64(readBufSizeBytes)
	d := req.GetFileRecord().GetDigest()
	if d.GetSizeBytes() > 0 && d.GetSizeBytes() < bufSize {
		bufSize = d.GetSizeBytes()
	}
	copyBuf := make([]byte, bufSize)
	_, err = io.CopyBuffer(&streamWriter{stream}, readCloser, copyBuf)
	return err
}

func (s *Store) Write(stream rfspb.Api_WriteServer) error {
	var bytesWritten int64
	var fileMetadataKey []byte
	var writeCloser filestore.WriteCloserMetadata
	var batch *pebble.Batch
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if writeCloser == nil {
			s.replicaMu.RLock()
			defer s.replicaMu.RUnlock()
			r, ok := s.replicas[req.GetHeader().GetRangeId()]
			if !ok {
				return status.FailedPreconditionError("Range was active but replica not found; this should not happen")
			}
			db, err := r.DB()
			if err != nil {
				return err
			}
			batch = db.NewBatch()
			fileMetadataKey, err = constants.FileMetadataKey(req.GetFileRecord())
			if err != nil {
				return err
			}
			writeCloser, err = filestore.NewWriter(stream.Context(), s.fileDir, batch, req.GetFileRecord())
			if err != nil {
				return err
			}
		}
		n, err := writeCloser.Write(req.Data)
		if err != nil {
			return err
		}
		bytesWritten += int64(n)
		if req.FinishWrite {
			if err := writeCloser.Close(); err != nil {
				return err
			}
			md := &rfpb.FileMetadata{
				FileRecord:      req.GetFileRecord(),
				StorageMetadata: writeCloser.Metadata(),
			}
			protoBytes, err := proto.Marshal(md)
			if err != nil {
				return err
			}
			if err := batch.Set(fileMetadataKey, protoBytes, nil /*ignored write options*/); err != nil {
				return err
			}
			if err := batch.Commit(&pebble.WriteOptions{Sync: true}); err != nil {
				return err
			}
			return stream.SendAndClose(&rfpb.WriteResponse{
				CommittedSize: bytesWritten,
			})
		}
	}
	return nil
}

package store

import (
	"bytes"
	"context"
	"io"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangelease"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	raftConfig "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

const (
	readBufSizeBytes = 1000000 // 1MB

	// If a node's disk is fuller than this (by percentage), it is not
	// eligible to receive ranges moved from other nodes.
	maximumDiskCapacity = .95
)

type Store struct {
	rootDir  string
	fileDir  string
	grpcAddr string

	nodeHost      *dragonboat.NodeHost
	gossipManager *gossip.GossipManager
	sender        *sender.Sender
	registry      registry.NodeRegistry
	grpcServer    *grpc.Server
	apiClient     *client.APIClient
	liveness      *nodeliveness.Liveness

	rangeMu       sync.RWMutex
	clusterRanges map[uint64]*rfpb.RangeDescriptor

	leaseMu sync.RWMutex
	leases  map[uint64]*rangelease.Lease

	replicaMu sync.RWMutex
	replicas  map[uint64]*replica.Replica
}

func New(rootDir, fileDir string, nodeHost *dragonboat.NodeHost, gossipManager *gossip.GossipManager, sender *sender.Sender, registry registry.NodeRegistry, apiClient *client.APIClient) *Store {
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
	gossipManager.AddListener(s)
	cb := listener.LeaderCB(s.leaderUpdated)
	listener.DefaultListener().RegisterLeaderUpdatedCB(&cb)
	s.gossipUsage()
	return s
}

func (s *Store) Start(grpcAddress string) error {
	// A grpcServer is run which is responsible for presenting a meta API
	// to manage raft nodes on each host, as well as an API to shuffle data
	// around between nodes, outside of raft.
	s.grpcServer = grpc.NewServer()
	reflection.Register(s.grpcServer)
	rfspb.RegisterApiServer(s.grpcServer, s)

	lis, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		return err
	}
	go func() {
		s.grpcServer.Serve(lis)
	}()
	s.grpcAddr = grpcAddress
	return nil
}

func (s *Store) Stop(ctx context.Context) error {
	return grpc_server.GRPCShutdown(ctx, s.grpcServer)
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
		log.Errorf("error getting fs usage for %q: %s", s.fileDir, err)
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
		if h.GetReplica().GetClusterId() == except.GetClusterId() &&
			h.GetReplica().GetNodeId() == except.GetNodeId() {
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
		rc = r
		return nil
	})
	return rc, err
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

	// If we are the last member in the cluster, we'll do the syncPropose.
	nodeIDs := make([]uint64, 0, len(req.GetInitialMember()))
	for nodeID, _ := range req.GetInitialMember() {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })
	if req.GetNodeId() == nodeIDs[len(nodeIDs)-1] {
		time.Sleep(100 * time.Millisecond)
		batchResponse, err := client.SyncProposeLocal(ctx, s.nodeHost, req.GetClusterId(), req.GetBatch())
		if err != nil {
			return nil, err
		}
		rsp.Batch = batchResponse
	}
	return rsp, nil
}

func (s *Store) RemoveData(ctx context.Context, req *rfpb.RemoveDataRequest) (*rfpb.RemoveDataResponse, error) {
	err := client.RunNodehostFn(ctx, func(ctx context.Context) error {
		return s.nodeHost.SyncRemoveData(ctx, req.GetClusterId(), req.GetNodeId())
	})
	if err != nil {
		return nil, err
	}
	return &rfpb.RemoveDataResponse{}, nil
}

func (s *Store) SyncPropose(ctx context.Context, req *rfpb.SyncProposeRequest) (*rfpb.SyncProposeResponse, error) {
	if !s.RangeIsActive(req.GetHeader().GetRangeId()) {
		err := status.OutOfRangeErrorf("Range %d not present", req.GetHeader().GetRangeId())
		return nil, err
	}
	batchResponse, err := client.SyncProposeLocal(ctx, s.nodeHost, req.GetHeader().GetReplica().GetClusterId(), req.GetBatch())
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
	batchResponse, err := client.SyncReadLocal(ctx, s.nodeHost, req.GetHeader().GetReplica().GetClusterId(), req.GetBatch())
	if err != nil {
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

func (s *Store) OnEvent(updateType serf.EventType, event serf.Event) {
	if updateType != serf.EventQuery {
		return
	}

	query, ok := event.(*serf.Query)
	if !ok || query.Payload == nil {
		return
	}

	ctx, cancel := context.WithDeadline(context.Background(), query.Deadline())
	defer cancel()

	switch query.Name {
	case constants.PlacementDriverQueryEvent:
		s.handlePlacementQuery(ctx, query)
	}
}

func (s *Store) handlePlacementQuery(ctx context.Context, query *serf.Query) {
	pq := &rfpb.PlacementQuery{}
	if err := proto.Unmarshal(query.Payload, pq); err != nil {
		return
	}
	nodeHostInfo := s.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	if nodeHostInfo != nil {
		for _, logInfo := range nodeHostInfo.LogInfo {
			if pq.GetTargetClusterId() == logInfo.ClusterID {
				log.Printf("Skipping %q because it already had cluster %d", s.nodeHost.ID(), logInfo.ClusterID)
				return
			}
		}
	}

	// Do not respond if this node is over 95% full.
	member := s.gossipManager.LocalMember()
	usageBuf, ok := member.Tags[constants.NodeUsageTag]
	if !ok {
		log.Errorf("Ignoring placement query: couldn't determine node usage")
		return
	}
	usage := &rfpb.NodeUsage{}
	if err := proto.UnmarshalText(usageBuf, usage); err != nil {
		return
	}
	myDiskUsage := float64(usage.GetDiskBytesUsed()) / float64(usage.GetDiskBytesTotal())
	if myDiskUsage > maximumDiskCapacity {
		log.Debugf("Ignoring placement queryy: node is over capacity")
		return
	}

	nodeBuf, err := proto.Marshal(s.MyNodeDescriptor())
	if err != nil {
		return
	}
	if err := query.Respond(nodeBuf); err != nil {
		log.Warningf("Error responding to gossip query: %s", err)
	}
}

func (s *Store) MyNodeDescriptor() *rfpb.NodeDescriptor {
	return &rfpb.NodeDescriptor{
		Nhid:        s.nodeHost.ID(),
		RaftAddress: s.nodeHost.RaftAddress(),
		GrpcAddress: s.grpcAddr,
	}
}

func (s *Store) GetClusterMembership(ctx context.Context, clusterID uint64) ([]*rfpb.ReplicaDescriptor, error) {
	var membership *dragonboat.Membership
	var err error
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		membership, err = s.nodeHost.GetClusterMembership(ctx, clusterID)
		return err
	})
	if err != nil {
		return nil, err
	}

	replicas := make([]*rfpb.ReplicaDescriptor, 0, len(membership.Nodes))
	for nodeID, _ := range membership.Nodes {
		replicas = append(replicas, &rfpb.ReplicaDescriptor{
			ClusterId: clusterID,
			NodeId:    nodeID,
		})
	}
	return replicas, nil
}

func (s *Store) GetNodeUsage(ctx context.Context, replica *rfpb.ReplicaDescriptor) (*rfpb.NodeUsage, error) {
	targetNHID, _, err := s.registry.ResolveNHID(replica.GetClusterId(), replica.GetNodeId())
	if err != nil {
		return nil, err
	}
	for _, member := range s.membersInState(serf.StatusAlive) {
		if member.Tags[constants.NodeHostIDTag] != targetNHID {
			continue
		}
		if buf, ok := member.Tags[constants.NodeUsageTag]; ok {
			usage := &rfpb.NodeUsage{}
			if err := proto.UnmarshalText(buf, usage); err != nil {
				return nil, err
			}
			return usage, nil
		}
	}
	return nil, status.NotFoundErrorf("Usage not found for c%dn%d", replica.GetClusterId(), replica.GetNodeId())
}

func (s *Store) FindNodes(ctx context.Context, query *rfpb.PlacementQuery) ([]*rfpb.NodeDescriptor, error) {
	buf, err := proto.Marshal(query)
	if err != nil {
		return nil, err
	}
	rsp, err := s.gossipManager.Query(constants.PlacementDriverQueryEvent, buf, nil)
	if err != nil {
		return nil, err
	}

	foundNodes := make([]*rfpb.NodeDescriptor, 0)
	for nodeRsp := range rsp.ResponseCh() {
		if nodeRsp.Payload == nil {
			continue
		}
		node := &rfpb.NodeDescriptor{}
		if err := proto.Unmarshal(nodeRsp.Payload, node); err == nil {
			foundNodes = append(foundNodes, node)
		}
	}
	return foundNodes, nil
}

func (s *Store) membersInState(memberStatus serf.MemberStatus) []serf.Member {
	members := make([]serf.Member, 0)
	for _, member := range s.gossipManager.Members() {
		if member.Status == memberStatus {
			members = append(members, member)
		}
	}
	return members
}

func (s *Store) SplitRange(ctx context.Context, clusterID uint64) error {
	// TODO(tylerw): implement
	return nil
}

func (s *Store) getConfigChangeID(ctx context.Context, clusterID uint64) (uint64, error) {
	var membership *dragonboat.Membership
	var err error
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		// Get the config change index for this cluster.
		membership, err = s.nodeHost.SyncGetClusterMembership(ctx, clusterID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return membership.ConfigChangeID, nil
}

func (s *Store) AddNodeToCluster(ctx context.Context, rangeDescriptor *rfpb.RangeDescriptor, node *rfpb.NodeDescriptor) error {
	if len(rangeDescriptor.GetReplicas()) == 0 {
		return status.FailedPreconditionErrorf("No replicas in range: %+v", rangeDescriptor)
	}
	clusterID := rangeDescriptor.GetReplicas()[0].GetClusterId()

	// Reserve a new node ID for the node about to be added.
	nodeIDs, err := s.reserveNodeIDs(ctx, 1)
	if err != nil {
		return err
	}
	nodeID := nodeIDs[0]

	// Get the config change index for this cluster.
	configChangeID, err := s.getConfigChangeID(ctx, clusterID)
	if err != nil {
		return err
	}

	// Gossip the address of the node that is about to be added.
	s.registry.Add(clusterID, nodeID, node.GetNhid())
	s.registry.AddNode(node.GetNhid(), node.GetRaftAddress(), node.GetGrpcAddress())

	// Propose the config change (this adds the node to the raft cluster).
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestAddNode(ctx, clusterID, nodeID, node.GetNhid(), configChangeID)
	})
	if err != nil {
		return err
	}

	// Start the cluster on the newly added node.
	c, err := s.apiClient.Get(ctx, node.GetGrpcAddress())
	if err != nil {
		return err
	}
	_, err = c.StartCluster(ctx, &rfpb.StartClusterRequest{
		ClusterId: clusterID,
		NodeId:    nodeID,
		Join:      true,
	})
	if err != nil {
		return err
	}

	// Finally, update the range descriptor information to reflect the
	// membership of this new node in the range.
	return s.addReplicaToRangeDescriptor(ctx, clusterID, nodeID, rangeDescriptor)
}

func (s *Store) RemoveNodeFromCluster(ctx context.Context, rangeDescriptor *rfpb.RangeDescriptor, targetNodeID uint64) error {
	var clusterID, nodeID uint64
	for _, replica := range rangeDescriptor.GetReplicas() {
		if replica.GetNodeId() == targetNodeID {
			clusterID = replica.GetClusterId()
			nodeID = replica.GetNodeId()
			break
		}
	}
	if clusterID == 0 && nodeID == 0 {
		return status.FailedPreconditionErrorf("No node with id %d found in range: %+v", targetNodeID, rangeDescriptor)
	}

	grpcAddr, _, err := s.registry.ResolveGRPC(clusterID, nodeID)
	if err != nil {
		log.Errorf("error resolving grpc addr: %s", err)
		return err
	}

	configChangeID, err := s.getConfigChangeID(ctx, clusterID)
	if err != nil {
		return err
	}

	// Propose the config change (this removes the node from the raft cluster).
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestDeleteNode(ctx, clusterID, nodeID, configChangeID)
	})
	if err != nil {
		return err
	}

	// Remove the data from the now stopped node.
	c, err := s.apiClient.Get(ctx, grpcAddr)
	if err != nil {
		log.Errorf("err getting api client: %s", err)
		return err
	}
	_, err = c.RemoveData(ctx, &rfpb.RemoveDataRequest{
		ClusterId: clusterID,
		NodeId:    nodeID,
	})
	if err != nil {
		log.Errorf("remove data err: %s", err)
		return err
	}

	// Finally, update the range descriptor information to reflect the
	// new membership of this range without the removed node.
	return s.removeReplicaFromRangeDescriptor(ctx, clusterID, nodeID, rangeDescriptor)
}

func (s *Store) reserveNodeIDs(ctx context.Context, n int) ([]uint64, error) {
	newVal, err := s.sender.Increment(ctx, constants.LastNodeIDKey, uint64(n))
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, newVal-uint64(i))
	}
	return ids, nil
}

func (s *Store) updateRangeDescriptor(ctx context.Context, clusterID uint64, old, new *rfpb.RangeDescriptor) error {
	oldBuf, err := proto.Marshal(old)
	if err != nil {
		return err
	}
	newBuf, err := proto.Marshal(new)
	if err != nil {
		return err
	}
	rangeLocalBatch, err := rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: newBuf,
		},
		ExpectedValue: oldBuf,
	}).ToProto()
	if err != nil {
		return err
	}

	metaRangeDescriptorKey := keys.RangeMetaKey(new.GetRight())
	metaRangeBatch, err := rbuilder.NewBatchBuilder().Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   metaRangeDescriptorKey,
			Value: newBuf,
		},
		ExpectedValue: oldBuf,
	}).ToProto()
	if err != nil {
		return err
	}

	// first update the range descriptor in the range itself
	rangeLocalRsp, err := client.SyncProposeLocal(ctx, s.nodeHost, clusterID, rangeLocalBatch)
	if err != nil {
		return err
	}
	_, err = rbuilder.NewBatchResponseFromProto(rangeLocalRsp).CASResponse(0)
	if err != nil {
		return err
	}

	// then update the metarange
	metaRangeRsp, err := s.sender.SyncPropose(ctx, metaRangeDescriptorKey, metaRangeBatch)
	if err != nil {
		return err
	}
	_, err = rbuilder.NewBatchResponseFromProto(metaRangeRsp).CASResponse(0)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) addReplicaToRangeDescriptor(ctx context.Context, clusterID, nodeID uint64, oldDescriptor *rfpb.RangeDescriptor) error {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	newDescriptor.Replicas = append(newDescriptor.Replicas, &rfpb.ReplicaDescriptor{
		ClusterId: clusterID,
		NodeId:    nodeID,
	})
	return s.updateRangeDescriptor(ctx, clusterID, oldDescriptor, newDescriptor)
}

func (s *Store) removeReplicaFromRangeDescriptor(ctx context.Context, clusterID, nodeID uint64, oldDescriptor *rfpb.RangeDescriptor) error {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	for i, replica := range newDescriptor.Replicas {
		if replica.GetNodeId() == nodeID {
			newDescriptor.Replicas = append(newDescriptor.Replicas[:i], newDescriptor.Replicas[i+1:]...)
			break
		}
	}
	return s.updateRangeDescriptor(ctx, clusterID, oldDescriptor, newDescriptor)
}

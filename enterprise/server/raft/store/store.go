package store

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
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
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

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
	grpcAddr string

	nodeHost      *dragonboat.NodeHost
	gossipManager *gossip.GossipManager
	sender        *sender.Sender
	registry      registry.NodeRegistry
	grpcServer    *grpc.Server
	apiClient     *client.APIClient
	liveness      *nodeliveness.Liveness
	log           log.Logger

	rangeMu    sync.RWMutex
	openRanges map[uint64]*rfpb.RangeDescriptor

	leases   sync.Map // map of uint64 rangeID -> *rangelease.Lease
	replicas sync.Map // map of uint64 rangeID -> *replica.Replica

	metaRangeData   string
	leaderUpdatedCB listener.LeaderCB

	fileStorer filestore.Store
}

func New(rootDir string, nodeHost *dragonboat.NodeHost, gossipManager *gossip.GossipManager, sender *sender.Sender, registry registry.NodeRegistry, apiClient *client.APIClient) *Store {
	s := &Store{
		rootDir:       rootDir,
		nodeHost:      nodeHost,
		gossipManager: gossipManager,
		sender:        sender,
		registry:      registry,
		apiClient:     apiClient,
		liveness:      nodeliveness.New(nodeHost.ID(), sender),
		log:           log.NamedSubLogger(nodeHost.ID()),

		rangeMu:    sync.RWMutex{},
		openRanges: make(map[uint64]*rfpb.RangeDescriptor),

		leases:   sync.Map{},
		replicas: sync.Map{},

		metaRangeData: "",
		fileStorer:    filestore.New(true /*=isolateByGroupIDs*/),
	}
	s.leaderUpdatedCB = listener.LeaderCB(s.onLeaderUpdated)
	gossipManager.AddListener(s)

	listener.DefaultListener().RegisterLeaderUpdatedCB(&s.leaderUpdatedCB)
	statusz.AddSection("raft_store", "Store", s)
	return s
}

func (s *Store) replicaString(r *replica.Replica) string {
	ru, err := r.Usage()
	if err != nil {
		return "UNKNOWN"
	}
	clusterString := fmt.Sprintf("(c%dn%d)", ru.GetReplica().GetClusterId(), ru.GetReplica().GetNodeId())
	rangeLeaseString := ""
	if rd := s.lookupRange(ru.GetReplica().GetClusterId()); rd != nil {
		clusterString = fmt.Sprintf("%d: [%q %q)\t", rd.GetRangeId(), rd.GetLeft(), rd.GetRight()) + clusterString
		if rlIface, ok := s.leases.Load(rd.GetRangeId()); ok {
			if rl, ok := rlIface.(*rangelease.Lease); ok {
				rangeLeaseString = rl.String()
			}
		}
	}
	mbUsed := ru.GetEstimatedDiskBytesUsed() / 1e6
	return fmt.Sprintf("\t%s Usage: %dMB, Lease: %s\n", clusterString, mbUsed, rangeLeaseString)
}

func (s *Store) Statusz(ctx context.Context) string {
	buf := "<pre>"
	buf += fmt.Sprintf("NHID: %s\n", s.nodeHost.ID())
	buf += fmt.Sprintf("Liveness lease: %s\n", s.liveness)

	replicaStrings := make([]string, 0)
	s.replicas.Range(func(key, value any) bool {
		if r, ok := value.(*replica.Replica); ok {
			replicaStrings = append(replicaStrings, s.replicaString(r))
		}
		return true
	})
	buf += "Replicas:\n"
	sort.Strings(replicaStrings)
	for _, replicaString := range replicaStrings {
		buf += replicaString
	}
	buf += "</pre>"
	return buf
}

func (s *Store) onLeaderUpdated(info raftio.LeaderInfo) {
	if !s.isLeader(info.ClusterID) {
		return
	}
	rd := s.lookupRange(info.ClusterID)
	if rd == nil {
		return
	}
	go s.maybeAcquireRangeLease(rd)
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
	listener.DefaultListener().UnregisterLeaderUpdatedCB(&s.leaderUpdatedCB)
	return grpc_server.GRPCShutdown(ctx, s.grpcServer)
}

func (s *Store) lookupRange(clusterID uint64) *rfpb.RangeDescriptor {
	s.rangeMu.RLock()
	defer s.rangeMu.RUnlock()

	for _, rangeDescriptor := range s.openRanges {
		if len(rangeDescriptor.GetReplicas()) == 0 {
			continue
		}
		if clusterID == rangeDescriptor.GetReplicas()[0].GetClusterId() {
			return rangeDescriptor
		}
	}
	return nil
}

func (s *Store) maybeAcquireRangeLease(rd *rfpb.RangeDescriptor) {
	if len(rd.GetReplicas()) == 0 {
		s.log.Debugf("Not acquiring range %d lease: no replicas", rd.GetRangeId())
		return
	}

	clusterID := rd.GetReplicas()[0].GetClusterId()
	if !s.isLeader(clusterID) {
		return
	}

	rangeID := rd.GetRangeId()
	rlIface, _ := s.leases.LoadOrStore(rangeID, rangelease.New(s.sender, s.liveness, rd))
	rl, ok := rlIface.(*rangelease.Lease)
	if !ok {
		alert.UnexpectedEvent("unexpected_leases_map_type_error")
		return
	}

	for attempt := 0; attempt < 3; attempt++ {
		if !s.isLeader(clusterID) {
			break
		}
		if rl.Valid() {
			break
		}
		err := rl.Lease()
		if err == nil {
			break
		}
		s.log.Warningf("Error leasing range: %s: %s, will try again.", rl, err)
	}
}

func (s *Store) releaseRangeLease(rangeID uint64) {
	rlIface, ok := s.leases.Load(rangeID)
	if !ok {
		return
	}
	rl, ok := rlIface.(*rangelease.Lease)
	if !ok {
		alert.UnexpectedEvent("unexpected_leases_map_type_error")
		return
	}
	s.leases.Delete(rangeID)
	if rl.Valid() {
		rl.Release()
	}
}

func (s *Store) GetRange(clusterID uint64) *rfpb.RangeDescriptor {
	return s.lookupRange(clusterID)
}

// We need to implement the Add/RemoveRange interface so that stores opened and
// closed on this node will notify us when their range appears and disappears.
// We'll use this information to drive the range tags we broadcast.
func (s *Store) AddRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {
	s.log.Debugf("Adding range %d: [%q, %q) gen %d", rd.GetRangeId(), rd.GetLeft(), rd.GetRight(), rd.GetGeneration())
	_, loaded := s.replicas.LoadOrStore(rd.GetRangeId(), r)
	if loaded {
		s.log.Warningf("AddRange stomped on another range. Did you forget to call RemoveRange?")
	}

	s.rangeMu.Lock()
	s.openRanges[rd.GetRangeId()] = rd
	s.rangeMu.Unlock()

	if len(rd.GetReplicas()) == 0 {
		s.log.Debugf("range %d has no replicas (yet?)", rd.GetRangeId())
		return
	}

	if rd.GetLeft() == nil && rd.GetRight() == nil {
		s.log.Debugf("range %d has no bounds (yet?)", rd.GetRangeId())
		return
	}

	if rangelease.ContainsMetaRange(rd) {
		// If we own the metarange, use gossip to notify other nodes
		// of that fact.
		buf, err := proto.Marshal(rd)
		if err != nil {
			s.log.Errorf("Error marshaling metarange descriptor: %s", err)
			return
		}
		go s.gossipManager.SetTags(map[string]string{constants.MetaRangeTag: string(buf)})
	}

	// Start goroutines for these so that Adding ranges is quick.
	go s.maybeAcquireRangeLease(rd)
}

func (s *Store) RemoveRange(rd *rfpb.RangeDescriptor, r *replica.Replica) {
	s.log.Debugf("Removing range %d: [%q, %q) gen %d", rd.GetRangeId(), rd.GetLeft(), rd.GetRight(), rd.GetGeneration())
	s.replicas.Delete(rd.GetRangeId())

	s.rangeMu.Lock()
	delete(s.openRanges, rd.GetRangeId())
	s.rangeMu.Unlock()

	if len(rd.GetReplicas()) == 0 {
		s.log.Debugf("range descriptor had no replicas yet")
		return
	}

	// Start goroutines for these so that Removing ranges is quick.
	go s.releaseRangeLease(rd.GetRangeId())
}

// validatedRange verifies that the header is valid and the client is using
// an up-to-date range descriptor. In most cases, it's also necessary to verify
// that a local replica has a range lease for the given range ID which can be
// done by using the LeasedRange function.
func (s *Store) validatedRange(header *rfpb.Header) (*replica.Replica, *rfpb.RangeDescriptor, error) {
	if header == nil {
		return nil, nil, status.FailedPreconditionError("Nil header not allowed")
	}

	s.rangeMu.RLock()
	rd, rangeOK := s.openRanges[header.GetRangeId()]
	s.rangeMu.RUnlock()
	if !rangeOK {
		return nil, nil, status.OutOfRangeErrorf("%s: range %d", constants.RangeNotFoundMsg, header.GetRangeId())
	}

	if len(rd.GetReplicas()) == 0 {
		return nil, nil, status.OutOfRangeErrorf("%s: range had no replicas %d", constants.RangeNotFoundMsg, header.GetRangeId())
	}

	r, err := s.GetReplica(header.GetRangeId())
	if err != nil {
		return nil, nil, err
	}

	// Ensure the header generation matches what we have locally -- if not,
	// force client to go back and re-pull the rangeDescriptor from the meta
	// range.
	if rd.GetGeneration() != header.GetGeneration() {
		if r.IsSplitting() {
			return nil, nil, status.OutOfRangeErrorf("%s: id %d generation: %d requested: %d", constants.RangeSplittingMsg, rd.GetRangeId(), rd.GetGeneration(), header.GetGeneration())
		}
		return nil, nil, status.OutOfRangeErrorf("%s: id %d generation: %d requested: %d", constants.RangeNotCurrentMsg, rd.GetRangeId(), rd.GetGeneration(), header.GetGeneration())
	}

	return r, rd, nil
}

// LeasedRange verifies that the header is valid and the client is using
// an up-to-date range descriptor. It also checks that a local replica owns
// the range lease for the requested range.
func (s *Store) LeasedRange(header *rfpb.Header) (*replica.Replica, error) {
	r, rd, err := s.validatedRange(header)
	if err != nil {
		return nil, err
	}

	if rlIface, ok := s.leases.Load(header.GetRangeId()); ok {
		if rl, ok := rlIface.(*rangelease.Lease); ok {
			if rl.Valid() {
				return r, nil
			}
		} else {
			alert.UnexpectedEvent("unexpected_leases_map_type_error")
		}
	}

	go s.maybeAcquireRangeLease(rd)
	return nil, status.OutOfRangeErrorf("%s: no lease found for range: %d", constants.RangeLeaseInvalidMsg, header.GetRangeId())
}

func (s *Store) ReplicaFactoryFn(clusterID, nodeID uint64) dbsm.IOnDiskStateMachine {
	return replica.New(s.rootDir, clusterID, nodeID, s)
}

func (s *Store) Sender() *sender.Sender {
	return s.sender
}

func (s *Store) ReadFileFromPeer(ctx context.Context, except *rfpb.ReplicaDescriptor, fileRecord *rfpb.FileRecord) (io.ReadCloser, error) {
	fileMetadataKey, err := s.fileStorer.FileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}
	var rc io.ReadCloser
	err = s.sender.Run(ctx, fileMetadataKey, func(c rfspb.ApiClient, h *rfpb.Header) error {
		if h.GetReplica().GetClusterId() == except.GetClusterId() &&
			h.GetReplica().GetNodeId() == except.GetNodeId() {
			return status.OutOfRangeError("except node")
		}
		req := &rfpb.ReadRequest{
			Header:     h,
			FileRecord: fileRecord,
			Offset:     0,
			Limit:      0,
		}
		r, err := s.apiClient.RemoteReader(ctx, c, req)
		if err != nil {
			return err
		}
		rc = r
		return nil
	})
	return rc, err
}

func (s *Store) GetReplica(rangeID uint64) (*replica.Replica, error) {
	// This code will be called by all replicas in a range when
	// doing a split, so we do not check for range leases here.
	rIface, ok := s.replicas.Load(rangeID)
	if !ok {
		return nil, status.OutOfRangeErrorf("%s: replica for range %d not found", constants.RangeNotFoundMsg, rangeID)
	}
	r, ok := rIface.(*replica.Replica)
	if !ok {
		alert.UnexpectedEvent("unexpected_replicas_map_type_error")
		return nil, status.FailedPreconditionError("Replica type-mismatch; this should not happen")
	}
	return r, nil
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
		s.log.Errorf("Got a WaitForClusterReady error: %s", err)
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
		err := s.nodeHost.SyncRemoveData(ctx, req.GetClusterId(), req.GetNodeId())
		if err == dragonboat.ErrClusterNotStopped {
			err = dragonboat.ErrTimeout
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &rfpb.RemoveDataResponse{}, nil
}

func (s *Store) SyncPropose(ctx context.Context, req *rfpb.SyncProposeRequest) (*rfpb.SyncProposeResponse, error) {
	r, _, err := s.validatedRange(req.GetHeader())
	if err != nil {
		return nil, err
	}
	clusterID := req.GetHeader().GetReplica().GetClusterId()
	// Normal Read or Write RPCs to the replica will acquire a lease on the
	// DB which will fail during splitting. SyncPropose, however, proposes
	// a cmd to the raft statemachine, which (with some exceptions), cannot
	// apply during a split. To avoid SyncProposing anything into the raft
	// log during a range split, we check here if the replica is splitting
	// before doing the syncPropose.
	if r.IsSplitting() {
		return nil, status.OutOfRangeErrorf("%s: cluster %d not found", constants.RangeLeaseInvalidMsg, clusterID)
	}

	batchResponse, err := client.SyncProposeLocal(ctx, s.nodeHost, clusterID, req.GetBatch())
	if err != nil {
		if err == dragonboat.ErrClusterNotFound {
			return nil, status.OutOfRangeErrorf("%s: cluster %d not found", constants.RangeLeaseInvalidMsg, clusterID)
		}
		return nil, err
	}
	return &rfpb.SyncProposeResponse{
		Batch: batchResponse,
	}, nil
}

func (s *Store) SyncRead(ctx context.Context, req *rfpb.SyncReadRequest) (*rfpb.SyncReadResponse, error) {
	clusterID := req.GetHeader().GetReplica().GetClusterId()
	batchResponse, err := client.SyncReadLocal(ctx, s.nodeHost, clusterID, req.GetBatch())
	if err != nil {
		if err == dragonboat.ErrClusterNotFound {
			return nil, status.OutOfRangeErrorf("%s: cluster %d not found", constants.RangeLeaseInvalidMsg, clusterID)
		}
		return nil, err
	}

	return &rfpb.SyncReadResponse{
		Batch: batchResponse,
	}, nil
}

func (s *Store) FindMissing(ctx context.Context, req *rfpb.FindMissingRequest) (*rfpb.FindMissingResponse, error) {
	r, err := s.LeasedRange(req.GetHeader())
	if err != nil {
		return nil, err
	}
	missing, err := r.FindMissing(ctx, req.GetHeader(), req.GetFileRecord())
	if err != nil {
		return nil, err
	}
	return &rfpb.FindMissingResponse{
		FileRecord: missing,
	}, nil
}

func (s *Store) GetMulti(ctx context.Context, req *rfpb.GetMultiRequest) (*rfpb.GetMultiResponse, error) {
	r, err := s.LeasedRange(req.GetHeader())
	if err != nil {
		return nil, err
	}
	data, err := r.GetMulti(ctx, req.GetHeader(), req.GetFileRecord())
	if err != nil {
		return nil, err
	}
	return &rfpb.GetMultiResponse{
		Data: data,
	}, nil
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
	r, err := s.LeasedRange(req.GetHeader())
	if err != nil {
		return err
	}

	readCloser, err := r.Reader(stream.Context(), req.GetHeader(), req.GetFileRecord(), req.GetOffset(), req.GetLimit())
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
	var writeCloser interfaces.CommittedMetadataWriteCloser
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if writeCloser == nil {
			// It's expected that clients will directly write bytes
			// to all replicas in a range and then syncpropose a
			// write which confirms the data is in place. For that
			// reason, we don't check if the range is leased here.
			r, _, err := s.validatedRange(req.GetHeader())
			if err != nil {
				return err
			}
			writeCloser, err = r.Writer(stream.Context(), req.GetHeader(), req.GetFileRecord())
			if err != nil {
				return err
			}
			defer writeCloser.Close()
			// Send the client an empty write response as an indicator that we
			// have accepted the write.
			if err := stream.Send(&rfpb.WriteResponse{}); err != nil {
				return err
			}
		}
		n, err := writeCloser.Write(req.Data)
		if err != nil {
			return err
		}
		bytesWritten += int64(n)
		if req.FinishWrite {
			if err := writeCloser.Commit(); err != nil {
				return err
			}
			return stream.Send(&rfpb.WriteResponse{
				CommittedSize: bytesWritten,
			})
		}
	}
	return nil
}

func (s *Store) OnEvent(updateType serf.EventType, event serf.Event) {
	switch updateType {
	case serf.EventQuery:
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
	case serf.EventMemberJoin, serf.EventMemberUpdate:
		memberEvent, _ := event.(serf.MemberEvent)
		for _, member := range memberEvent.Members {
			if metaRangeData, ok := member.Tags[constants.MetaRangeTag]; ok {
				// Whenever the metarange data changes, for any
				// reason, start a goroutine that ensures the
				// node liveness record is up to date.
				if s.metaRangeData != metaRangeData {
					s.metaRangeData = metaRangeData
					// Start this in a goroutine so that
					// other gossip callbacks are not
					// blocked.
					go s.renewNodeLiveness()
				}
			}
		}
	default:
		return
	}
}

func (s *Store) renewNodeLiveness() {
	for {
		if s.liveness.Valid() {
			return
		}
		err := s.liveness.Lease()
		if err == nil {
			return
		}
		s.log.Errorf("Error leasing node liveness record: %s", err)
		time.Sleep(time.Second)
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
				s.log.Debugf("%q ignoring placement query: already have cluster %d", s.nodeHost.ID(), logInfo.ClusterID)
				return
			}
		}
	}

	// Do not respond if this node is over 95% full.
	member := s.gossipManager.LocalMember()
	usageBuf, ok := member.Tags[constants.NodeUsageTag]
	if !ok {
		s.log.Errorf("Ignoring placement query: couldn't determine node usage")
		return
	}
	usage := &rfpb.NodeUsage{}
	if err := prototext.Unmarshal([]byte(usageBuf), usage); err != nil {
		return
	}
	myDiskUsage := float64(usage.GetDiskBytesUsed()) / float64(usage.GetDiskBytesTotal())
	if myDiskUsage > maximumDiskCapacity {
		s.log.Debugf("Ignoring placement query: node is over capacity")
		return
	}

	nodeBuf, err := proto.Marshal(s.MyNodeDescriptor())
	if err != nil {
		return
	}
	if err := query.Respond(nodeBuf); err != nil {
		s.log.Errorf("Error responding to gossip query: %s", err)
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
		membership, err = s.nodeHost.SyncGetClusterMembership(ctx, clusterID)
		if err != nil {
			return err
		}
		// Trick client.RunNodehostFn into running this again if we got a nil
		// membership back
		if membership == nil {
			return status.OutOfRangeErrorf("cluster not ready")
		}
		return nil
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

// createSnapshot saves a snapshot of a replica's pebble database only to a
// snapshot file on the local filesystem. Stored file data is not part of this
// snapshot. An identifier for the snapshot is returned.
func (s *Store) createSnapshot(ctx context.Context, req *rfpb.CreateSnapshotRequest) (*rfpb.CreateSnapshotResponse, error) {
	r, err := s.GetReplica(req.GetHeader().GetRangeId())
	if err != nil {
		return nil, err
	}
	snapFile, err := os.CreateTemp(s.rootDir, "snapfile-*")
	if err != nil {
		return nil, err
	}
	pSnap, err := r.PrepareSnapshot()
	if err != nil {
		return nil, err
	}
	if err := r.SaveSnapshotRange(pSnap, snapFile, req.GetStart(), req.GetEnd()); err != nil {
		return nil, err
	}
	if err := snapFile.Close(); err != nil {
		return nil, err
	}
	return &rfpb.CreateSnapshotResponse{
		SnapId: snapFile.Name(),
	}, nil
}

// loadSnapshot ingests an already created snapshot (see createSnapshot above)
// into a range by sending all records in the snapshot over raft to the new
// range. This may require copying stored file data -- that can be prevented by
// first sending a CopyStoredFilesRequest RAFT command to pre-load the stored
// data onto the replicas in the new range..
func (s *Store) loadSnapshot(ctx context.Context, req *rfpb.LoadSnapshotRequest) (*rfpb.LoadSnapshotResponse, error) {
	r, err := s.GetReplica(req.GetHeader().GetRangeId())
	if err != nil {
		return nil, err
	}
	exists, err := disk.FileExists(ctx, req.GetSnapId())
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, status.FailedPreconditionErrorf("snap with ID %q not found", req.GetSnapId())
	}
	f, err := os.Open(req.GetSnapId())
	if err != nil {
		return nil, status.FailedPreconditionErrorf("error opening snap %q: %s", req.GetSnapId(), err)
	}
	defer f.Close()

	batch := rbuilder.NewBatchBuilder()
	flush := func() error {
		if batch.Size() == 0 {
			return nil
		}
		if err := client.SyncProposeLocalBatchNoRsp(ctx, s.nodeHost, r.ClusterID, batch); err != nil {
			return err
		}
		batch = rbuilder.NewBatchBuilder()
		return nil
	}

	for record := range r.ParseSnapshot(ctx, f) {
		if record.Error != nil {
			return nil, record.Error
		}
		batch = batch.Add(record.PB)
		if batch.Size() > 100 {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}

	return &rfpb.LoadSnapshotResponse{}, nil
}

func casRevert(cas *rfpb.CASRequest) *rfpb.CASRequest {
	return &rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   cas.GetKv().GetKey(),
			Value: cas.GetExpectedValue(),
		},
		ExpectedValue: cas.GetKv().GetValue(),
	}
}

// SplitCluster splits a raft range into two roughly equal parts. For the time
// being, splits are only supported for ranges active on this cluster.
//
// Splits happen in the following way:
//
// 1) The range to be split is locked for splitting. This prevents all
//   further reads and writes.
// 2) A new range is brought up on the same nodes as the range to be split.
// 3) A split point is determined, and data is copied from the range to be split
//   to the newly created range.
// 4) The newly created range is activated (locally).
// 5) The metarange is updated with the new range info, and the split range's
//   new endpoints.
// 6) The split range is unlocked.
func (s *Store) SplitCluster(ctx context.Context, req *rfpb.SplitClusterRequest) (*rfpb.SplitClusterResponse, error) {
	if _, err := s.LeasedRange(req.GetHeader()); err != nil {
		return nil, err
	}

	sourceRange := req.GetRange()
	if sourceRange == nil {
		return nil, status.FailedPreconditionErrorf("no range provided to split: %+v", req)
	}
	if len(sourceRange.GetReplicas()) == 0 {
		return nil, status.FailedPreconditionErrorf("no replicas in range: %+v", sourceRange)
	}
	clusterID := sourceRange.GetReplicas()[0].GetClusterId()

	s.rangeMu.RLock()
	oldLeft, rangeOK := s.openRanges[req.GetHeader().GetRangeId()]
	s.rangeMu.RUnlock()
	if !rangeOK {
		return nil, status.FailedPreconditionErrorf("Range %d not found on this node", req.GetHeader().GetRangeId())
	}

	// start a new cluster in parallel to the existing cluster
	existingMembers, err := s.GetClusterMembership(ctx, clusterID)
	if err != nil {
		return nil, err
	}
	newIDs, err := s.reserveIDsForNewCluster(ctx, len(existingMembers))
	if err != nil {
		return nil, err
	}

	nodeGrpcAddrs := make(map[string]string)
	for _, replica := range existingMembers {
		nhid, _, err := s.registry.ResolveNHID(replica.GetClusterId(), replica.GetNodeId())
		if err != nil {
			return nil, err
		}
		grpcAddr, _, err := s.registry.ResolveGRPC(replica.GetClusterId(), replica.GetNodeId())
		if err != nil {
			return nil, err
		}
		nodeGrpcAddrs[nhid] = grpcAddr
	}

	firstNodeID := newIDs.maxNodeID - uint64(len(existingMembers))
	bootStrapInfo := bringup.MakeBootstrapInfo(newIDs.clusterID, firstNodeID, nodeGrpcAddrs)

	// Bump the local range descriptor so that outstanding reqeusts are rejected.
	oldLeftNewGen := proto.Clone(oldLeft).(*rfpb.RangeDescriptor)
	oldLeftNewGen.Generation += 1

	// Create a new range descriptor for the left range. This will be inserted
	// when the split lock is released, if the split succeeds successfully.
	newLeft := proto.Clone(oldLeftNewGen).(*rfpb.RangeDescriptor)
	newLeft.Generation += 1 // increment rd generation upon split

	// Initially, insert a range descriptor that does not contain replicas.
	// This will keep the range from being marked as "active" in the store
	// or acquiring a range lease (which wouldn't work because the metarange
	// is not yet updated). Just before unlocking the left range, we'll
	// update this range descriptor to include the replicas.
	newRight := &rfpb.RangeDescriptor{
		RangeId:    newIDs.rangeID,
		Generation: newLeft.Generation + 1,
		Left:       oldLeft.GetLeft(),
		Right:      oldLeft.GetRight(),
	}
	newRightBuf, err := proto.Marshal(newRight)
	if err != nil {
		return nil, err
	}
	newRightBatch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: newRightBuf,
		},
	})
	if err := bringup.StartCluster(ctx, s.apiClient, bootStrapInfo, newRightBatch); err != nil {
		return nil, err
	}

	cas, err := casRangeEdit(constants.LocalRangeKey, oldLeft, oldLeftNewGen)
	if err != nil {
		return nil, err
	}
	if err := client.SyncProposeLocalBatchNoRsp(ctx, s.nodeHost, clusterID, rbuilder.NewBatchBuilder().Add(cas)); err != nil {
		return nil, err
	}
	// Lock the cluster that is to be split.
	// TODO(tylerw): add lease renewal goroutine instead of using such a long
	// lease.
	leaseReq := rbuilder.NewBatchBuilder().Add(&rfpb.SplitLeaseRequest{
		CasOnExpiry:     casRevert(cas),
		DurationSeconds: 10,
	})
	if err := client.SyncProposeLocalBatchNoRsp(ctx, s.nodeHost, clusterID, leaseReq); err != nil {
		return nil, err
	}

	// Find an appropriate split point.
	findSplitReq := rbuilder.NewBatchBuilder().Add(&rfpb.FindSplitPointRequest{})
	findSplitBatchRsp, err := client.SyncProposeLocalBatch(ctx, s.nodeHost, clusterID, findSplitReq)
	if err != nil {
		return nil, err
	}
	findSplitRsp, err := findSplitBatchRsp.FindSplitPointResponse(0)
	if err != nil {
		return nil, err
	}

	oldRight := proto.Clone(newRight).(*rfpb.RangeDescriptor)
	newLeft.Right = findSplitRsp.GetSplit()

	// Copy stored data from the old range -> new range by creating a
	// snapshot of the db, copying stored files, then loading the snapshot
	// of the db onto the new replica via RAFT.
	createSnapshotRsp, err := s.createSnapshot(ctx, &rfpb.CreateSnapshotRequest{
		Header: req.GetHeader(),
		Start:  findSplitRsp.GetSplit(),
		End:    oldLeft.GetRight(),
	})
	if err != nil {
		return nil, err
	}

	copyStoredFiles := rbuilder.NewBatchBuilder().Add(&rfpb.CopyStoredFilesRequest{
		SourceRange:   oldLeft,
		TargetRangeId: newIDs.rangeID,
		Start:         findSplitRsp.GetSplit(),
		End:           oldLeft.GetRight(),
	})
	if err := client.SyncProposeLocalBatchNoRsp(ctx, s.nodeHost, clusterID, copyStoredFiles); err != nil {
		return nil, err
	}

	loadSnapReq := &rfpb.LoadSnapshotRequest{
		Header: &rfpb.Header{
			RangeId:    newIDs.rangeID,
			Generation: newRight.Generation,
		},
		SnapId: createSnapshotRsp.GetSnapId(),
	}
	if _, err := s.loadSnapshot(ctx, loadSnapReq); err != nil {
		return nil, err
	}

	// As mentioned above, add the replicas to right range now that it is
	// about to be activated, and set the Left/Right correctly.
	newRight.Left = findSplitRsp.GetSplit()
	newRight.Right = oldLeft.GetRight()
	newRight.Replicas = bootStrapInfo.Replicas
	b := rbuilder.NewBatchBuilder()
	if err := addLocalRangeEdits(oldRight, newRight, b); err != nil {
		return nil, err
	}

	if err := client.SyncProposeLocalBatchNoRsp(ctx, s.nodeHost, newIDs.clusterID, b); err != nil {
		return nil, err
	}

	// Update the metarange to add the new right range.
	if err := s.updateMetarange(ctx, oldLeft, newLeft, newRight); err != nil {
		return nil, err
	}

	// Finally, update this ranges RangeDescriptor to reflect the fact that
	// it is now split, and unlock it.
	b = rbuilder.NewBatchBuilder()
	if err := addLocalRangeEdits(oldLeftNewGen, newLeft, b); err != nil {
		return nil, err
	}
	batchProto, err := b.ToProto()
	if err != nil {
		return nil, err
	}
	releaseReq := rbuilder.NewBatchBuilder().Add(&rfpb.SplitReleaseRequest{
		Batch: batchProto,
	})

	if err := client.SyncProposeLocalBatchNoRsp(ctx, s.nodeHost, clusterID, releaseReq); err != nil {
		return nil, err
	}

	// Delete old data from left range
	deleteReq := rbuilder.NewBatchBuilder().Add(&rfpb.DeleteRangeRequest{
		Start: newLeft.Right,
		End:   oldLeft.Right,
	})
	if err := client.SyncProposeLocalBatchNoRsp(ctx, s.nodeHost, clusterID, deleteReq); err != nil {
		return nil, err
	}

	splitRsp := &rfpb.SplitClusterResponse{
		Left:  newLeft,
		Right: newRight,
	}
	s.log.Infof("SplitResponse: %+v", splitRsp)
	return splitRsp, nil
}

func (s *Store) getConfigChangeID(ctx context.Context, clusterID uint64) (uint64, error) {
	var membership *dragonboat.Membership
	var err error
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		// Get the config change index for this cluster.
		membership, err = s.nodeHost.SyncGetClusterMembership(ctx, clusterID)
		if err != nil {
			return err
		}
		// Trick client.RunNodehostFn into running this again if we got a nil
		// membership back
		if membership == nil {
			return status.OutOfRangeErrorf("cluster not ready")
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if membership == nil {
		return 0, status.InternalErrorf("null cluster membership for cluster: %d", clusterID)
	}
	return membership.ConfigChangeID, nil
}

// AddClusterNode adds a new node to the specified cluster if pre-reqs are met.
// Pre-reqs are:
//  * The request must be valid and contain all information
//  * This node must be a member of the cluster that is being added to
//  * The provided range descriptor must be up to date
func (s *Store) AddClusterNode(ctx context.Context, req *rfpb.AddClusterNodeRequest) (*rfpb.AddClusterNodeResponse, error) {
	// Check the request looks valid.
	if len(req.GetRange().GetReplicas()) == 0 {
		return nil, status.FailedPreconditionErrorf("No replicas in range: %+v", req.GetRange())
	}
	node := req.GetNode()
	if node.GetNhid() == "" || node.GetRaftAddress() == "" || node.GetGrpcAddress() == "" {
		return nil, status.FailedPreconditionErrorf("Incomplete node descriptor: %+v", node)
	}

	// Check this is a range we have and the range descriptor provided is up to date
	s.rangeMu.RLock()
	rd, rangeOK := s.openRanges[req.GetRange().GetRangeId()]
	s.rangeMu.RUnlock()

	if !rangeOK {
		return nil, status.OutOfRangeErrorf("%s: range %d", constants.RangeNotFoundMsg, req.GetRange().GetRangeId())
	}
	if rd.GetGeneration() != req.GetRange().GetGeneration() {
		return nil, status.OutOfRangeErrorf("%s: generation: %d requested: %d", constants.RangeNotCurrentMsg, rd.GetGeneration(), req.GetRange().GetGeneration())
	}

	clusterID := req.GetRange().GetReplicas()[0].GetClusterId()

	// Reserve a new node ID for the node about to be added.
	nodeIDs, err := s.reserveNodeIDs(ctx, 1)
	if err != nil {
		return nil, err
	}
	newNodeID := nodeIDs[0]

	// Get the config change index for this cluster.
	configChangeID, err := s.getConfigChangeID(ctx, clusterID)
	if err != nil {
		return nil, err
	}

	// Gossip the address of the node that is about to be added.
	s.registry.Add(clusterID, newNodeID, node.GetNhid())
	s.registry.AddNode(node.GetNhid(), node.GetRaftAddress(), node.GetGrpcAddress())

	// Propose the config change (this adds the node to the raft cluster).
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestAddNode(ctx, clusterID, newNodeID, node.GetNhid(), configChangeID)
	})
	if err != nil {
		return nil, err
	}

	// Start the cluster on the newly added node.
	c, err := s.apiClient.Get(ctx, node.GetGrpcAddress())
	if err != nil {
		return nil, err
	}
	_, err = c.StartCluster(ctx, &rfpb.StartClusterRequest{
		ClusterId: clusterID,
		NodeId:    newNodeID,
		Join:      true,
	})
	if err != nil {
		return nil, err
	}

	// Finally, update the range descriptor information to reflect the
	// membership of this new node in the range.
	rd, err = s.addReplicaToRangeDescriptor(ctx, clusterID, newNodeID, rd)
	if err != nil {
		return nil, err
	}

	return &rfpb.AddClusterNodeResponse{
		Range: rd,
	}, nil
}

// RemoveClusterNode removes a new node from the specified cluster if pre-reqs are
// met. Pre-reqs are:
//  * The request must be valid and contain all information
//  * This node must be a member of the cluster that is being removed from
//  * The provided range descriptor must be up to date
func (s *Store) RemoveClusterNode(ctx context.Context, req *rfpb.RemoveClusterNodeRequest) (*rfpb.RemoveClusterNodeResponse, error) {
	// Check this is a range we have and the range descriptor provided is up to date
	s.rangeMu.RLock()
	rd, rangeOK := s.openRanges[req.GetRange().GetRangeId()]
	s.rangeMu.RUnlock()

	if !rangeOK {
		return nil, status.OutOfRangeErrorf("%s: range %d", constants.RangeNotFoundMsg, req.GetRange().GetRangeId())
	}
	if rd.GetGeneration() != req.GetRange().GetGeneration() {
		return nil, status.OutOfRangeErrorf("%s: generation: %d requested: %d", constants.RangeNotCurrentMsg, rd.GetGeneration(), req.GetRange().GetGeneration())
	}

	var clusterID, nodeID uint64
	for _, replica := range req.GetRange().GetReplicas() {
		if replica.GetNodeId() == req.GetNodeId() {
			clusterID = replica.GetClusterId()
			nodeID = replica.GetNodeId()
			break
		}
	}
	if clusterID == 0 && nodeID == 0 {
		return nil, status.FailedPreconditionErrorf("No node with id %d found in range: %+v", req.GetNodeId(), req.GetRange())
	}

	configChangeID, err := s.getConfigChangeID(ctx, clusterID)
	if err != nil {
		return nil, err
	}

	// Propose the config change (this removes the node from the raft cluster).
	err = client.RunNodehostFn(ctx, func(ctx context.Context) error {
		return s.nodeHost.SyncRequestDeleteNode(ctx, clusterID, nodeID, configChangeID)
	})
	if err != nil {
		return nil, err
	}

	grpcAddr, _, err := s.registry.ResolveGRPC(clusterID, nodeID)
	if err != nil {
		s.log.Errorf("error resolving grpc addr for c%dn%d: %s", clusterID, nodeID, err)
		return nil, err
	}
	// Remove the data from the now stopped node.
	c, err := s.apiClient.Get(ctx, grpcAddr)
	if err != nil {
		s.log.Errorf("err getting api client: %s", err)
		return nil, err
	}
	_, err = c.RemoveData(ctx, &rfpb.RemoveDataRequest{
		ClusterId: clusterID,
		NodeId:    nodeID,
	})
	if err != nil {
		s.log.Errorf("remove data err: %s", err)
		return nil, err
	}

	// Finally, update the range descriptor information to reflect the
	// new membership of this range without the removed node.
	rd, err = s.removeReplicaFromRangeDescriptor(ctx, clusterID, nodeID, req.GetRange())
	if err != nil {
		return nil, err
	}
	return &rfpb.RemoveClusterNodeResponse{
		Range: rd,
	}, nil
}

func (s *Store) ListCluster(ctx context.Context, req *rfpb.ListClusterRequest) (*rfpb.ListClusterResponse, error) {
	s.rangeMu.RLock()
	openRanges := make([]*rfpb.RangeDescriptor, 0, len(s.openRanges))
	for _, rd := range s.openRanges {
		openRanges = append(openRanges, rd)
	}
	s.rangeMu.RUnlock()

	rsp := &rfpb.ListClusterResponse{
		Node: s.MyNodeDescriptor(),
	}
	for _, rd := range openRanges {
		if req.GetLeasedOnly() {
			header := &rfpb.Header{
				RangeId:    rd.GetRangeId(),
				Generation: rd.GetGeneration(),
			}
			if _, err := s.LeasedRange(header); err != nil {
				continue
			}
		}
		rr := &rfpb.RangeReplica{
			Range: rd,
		}
		if replica, err := s.GetReplica(rd.GetRangeId()); err == nil {
			usage, err := replica.Usage()
			if err == nil {
				rr.ReplicaUsage = usage
			}
		}
		rsp.RangeReplicas = append(rsp.RangeReplicas, rr)
	}
	return rsp, nil
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

type newClusterIDs struct {
	clusterID uint64
	rangeID   uint64
	maxNodeID uint64
}

func (s *Store) reserveIDsForNewCluster(ctx context.Context, numNodes int) (*newClusterIDs, error) {
	metaRangeBatch, err := rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{
		Key:   constants.LastClusterIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
		Delta: uint64(1),
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastNodeIDKey,
		Delta: uint64(numNodes),
	}).ToProto()
	if err != nil {
		return nil, err
	}
	metaRangeRsp, err := s.sender.SyncPropose(ctx, constants.MetaRangePrefix, metaRangeBatch)
	if err != nil {
		return nil, err
	}
	clusterIncrRsp, err := rbuilder.NewBatchResponseFromProto(metaRangeRsp).IncrementResponse(0)
	if err != nil {
		return nil, err
	}
	rangeIDIncrRsp, err := rbuilder.NewBatchResponseFromProto(metaRangeRsp).IncrementResponse(1)
	if err != nil {
		return nil, err
	}
	nodeIDsIncrRsp, err := rbuilder.NewBatchResponseFromProto(metaRangeRsp).IncrementResponse(2)
	if err != nil {
		return nil, err
	}
	ids := &newClusterIDs{
		clusterID: clusterIncrRsp.GetValue(),
		rangeID:   rangeIDIncrRsp.GetValue(),
		maxNodeID: nodeIDsIncrRsp.GetValue(),
	}

	return ids, nil
}

func casRangeEdit(key []byte, old, new *rfpb.RangeDescriptor) (*rfpb.CASRequest, error) {
	newBuf, err := proto.Marshal(new)
	if err != nil {
		return nil, err
	}
	oldBuf, err := proto.Marshal(old)
	if err != nil {
		return nil, err
	}
	return &rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   key,
			Value: newBuf,
		},
		ExpectedValue: oldBuf,
	}, nil
}

func addLocalRangeEdits(oldLeft, newLeft *rfpb.RangeDescriptor, b *rbuilder.BatchBuilder) error {
	cas, err := casRangeEdit(constants.LocalRangeKey, oldLeft, newLeft)
	if err != nil {
		return err
	}
	b = b.Add(cas)
	return nil
}

func addMetaRangeEdits(oldLeft, newLeft, newRight *rfpb.RangeDescriptor, b *rbuilder.BatchBuilder) error {
	newLeftBuf, err := proto.Marshal(newLeft)
	if err != nil {
		return err
	}
	oldLeftBuf, err := proto.Marshal(oldLeft)
	if err != nil {
		return err
	}
	newRightBuf, err := proto.Marshal(newRight)
	if err != nil {
		return err
	}

	// Send a single request that:
	//  - CAS sets the newLeft value to newNewLeftBuf
	//  - inserts the new newRightBuf
	//
	// if the CAS fails, check the existing value
	//  if it's generation is past ours, ignore the error, we're out of date
	//  if the existing value already matches what we were trying to set, we're done.
	//  else return an error
	b = b.Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(newRight.GetRight()),
			Value: newRightBuf,
		},
		ExpectedValue: oldLeftBuf,
	})
	b = b.Add(&rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(newLeft.GetRight()),
			Value: newLeftBuf,
		},
	})
	return nil
}

func (s *Store) updateMetarange(ctx context.Context, oldLeft, left, right *rfpb.RangeDescriptor) error {
	b := rbuilder.NewBatchBuilder()
	if err := addMetaRangeEdits(oldLeft, left, right, b); err != nil {
		return err
	}
	batchProto, err := b.ToProto()
	if err != nil {
		return err
	}
	rsp, err := s.Sender().SyncPropose(ctx, keys.RangeMetaKey(right.GetRight()), batchProto)
	if err != nil {
		return err
	}
	batchRsp := rbuilder.NewBatchResponseFromProto(rsp)
	if _, err := batchRsp.CASResponse(0); err != nil {
		return err // shouldn't happen.
	}
	return nil
}

func (s *Store) updateRangeDescriptor(ctx context.Context, clusterID uint64, old, new *rfpb.RangeDescriptor) error {
	// TODO(tylerw): this should use 2PC.
	oldBuf, err := proto.Marshal(old)
	if err != nil {
		return err
	}
	newBuf, err := proto.Marshal(new)
	if err != nil {
		return err
	}

	metaRangeBatch := rbuilder.NewBatchBuilder()
	localBatch := rbuilder.NewBatchBuilder()
	if err := addLocalRangeEdits(old, new, localBatch); err != nil {
		return err
	}
	metaRangeDescriptorKey := keys.RangeMetaKey(new.GetRight())
	metaRangeCasReq := &rfpb.CASRequest{
		Kv: &rfpb.KV{
			Key:   metaRangeDescriptorKey,
			Value: newBuf,
		},
		ExpectedValue: oldBuf,
	}

	if clusterID == constants.InitialClusterID {
		localBatch.Add(metaRangeCasReq)
	} else {
		metaRangeBatch.Add(metaRangeCasReq)
	}

	localReq, err := localBatch.ToProto()
	if err != nil {
		return err
	}

	// Update the local range.
	localRsp, err := client.SyncProposeLocal(ctx, s.nodeHost, clusterID, localReq)
	if err != nil {
		return err
	}
	_, err = rbuilder.NewBatchResponseFromProto(localRsp).CASResponse(0)
	if err != nil {
		return err
	}
	// If both changes (to local and metarange descriptors) applied to the
	// MetaRange, they were applied in the localReq, and there's nothing
	// remaining to do.
	if metaRangeBatch.Size() == 0 {
		return nil
	}

	// Update the metarange.
	metaReq, err := metaRangeBatch.ToProto()
	if err != nil {
		return err
	}
	metaRangeRsp, err := s.sender.SyncPropose(ctx, metaRangeDescriptorKey, metaReq)
	if err != nil {
		return err
	}
	_, err = rbuilder.NewBatchResponseFromProto(metaRangeRsp).CASResponse(0)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) addReplicaToRangeDescriptor(ctx context.Context, clusterID, nodeID uint64, oldDescriptor *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	newDescriptor.Replicas = append(newDescriptor.Replicas, &rfpb.ReplicaDescriptor{
		ClusterId: clusterID,
		NodeId:    nodeID,
	})
	newDescriptor.Generation = oldDescriptor.GetGeneration() + 1
	if err := s.updateRangeDescriptor(ctx, clusterID, oldDescriptor, newDescriptor); err != nil {
		return nil, err
	}
	return newDescriptor, nil
}

func (s *Store) removeReplicaFromRangeDescriptor(ctx context.Context, clusterID, nodeID uint64, oldDescriptor *rfpb.RangeDescriptor) (*rfpb.RangeDescriptor, error) {
	newDescriptor := proto.Clone(oldDescriptor).(*rfpb.RangeDescriptor)
	for i, replica := range newDescriptor.Replicas {
		if replica.GetNodeId() == nodeID {
			newDescriptor.Replicas = append(newDescriptor.Replicas[:i], newDescriptor.Replicas[i+1:]...)
			break
		}
	}
	newDescriptor.Generation = oldDescriptor.GetGeneration() + 1
	if err := s.updateRangeDescriptor(ctx, clusterID, oldDescriptor, newDescriptor); err != nil {
		return nil, err
	}
	return newDescriptor, nil
}

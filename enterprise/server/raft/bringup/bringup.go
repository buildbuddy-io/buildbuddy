package bringup

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v3"
	"golang.org/x/sync/errgroup"

	raftConfig "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	raftio "github.com/lni/dragonboat/v3/raftio"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

type ClusterStarter struct {
	// The nodehost and function used to create
	// on disk statemachines for new clusters.
	nodeHost             *dragonboat.NodeHost
	createStateMachineFn dbsm.CreateOnDiskStateMachineFunc

	grpcAddr string

	// the set of hosts passed to the Join arg
	listenAddr    string
	join          []string
	gossipManager *gossip.GossipManager

	bootstrapped bool

	doneOnce  sync.Once
	doneSetup chan struct{}
	// a mutex that protects the function sending bringup
	// requests so we only attempt one auto bringup at a time.
	sendingStartRequestsMu sync.Mutex

	log log.Logger
}

func NewClusterStarter(nodeHost *dragonboat.NodeHost, grpcAddr string, createStateMachineFn dbsm.CreateOnDiskStateMachineFunc, gossipMan *gossip.GossipManager) *ClusterStarter {
	cs := &ClusterStarter{}
	cs.Initialize(nodeHost, grpcAddr, createStateMachineFn, gossipMan)
	return cs
}

func (cs *ClusterStarter) Initialize(nodeHost *dragonboat.NodeHost, grpcAddr string, createStateMachineFn dbsm.CreateOnDiskStateMachineFunc, gossipMan *gossip.GossipManager) {
	cs.nodeHost = nodeHost
	cs.createStateMachineFn = createStateMachineFn
	cs.grpcAddr = grpcAddr
	cs.listenAddr = gossipMan.ListenAddr
	cs.join = gossipMan.Join
	cs.gossipManager = gossipMan
	cs.bootstrapped = false
	cs.doneOnce = sync.Once{}
	cs.doneSetup = make(chan struct{})
	cs.log = log.NamedSubLogger(nodeHost.ID())

	sort.Strings(cs.join)

	// Only nodes in the "join" list should register as gossip listeners to
	// be eligible for auto bringup.
	for _, joinAddr := range cs.join {
		if cs.listenAddr == joinAddr {
			gossipMan.AddListener(cs)
			break
		}
	}
}

func (cs *ClusterStarter) LeaderUpdated(info raftio.LeaderInfo) {
	if cs.nodeHost == nil {
		// not initialized yet
		return
	}
	nodeHostInfo := cs.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	if nodeHostInfo == nil {
		return
	}
	for _, clusterInfo := range nodeHostInfo.ClusterInfoList {
		if clusterInfo.ClusterID == info.ClusterID {
			cs.markBringupComplete()
		}
	}
}

func (cs *ClusterStarter) markBringupComplete() {
	cs.doneOnce.Do(func() {
		close(cs.doneSetup)
	})
}

func (cs *ClusterStarter) rejoinConfiguredClusters() (int, error) {
	nodeHostInfo := cs.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	clustersAlreadyConfigured := 0
	for _, logInfo := range nodeHostInfo.LogInfo {
		if cs.nodeHost.HasNodeInfo(logInfo.ClusterID, logInfo.NodeID) {
			cs.log.Infof("Had info for cluster: %d, node: %d.", logInfo.ClusterID, logInfo.NodeID)
			r := raftConfig.GetRaftConfig(logInfo.ClusterID, logInfo.NodeID)
			if err := cs.nodeHost.StartOnDiskCluster(nil, false /*=join*/, cs.createStateMachineFn, r); err != nil {
				return clustersAlreadyConfigured, err
			}
			clustersAlreadyConfigured += 1
			cs.log.Infof("Recreated cluster: %d, node: %d.", logInfo.ClusterID, logInfo.NodeID)
		}
	}
	return clustersAlreadyConfigured, nil
}

func (cs *ClusterStarter) InitializeClusters() error {
	// Attempt to rejoin any configured clusters. This looks at what is
	// stored on disk and attempts to rejoin any clusters that this nodehost
	// was previously a member of. If none were found, we'll attempt auto
	// auto bringup below...
	clustersAlreadyConfigured, err := cs.rejoinConfiguredClusters()
	if err != nil {
		cs.markBringupComplete()
		return err
	}

	// Set a flag indicating if initial cluster bringup still needs to
	// happen. If so, bringup will be triggered when all of the nodes
	// in the Join list have announced themselves to us.
	cs.bootstrapped = clustersAlreadyConfigured > 0
	cs.log.Infof("%d clusters already configured. (bootstrapped: %t)", clustersAlreadyConfigured, cs.bootstrapped)

	autoBringupEligible := false
	for _, joinAddr := range cs.join {
		if cs.listenAddr == joinAddr {
			autoBringupEligible = true
		}
	}

	if cs.bootstrapped || !autoBringupEligible {
		cs.markBringupComplete()
	}

	// Exit early if we are not the first node in the join list.
	if cs.listenAddr != cs.join[0] {
		return nil
	}

	// Start a goroutine that will query the gossip network until
	// all nodes in the join list are online, then will initiate new cluster
	// bringup.
	go func() {
		for !cs.bootstrapped {
			if err := cs.attemptQueryAndBringupOnce(); err != nil {
				cs.log.Debugf("attemptQueryAndBringupOnce did not succeed yet: %s", err)
				continue
			}
			cs.bootstrapped = true
		}
	}()
	return nil
}

func (cs *ClusterStarter) Done() bool {
	done := false
	select {
	case <-cs.doneSetup:
		done = true
	default:
		done = false
	}
	return done
}

// Runs one round of the algorithm that queries gossip for other bringup nodes
// and attempts cluster bringup.
func (cs *ClusterStarter) attemptQueryAndBringupOnce() error {
	rsp, err := cs.gossipManager.Query(constants.AutoBringupEvent, nil, nil)
	if err != nil {
		return err
	}
	bootstrapInfo := make(map[string]string, 0)
	for nodeRsp := range rsp.ResponseCh() {
		if nodeRsp.Payload == nil {
			continue
		}
		br := &rfpb.BringupResponse{}
		if err := proto.Unmarshal(nodeRsp.Payload, br); err != nil {
			continue
		}
		if br.GetNhid() == "" || br.GetGrpcAddress() == "" {
			continue
		}
		bootstrapInfo[br.GetNhid()] = br.GetGrpcAddress()
		if len(bootstrapInfo) == len(cs.join) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return cs.sendStartClusterRequests(ctx, bootstrapInfo)
		}
	}
	return status.FailedPreconditionErrorf("Unable to find other join nodes: %+s", bootstrapInfo)
}

func (cs *ClusterStarter) OnEvent(updateType serf.EventType, event serf.Event) {
	if cs.Done() {
		return
	}
	switch updateType {
	case serf.EventQuery:
		query, _ := event.(*serf.Query)
		if query.Name != constants.AutoBringupEvent {
			return
		}
		rsp := &rfpb.BringupResponse{
			Nhid:        cs.nodeHost.ID(),
			GrpcAddress: cs.grpcAddr,
		}
		buf, err := proto.Marshal(rsp)
		if err != nil {
			return
		}
		if err := query.Respond(buf); err != nil {
			cs.log.Debugf("Error responding to gossip query: %s", err)
		}
	default:
		break
	}
}

type bootstrapNode struct {
	grpcAddress string
	nodeHostID  string
	index       uint64
}

type ClusterBootstrapInfo struct {
	clusterID      uint64
	nodes          []bootstrapNode
	initialMembers map[uint64]string
	Replicas       []*rfpb.ReplicaDescriptor
}

func MakeBootstrapInfo(clusterID uint64, nodeGrpcAddrs map[string]string) *ClusterBootstrapInfo {
	bi := &ClusterBootstrapInfo{
		clusterID:      clusterID,
		initialMembers: make(map[uint64]string, len(nodeGrpcAddrs)),
		nodes:          make([]bootstrapNode, 0, len(nodeGrpcAddrs)),
		Replicas:       make([]*rfpb.ReplicaDescriptor, 0, len(nodeGrpcAddrs)),
	}
	i := uint64(1)
	for nhid, grpcAddress := range nodeGrpcAddrs {
		bi.nodes = append(bi.nodes, bootstrapNode{
			grpcAddress: grpcAddress,
			nodeHostID:  nhid,
			index:       i,
		})
		bi.Replicas = append(bi.Replicas, &rfpb.ReplicaDescriptor{
			ClusterId: clusterID,
			NodeId:    i,
		})
		bi.initialMembers[i] = nhid
		i += 1
	}
	return bi
}

func StartCluster(ctx context.Context, bootstrapInfo *ClusterBootstrapInfo, batch *rbuilder.BatchBuilder) error {
	log.Debugf("StartCluster called with bootstrapInfo: %+v", bootstrapInfo)
	rangeSetupTime := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeSetupTimeKey,
			Value: []byte(fmt.Sprintf("%d", time.Now().UnixNano())),
		},
	})
	batch = batch.Merge(rangeSetupTime)
	batchProto, err := batch.ToProto()
	if err != nil {
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)
	for _, node := range bootstrapInfo.nodes {
		node := node
		eg.Go(func() error {
			conn, err := grpc_client.DialTarget("grpc://" + node.grpcAddress)
			if err != nil {
				return err
			}
			client := rfspb.NewApiClient(conn)
			_, err = client.StartCluster(ctx, &rfpb.StartClusterRequest{
				ClusterId:     bootstrapInfo.clusterID,
				NodeId:        node.index,
				InitialMember: bootstrapInfo.initialMembers,
				Batch:         batchProto,
			})
			if err != nil {
				if !status.IsAlreadyExistsError(err) {
					log.Errorf("Start cluster returned err: %s", err)
					return err
				}
			}
			return nil
		})
	}

	return eg.Wait()
}

func (cs *ClusterStarter) syncProposeLocal(ctx context.Context, clusterID uint64, batch *rbuilder.BatchBuilder) (*rbuilder.BatchResponse, error) {
	batchProto, err := batch.ToProto()
	if err != nil {
		return nil, err
	}
	rsp, err := client.SyncProposeLocal(ctx, cs.nodeHost, clusterID, batchProto)
	return rbuilder.NewBatchResponseFromProto(rsp), nil
}

// This function is called to send RPCs to the other nodes listed in the Join
// list requesting that they bring up initial cluster(s).
func (cs *ClusterStarter) sendStartClusterRequests(ctx context.Context, nodeGrpcAddrs map[string]string) error {
	startingRanges := []*rfpb.RangeDescriptor{
		&rfpb.RangeDescriptor{
			Left:  keys.Key{constants.MinByte},
			Right: keys.MakeKey([]byte("l")),
		},
		&rfpb.RangeDescriptor{
			Left:  keys.MakeKey([]byte("l")),
			Right: keys.Key{constants.MaxByte},
		},
	}
	clusterID := uint64(constants.InitialClusterID)
	rangeID := uint64(constants.InitialRangeID)

	for _, rangeDescriptor := range startingRanges {
		bootstrapInfo := MakeBootstrapInfo(clusterID, nodeGrpcAddrs)
		rangeDescriptor.Replicas = bootstrapInfo.Replicas
		rangeDescriptor.RangeId = rangeID
		rdBuf, err := proto.Marshal(rangeDescriptor)
		if err != nil {
			return err
		}

		batch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   constants.LocalRangeKey,
				Value: rdBuf,
			},
		})

		// If this is the first range, we need to write some special
		// information to the metarange on startup. Do that here.
		if clusterID == uint64(constants.InitialClusterID) {
			batch = batch.Add(&rfpb.IncrementRequest{
				Key:   constants.LastClusterIDKey,
				Delta: 1,
			})
			batch = batch.Add(&rfpb.IncrementRequest{
				Key:   constants.LastRangeIDKey,
				Delta: 1,
			})
			batch = batch.Add(&rfpb.DirectWriteRequest{
				Kv: &rfpb.KV{
					Key:   keys.RangeMetaKey(rangeDescriptor.GetRight()),
					Value: rdBuf,
				},
			})
			batch = batch.Add(&rfpb.DirectWriteRequest{
				Kv: &rfpb.KV{
					Key:   constants.InitClusterSetupTimeKey,
					Value: []byte(fmt.Sprintf("%d", time.Now().UnixNano())),
				},
			})
		}
		if err := StartCluster(ctx, bootstrapInfo, batch); err != nil {
			return err
		}
		log.Printf("StartCluster started a new cluster successfully!")

		// Increment clusterID and rangeID before creating the next cluster.
		batch = rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{
			Key:   constants.LastClusterIDKey,
			Delta: 1,
		})
		batch = batch.Add(&rfpb.IncrementRequest{
			Key:   constants.LastRangeIDKey,
			Delta: 1,
		})
		if clusterID != uint64(constants.InitialClusterID) {
			// if this was not the first cluster, aka the metarange cluster,
			// then insert the range descriptor of the just created cluster
			// into the metarange.
			batch = batch.Add(&rfpb.DirectWriteRequest{
				Kv: &rfpb.KV{
					Key:   keys.RangeMetaKey(rangeDescriptor.GetRight()),
					Value: rdBuf,
				},
			})
		}
		rsp, err := cs.syncProposeLocal(ctx, constants.InitialClusterID, batch)
		if err != nil {
			return err
		}
		clusterIncrResponse, err := rsp.IncrementResponse(0)
		if err != nil {
			return err
		}
		clusterID = clusterIncrResponse.GetValue()
		log.Printf("Cluster ID is now: %d", clusterID)

		rangeIncrResponse, err := rsp.IncrementResponse(1)
		if err != nil {
			return err
		}
		rangeID = rangeIncrResponse.GetValue()
		log.Printf("Range ID is now: %d", rangeID)
	}
	return nil
}

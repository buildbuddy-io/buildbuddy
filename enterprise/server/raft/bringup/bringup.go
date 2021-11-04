package bringup

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v3"

	raftConfig "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

type ClusterStarter struct {
	// The nodehost and function used to create
	// on disk statemachines for new clusters.
	nodeHost             *dragonboat.NodeHost
	createStateMachineFn dbsm.CreateOnDiskStateMachineFunc

	// the set of hosts passed to the Join arg
	listenAddr string
	join       []string

	// map of grpc_address => node host ID
	bootstrapInfo map[string]string
	bootstrapped  bool
}

func NewClusterStarter(nodeHost *dragonboat.NodeHost, createStateMachineFn dbsm.CreateOnDiskStateMachineFunc, listenAddr string, join []string) *ClusterStarter {
	cs := &ClusterStarter{
		nodeHost:             nodeHost,
		createStateMachineFn: createStateMachineFn,
		listenAddr:           listenAddr,
		join:                 join,
		bootstrapInfo:        make(map[string]string, 0),
		bootstrapped:         false,
	}
	return cs
}

func (cs *ClusterStarter) rejoinConfiguredClusters() (int, error) {
	nodeHostInfo := cs.nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	clustersAlreadyConfigured := 0
	for _, logInfo := range nodeHostInfo.LogInfo {
		if cs.nodeHost.HasNodeInfo(logInfo.ClusterID, logInfo.NodeID) {
			log.Printf("NodeHost %q had info for cluster: %d, node: %d.", cs.nodeHost.ID(), logInfo.ClusterID, logInfo.NodeID)
			r := raftConfig.GetRaftConfig(logInfo.ClusterID, logInfo.NodeID)
			if err := cs.nodeHost.StartOnDiskCluster(nil, false /*=join*/, cs.createStateMachineFn, r); err != nil {
				return clustersAlreadyConfigured, err
			}
			clustersAlreadyConfigured += 1
			log.Printf("NodeHost %q recreated cluster: %d, node: %d.", cs.nodeHost.ID(), logInfo.ClusterID, logInfo.NodeID)
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
		return err
	}

	// Set a flag indicating if initial cluster bringup still needs to
	// happen. If so, bringup will be triggered when all of the nodes
	// in the Join list have announced themselves to us.
	cs.bootstrapped = clustersAlreadyConfigured > 0

	log.Printf("%d clusters already configured. bootstrapped: %t", clustersAlreadyConfigured, cs.bootstrapped)
	return nil
}

// OnEvent is called when a node joins, leaves, or is updated. This
// function is effectively a no-op if this node is not node[0] in the join list
// (the one responsible for orchestrating the initial cluster bringup) or if
// this node was already bootstrapped from stored cluster state on disk.
func (cs *ClusterStarter) OnEvent(updateType serf.EventType, event serf.Event) {
	if cs.bootstrapped {
		return
	}
	switch updateType {
	case serf.EventMemberJoin, serf.EventMemberUpdate:
		memberEvent, _ := event.(serf.MemberEvent)
		for _, member := range memberEvent.Members {
			address := fmt.Sprintf("%s:%d", member.Addr.String(), member.Port)
			for _, joinPeer := range cs.join {
				if joinPeer == address {
					nhid := member.Tags[constants.NodeHostIDTag]
					grpcAddress := member.Tags[constants.GRPCAddressTag]
					if grpcAddress != "" && nhid != "" {
						cs.bootstrapInfo[grpcAddress] = nhid
					}
				}
			}
		}
	default:
		break
	}

	log.Printf("cs.bootstrapInfo is now: %+v", cs.bootstrapInfo)
	if len(cs.bootstrapInfo) == len(cs.join) {
		if err := cs.sendStartClusterRequests(); err == nil {
			cs.bootstrapped = true
		} else {
			log.Printf("Error setting up initial cluster: %s", err)
		}
	}
}

type bootstrapNode struct {
	grpcAddress string
	nodeHostID  string
	index       uint64
}

// This function is called to send RPCs to the other nodes listed in the Join
// list requesting that they bringup an initial cluster.
func (cs *ClusterStarter) sendStartClusterRequests() error {
	if cs.listenAddr != cs.join[0] {
		return nil
	}

	i := uint64(1)
	initialMembers := make(map[uint64]string, 0)
	nodes := make([]bootstrapNode, 0, len(cs.bootstrapInfo))
	for grpcAddress, nhid := range cs.bootstrapInfo {
		nodes = append(nodes, bootstrapNode{
			grpcAddress: grpcAddress,
			nodeHostID:  nhid,
			index:       i,
		})
		initialMembers[i] = nhid
		i += 1
	}

	log.Debugf("I am %q sending cluster bringup requests to %+v", cs.listenAddr, nodes)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, node := range nodes {
		conn, err := grpc_client.DialTarget("grpc://" + node.grpcAddress)
		if err != nil {
			return err
		}
		client := rfspb.NewApiClient(conn)
		_, err = client.StartCluster(ctx, &rfpb.StartClusterRequest{
			ClusterId:     constants.InitialClusterID,
			NodeId:        node.index,
			InitialMember: initialMembers,
		})
		if err != nil {
			log.Errorf("Start cluster returned err: %s", err)
			return err
		}
	}

	setupTimeBuf, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.InitClusterSetupTimeKey,
			Value: []byte(fmt.Sprintf("%d", time.Now().UnixNano())),
		},
	}).ToBuf()
	if err != nil {
		return err
	}

	proposedFirstVal := false
	for !proposedFirstVal {
		select {
		case <-ctx.Done():
			return err
		case <-time.After(100 * time.Millisecond):
			sesh := cs.nodeHost.GetNoOPSession(constants.InitialClusterID)
			_, err = cs.nodeHost.SyncPropose(ctx, sesh, setupTimeBuf)
			if err == nil {
				proposedFirstVal = true
			}
		}
	}

	return cs.setupInitialMetadata(ctx, constants.InitialClusterID)
}

func (cs *ClusterStarter) setupInitialMetadata(ctx context.Context, clusterID uint64) error {
	// Set the last cluster ID to 1
	sesh := cs.nodeHost.GetNoOPSession(constants.InitialClusterID)

	reqBuf, err := rbuilder.NewBatchBuilder().Add(&rfpb.IncrementRequest{
		Key:   constants.LastClusterIDKey,
		Delta: 1,
	}).ToBuf()

	if err != nil {
		return err
	}

	if _, err := cs.nodeHost.SyncPropose(ctx, sesh, reqBuf); err != nil {
		return err
	}

	// Set the range of this first cluster to [minbyte, maxbyte)
	rangeDescriptor := &rfpb.RangeDescriptor{
		Left:  keys.Key{constants.MinByte},
		Right: keys.Key{constants.MaxByte},
	}
	membership, err := cs.nodeHost.GetClusterMembership(ctx, constants.InitialClusterID)
	if err != nil {
		return err
	}
	for nodeID, _ := range membership.Nodes {
		rangeDescriptor.Replicas = append(rangeDescriptor.Replicas, &rfpb.ReplicaDescriptor{
			ClusterId: constants.InitialClusterID,
			NodeId:    nodeID,
		})
	}
	rdBuf, err := proto.Marshal(rangeDescriptor)
	if err != nil {
		return err
	}
	// This entry goes to meta1.
	batch := rbuilder.NewBatchBuilder()
	batch = batch.Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(rangeDescriptor.GetRight()),
			Value: rdBuf,
		},
	})
	batch = batch.Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: rdBuf,
		},
	})
	reqBuf, err = batch.ToBuf()
	if err != nil {
		return err
	}

	if _, err := cs.nodeHost.SyncPropose(ctx, sesh, reqBuf); err != nil {
		return err
	}
	return nil
}

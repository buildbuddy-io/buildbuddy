package bringup

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v4"
	"golang.org/x/sync/errgroup"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

type IStore interface {
	ConfiguredClusters() int
	APIClient() *client.APIClient
	NodeHost() *dragonboat.NodeHost
}

type ClusterStarter struct {
	store    IStore
	grpcAddr string

	// the set of hosts passed to the Join arg
	listenAddr    string
	join          []string
	gossipManager interfaces.GossipService

	bootstrapped bool

	doneOnce  sync.Once
	doneSetup chan struct{}
	// a mutex that protects the function sending bringup
	// requests so we only attempt one auto bringup at a time.
	sendingStartRequestsMu sync.Mutex

	log log.Logger
}

func New(grpcAddr string, gossipMan interfaces.GossipService, store IStore) *ClusterStarter {
	joinList := gossipMan.JoinList()
	cs := &ClusterStarter{
		store:         store,
		grpcAddr:      grpcAddr,
		listenAddr:    gossipMan.ListenAddr(),
		join:          joinList,
		gossipManager: gossipMan,
		bootstrapped:  false,
		doneOnce:      sync.Once{},
		doneSetup:     make(chan struct{}),
		log:           log.NamedSubLogger(store.NodeHost().ID()),
	}
	sort.Strings(cs.join)

	// Only nodes in the "join" list should register as gossip listeners to
	// be eligible for auto bringup.
	for _, joinAddr := range cs.join {
		if inJoinList, err := cs.matchesListenAddress(joinAddr); err == nil && inJoinList {
			gossipMan.AddListener(cs)
			break
		}
	}

	return cs
}

func (cs *ClusterStarter) markBringupComplete() {
	cs.doneOnce.Do(func() {
		cs.log.Info("Bringup is complete!")
		close(cs.doneSetup)
	})
}

func getMyIPs() ([]net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var myIPs []net.IP

	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			myIPs = append(myIPs, ip)
		}
	}
	return myIPs, nil
}

func (cs *ClusterStarter) matchesListenAddress(hostAndPort string) (bool, error) {
	// This is the port we're listening on.
	_, listenPort, err := net.SplitHostPort(cs.listenAddr)
	if err != nil {
		return false, err
	}

	host, port, err := net.SplitHostPort(hostAndPort)
	if err != nil {
		return false, err
	}

	if port != listenPort {
		return false, nil
	}

	addrs, err := net.LookupIP(host)
	if err != nil {
		return false, err
	}
	myIPs, err := getMyIPs()
	if err != nil {
		return false, err
	}
	for _, addr := range addrs {
		for _, myIP := range myIPs {
			if addr.String() == myIP.String() {
				return true, nil
			}
		}
	}
	return false, nil
}

func (cs *ClusterStarter) InitializeClusters() error {
	// Set a flag indicating if initial cluster bringup still needs to
	// happen. If so, bringup will be triggered when all of the nodes
	// in the Join list have announced themselves to us.
	cs.bootstrapped = cs.store.ConfiguredClusters() > 0
	cs.log.Infof("%d clusters already configured. (bootstrapped: %t)", cs.store.ConfiguredClusters(), cs.bootstrapped)

	isBringupCoordinator, err := cs.matchesListenAddress(cs.join[0])
	if err != nil {
		return err
	}
	if cs.bootstrapped || !isBringupCoordinator {
		cs.markBringupComplete()
		return nil
	}

	// Start a goroutine that will query the gossip network until
	// all nodes in the join list are online, then will initiate new cluster
	// bringup.
	go func() {
		for !cs.bootstrapped {
			cs.log.Debugf("not bootstrapped yet; calling attemptQueryAndBringupOnce")
			if err := cs.attemptQueryAndBringupOnce(); err != nil {
				cs.log.Debugf("attemptQueryAndBringupOnce did not succeed yet: %s", err)
				continue
			}
			cs.bootstrapped = true
			cs.markBringupComplete()
			cs.log.Debugf("bootstrapping complete")
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
	nodeHost := cs.store.NodeHost()
	apiClient := cs.store.APIClient()

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
			err = SendStartShardRequests(ctx, nodeHost, apiClient, bootstrapInfo)
			cancel()
			return err
		}
	}
	return status.FailedPreconditionErrorf("Unable to find other join nodes: %+s", bootstrapInfo)
}

func (cs *ClusterStarter) OnEvent(updateType serf.EventType, event serf.Event) {
	switch updateType {
	case serf.EventQuery:
		query, _ := event.(*serf.Query)
		if query.Name != constants.AutoBringupEvent {
			return
		}
		rsp := &rfpb.BringupResponse{
			Nhid:        cs.store.NodeHost().ID(),
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
	index       uint64
}

type ClusterBootstrapInfo struct {
	shardID        uint64
	nodes          []bootstrapNode
	initialMembers map[uint64]string
	Replicas       []*rfpb.ReplicaDescriptor
}

func MakeBootstrapInfo(shardID, firstReplicaID uint64, nodeGrpcAddrs map[string]string) *ClusterBootstrapInfo {
	bi := &ClusterBootstrapInfo{
		shardID:        shardID,
		initialMembers: make(map[uint64]string, len(nodeGrpcAddrs)),
		nodes:          make([]bootstrapNode, 0, len(nodeGrpcAddrs)),
		Replicas:       make([]*rfpb.ReplicaDescriptor, 0, len(nodeGrpcAddrs)),
	}
	i := uint64(1)
	for nhid, grpcAddress := range nodeGrpcAddrs {
		replicaID := i - 1 + firstReplicaID
		bi.nodes = append(bi.nodes, bootstrapNode{
			grpcAddress: grpcAddress,
			index:       replicaID,
		})
		bi.Replicas = append(bi.Replicas, &rfpb.ReplicaDescriptor{
			ShardId:   shardID,
			ReplicaId: replicaID,
			Nhid:      proto.String(nhid),
		})
		bi.initialMembers[replicaID] = nhid
		i += 1
	}
	return bi
}

func StartShard(ctx context.Context, apiClient *client.APIClient, bootstrapInfo *ClusterBootstrapInfo, batch *rbuilder.BatchBuilder) error {
	log.Debugf("StartShard called with bootstrapInfo: %+v", bootstrapInfo)
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
			apiClient, err := apiClient.Get(ctx, node.grpcAddress)
			if err != nil {
				return err
			}
			_, err = apiClient.StartShard(ctx, &rfpb.StartShardRequest{
				ShardId:       bootstrapInfo.shardID,
				ReplicaId:     node.index,
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

// This function is called to send RPCs to the other nodes listed in the Join
// list requesting that they bring up initial cluster(s).
func SendStartShardRequests(ctx context.Context, nodeHost *dragonboat.NodeHost, apiClient *client.APIClient, nodeGrpcAddrs map[string]string) error {
	startingRanges := []*rfpb.RangeDescriptor{
		&rfpb.RangeDescriptor{
			Start:      keys.MinByte,
			End:        keys.Key{constants.UnsplittableMaxByte},
			Generation: 1,
		},
		&rfpb.RangeDescriptor{
			Start:      keys.Key{constants.UnsplittableMaxByte},
			End:        keys.MaxByte,
			Generation: 1,
		},
	}
	return SendStartShardRequestsWithRanges(ctx, nodeHost, apiClient, nodeGrpcAddrs, startingRanges)
}

func SendStartShardRequestsWithRanges(ctx context.Context, nodeHost *dragonboat.NodeHost, apiClient *client.APIClient, nodeGrpcAddrs map[string]string, startingRanges []*rfpb.RangeDescriptor) error {
	shardID := uint64(constants.InitialShardID)
	replicaID := uint64(constants.InitialReplicaID)
	rangeID := uint64(constants.InitialRangeID)

	for _, rangeDescriptor := range startingRanges {
		bootstrapInfo := MakeBootstrapInfo(shardID, replicaID, nodeGrpcAddrs)
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
		batch = batch.Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   constants.ClusterSetupTimeKey,
				Value: []byte(fmt.Sprintf("%d", time.Now().UnixNano())),
			},
		})

		// If this is the first range, we need to write some special
		// information to the metarange on startup. Do that here.
		if shardID == uint64(constants.InitialShardID) {
			batch = batch.Add(&rfpb.IncrementRequest{
				Key:   constants.LastShardIDKey,
				Delta: uint64(constants.InitialShardID),
			})
			batch = batch.Add(&rfpb.IncrementRequest{
				Key:   constants.LastReplicaIDKey,
				Delta: uint64(constants.InitialReplicaID),
			})
			batch = batch.Add(&rfpb.IncrementRequest{
				Key:   constants.LastRangeIDKey,
				Delta: uint64(constants.InitialRangeID),
			})
			batch = batch.Add(&rfpb.DirectWriteRequest{
				Kv: &rfpb.KV{
					Key:   keys.RangeMetaKey(rangeDescriptor.GetEnd()),
					Value: rdBuf,
				},
			})
		}
		log.Debugf("Attempting to start cluster %d on: %+v", shardID, bootstrapInfo)
		if err := StartShard(ctx, apiClient, bootstrapInfo, batch); err != nil {
			return err
		}
		log.Debugf("Cluster %d started on: %+v", shardID, bootstrapInfo)

		// Increment shardID, replicaID and rangeID before creating the next cluster.
		metaRangeBatch := rbuilder.NewBatchBuilder()
		metaRangeBatch = metaRangeBatch.Add(&rfpb.IncrementRequest{
			Key:   constants.LastShardIDKey,
			Delta: 1,
		})
		metaRangeBatch = metaRangeBatch.Add(&rfpb.IncrementRequest{
			Key:   constants.LastReplicaIDKey,
			Delta: uint64(len(bootstrapInfo.Replicas)),
		})
		metaRangeBatch = metaRangeBatch.Add(&rfpb.IncrementRequest{
			Key:   constants.LastRangeIDKey,
			Delta: 1,
		})
		metaRangeBatch = metaRangeBatch.Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   keys.RangeMetaKey(rangeDescriptor.GetEnd()),
				Value: rdBuf,
			},
		})

		batchProto, err := metaRangeBatch.ToProto()
		if err != nil {
			return err
		}
		batchRsp, err := client.SyncProposeLocal(ctx, nodeHost, constants.InitialShardID, batchProto)
		if err != nil {
			return err
		}
		rsp := rbuilder.NewBatchResponseFromProto(batchRsp)

		clusterIncrResponse, err := rsp.IncrementResponse(0)
		if err != nil {
			return err
		}
		shardID = clusterIncrResponse.GetValue()
		nodeIncrResponse, err := rsp.IncrementResponse(1)
		if err != nil {
			return err
		}
		replicaID = nodeIncrResponse.GetValue()
		rangeIncrResponse, err := rsp.IncrementResponse(2)
		if err != nil {
			return err
		}
		rangeID = rangeIncrResponse.GetValue()
	}

	return nil
}

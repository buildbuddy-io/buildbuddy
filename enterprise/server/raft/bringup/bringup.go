package bringup

import (
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"math/big"
	"net"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/txn"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
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
	Sender() *sender.Sender
	TxnCoordinator() *txn.Coordinator
	ReserveRangeIDs(ctx context.Context, n int) ([]uint64, error)
}

type ClusterStarter struct {
	store    IStore
	session  *client.Session
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

	defaultPartition disk.Partition
	rangesToCreate   []*rfpb.RangeDescriptor
}

func New(grpcAddr string, gossipMan interfaces.GossipService, store IStore, partitions []disk.Partition) (*ClusterStarter, error) {
	joinList := gossipMan.JoinList()
	var defaultPartition disk.Partition
	for _, partition := range partitions {
		if partition.ID == constants.DefaultPartitionID {
			defaultPartition = partition
			break
		}
	}
	if len(defaultPartition.ID) == 0 {
		return nil, status.InvalidArgumentError("default partition is not present.")
	}
	cs := &ClusterStarter{
		store:            store,
		session:          client.NewSession(),
		grpcAddr:         grpcAddr,
		listenAddr:       gossipMan.ListenAddr(),
		join:             joinList,
		gossipManager:    gossipMan,
		bootstrapped:     false,
		doneOnce:         sync.Once{},
		doneSetup:        make(chan struct{}),
		log:              log.NamedSubLogger(store.NodeHost().ID()),
		defaultPartition: defaultPartition,
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

	return cs, nil
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
			if err := InitializeShardsForMetaRange(ctx, cs.session, cs.store, bootstrapInfo); err != nil {
				cancel()
				return err
			}
			err = InitializeShardsForPartition(ctx, cs.store, bootstrapInfo, cs.defaultPartition)
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
	rangeID        uint64
	nodes          []bootstrapNode
	initialMembers map[uint64]string
	Replicas       []*rfpb.ReplicaDescriptor
}

func (cbi ClusterBootstrapInfo) InitialMembersForTesting() map[uint64]string {
	return cbi.initialMembers
}

func MakeBootstrapInfo(rangeID, firstReplicaID uint64, nodeGrpcAddrs map[string]string) *ClusterBootstrapInfo {
	bi := &ClusterBootstrapInfo{
		rangeID:        rangeID,
		initialMembers: make(map[uint64]string, len(nodeGrpcAddrs)),
		nodes:          make([]bootstrapNode, 0, len(nodeGrpcAddrs)),
		Replicas:       make([]*rfpb.ReplicaDescriptor, 0, len(nodeGrpcAddrs)),
	}
	i := uint64(1)
	// sort nodeGrpcAddrs
	nhids := slices.Sorted(maps.Keys(nodeGrpcAddrs))
	for _, nhid := range nhids {
		grpcAddress := nodeGrpcAddrs[nhid]
		replicaID := i - 1 + firstReplicaID
		bi.nodes = append(bi.nodes, bootstrapNode{
			grpcAddress: grpcAddress,
			index:       replicaID,
		})
		bi.Replicas = append(bi.Replicas, &rfpb.ReplicaDescriptor{
			RangeId:   rangeID,
			ReplicaId: replicaID,
			Nhid:      proto.String(nhid),
		})
		bi.initialMembers[replicaID] = nhid
		i += 1
	}
	return bi
}

func startShard(ctx context.Context, store IStore, bootstrapInfo *ClusterBootstrapInfo) error {
	log.Debugf("StartShard called with bootstrapInfo: %+v", bootstrapInfo)

	eg, egCtx := errgroup.WithContext(ctx)
	for _, node := range bootstrapInfo.nodes {
		node := node
		eg.Go(func() error {
			apiClient, err := store.APIClient().Get(egCtx, node.grpcAddress)
			if err != nil {
				return err
			}
			_, err = apiClient.StartShard(egCtx, &rfpb.StartShardRequest{
				RangeId:       bootstrapInfo.rangeID,
				ReplicaId:     node.index,
				InitialMember: bootstrapInfo.initialMembers,
			})
			if err != nil {
				if !status.IsAlreadyExistsError(err) {
					err := status.WrapErrorf(err, "failed to start cluster c%dn%d on %s", bootstrapInfo.rangeID, node.index, node.grpcAddress)
					log.Error(err.Error())
					return err
				}
			}
			return nil
		})
	}

	return eg.Wait()
}

var maxHashAsBigInt = big.NewInt(0).SetBytes([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

var minHashAsBigInt = big.NewInt(0).SetBytes([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

// evenlyDividePartitionIntoRanges takes a splitConfig (partition ID and number
// of ranges) and returns a slice of range descriptors that completely cover that
// partition:
// [PT<partition_name>/,
// PT<partition_name>/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\xff)
func evenlyDividePartitionIntoRanges(partition disk.Partition) ([]*rfpb.RangeDescriptor, error) {
	numRanges := partition.NumRanges
	if numRanges <= 0 {
		return nil, status.InvalidArgumentErrorf("NumRanges must be positive, got %d", numRanges)
	}

	delta := new(big.Int).Sub(maxHashAsBigInt, minHashAsBigInt)
	interval := new(big.Int).Div(delta, big.NewInt(int64(numRanges)))
	if interval.Sign() != 1 {
		return nil, status.FailedPreconditionErrorf("delta (%s) < count (%d)", delta, numRanges)
	}
	ranges := make([]*rfpb.RangeDescriptor, 0)

	l := minHashAsBigInt
	// Append all ranges from start --> first split, first split -
	// -> 2nd split... nth split.
	for i := 0; i < numRanges-1; i++ {
		r := new(big.Int).Add(l, interval)
		ranges = append(ranges, &rfpb.RangeDescriptor{
			Start:      []byte(filestore.PartitionDirectoryPrefix + partition.ID + "/" + hex.EncodeToString(l.Bytes())),
			End:        []byte(filestore.PartitionDirectoryPrefix + partition.ID + "/" + hex.EncodeToString(r.Bytes())),
			Generation: 1,
		})
		l = r
	}
	// Append a final range from nth split --> end.
	ranges = append(ranges, &rfpb.RangeDescriptor{
		Start:      []byte(filestore.PartitionDirectoryPrefix + partition.ID + "/" + hex.EncodeToString(l.Bytes())),
		End:        keys.MakeKey([]byte(filestore.PartitionDirectoryPrefix+partition.ID+"/"+hex.EncodeToString(maxHashAsBigInt.Bytes())), keys.MaxByte),
		Generation: 1,
	})
	return ranges, nil
}

// InitializeShardsForMetaRange starts the shards for meta range and also writes
// the meta range descriptor into the range.
func InitializeShardsForMetaRange(ctx context.Context, session *client.Session, store IStore, nodeGrpcAddrs map[string]string) error {
	rangeID := uint64(constants.MetaRangeID)
	bootstrapInfo := MakeBootstrapInfo(rangeID, constants.InitialReplicaID, nodeGrpcAddrs)
	mrd := &rfpb.RangeDescriptor{
		RangeId:    rangeID,
		Start:      constants.MetaRangePrefix,
		End:        keys.Key{constants.UnsplittableMaxByte},
		Generation: 1,
		Replicas:   bootstrapInfo.Replicas,
	}

	if err := startShard(ctx, store, bootstrapInfo); err != nil {
		return err
	}
	log.Debugf("Cluster %d started on: %+v", rangeID, bootstrapInfo)
	rdBuf, err := proto.Marshal(mrd)
	if err != nil {
		return err
	}
	batch := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.LocalRangeKey,
			Value: rdBuf,
		},
	}).Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   constants.ClusterSetupTimeKey,
			Value: []byte(fmt.Sprintf("%d", time.Now().UnixNano())),
		},
	}).Add(&rfpb.DirectWriteRequest{
		Kv: &rfpb.KV{
			Key:   keys.RangeMetaKey(mrd.GetEnd()),
			Value: rdBuf,
		},
	}).Add(&rfpb.IncrementRequest{
		Key:   constants.LastRangeIDKey,
		Delta: 1,
	}).Add(&rfpb.IncrementRequest{
		Key:   keys.MakeKey(constants.LastReplicaIDKeyPrefix, []byte(fmt.Sprintf("%d", rangeID))),
		Delta: uint64(len(bootstrapInfo.Replicas)),
	})
	batchProto, err := batch.ToProto()
	if err != nil {
		return err
	}
	rsp, err := session.SyncProposeLocal(ctx, store.NodeHost(), constants.MetaRangeID, batchProto)
	if err != nil {
		return err
	}
	err = rbuilder.NewBatchResponseFromProto(rsp).AnyError()
	if err != nil {
		return err
	}
	return nil
}

// InitializeShardsForPartition starts the shards for a partition and also writes
// the range descriptor into both local range and meta range.
// Note: since we are writing the range descriptors in a transaction, it might
// have performance issues when we try to initialize tons of range descriptors at
// once.
func InitializeShardsForPartition(ctx context.Context, store IStore, nodeGrpcAddrs map[string]string, partition disk.Partition) error {
	ranges, err := evenlyDividePartitionIntoRanges(partition)
	if err != nil {
		return err
	}

	log.Info("The following partitions will be configured:")
	for i, rd := range ranges {
		log.Infof("%d [%q, %q)", i, rd.GetStart(), rd.GetEnd())
	}
	retrier := retry.DefaultWithContext(ctx)
	var mrd *rfpb.RangeDescriptor
	for retrier.Next() {
		mrd = store.Sender().GetMetaRangeDescriptor()
		if mrd != nil {
			break
		}
		log.CtxWarning(ctx, "RangeCache did not have meta range yet")
	}

	rangeIDs, err := store.ReserveRangeIDs(ctx, len(ranges))
	if err != nil {
		return err
	}

	eg := &errgroup.Group{}
	tx := rbuilder.NewTxn()
	metaRangeBatch := rbuilder.NewBatchBuilder()
	for i, rd := range ranges {
		rangeID := rangeIDs[i]
		bootstrapInfo := MakeBootstrapInfo(rangeID, uint64(constants.InitialReplicaID), nodeGrpcAddrs)
		rd.Replicas = bootstrapInfo.Replicas
		rd.RangeId = rangeID

		eg.Go(func() error {
			if err := startShard(ctx, store, bootstrapInfo); err != nil {
				return err
			}
			log.Debugf("Cluster %d started on: %+v", rangeID, bootstrapInfo)
			return nil
		})
		rdBuf, err := proto.Marshal(rd)
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
		stmt := tx.AddStatement()
		stmt.SetRangeDescriptor(rd).SetBatch(batch)
		// The range descriptor is a new one.
		stmt.SetRangeValidationRequired(false)

		metaRangeBatch = metaRangeBatch.Add(&rfpb.IncrementRequest{
			Key:   keys.MakeKey(constants.LastReplicaIDKeyPrefix, []byte(fmt.Sprintf("%d", rangeID))),
			Delta: uint64(len(bootstrapInfo.Replicas)),
		})
		metaRangeBatch = metaRangeBatch.Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   keys.RangeMetaKey(rd.GetEnd()),
				Value: rdBuf,
			},
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	log.Debugf("Attempting to start %d cluster with starting range_id=%d on: %+v", len(ranges), rangeIDs[0], nodeGrpcAddrs)

	stmt := tx.AddStatement()
	stmt.SetRangeDescriptor(mrd).SetBatch(metaRangeBatch)
	stmt.SetRangeValidationRequired(true)

	err = store.TxnCoordinator().RunTxn(ctx, tx)
	if err != nil {
		return status.InternalErrorf("failed to run txn to start %d shards with starting range_id=%d, err: %s", len(ranges), rangeIDs[0], err)
	}
	return nil
}

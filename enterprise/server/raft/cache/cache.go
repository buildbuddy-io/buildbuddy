package cache

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/api"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/gossip"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/statemachine"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/network"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	dbConfig "github.com/lni/dragonboat/v3/config"
	dbLogger "github.com/lni/dragonboat/v3/logger"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
)

type Config struct {
	// Required fields.
	RootDir string

	// Gossip Config
	ListenAddress string
	Join          []string

	// Raft Config
	HTTPPort int

	// GRPC API Config
	GRPCPort int
}

type RaftCache struct {
	env  environment.Env
	conf *Config

	raftAddress string

	registry      *registry.DynamicNodeRegistry
	gossipManager *gossip.GossipManager
	setTagFn      func(tagName, tagValue string) error

	nodeHost             *dragonboat.NodeHost
	apiServer            *api.Server
	apiClient            *client.APIClient
	rangeCache           *rangecache.RangeCache
	createStateMachineFn dbsm.CreateOnDiskStateMachineFunc

	isolation *rfpb.Isolation

	localStoreRanges []*rfpb.RangeDescriptor
}

// We need to provide a factory method that creates the DynamicNodeRegistry, and
// hand this to the raft library when we set things up. When nodeHost is created
// it will call this method to create the registry a and use it until nodehost
// close.
func (rc *RaftCache) Create(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
	r, err := registry.NewDynamicNodeRegistry(nhid, rc.raftAddress, streamConnections, v)
	if err != nil {
		return nil, err
	}
	// Register the node registry with the gossip manager so it gets updates
	// on node membership.
	rc.gossipManager.AddBroker(r)
	rc.registry = r
	return r, nil
}

// We need to implement the RangeTracker interface so that stores opened and
// closed on this node will notify us when their range appears and disappears.
// We'll use this information to drive the range tags we broadcast.
func (rc *RaftCache) AddRange(rd *rfpb.RangeDescriptor) {
	rc.localStoreRanges = append(rc.localStoreRanges, rd)

	// Broadcast the meta1 range.
	r := rangemap.Range{Left: rd.Left, Right: rd.Right}
	if r.Contains([]byte{constants.MinByte}) && r.Contains([]byte{constants.MaxByte}) {
		buf, err := proto.Marshal(rd)
		if err != nil {
			log.Errorf("Error marshing meta1 range descriptor: %s", err)
			return
		}
		rc.setTagFn(constants.Meta1RangeTag, string(buf))
	}
}

func (rc *RaftCache) RemoveRange(rd *rfpb.RangeDescriptor) {
	for i, t := range rc.localStoreRanges {
		if t == rd {
			rc.localStoreRanges = append(rc.localStoreRanges[:i], rc.localStoreRanges[i+1:]...)
			return
		}
	}
}

func NewRaftCache(env environment.Env, conf *Config) (*RaftCache, error) {
	log.Debugf("conf is: %+v", conf)
	rc := &RaftCache{
		env:        env,
		conf:       conf,
		rangeCache: rangecache.New(),
	}

	if len(conf.Join) < 3 {
		return nil, status.InvalidArgumentError("Join must contain at least 3 nodes.")
	}

	// Parse the listenAddress into host and port; we'll listen for grpc and
	// raft traffic on the same interface but on different ports.
	listenHost, _, err := network.ParseAddress(conf.ListenAddress)
	if err != nil {
		return nil, err
	}
	rc.raftAddress = net.JoinHostPort(listenHost, strconv.Itoa(conf.HTTPPort))
	grpcAddress := net.JoinHostPort(listenHost, strconv.Itoa(conf.GRPCPort))

	// Initialize a gossip manager, which will contact other nodes
	// and exchange information.
	gossipManager, err := gossip.NewGossipManager(conf.ListenAddress, conf.Join)
	if err != nil {
		return nil, err
	}
	rc.gossipManager = gossipManager

	// Quiet the raft logs
	dbLogger.GetLogger("raft").SetLevel(dbLogger.ERROR)
	dbLogger.GetLogger("rsm").SetLevel(dbLogger.ERROR)
	dbLogger.GetLogger("transport").SetLevel(dbLogger.ERROR)
	dbLogger.GetLogger("dragonboat").SetLevel(dbLogger.ERROR)
	dbLogger.GetLogger("raftpb").SetLevel(dbLogger.ERROR)
	dbLogger.GetLogger("logdb").SetLevel(dbLogger.ERROR)

	// A NodeHost is basically a single node (think 'computer') that can be
	// a member of raft clusters. This nodehost is configured with a dynamic
	// NodeRegistryFactory that allows raft to resolve other nodehosts that
	// may have changed IP addresses after restart, but still contain the
	// same data they did previously.
	nhc := dbConfig.NodeHostConfig{
		WALDir:         filepath.Join(conf.RootDir, "wal"),
		NodeHostDir:    filepath.Join(conf.RootDir, "nodehost"),
		RTTMillisecond: 1,
		RaftAddress:    rc.raftAddress,
		Expert: dbConfig.ExpertConfig{
			NodeRegistryFactory: rc,
		},
	}
	nodeHost, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	rc.nodeHost = nodeHost

	nodeHostInfo := nodeHost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{})
	log.Printf("nodeHostInfo: %+v", nodeHostInfo)

	// PebbleLogDir is a parent directory for pebble data. Within this
	// directory, subdirs will be created per raft (cluster_id, node_id).
	pebbleLogDir := filepath.Join(conf.RootDir, "pebble")

	// FileDir is a parent directory where files will be stored. This data
	// is managed entirely by the raft statemachine.
	fileDir := filepath.Join(conf.RootDir, "files")

	// smFunc is a function that creates a new statemachine for a given
	// (cluster_id, node_id), within the pebbleLogDir. Data written via raft
	// will live in a pebble database driven by this statemachine.
	rc.createStateMachineFn = statemachine.MakeCreatePebbleDiskStateMachineFactory(pebbleLogDir, fileDir, rc)
	apiServer, err := api.NewServer(fileDir, nodeHost, rc.createStateMachineFn)
	if err != nil {
		return nil, err
	}
	rc.apiServer = apiServer
	rc.apiClient = client.NewAPIClient(env, nodeHostInfo.NodeHostID)

	// grpcServer is responsible for presenting an API to manage raft nodes
	// on each host, as well as an API to shuffle data around between nodes,
	// outside of raft.
	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)
	rfspb.RegisterApiServer(grpcServer, rc.apiServer)

	lis, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		return nil, err
	}
	go func() {
		grpcServer.Serve(lis)
	}()
	env.GetHealthChecker().RegisterShutdownFunction(grpc_server.GRPCShutdownFunc(grpcServer))

	// register us to get callbacks
	// and broadcast some basic tags
	rc.gossipManager.AddBroker(rc)
	rc.gossipManager.AddBroker(rc.rangeCache)
	rc.setTagFn(constants.NodeHostIDTag, nodeHost.ID())
	rc.setTagFn(constants.RaftAddressTag, rc.raftAddress)
	rc.setTagFn(constants.GRPCAddressTag, grpcAddress)

	// bring up any clusters that were previously configured, or
	// bootstrap a new one based on the join params in the config.
	clusterStarter := bringup.NewClusterStarter(nodeHost, rc.createStateMachineFn, conf.ListenAddress, conf.Join)
	clusterStarter.InitializeClusters()
	rc.gossipManager.AddBroker(clusterStarter)

	return rc, nil
}

func (rc *RaftCache) Check(ctx context.Context) error {
	// We are ready to serve when:
	//  - we're able to read meta1 range
	// TODO(tylerw): fix health check.

	// raft library will complain if sync read context does not
	// have a timeout set, so ensure there is one.
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	readCmd := rbuilder.DirectReadRequestBuf(constants.InitClusterSetupTimeKey)
	rsp, err := rc.nodeHost.SyncRead(ctx, constants.InitialClusterID, readCmd)
	if err != nil {
		return err
	}
	kv := rbuilder.DirectReadResponse(rsp)
	log.Printf("KV direct read response: %+v", kv)
	if string(kv.GetKey()) == string(constants.InitClusterSetupTimeKey) {
		return nil
	}
	return status.FailedPreconditionError("Malformed value type")
}

func (rc *RaftCache) WithIsolation(ctx context.Context, cacheType interfaces.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
	newIsolation := &rfpb.Isolation{}
	switch cacheType {
	case interfaces.CASCacheType:
		newIsolation.CacheType = rfpb.Isolation_CAS_CACHE
	case interfaces.ActionCacheType:
		newIsolation.CacheType = rfpb.Isolation_ACTION_CACHE
	default:
		return nil, status.InvalidArgumentErrorf("Unknown cache type %v", cacheType)
	}
	newIsolation.RemoteInstanceName = remoteInstanceName

	clone := *rc
	clone.isolation = newIsolation

	return &clone, nil
}

func (rc *RaftCache) makeFileRecord(ctx context.Context, d *repb.Digest) (*rfpb.FileRecord, error) {
	_, err := digest.Validate(d)
	if err != nil {
		return nil, err
	}
	userPrefix, err := prefix.UserPrefixFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return &rfpb.FileRecord{
		GroupId:   strings.TrimSuffix(userPrefix, "/"),
		Isolation: rc.isolation,
		Digest:    d,
	}, nil
}

// Next Steps (for tomorrow):
//  - implement reader
//  - write a test that writes some data, kills a node, writes more data, brings
//    it back up and ensures that all data is correctly replicated after some time
//  - write a test that writes data, nukes a nude, brings up a new one, and
//    ensures that all data is correctly replicated
func (rc *RaftCache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	// - look in meta1 (which is gossiped everywhere) to find meta2 for our key
	// - look in meta2 to find the the range owners for our key
	// - connect to the range members, and do a linearizable read
	//   rangeOwner does:
	//    - ReadIndex
	// 	- ReadLocal
	// 	- return file
	return nil, nil
}

type raftWriteCloser struct {
	io.WriteCloser
	closeFn func() error
}

func (rwc *raftWriteCloser) Close() error {
	if err := rwc.WriteCloser.Close(); err != nil {
		return err
	}
	return rwc.closeFn()
}

func (rc *RaftCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	// - look in meta1 (which is gossiped everywhere) to find meta2 for our key
	// - look in meta2 to find the the range owners for our key
	//   - GetClusterMembership to get the current cluster membership
	//   - write the file to each member of the cluster
	//   - syncPropose a write to add the file to pebble
	start := time.Now()
	membership, err := rc.nodeHost.GetClusterMembership(ctx, constants.InitialClusterID)
	if err != nil {
		return nil, err
	}
	peers := make([]string, 0, len(membership.Nodes))
	for _, nodeHostID := range membership.Nodes {
		grpcAddress, err := rc.registry.ResolveGRPCAddress(nodeHostID)
		if err != nil {
			log.Errorf("Skipping nodeHost: %q", nodeHostID)
			continue
		}
		log.Printf("member: nodeID: %s, grpcAddress: %q", nodeHostID, grpcAddress)
		peers = append(peers, grpcAddress)
	}
	log.Errorf("Took %s to get cluster membership", time.Since(start))
	fileRecord, err := rc.makeFileRecord(ctx, d)
	if err != nil {
		return nil, err
	}
	mwc, err := rc.apiClient.MultiWriter(ctx, peers, fileRecord)
	if err != nil {
		return nil, err
	}

	rwc := &raftWriteCloser{mwc, func() error {
		sesh := rc.nodeHost.GetNoOPSession(constants.InitialClusterID)
		_, err := rbuilder.ResponseUnion(rc.nodeHost.SyncPropose(ctx, sesh, rbuilder.FileWriteRequestBuf(fileRecord)))
		return err
	}}

	return rwc, nil
}

// RegisterTagProviderFn gets a callback function that can  be used to set tags.
func (rc *RaftCache) RegisterTagProviderFn(setTagFn func(tagName, tagValue string) error) {
	rc.setTagFn = setTagFn
}

// MemberEvent is called when a node joins, leaves, or is updated.
func (rc *RaftCache) MemberEvent(updateType serf.EventType, member *serf.Member) {
	return
}

func (rc *RaftCache) Stop() error {
	rc.nodeHost.Stop()
	rc.gossipManager.Leave()
	rc.gossipManager.Shutdown()
	return nil
}

func (rc *RaftCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	return false, nil
}
func (rc *RaftCache) ContainsMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	return nil, nil
}
func (rc *RaftCache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	return nil, nil
}
func (rc *RaftCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	return nil, nil
}
func (rc *RaftCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	return nil, nil
}
func (rc *RaftCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	return nil
}
func (rc *RaftCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	return nil
}
func (rc *RaftCache) Delete(ctx context.Context, d *repb.Digest) error {
	return nil
}

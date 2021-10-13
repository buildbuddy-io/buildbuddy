package cache

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"time"
	
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/api"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/gossip"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/statemachine"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/network"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/hashicorp/serf/serf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	
	dbConfig "github.com/lni/dragonboat/v3/config"
	dbsm "github.com/lni/dragonboat/v3/statemachine"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type Config struct {
	// Required fields.
	RootDir string

	// Gossip Config
	ListenAddress    string
	Join             []string

	// Raft Config
	HTTPPort int

	// GRPC API Config
	GRPCPort int
}

type registryFactory struct {
	raftAddress   string
	gossipManager    *gossip.GossipManager
}

// We need to provide a factory method that creates the DynamicNodeRegistry, and
// hand this to the raft library when we set things up. It will create a single
// DynamicNodeRegistry and use it to resolve all other raft nodes until the
// process shuts down.
func (rf *registryFactory) Create(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
	r, err := registry.NewDynamicNodeRegistry(nhid, rf.raftAddress, streamConnections, v)
	if err != nil {
		return nil, err
	}

	// Register the node registry with the gossip manager so it gets updates
	// on node membership.
	rf.gossipManager.AddBroker(r)
	return r, nil
}

type RaftCache struct {
	env environment.Env
	conf *Config
	
	gossipManager    *gossip.GossipManager
	setTagFn func(tagName, tagValue string) error

	nodeHost  *dragonboat.NodeHost
	apiServer *api.Server

	createStateMachineFn   dbsm.CreateOnDiskStateMachineFunc
}

func NewRaftCache(env environment.Env, conf *Config) (*RaftCache, error) {
	log.Debugf("conf is: %+v", conf)
	rc := &RaftCache{
		env: env,
		conf: conf,
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
	raftAddress := net.JoinHostPort(listenHost, strconv.Itoa(conf.HTTPPort))
	grpcAddress := net.JoinHostPort(listenHost, strconv.Itoa(conf.GRPCPort))
	
	// Initialize a gossip manager, which will contact other nodes
	// and exchange information.
	gossipManager, err := gossip.NewGossipManager(conf.ListenAddress, conf.Join)
	if err != nil {
		return nil, err
	}
	rc.gossipManager = gossipManager
	
	// A NodeHost is basically a single node (think 'computer') that can be
	// a member of raft clusters. This nodehost is configured with a dynamic
	// NodeRegistryFactory that allows raft to resolve other nodehosts that
	// may have changed IP addresses after restart, but still contain the
	// same data they did previously.
	nhc := dbConfig.NodeHostConfig{
		WALDir:         filepath.Join(conf.RootDir, "wal"),
		NodeHostDir:    filepath.Join(conf.RootDir, "nodehost"),
		RTTMillisecond: 10,
		RaftAddress:    raftAddress,
		Expert:         dbConfig.ExpertConfig{
			NodeRegistryFactory: &registryFactory{
				raftAddress: raftAddress,
				gossipManager: gossipManager,
			},
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
	rc.createStateMachineFn = statemachine.MakeCreatePebbleDiskStateMachineFactory(pebbleLogDir)
	apiServer, err := api.NewServer(fileDir, nodeHost, rc.createStateMachineFn)
	if err != nil {
		return nil, err
	}
	rc.apiServer = apiServer

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

	rc.setTagFn(constants.NodeHostIDTag, nodeHost.ID())
	rc.setTagFn(constants.RaftAddressTag, raftAddress)
	rc.setTagFn(constants.GRPCAddressTag, grpcAddress)

	clusterStarter := bringup.NewClusterStarter(nodeHost, rc.createStateMachineFn, conf.ListenAddress, conf.Join)
	clusterStarter.InitializeClusters()
	rc.gossipManager.AddBroker(clusterStarter)
	
	return rc, nil
}

func (rc *RaftCache) Check(ctx context.Context) error {
	// raft library will complain if sync read context does not
	// have a timeout set, so ensure there is one.
	ctx, cancel := context.WithTimeout(ctx, 1 * time.Second)
	defer cancel()
	rsp, err := rc.nodeHost.SyncRead(ctx, constants.InitialClusterID, constants.InitClusterSetupTimeKey)
	if err != nil {
		return err
	}
	if value, ok := rsp.([]byte); ok && len(value) > 0 {
		return nil
	}
	return status.FailedPreconditionError("Malformed value type")
}

func (c *RaftCache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	// - look in meta1 (which is gossiped everywhere) to find meta2 for our key
	// - look in meta2 to find the the range owners for our key
	// - connect to the range members, and do a linearizable read
	//   rangeOwner does:
	//    - ReadIndex
	// 	- ReadLocal
	// 	- return file
	return nil, nil
}
func (c *RaftCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	// - look in meta1 (which is gossiped everywhere) to find meta2 for our key
	// - look in meta2 to find the the range owners for our key
	//   - GetClusterMembership to get the current cluster membership
	//   - write the file to each member of the cluster
	//   - syncPropose a write to add the file to pebble
	return nil, nil
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

package cache

import (
	"context"
	"flag"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/driver"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/listener"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/network"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	dbConfig "github.com/lni/dragonboat/v3/config"
)

var (
	rootDirectory = flag.String("cache.raft.root_directory", "", "The root directory to use for storing cached data.")
	listenAddr    = flag.String("cache.raft.listen_addr", "", "The address to listen for local gossip traffic on. Ex. 'localhost:1991")
	join          = flagutil.New("cache.raft.join", []string{}, "The list of nodes to use when joining clusters Ex. '1.2.3.4:1991,2.3.4.5:1991...'")
	httpPort      = flag.Int("cache.raft.http_port", 0, "The address to listen for HTTP raft traffic. Ex. '1992'")
	gRPCPort      = flag.Int("cache.raft.grpc_port", 0, "The address to listen for internal API traffic on. Ex. '1993'")
)

const (
	DefaultPartitionID = "default"
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

	Partitions        []disk.Partition
	PartitionMappings []disk.PartitionMapping
}

type RaftCache struct {
	env  environment.Env
	conf *Config

	raftAddress string
	grpcAddress string

	registry      registry.NodeRegistry
	gossipManager *gossip.GossipManager

	nodeHost       *dragonboat.NodeHost
	store          *store.Store
	apiClient      *client.APIClient
	rangeCache     *rangecache.RangeCache
	sender         *sender.Sender
	clusterStarter *bringup.ClusterStarter
	driver         *driver.Driver

	isolation    *rfpb.Isolation
	shutdown     chan struct{}
	shutdownOnce *sync.Once
}

// We need to provide a factory method that creates the DynamicNodeRegistry, and
// hand this to the raft library when we set things up. When nodeHost is created
// it will call this method to create the registry a and use it until nodehost
// close.
func (rc *RaftCache) Create(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
	r := registry.NewDynamicNodeRegistry(rc.gossipManager, streamConnections, v)
	rc.registry = r
	r.AddNode(nhid, rc.raftAddress, rc.grpcAddress)
	return r, nil
}

func Register(env environment.Env) error {
	if *listenAddr == "" {
		return nil
	}
	rcConfig := &Config{
		RootDir:       *rootDirectory,
		ListenAddress: *listenAddr,
		Join:          *join,
		HTTPPort:      *httpPort,
		GRPCPort:      *gRPCPort,
	}
	rc, err := NewRaftCache(env, rcConfig)
	if err != nil {
		return status.InternalErrorf("Error enabling raft cache: %s", err.Error())
	}
	env.SetCache(rc)
	env.GetHealthChecker().RegisterShutdownFunction(
		func(ctx context.Context) error {
			rc.Stop()
			return nil
		},
	)
	return nil
}

func NewRaftCache(env environment.Env, conf *Config) (*RaftCache, error) {
	rc := &RaftCache{
		env:          env,
		conf:         conf,
		rangeCache:   rangecache.New(),
		shutdown:     make(chan struct{}),
		shutdownOnce: &sync.Once{},
	}

	if len(conf.Join) < 3 {
		return nil, status.InvalidArgumentError("Join must contain at least 3 nodes.")
	}
	if err := disk.EnsureDirectoryExists(conf.RootDir); err != nil {
		return nil, err
	}

	// Parse the listenAddress into host and port; we'll listen for grpc and
	// raft traffic on the same interface but on different ports.
	listenHost, _, err := network.ParseAddress(conf.ListenAddress)
	if err != nil {
		return nil, err
	}
	rc.raftAddress = net.JoinHostPort(listenHost, strconv.Itoa(conf.HTTPPort))
	rc.grpcAddress = net.JoinHostPort(listenHost, strconv.Itoa(conf.GRPCPort))

	// Initialize a gossip manager, which will contact other nodes
	// and exchange information.
	gossipManager, err := gossip.NewGossipManager(conf.ListenAddress, conf.Join)
	if err != nil {
		return nil, err
	}
	rc.gossipManager = gossipManager

	// Register the range cache as a gossip listener, so that it can
	// discover which nodes maintain the meta range..
	rc.gossipManager.AddListener(rc.rangeCache)

	// A NodeHost is basically a single node (think 'computer') that can be
	// a member of raft clusters. This nodehost is configured with a dynamic
	// NodeRegistryFactory that allows raft to resolve other nodehosts that
	// may have changed IP addresses after restart, but still contain the
	// same data they did previously.
	//
	// Because not all nodes are members of every raft cluster, we contact
	// nodes maintaining the meta range, who broadcast their presence, and
	// use the metarange to find which other clusters / nodes contain which
	// keys. The rangeCache is where this information is cached, and the
	// sender interface is what makes it simple to address data across the
	// cluster.
	raftListener := listener.DefaultListener()
	nhc := dbConfig.NodeHostConfig{
		WALDir:         filepath.Join(conf.RootDir, "wal"),
		NodeHostDir:    filepath.Join(conf.RootDir, "nodehost"),
		RTTMillisecond: 10,
		RaftAddress:    rc.raftAddress,
		Expert: dbConfig.ExpertConfig{
			NodeRegistryFactory: rc,
		},
		RaftEventListener:   raftListener,
		SystemEventListener: raftListener,
	}
	nodeHost, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	rc.nodeHost = nodeHost

	// PebbleLogDir is a parent directory for pebble data. Within this
	// directory, subdirs will be created per raft (cluster_id, node_id).
	pebbleLogDir := filepath.Join(conf.RootDir, "pebble")

	// FileDir is a parent directory where files will be stored. This data
	// is managed entirely by the raft statemachine.
	fileDir := filepath.Join(conf.RootDir, "files")
	if err := disk.EnsureDirectoryExists(fileDir); err != nil {
		return nil, err
	}

	rc.apiClient = client.NewAPIClient(env, rc.nodeHost.ID())
	rc.sender = sender.New(rc.rangeCache, rc.registry, rc.apiClient)
	rc.store = store.New(pebbleLogDir, fileDir, rc.nodeHost, rc.gossipManager, rc.sender, rc.registry, rc.apiClient)
	if err := rc.store.Start(rc.grpcAddress); err != nil {
		return nil, err
	}

	// bring up any clusters that were previously configured, or
	// bootstrap a new one based on the join params in the config.
	rc.clusterStarter = bringup.New(nodeHost, rc.grpcAddress, rc.store.ReplicaFactoryFn, rc.gossipManager, rc.apiClient)
	rc.clusterStarter.InitializeClusters()

	// start the driver once bringup is complete.
	rc.driver = driver.New(rc.store, rc.gossipManager, driver.TestingOpts())
	go func() {
		for !rc.clusterStarter.Done() {
			time.Sleep(100 * time.Millisecond)
		}
		if err := rc.driver.Start(); err != nil {
			log.Errorf("Error starting driver: %s", err)
		}
	}()

	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		return rc.Stop()
	})
	return rc, nil
}

func (rc *RaftCache) Check(ctx context.Context) error {
	// We are ready to serve when we know which nodes contain the meta range
	// and can contact those nodes. We test this by doing a SyncRead of the
	// initial cluster setup time key/val which is stored in the Meta Range.
	select {
	case <-rc.shutdown:
		return status.FailedPreconditionError("node is shutdown")
	default:
		break
	}

	// Before this check, the tests were somewhat flaky. This check
	// ensures that the clusterStarter has "finished", meaning it has read
	// clusters from disk, created them automatically, or decided that it's
	// not going to do either.
	if !rc.clusterStarter.Done() {
		return status.UnavailableError("node is still initializing")
	}

	key := constants.ClusterSetupTimeKey
	readReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectReadRequest{
		Key: key,
	}).ToProto()
	if err != nil {
		return err
	}
	return rc.sender.Run(ctx, key, func(c rfspb.ApiClient, h *rfpb.Header) error {
		_, err := c.SyncRead(ctx, &rfpb.SyncReadRequest{
			Header: h,
			Batch:  readReq,
		})
		return err
	})
}

func (rc *RaftCache) lookupPartitionID(ctx context.Context, remoteInstanceName string) (string, error) {
	auth := rc.env.GetAuthenticator()
	if auth == nil {
		return DefaultPartitionID, nil
	}
	user, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return DefaultPartitionID, nil
	}
	for _, pm := range rc.conf.PartitionMappings {
		if pm.GroupID == user.GetGroupID() && strings.HasPrefix(remoteInstanceName, pm.Prefix) {
			return pm.PartitionID, nil
		}
	}
	return DefaultPartitionID, nil
}

func (rc *RaftCache) WithIsolation(ctx context.Context, cacheType interfaces.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
	partID, err := rc.lookupPartitionID(ctx, remoteInstanceName)
	if err != nil {
		return nil, err
	}

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
	newIsolation.PartitionId = partID

	clone := *rc
	clone.isolation = newIsolation

	return &clone, nil
}

func (rc *RaftCache) makeFileRecord(ctx context.Context, d *repb.Digest) (*rfpb.FileRecord, error) {
	_, err := digest.Validate(d)
	if err != nil {
		return nil, err
	}

	return &rfpb.FileRecord{
		Isolation: rc.isolation,
		Digest:    d,
	}, nil
}

func (rc *RaftCache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	fileRecord, err := rc.makeFileRecord(ctx, d)
	if err != nil {
		return nil, err
	}
	fileMetadataKey, err := constants.FileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}

	var readCloser io.ReadCloser
	err = rc.sender.Run(ctx, fileMetadataKey, func(c rfspb.ApiClient, h *rfpb.Header) error {
		req := &rfpb.ReadRequest{
			Header:     h,
			FileRecord: fileRecord,
			Offset:     offset,
			Limit:      limit,
		}
		r, err := rc.apiClient.RemoteReader(ctx, c, req)
		if err != nil {
			return err
		}
		readCloser = r
		return nil
	})
	return readCloser, err
}

func (rc *RaftCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	fileRecord, err := rc.makeFileRecord(ctx, d)
	if err != nil {
		return nil, err
	}
	fileMetadataKey, err := constants.FileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}
	var writeCloser io.WriteCloser
	err = rc.sender.Run(ctx, fileMetadataKey, func(c rfspb.ApiClient, h *rfpb.Header) error {
		wc, err := client.RemoteSyncWriter(ctx, c, h, fileRecord)
		if err != nil {
			log.Printf("RemoteSyncWriter err: %s", err)
			return err
		}
		writeCloser = wc
		return nil
	})
	return writeCloser, err
}

func (rc *RaftCache) Stop() error {
	rc.shutdownOnce.Do(func() {
		close(rc.shutdown)
		rc.driver.Stop()
		rc.store.Stop(context.Background())
		rc.nodeHost.Stop()
		rc.gossipManager.Leave()
		rc.gossipManager.Shutdown()
	})
	return nil
}

func (rc *RaftCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	missing, err := rc.FindMissing(ctx, []*repb.Digest{d})
	if err != nil {
		return false, err
	}
	return len(missing) == 0, nil
}

func (rc *RaftCache) Metadata(ctx context.Context, d *repb.Digest) (*interfaces.CacheMetadata, error) {
	return nil, status.UnimplementedError("not implemented")
}

func (rc *RaftCache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	reqs := make(map[uint64]*rfpb.FindMissingRequest)
	for _, d := range digests {
		fileRecord, err := rc.makeFileRecord(ctx, d)
		if err != nil {
			return nil, err
		}
		fileMetadataKey, err := constants.FileMetadataKey(fileRecord)
		if err != nil {
			return nil, err
		}
		rangeDescriptor, err := rc.sender.LookupRangeDescriptor(ctx, fileMetadataKey, false /*skipCache*/)
		if err != nil {
			return nil, err
		}
		if _, ok := reqs[rangeDescriptor.GetRangeId()]; !ok {
			reqs[rangeDescriptor.GetRangeId()] = &rfpb.FindMissingRequest{}
		}
		req := reqs[rangeDescriptor.GetRangeId()]
		req.FileRecord = append(req.FileRecord, fileRecord)
	}

	missingDigests := make([]*repb.Digest, 0)
	for _, req := range reqs {
		fileMetadataKey, err := constants.FileMetadataKey(req.GetFileRecord()[0])
		if err != nil {
			return nil, err
		}
		err = rc.sender.Run(ctx, fileMetadataKey, func(c rfspb.ApiClient, h *rfpb.Header) error {
			req.Header = h
			rsp, err := c.FindMissing(ctx, req)
			if err != nil {
				return err
			}
			for _, fr := range rsp.GetFileRecord() {
				missingDigests = append(missingDigests, fr.GetDigest())
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return missingDigests, nil
}

func (rc *RaftCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	r, err := rc.Reader(ctx, d, 0, 0)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (rc *RaftCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	return nil, nil
}

func (rc *RaftCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	wc, err := rc.Writer(ctx, d)
	if err != nil {
		return err
	}
	if _, err := wc.Write(data); err != nil {
		return err
	}
	return wc.Close()
}

func (rc *RaftCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	return nil
}

func (rc *RaftCache) Delete(ctx context.Context, d *repb.Digest) error {
	return nil
}

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
	"github.com/buildbuddy-io/buildbuddy/server/gossip"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/network"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/rangemap"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"

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
	grpcAddress string

	registry      *registry.DynamicNodeRegistry
	gossipManager *gossip.GossipManager

	nodeHost   *dragonboat.NodeHost
	apiServer  *api.Server
	apiClient  *client.APIClient
	rangeCache *rangecache.RangeCache
	sender     *sender.Sender

	createStateMachineFn dbsm.CreateOnDiskStateMachineFunc

	isolation *rfpb.Isolation

	localStoreRanges []*rfpb.RangeDescriptor
}

// We need to provide a factory method that creates the DynamicNodeRegistry, and
// hand this to the raft library when we set things up. When nodeHost is created
// it will call this method to create the registry a and use it until nodehost
// close.
func (rc *RaftCache) Create(nhid string, streamConnections uint64, v dbConfig.TargetValidator) (raftio.INodeRegistry, error) {
	r, err := registry.NewDynamicNodeRegistry(nhid, rc.raftAddress, rc.grpcAddress, rc.gossipManager, streamConnections, v)
	if err != nil {
		return nil, err
	}
	// Register the node registry with the gossip manager so it gets updates
	// on node membership.
	rc.gossipManager.AddListener(r)
	rc.registry = r
	return r, nil
}

func containsMetaRange(rd *rfpb.RangeDescriptor) bool {
	r := rangemap.Range{Left: rd.Left, Right: rd.Right}
	return r.Contains([]byte{constants.MinByte}) && r.Contains([]byte{constants.MaxByte - 1})
}

// We need to implement the RangeTracker interface so that stores opened and
// closed on this node will notify us when their range appears and disappears.
// We'll use this information to drive the range tags we broadcast.
func (rc *RaftCache) AddRange(rd *rfpb.RangeDescriptor) {
	log.Errorf("AddRange called!")
	rc.localStoreRanges = append(rc.localStoreRanges, rd)

	if containsMetaRange(rd) {
		// If we own the metarange, use gossip to notify other nodes
		// of that fact.
		buf, err := proto.Marshal(rd)
		if err != nil {
			log.Errorf("Error marshaling metarange descriptor: %s", err)
			return
		}
		rc.gossipManager.SetTag(constants.MetaRangeTag, string(buf))
	}
}

func (rc *RaftCache) RemoveRange(rd *rfpb.RangeDescriptor) {
	// If we had the metarange and no longer do, clear that tag.
	for i, t := range rc.localStoreRanges {
		if t == rd {
			rc.localStoreRanges = append(rc.localStoreRanges[:i], rc.localStoreRanges[i+1:]...)
			return
		}
	}

	if containsMetaRange(rd) {
		rc.gossipManager.SetTag(constants.MetaRangeTag, "")
	}
}

type nullLogger struct{}

func (nullLogger) SetLevel(dbLogger.LogLevel)                  {}
func (nullLogger) Debugf(format string, args ...interface{})   {}
func (nullLogger) Infof(format string, args ...interface{})    {}
func (nullLogger) Warningf(format string, args ...interface{}) {}
func (nullLogger) Errorf(format string, args ...interface{})   {}
func (nullLogger) Panicf(format string, args ...interface{})   {}

type dbCompatibleLogger struct {
	log.Logger
}

// Don't panic in server code.
func (l *dbCompatibleLogger) Panicf(format string, args ...interface{}) {
	l.Errorf(format, args...)
}

// Ignore SetLevel commands.
func (l *dbCompatibleLogger) SetLevel(level dbLogger.LogLevel) {}

func init() {
	dbLogger.SetLoggerFactory(func(pkgName string) dbLogger.ILogger {
		switch pkgName {
		case "raft", "rsm", "transport", "dragonboat", "raftpb", "logdb":
			// Make the raft library be quieter.
			return &nullLogger{}
		default:
			l := log.NamedSubLogger(pkgName)
			return &dbCompatibleLogger{l}
		}
	})
}

func NewRaftCache(env environment.Env, conf *Config) (*RaftCache, error) {
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
	rc.grpcAddress = net.JoinHostPort(listenHost, strconv.Itoa(conf.GRPCPort))

	// Initialize a gossip manager, which will contact other nodes
	// and exchange information.
	gossipManager, err := gossip.NewGossipManager(conf.ListenAddress, conf.Join)
	if err != nil {
		return nil, err
	}
	rc.gossipManager = gossipManager

	// Register the range cache to get callbacks.
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
	// sender interface is what makes it simple to use.
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

	rc.apiClient = client.NewAPIClient(env, nodeHostInfo.NodeHostID)
	rc.sender = sender.New(rc.rangeCache, rc.registry, rc.apiClient)

	// smFunc is a function that creates a new statemachine for a given
	// (cluster_id, node_id), within the pebbleLogDir. Data written via raft
	// will live in a pebble database driven by this statemachine.
	rc.createStateMachineFn = replica.NewStateMachineFactory(pebbleLogDir, fileDir, rc, rc.sender, rc.apiClient)
	apiServer, err := api.NewServer(fileDir, nodeHost, rc.createStateMachineFn)
	if err != nil {
		return nil, err
	}
	rc.apiServer = apiServer
	if err := rc.apiServer.Start(rc.grpcAddress); err != nil {
		return nil, err
	}

	rc.gossipManager.SetTag(constants.NodeHostIDTag, nodeHost.ID())
	rc.gossipManager.SetTag(constants.RaftAddressTag, rc.raftAddress)
	rc.gossipManager.SetTag(constants.GRPCAddressTag, rc.grpcAddress)

	// bring up any clusters that were previously configured, or
	// bootstrap a new one based on the join params in the config.
	clusterStarter := bringup.NewClusterStarter(nodeHost, rc.grpcAddress, rc.createStateMachineFn, rc.gossipManager)
	rc.gossipManager.AddListener(clusterStarter)

	clusterStarter.InitializeClusters()
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		return rc.Stop()
	})
	return rc, nil
}

func (rc *RaftCache) Check(ctx context.Context) error {
	// We are ready to serve when we know which nodes contain the meta range
	// and can contact those nodes. We test this by doing a SyncRead of the
	// initial cluster setup time key/val which is stored in the Meta Range.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	key := constants.InitClusterSetupTimeKey
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

func (rc *RaftCache) GetSender() *sender.Sender {
	return rc.sender
}

func (rc *RaftCache) GetNHID() string {
	return rc.nodeHost.ID()
}

func (rc *RaftCache) GetGrpcAddress() string {
	return rc.grpcAddress
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

func (rc *RaftCache) Reader(ctx context.Context, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	fileRecord, err := rc.makeFileRecord(ctx, d)
	if err != nil {
		return nil, err
	}
	fileKey, err := constants.FileKey(fileRecord)
	if err != nil {
		return nil, err
	}
	peers, err := rc.sender.GetAllNodes(ctx, fileKey)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, peer := range peers {
		r, err := rc.apiClient.RemoteReader(ctx, peer, fileRecord, offset)
		if err != nil {
			lastErr = err
			continue
		}
		return r, nil
	}
	return nil, lastErr
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
	fileRecord, err := rc.makeFileRecord(ctx, d)
	if err != nil {
		return nil, err
	}
	fileKey, err := constants.FileKey(fileRecord)
	if err != nil {
		return nil, err
	}
	peers, err := rc.sender.GetAllNodes(ctx, fileKey)
	if err != nil {
		return nil, err
	}

	mwc, err := rc.apiClient.MultiWriter(ctx, peers, fileRecord)
	if err != nil {
		return nil, err
	}

	rwc := &raftWriteCloser{mwc, func() error {
		writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.FileWriteRequest{
			FileRecord: fileRecord,
		}).ToProto()
		if err != nil {
			return err
		}
		return rc.sender.Run(ctx, fileKey, func(c rfspb.ApiClient, h *rfpb.Header) error {
			log.Errorf("Bout to run SyncPropose")
			_, err := c.SyncPropose(ctx, &rfpb.SyncProposeRequest{
				Header: h,
				Batch:  writeReq,
			})
			log.Errorf("SyncPropose returned err: %s", err)
			return err
		})
	}}
	return rwc, nil
}

func (rc *RaftCache) Stop() error {
	rc.nodeHost.Stop()
	// rc.gossipManager.Leave()
	rc.gossipManager.Shutdown()
	rc.apiServer.Shutdown(context.Background())
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

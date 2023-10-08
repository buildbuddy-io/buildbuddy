package cache

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/driver"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/raftio"
	"google.golang.org/protobuf/proto"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
	dbConfig "github.com/lni/dragonboat/v4/config"
)

var (
	rootDirectory       = flag.String("cache.raft.root_directory", "", "The root directory to use for storing cached data.")
	listenAddr          = flag.String("cache.raft.listen_addr", "", "The address to listen for local gossip traffic on. Ex. 'localhost:1991")
	join                = flag.Slice("cache.raft.join", []string{}, "The list of nodes to use when joining clusters Ex. '1.2.3.4:1991,2.3.4.5:1991...'")
	httpAddr            = flag.String("cache.raft.http_addr", "", "The address to listen for HTTP raft traffic. Ex. '1992'")
	gRPCAddr            = flag.String("cache.raft.grpc_addr", "", "The address to listen for internal API traffic on. Ex. '1993'")
	clearCacheOnStartup = flag.Bool("cache.raft.clear_cache_on_startup", false, "If set, remove all raft + cache data on start")
	partitions          = flag.Slice("cache.raft.partitions", []disk.Partition{}, "")
	partitionMappings   = flag.Slice("cache.raft.partition_mappings", []disk.PartitionMapping{}, "")
)

const (
	DefaultPartitionID = "default"
)

type Config struct {
	// Required fields.
	RootDir string

	// Gossip Address
	ListenAddress string
	Join          []string

	// Raft Address
	HTTPAddr string

	// GRPC Address
	GRPCAddr string

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

	shutdown     chan struct{}
	shutdownOnce *sync.Once

	fileStorer filestore.Store
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

	ps := *partitions
	haveDefault := false
	for _, p := range ps {
		if p.ID == DefaultPartitionID {
			haveDefault = true
			break
		}
	}
	if !haveDefault {
		ps = append(ps, disk.Partition{
			ID:           DefaultPartitionID,
			MaxSizeBytes: cache_config.MaxSizeBytes(),
		})
	}

	rcConfig := &Config{
		RootDir:           *rootDirectory,
		ListenAddress:     *listenAddr,
		Join:              *join,
		HTTPAddr:          *httpAddr,
		GRPCAddr:          *gRPCAddr,
		Partitions:        ps,
		PartitionMappings: *partitionMappings,
	}
	rc, err := NewRaftCache(env, rcConfig)
	if err != nil {
		return status.InternalErrorf("Error enabling raft cache: %s", err.Error())
	}
	env.SetCache(rc)
	statusz.AddSection("raft_cache", "Raft Cache", rc)
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
		fileStorer:   filestore.New(),
	}

	if len(conf.Join) < 3 {
		return nil, status.InvalidArgumentError("Join must contain at least 3 nodes.")
	}
	if *clearCacheOnStartup {
		log.Warningf("Clearing cache on startup!")
		if err := os.RemoveAll(conf.RootDir); err != nil {
			return nil, err
		}
	}
	if err := disk.EnsureDirectoryExists(conf.RootDir); err != nil {
		return nil, err
	}

	rc.raftAddress = conf.HTTPAddr
	rc.grpcAddress = conf.GRPCAddr

	myName := os.Getenv("MY_POD_NAME")
	if myName == "" {
		u, err := uuid.NewRandom()
		if err != nil {
			return nil, err
		}
		myName = u.String()
	}
	// Initialize a gossip manager, which will contact other nodes
	// and exchange information.
	gossipManager, err := gossip.NewGossipManager(myName, conf.ListenAddress, conf.Join)
	if err != nil {
		return nil, err
	}
	rc.gossipManager = gossipManager

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
	raftListener := listener.NewRaftListener()
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

	rc.apiClient = client.NewAPIClient(env, rc.nodeHost.ID())
	rc.sender = sender.New(rc.rangeCache, rc.registry, rc.apiClient)
	store, err := store.New(conf.RootDir, rc.nodeHost, rc.gossipManager, rc.sender, rc.registry, raftListener, rc.apiClient, rc.grpcAddress, rc.conf.Partitions)
	if err != nil {
		return nil, err
	}
	rc.store = store
	if err := rc.store.Start(); err != nil {
		return nil, err
	}

	// bring up any clusters that were previously configured, or
	// bootstrap a new one based on the join params in the config.
	rc.clusterStarter = bringup.New(nodeHost, rc.grpcAddress, rc.store.ReplicaFactoryFn, rc.gossipManager, rc.apiClient)
	if err := rc.clusterStarter.InitializeClusters(); err != nil {
		return nil, err
	}

	// start the driver once bringup is complete.
	rc.driver = driver.New(rc.store, rc.gossipManager)
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

func (rc *RaftCache) Statusz(ctx context.Context) string {
	buf := "<pre>"
	buf += fmt.Sprintf("Root directory: %q\n", rc.conf.RootDir)
	buf += fmt.Sprintf("Listen addr: %q\n", rc.conf.ListenAddress)
	buf += fmt.Sprintf("Raft (HTTP) addr: %s\n", rc.conf.HTTPAddr)
	buf += fmt.Sprintf("GRPC addr: %s\n", rc.conf.GRPCAddr)
	buf += fmt.Sprintf("Join: %q\n", strings.Join(rc.conf.Join, ", "))
	buf += fmt.Sprintf("ClusterStarter complete: %t\n", rc.clusterStarter.Done())
	buf += "</pre>"
	return buf
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

func (rc *RaftCache) lookupGroupAndPartitionID(ctx context.Context, remoteInstanceName string) (string, string, error) {
	auth := rc.env.GetAuthenticator()
	if auth == nil {
		return interfaces.AuthAnonymousUser, DefaultPartitionID, nil
	}
	user, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser, DefaultPartitionID, nil
	}
	for _, pm := range rc.conf.PartitionMappings {
		if pm.GroupID == user.GetGroupID() && strings.HasPrefix(remoteInstanceName, pm.Prefix) {
			return user.GetGroupID(), pm.PartitionID, nil
		}
	}
	return user.GetGroupID(), DefaultPartitionID, nil
}

func (rc *RaftCache) makeFileRecord(ctx context.Context, r *rspb.ResourceName) (*rfpb.FileRecord, error) {
	rn := digest.ResourceNameFromProto(r)
	if err := rn.Validate(); err != nil {
		return nil, err
	}

	groupID, partID, err := rc.lookupGroupAndPartitionID(ctx, rn.GetInstanceName())
	if err != nil {
		return nil, err
	}

	return &rfpb.FileRecord{
		Isolation: &rfpb.Isolation{
			CacheType:          rn.GetCacheType(),
			RemoteInstanceName: rn.GetInstanceName(),
			PartitionId:        partID,
			GroupId:            groupID,
		},
		Digest:         rn.GetDigest(),
		DigestFunction: rn.GetDigestFunction(),
		Compressor:     rn.GetCompressor(),
		Encryption:     nil,
	}, nil
}

func (rc *RaftCache) fileMetadataKey(fr *rfpb.FileRecord) ([]byte, error) {
	pebbleKey, err := rc.fileStorer.PebbleKey(fr)
	if err != nil {
		return nil, err
	}
	return pebbleKey.Bytes(filestore.Version5)
}

func (rc *RaftCache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	fileRecord, err := rc.makeFileRecord(ctx, r)
	if err != nil {
		return nil, err
	}
	fileMetadataKey, err := rc.fileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}

	var readCloser io.ReadCloser
	err = rc.sender.Run(ctx, fileMetadataKey, func(c rfspb.ApiClient, h *rfpb.Header) error {
		req := &rfpb.ReadRequest{
			Header:     h,
			FileRecord: fileRecord,
			Offset:     uncompressedOffset,
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

type raftWriteCloser struct {
	io.WriteCloser
	closeFn func() error
}

func (rwc *raftWriteCloser) Commit() error {
	if err := rwc.WriteCloser.Close(); err != nil {
		return status.UnavailableError(err.Error())
	}
	return rwc.closeFn()
}
func (rwc *raftWriteCloser) Close() error {
	return nil
}

func (rc *RaftCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	fileRecord, err := rc.makeFileRecord(ctx, r)
	if err != nil {
		return nil, err
	}
	fileMetadataKey, err := rc.fileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}
	writeCloserMetadata := rc.fileStorer.InlineWriter(ctx, fileRecord.GetDigest().GetSizeBytes())

	wc := ioutil.NewCustomCommitWriteCloser(writeCloserMetadata)
	wc.CommitFn = func(bytesWritten int64) error {
		now := time.Now()
		md := &rfpb.FileMetadata{
			FileRecord:      fileRecord,
			StorageMetadata: writeCloserMetadata.Metadata(),
			StoredSizeBytes: bytesWritten,
			LastModifyUsec:  now.UnixMicro(),
			LastAccessUsec:  now.UnixMicro(),
		}
		protoBytes, err := proto.Marshal(md)
		if err != nil {
			return err
		}
		writeReq, err := rbuilder.NewBatchBuilder().Add(&rfpb.DirectWriteRequest{
			Kv: &rfpb.KV{
				Key:   fileMetadataKey,
				Value: protoBytes,
			},
		}).ToProto()
		if err != nil {
			return err
		}
		writeRsp, err := rc.sender.SyncPropose(ctx, fileMetadataKey, writeReq)
		if err != nil {
			return err
		}
		return rbuilder.NewBatchResponseFromProto(writeRsp).AnyError()
	}
	return wc, nil
}

func (rc *RaftCache) Stop() error {
	rc.shutdownOnce.Do(func() {
		close(rc.shutdown)
		rc.driver.Stop()
		rc.store.Stop(context.Background())
		rc.nodeHost.Close()
		rc.gossipManager.Leave()
		rc.gossipManager.Shutdown()
	})
	return nil
}

func (rc *RaftCache) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	missing, err := rc.findMissingResourceNames(ctx, []*rspb.ResourceName{r})
	if err != nil {
		return false, err
	}
	return len(missing) == 0, nil
}

func (rc *RaftCache) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	return nil, status.UnimplementedError("not implemented")
}

func (rc *RaftCache) resourceNamesToKeyMetas(ctx context.Context, resourceNames []*rspb.ResourceName) ([]*sender.KeyMeta, error) {
	var keys []*sender.KeyMeta
	for _, rn := range resourceNames {
		fileRecord, err := rc.makeFileRecord(ctx, rn)
		if err != nil {
			return nil, err
		}
		fileMetadataKey, err := rc.fileMetadataKey(fileRecord)
		if err != nil {
			return nil, err
		}
		keys = append(keys, &sender.KeyMeta{Key: fileMetadataKey, Meta: fileRecord})
	}
	return keys, nil
}

func (rc *RaftCache) findMissingResourceNames(ctx context.Context, resourceNames []*rspb.ResourceName) ([]*repb.Digest, error) {
	keys, err := rc.resourceNamesToKeyMetas(ctx, resourceNames)
	if err != nil {
		return nil, err
	}

	rsps, err := rc.sender.RunMultiKey(ctx, keys, func(c rfspb.ApiClient, h *rfpb.Header, keys []*sender.KeyMeta) (interface{}, error) {
		req := &rfpb.FindMissingRequest{Header: h}
		for _, k := range keys {
			fr, ok := k.Meta.(*rfpb.FileRecord)
			if !ok {
				return nil, status.InternalError("type is not *rfpb.FileRecord")
			}
			req.FileRecords = append(req.FileRecords, fr)
		}
		return c.FindMissing(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	var missingDigests []*repb.Digest
	for _, rsp := range rsps {
		fmr, ok := rsp.(*rfpb.FindMissingResponse)
		if !ok {
			return nil, status.InternalError("response not of type *rfpb.FindMissingResponse")
		}
		for _, fr := range fmr.GetMissing() {
			missingDigests = append(missingDigests, fr.GetDigest())
		}
	}

	return missingDigests, nil
}

func (rc *RaftCache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	return rc.findMissingResourceNames(ctx, resources)
}

func (rc *RaftCache) Get(ctx context.Context, rn *rspb.ResourceName) ([]byte, error) {
	r, err := rc.Reader(ctx, rn, 0, 0)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (rc *RaftCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	keys, err := rc.resourceNamesToKeyMetas(ctx, resources)
	if err != nil {
		return nil, err
	}

	rsps, err := rc.sender.RunMultiKey(ctx, keys, func(c rfspb.ApiClient, h *rfpb.Header, keys []*sender.KeyMeta) (interface{}, error) {
		req := &rfpb.GetMultiRequest{Header: h}
		for _, k := range keys {
			fr, ok := k.Meta.(*rfpb.FileRecord)
			if !ok {
				return nil, status.InternalError("type is not *rfpb.FileRecord")
			}
			req.FileRecord = append(req.FileRecord, fr)
		}
		return c.GetMulti(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	dataMap := make(map[*repb.Digest][]byte)
	for _, rsp := range rsps {
		fmr, ok := rsp.(*rfpb.GetMultiResponse)
		if !ok {
			return nil, status.InternalError("response not of type *rfpb.FindMissingResponse")
		}
		for _, frd := range fmr.GetData() {
			dataMap[frd.GetFileRecord().GetDigest()] = frd.GetData()
		}
	}

	return dataMap, nil
}

func (rc *RaftCache) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
	wc, err := rc.Writer(ctx, r)
	if err != nil {
		return err
	}
	if _, err := wc.Write(data); err != nil {
		return err
	}
	return wc.Close()
}

func (rc *RaftCache) SetMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) error {
	return nil
}

func (rc *RaftCache) Delete(ctx context.Context, r *rspb.ResourceName) error {
	return nil
}

func (rc *RaftCache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	return compressor == repb.Compressor_IDENTITY
}

func (rc *RaftCache) SupportsEncryption(ctx context.Context) bool {
	return false
}

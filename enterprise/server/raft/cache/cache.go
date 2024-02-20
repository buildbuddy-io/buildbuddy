package cache

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/driver"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
)

var (
	rootDirectory       = flag.String("cache.raft.root_directory", "", "The root directory to use for storing cached data.")
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
	gossipManager interfaces.GossipService

	store          *store.Store
	clusterStarter *bringup.ClusterStarter
	driver         *driver.Driver

	shutdown     chan struct{}
	shutdownOnce *sync.Once

	fileStorer filestore.Store
}

func Register(env *real_environment.RealEnv) error {
	if *httpAddr == "" {
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
	env.GetHealthChecker().AddHealthCheck("raft_cache", rc)
	return nil
}

func NewRaftCache(env environment.Env, conf *Config) (*RaftCache, error) {
	rc := &RaftCache{
		env:          env,
		conf:         conf,
		shutdown:     make(chan struct{}),
		shutdownOnce: &sync.Once{},
		fileStorer:   filestore.New(),
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

	if env.GetGossipService() == nil {
		return nil, status.FailedPreconditionError("raft cache requires gossip be enabled")
	}
	rc.gossipManager = env.GetGossipService()

	store, err := store.New(rc.env, conf.RootDir, rc.raftAddress, rc.grpcAddress, rc.conf.Partitions)
	if err != nil {
		return nil, err
	}
	rc.store = store
	if err := rc.store.Start(); err != nil {
		return nil, err
	}

	// bring up any clusters that were previously configured, or
	// bootstrap a new one based on the join params in the config.
	rc.clusterStarter = bringup.New(rc.grpcAddress, rc.gossipManager, rc.store)
	if err := rc.clusterStarter.InitializeClusters(); err != nil {
		return nil, err
	}

	eventsCh := rc.store.AddEventListener()

	// start the driver once bringup is complete.
	rc.driver = driver.New(rc.store, rc.gossipManager, eventsCh)
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
	buf += fmt.Sprintf("Raft (HTTP) addr: %s\n", rc.conf.HTTPAddr)
	buf += fmt.Sprintf("GRPC addr: %s\n", rc.conf.GRPCAddr)
	buf += fmt.Sprintf("ClusterStarter complete: %t\n", rc.clusterStarter.Done())
	buf += "</pre>"
	return buf
}

func (rc *RaftCache) sender() *sender.Sender {
	return rc.store.Sender()
}

// Check implements the Checker interface and is called by the health checker to
// determine whether the service is ready to serve.
// The service is ready to serve when it knows which nodes contain the meta range
// and can contact those nodes. We test this by doing a SyncRead of the
// initial cluster setup time key/val which is stored in the Meta Range.
func (rc *RaftCache) Check(ctx context.Context) error {
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
	return rc.sender().Run(ctx, key, func(c rfspb.ApiClient, h *rfpb.Header) error {
		_, err := c.SyncRead(ctx, &rfpb.SyncReadRequest{
			Header: h,
			Batch:  readReq,
		})
		return err
	})
}

func (rc *RaftCache) lookupGroupAndPartitionID(ctx context.Context, remoteInstanceName string) (string, string, error) {
	user, err := rc.env.GetAuthenticator().AuthenticatedUser(ctx)
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

	req, err := rbuilder.NewBatchBuilder().Add(&rfpb.GetRequest{
		Key: fileMetadataKey,
	}).ToProto()
	if err != nil {
		return nil, err
	}
	rsp, err := rc.sender().SyncRead(ctx, fileMetadataKey, req, sender.WithConsistencyMode(rfpb.Header_RANGELEASE))
	if err != nil {
		return nil, err
	}
	rspBatch := rbuilder.NewBatchResponseFromProto(rsp)
	getRsp, err := rspBatch.GetResponse(0)
	if err != nil {
		return nil, err
	}
	md := getRsp.GetFileMetadata()
	return rc.fileStorer.InlineReader(md.GetStorageMetadata().GetInlineMetadata(), uncompressedOffset, limit)
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
		req, err := rbuilder.NewBatchBuilder().Add(&rfpb.SetRequest{
			Key:          fileMetadataKey,
			FileMetadata: md,
		}).ToProto()
		if err != nil {
			return err
		}
		writeRsp, err := rc.sender().SyncPropose(ctx, fileMetadataKey, req)
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

	rsps, err := rc.sender().RunMultiKey(ctx, keys, func(c rfspb.ApiClient, h *rfpb.Header, keys []*sender.KeyMeta) (interface{}, error) {
		batch := rbuilder.NewBatchBuilder()
		for _, k := range keys {
			batch.Add(&rfpb.FindRequest{
				Key: k.Key,
			})
		}
		batchProto, err := batch.ToProto()
		if err != nil {
			return nil, err
		}
		// TODO(tylerw): add a SyncReadMulti?
		rsp, err := c.SyncRead(ctx, &rfpb.SyncReadRequest{
			Header: h,
			Batch:  batchProto,
		})
		if err != nil {
			return nil, err
		}
		var missingDigests []*repb.Digest
		batchRsp := rbuilder.NewBatchResponseFromProto(rsp.GetBatch())
		for i, k := range keys {
			findRsp, err := batchRsp.FindResponse(i)
			if err != nil {
				return nil, err
			}
			if !findRsp.GetPresent() {
				fr := k.Meta.(*rfpb.FileRecord)
				missingDigests = append(missingDigests, fr.GetDigest())
			}
		}
		return missingDigests, nil
	}, sender.WithConsistencyMode(rfpb.Header_RANGELEASE))
	if err != nil {
		return nil, err
	}

	var allMissingDigests []*repb.Digest
	for _, rsp := range rsps {
		missing, ok := rsp.([]*repb.Digest)
		if !ok {
			return nil, status.InternalError("response not of type []*repb.Digest")
		}
		allMissingDigests = append(allMissingDigests, missing...)
	}

	return allMissingDigests, nil
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

	rsps, err := rc.sender().RunMultiKey(ctx, keys, func(c rfspb.ApiClient, h *rfpb.Header, keys []*sender.KeyMeta) (interface{}, error) {
		batch := rbuilder.NewBatchBuilder()
		for _, k := range keys {
			batch.Add(&rfpb.GetRequest{
				Key: k.Key,
			})
		}
		batchProto, err := batch.ToProto()
		if err != nil {
			return nil, err
		}
		// TODO(tylerw): add a SyncReadMulti?
		rsp, err := c.SyncRead(ctx, &rfpb.SyncReadRequest{
			Header: h,
			Batch:  batchProto,
		})
		if err != nil {
			return nil, err
		}
		found := make(map[*repb.Digest][]byte)
		batchRsp := rbuilder.NewBatchResponseFromProto(rsp.GetBatch())
		for i, k := range keys {
			r, err := batchRsp.GetResponse(i)
			if err == nil {
				fr := k.Meta.(*rfpb.FileRecord)
				found[fr.GetDigest()] = r.GetFileMetadata().GetStorageMetadata().GetInlineMetadata().GetData()
			}
		}
		return found, nil
	}, sender.WithConsistencyMode(rfpb.Header_RANGELEASE))
	if err != nil {
		return nil, err
	}

	allFound := make(map[*repb.Digest][]byte)
	for _, rsp := range rsps {
		for k, v := range rsp.(map[*repb.Digest][]byte) {
			allFound[k] = v
		}
	}

	return allFound, nil
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

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

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/bringup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/store"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/statusz"
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"

	stdFlag "flag"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
	_ "github.com/buildbuddy-io/buildbuddy/server/gossip"
)

var (
	rootDirectory = flag.String("cache.raft.root_directory", "", "The root directory to use for storing cached data.")
	httpPort      = flag.Int("cache.raft.http_port", 7238, "The address to listen for HTTP raft traffic. Ex. '1992'")
	gRPCPort      = flag.Int("cache.raft.grpc_port", 4772, "The port to listen for internal Raft API traffic on. Ex. '4772'")
	hostName      = flag.String("cache.raft.host_name", "", "The hostname of the raft store.")
	listen        = flag.String("cache.raft.listen", "0.0.0.0", "The interface to listen on (default:0.0.0.0)")

	clearCacheOnStartup     = flag.Bool("cache.raft.clear_cache_on_startup", false, "If set, remove all raft + cache data on start")
	clearPrevCacheOnStartup = flag.Bool("cache.raft.clear_prev_cache_on_startup", false, "If set, remove all raft + cache data from previous run on start")
	partitions              = flag.Slice("cache.raft.partitions", []disk.Partition{}, "")
	partitionMappings       = flag.Slice("cache.raft.partition_mappings", []disk.PartitionMapping{}, "")
	atimeUpdateThreshold    = flag.Duration("cache.raft.atime_update_threshold", 3*time.Hour, "Don't update atime if it was updated more recently than this")
	atimeBufferSize         = flag.Int("cache.raft.atime_buffer_size", 100000, "Buffer up to this many atime updates in a channel before dropping atime updates")
	atimeWriteBatchSize     = flag.Int("cache.raft.atime_write_batch_size", 100, "Buffer this many writes before writing atime data")

	// TODO(tylerw): remove after dev.
	// Store raft content in a subdirectory with the same name as the gossip
	// key, so that we can easily start fresh in dev by just changing the
	// gossip key.
	subdir = types.Alias[string](stdFlag.CommandLine, "gossip.secret_key")
)

const (
	// atimeFlushPeriod is the time interval that we will wait before
	// flushing any atime updates in an incomplete batch (that have not
	// already been flushed due to throughput)
	atimeFlushPeriod   = 10 * time.Second
	DefaultPartitionID = "default"
)

type Config struct {
	// Required fields.
	RootDir string

	Hostname string

	ListenAddr string

	HTTPPort int
	GRPCPort int

	Partitions        []disk.Partition
	PartitionMappings []disk.PartitionMapping

	LogDBConfigType store.LogDBConfigType
}

// data needed to update last access time.
type accessTimeUpdate struct {
	key []byte
}

type RaftCache struct {
	env  environment.Env
	conf *Config

	raftAddr string
	grpcAddr string

	registry      registry.NodeRegistry
	gossipManager interfaces.GossipService

	store          *store.Store
	clusterStarter *bringup.ClusterStarter

	shutdown     chan struct{}
	shutdownOnce *sync.Once

	fileStorer filestore.Store

	accesses chan *accessTimeUpdate
	eg       *errgroup.Group
	egCancel context.CancelFunc

	clock clockwork.Clock
}

func clearPrevCache(dir string, currentSubDir string) error {
	if exists, err := disk.FileExists(context.Background(), dir); err != nil || !exists {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return status.InternalErrorf("failed to read directory %q: %s", dir, err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if e.Name() == currentSubDir {
			continue
		}
		path := filepath.Join(dir, e.Name())
		if err := os.RemoveAll(path); err != nil {
			return status.InternalErrorf("failed to delete dir %q: %s", path, err)
		}
	}
	return nil
}

func Register(env *real_environment.RealEnv) error {
	if *hostName == "" {
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

	if *clearCacheOnStartup {
		log.Warningf("Clearing cache dir %q on startup!", *rootDirectory)
		if err := os.RemoveAll(*rootDirectory); err != nil {
			return err
		}
	} else if *clearPrevCacheOnStartup {
		if err := clearPrevCache(*rootDirectory, *subdir); err != nil {
			return status.InternalErrorf("failed to delete cache from previous run: %s", err)
		}
	}

	rcConfig := &Config{
		RootDir:           filepath.Join(*rootDirectory, *subdir),
		Hostname:          *hostName,
		ListenAddr:        *listen,
		HTTPPort:          *httpPort,
		GRPCPort:          *gRPCPort,
		Partitions:        ps,
		PartitionMappings: *partitionMappings,
		LogDBConfigType:   store.LargeMemLogDBConfigType,
	}
	rc, err := NewRaftCache(env, rcConfig)
	if err != nil {
		return status.InternalErrorf("Error enabling raft cache: %s", err.Error())
	}
	log.Debugf("Started raft cache on %q", rcConfig.RootDir)
	env.SetCache(rc)
	statusz.AddSection("raft_cache", "Raft Cache", rc)
	env.GetHealthChecker().RegisterShutdownFunction(
		func(ctx context.Context) error {
			rc.Stop(ctx)
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
		accesses:     make(chan *accessTimeUpdate, *atimeBufferSize),
		clock:        env.GetClock(),
	}

	if err := disk.EnsureDirectoryExists(conf.RootDir); err != nil {
		return nil, err
	}

	if env.GetGossipService() == nil {
		return nil, status.FailedPreconditionError("raft cache requires gossip be enabled")
	}
	rc.gossipManager = env.GetGossipService()

	rc.raftAddr = fmt.Sprintf("%s:%d", conf.Hostname, conf.HTTPPort)
	rc.grpcAddr = fmt.Sprintf("%s:%d", conf.Hostname, conf.GRPCPort)
	grpcListeningAddr := fmt.Sprintf("%s:%d", conf.ListenAddr, conf.GRPCPort)

	store, err := store.New(rc.env, conf.RootDir, rc.raftAddr, rc.grpcAddr, grpcListeningAddr, rc.conf.Partitions, rc.conf.LogDBConfigType)
	if err != nil {
		return nil, err
	}
	rc.store = store
	if err := rc.store.Start(); err != nil {
		return nil, err
	}

	// bring up any clusters that were previously configured, or
	// bootstrap a new one based on the join params in the config.
	rc.clusterStarter = bringup.New(rc.grpcAddr, rc.gossipManager, rc.store)
	if err := rc.clusterStarter.InitializeClusters(); err != nil {
		return nil, err
	}

	go func() {
		for !rc.clusterStarter.Done() {
			time.Sleep(100 * time.Millisecond)
		}
	}()

	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		return rc.Stop(ctx)
	})

	ctx, cancelFunc := context.WithCancel(env.GetServerContext())
	eg, gctx := errgroup.WithContext(ctx)
	rc.eg = eg
	rc.egCancel = cancelFunc
	atimeWriteBatchSize := *atimeWriteBatchSize
	rc.eg.Go(func() error {
		return rc.processAccessTimeUpdates(gctx, rc.shutdown, atimeWriteBatchSize)
	})

	return rc, nil
}

func (rc *RaftCache) Statusz(ctx context.Context) string {
	buf := "<pre>"
	buf += fmt.Sprintf("Root directory: %q\n", rc.conf.RootDir)
	buf += fmt.Sprintf("Raft (HTTP) addr: %s\n", rc.raftAddr)
	buf += fmt.Sprintf("GRPC addr: %s\n", rc.grpcAddr)
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

	if rc.store == nil {
		return status.UnavailableError("store is still initializing")
	}

	if !rc.store.ReplicasInitDone() {
		return status.UnavailableError("replicas are still initializing")
	}

	return nil
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

func (rc *RaftCache) makeFileRecord(ctx context.Context, r *rspb.ResourceName) (*sgpb.FileRecord, error) {
	rn := digest.ResourceNameFromProto(r)
	if err := rn.Validate(); err != nil {
		return nil, err
	}

	groupID, partID, err := rc.lookupGroupAndPartitionID(ctx, rn.GetInstanceName())
	if err != nil {
		return nil, err
	}

	return &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
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

func (rc *RaftCache) fileMetadataKey(fr *sgpb.FileRecord) ([]byte, error) {
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
	rc.sendAccessTimeUpdate(fileMetadataKey, md.GetLastAccessUsec())
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
		now := rc.clock.Now()
		md := &sgpb.FileMetadata{
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

func (rc *RaftCache) Stop(ctx context.Context) error {
	rc.shutdownOnce.Do(func() {
		close(rc.shutdown)
		if rc.egCancel != nil {
			rc.egCancel()
			rc.eg.Wait()
			log.Infof("raft cache waitgroups finished")
		}
		rc.store.Stop(ctx)
		log.Infof("raft cache store stopped")
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

type keyAndLastAccessUsec struct {
	key            []byte
	lastAccessUsec int64
}

type findResult struct {
	missingDigests []*repb.Digest
	atimeUpdates   []keyAndLastAccessUsec
}

type getMultiResult struct {
	found        map[*repb.Digest][]byte
	atimeUpdates []keyAndLastAccessUsec
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
		var res findResult
		batchRsp := rbuilder.NewBatchResponseFromProto(rsp.GetBatch())
		for i, k := range keys {
			findRsp, err := batchRsp.FindResponse(i)
			if err != nil {
				return nil, err
			}
			if findRsp.GetPresent() {
				res.atimeUpdates = append(res.atimeUpdates, keyAndLastAccessUsec{
					key:            k.Key,
					lastAccessUsec: findRsp.GetLastAccessUsec(),
				})
			} else {
				fr := k.Meta.(*sgpb.FileRecord)
				res.missingDigests = append(res.missingDigests, fr.GetDigest())
			}
		}
		return res, nil
	}, sender.WithConsistencyMode(rfpb.Header_RANGELEASE))
	if err != nil {
		return nil, err
	}

	var allMissingDigests []*repb.Digest
	for _, rsp := range rsps {
		res, ok := rsp.(findResult)
		if !ok {
			return nil, status.InternalError("response not of type findResult")
		}

		for _, p := range res.atimeUpdates {
			rc.sendAccessTimeUpdate(p.key, p.lastAccessUsec)
		}

		allMissingDigests = append(allMissingDigests, res.missingDigests...)
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
		res := getMultiResult{
			found: make(map[*repb.Digest][]byte),
		}
		batchRsp := rbuilder.NewBatchResponseFromProto(rsp.GetBatch())
		for i, k := range keys {
			r, err := batchRsp.GetResponse(i)
			if err == nil {
				fr := k.Meta.(*sgpb.FileRecord)
				res.found[fr.GetDigest()] = r.GetFileMetadata().GetStorageMetadata().GetInlineMetadata().GetData()
				res.atimeUpdates = append(res.atimeUpdates, keyAndLastAccessUsec{
					key:            k.Key,
					lastAccessUsec: r.GetFileMetadata().GetLastAccessUsec(),
				})
			}
		}
		return res, nil
	}, sender.WithConsistencyMode(rfpb.Header_RANGELEASE))
	if err != nil {
		return nil, err
	}

	allFound := make(map[*repb.Digest][]byte)
	for _, rsp := range rsps {
		res, ok := rsp.(getMultiResult)
		if !ok {
			return nil, status.InternalError("response not of type getMultiResult")
		}

		for k, v := range res.found {
			allFound[k] = v
		}

		for _, p := range res.atimeUpdates {
			rc.sendAccessTimeUpdate(p.key, p.lastAccessUsec)
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

func (rc *RaftCache) olderThanThreshold(t time.Time, threshold time.Duration) bool {
	age := rc.clock.Since(t)
	return age >= threshold
}

func (rc *RaftCache) sendAccessTimeUpdate(key []byte, lastAccessUsec int64) {
	atime := time.UnixMicro(lastAccessUsec)
	if rc.clock.Since(atime) < *atimeUpdateThreshold {
		return
	}

	up := &accessTimeUpdate{
		key: key,
	}

	// If the atimeBufferSize is 0, non-blocking writes do not make sense,
	// so in that case just do a regular channel send. Otherwise; use a non-
	// blocking channel send.
	if *atimeBufferSize == 0 {
		rc.accesses <- up
	} else {
		select {
		case rc.accesses <- up:
			return
		default:
			log.Warningf("Dropping atime update for %q", key)
		}
	}
}
func (rc *RaftCache) processAccessTimeUpdates(ctx context.Context, quitChan chan struct{}, atimeWriteBatchSize int) error {
	var keys []*sender.KeyMeta
	timer := time.NewTimer(atimeFlushPeriod)
	defer timer.Stop()
	ctx, cancel := background.ExtendContextForFinalization(ctx, 10*time.Second)
	defer cancel()

	flush := func() {
		if len(keys) == 0 {
			return
		}

		_, err := rc.sender().RunMultiKey(ctx, keys, func(c rfspb.ApiClient, h *rfpb.Header, keys []*sender.KeyMeta) (interface{}, error) {
			batch := rbuilder.NewBatchBuilder()
			for _, k := range keys {
				batch.Add(&rfpb.UpdateAtimeRequest{
					Key:            k.Key,
					AccessTimeUsec: k.Meta.(int64),
				})
			}
			batchProto, err := batch.ToProto()
			if err != nil {
				return nil, err
			}
			return c.SyncPropose(ctx, &rfpb.SyncProposeRequest{
				Header: h,
				Batch:  batchProto,
			})
		})
		if err != nil {
			log.Warningf("could not update atimes: %s", err)
			return
		}
		keys = nil

		timer.Reset(atimeFlushPeriod)
	}

	for {
		select {
		case accessTimeUpdate := <-rc.accesses:
			keys = append(keys, &sender.KeyMeta{
				Key:  accessTimeUpdate.key,
				Meta: rc.clock.Now().UnixMicro(),
			})
			if len(keys) >= atimeWriteBatchSize {
				flush()
			}
		case <-timer.C:
			flush()
		case <-quitChan:
			// Drain any updates in the queue before exiting.
			log.Infof("drain updates in queue")
			flush()
			return nil
		}
	}
}

func (rc *RaftCache) TestingWaitForGC() error {
	return rc.store.TestingWaitForGC()
}

func (rc *RaftCache) TestingFlush() {
	rc.store.TestingFlush()
}

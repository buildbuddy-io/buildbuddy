package metadata

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"

	stdFlag "flag"

	_ "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger"
	mdpb "github.com/buildbuddy-io/buildbuddy/proto/metadata"
	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	rfspb "github.com/buildbuddy-io/buildbuddy/proto/raft_service"
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
}

// data needed to update last access time.
type accessTimeUpdate struct {
	key []byte
}

type Server struct {
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

func NewFromFlags(env *real_environment.RealEnv) (*Server, error) {
	if *hostName == "" {
		return nil, status.FailedPreconditionError("raft hostname must be configured")
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
			return nil, err
		}
	} else if *clearPrevCacheOnStartup {
		if err := clearPrevCache(*rootDirectory, *subdir); err != nil {
			return nil, status.InternalErrorf("failed to delete cache from previous run: %s", err)
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
	}
	return New(env, rcConfig)
}

func New(env environment.Env, conf *Config) (*Server, error) {
	rc := &Server{
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

	store, err := store.New(rc.env, conf.RootDir, rc.raftAddr, rc.grpcAddr, grpcListeningAddr, rc.conf.Partitions)
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
	rc.eg.Go(func() error {
		return rc.processAccessTimeUpdates(gctx, rc.shutdown)
	})

	return rc, nil
}

func (rc *Server) Statusz(ctx context.Context) string {
	buf := "<pre>"
	buf += fmt.Sprintf("Root directory: %q\n", rc.conf.RootDir)
	buf += fmt.Sprintf("Raft (HTTP) addr: %s\n", rc.raftAddr)
	buf += fmt.Sprintf("GRPC addr: %s\n", rc.grpcAddr)
	buf += fmt.Sprintf("ClusterStarter complete: %t\n", rc.clusterStarter.Done())
	buf += "</pre>"
	return buf
}

func (rc *Server) sender() *sender.Sender {
	return rc.store.Sender()
}

// Check implements the Checker interface and is called by the health checker to
// determine whether the service is ready to serve.
// The service is ready to serve when it knows which nodes contain the meta range
// and can contact those nodes. We test this by doing a SyncRead of the
// initial cluster setup time key/val which is stored in the Meta Range.
func (rc *Server) Check(ctx context.Context) error {
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

func (rc *Server) Stop(ctx context.Context) error {
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

func (rc *Server) fileMetadataKey(fr *sgpb.FileRecord) ([]byte, error) {
	pebbleKey, err := rc.fileStorer.PebbleKey(fr)
	if err != nil {
		return nil, err
	}
	return pebbleKey.Bytes(filestore.Version5)
}

func (rc *Server) fileRecordsToKeyMetas(fileRecords []*sgpb.FileRecord) ([]*sender.KeyMeta, error) {
	var keys []*sender.KeyMeta
	for _, fileRecord := range fileRecords {
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

func (rc *Server) olderThanThreshold(t time.Time, threshold time.Duration) bool {
	age := rc.clock.Since(t)
	return age >= threshold
}

func (rc *Server) sendAccessTimeUpdate(key []byte, lastAccessUsec int64) {
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
func (rc *Server) processAccessTimeUpdates(ctx context.Context, quitChan chan struct{}) error {
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
			if len(keys) >= *atimeWriteBatchSize {
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

type getMetadataResult struct {
	found        map[*sgpb.FileRecord]*sgpb.FileMetadata
	atimeUpdates []keyAndLastAccessUsec
}

func (rc *Server) Get(ctx context.Context, req *mdpb.GetRequest) (*mdpb.GetResponse, error) {
	keys, err := rc.fileRecordsToKeyMetas(req.GetFileRecords())
	if err != nil {
		return nil, err
	}

	// Shard the query by key and query shards in parallel.
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
		rsp, err := c.SyncRead(ctx, &rfpb.SyncReadRequest{
			Header: h,
			Batch:  batchProto,
		})
		if err != nil {
			return nil, err
		}
		res := getMetadataResult{
			found: make(map[*sgpb.FileRecord]*sgpb.FileMetadata),
		}
		batchRsp := rbuilder.NewBatchResponseFromProto(rsp.GetBatch())
		for i, k := range keys {
			r, err := batchRsp.GetResponse(i)
			if err == nil {
				fr := k.Meta.(*sgpb.FileRecord)
				res.found[fr] = r.GetFileMetadata()
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

	// Combine the partial responses from each shard.
	allFound := make(map[*sgpb.FileRecord]*sgpb.FileMetadata, len(req.GetFileRecords()))
	for _, rsp := range rsps {
		res, ok := rsp.(getMetadataResult)
		if !ok {
			return nil, status.InternalError("response not of type getMetadataResult")
		}

		for k, v := range res.found {
			allFound[k] = v
		}

		for _, p := range res.atimeUpdates {
			rc.sendAccessTimeUpdate(p.key, p.lastAccessUsec)
		}
	}

	// Assemble the response proto.
	rsp := &mdpb.GetResponse{
		FileMetadatas: make([]*sgpb.FileMetadata, len(req.GetFileRecords())),
	}
	for i, fileRecord := range req.GetFileRecords() {
		rsp.FileMetadatas[i] = allFound[fileRecord]
	}
	return rsp, nil
}

type findMetadataResult struct {
	found        map[*sgpb.FileRecord]bool
	atimeUpdates []keyAndLastAccessUsec
}

func (rc *Server) Find(ctx context.Context, req *mdpb.FindRequest) (*mdpb.FindResponse, error) {
	keys, err := rc.fileRecordsToKeyMetas(req.GetFileRecords())
	if err != nil {
		return nil, err
	}

	// Shard the query by key and query shards in parallel.
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
		rsp, err := c.SyncRead(ctx, &rfpb.SyncReadRequest{
			Header: h,
			Batch:  batchProto,
		})
		if err != nil {
			return nil, err
		}
		res := findMetadataResult{
			found: make(map[*sgpb.FileRecord]bool),
		}
		batchRsp := rbuilder.NewBatchResponseFromProto(rsp.GetBatch())
		for i, k := range keys {
			findRsp, err := batchRsp.FindResponse(i)
			if err != nil {
				return nil, err
			}
			present := findRsp.GetPresent()
			res.found[k.Meta.(*sgpb.FileRecord)] = present
			if present {
				res.atimeUpdates = append(res.atimeUpdates, keyAndLastAccessUsec{
					key:            k.Key,
					lastAccessUsec: findRsp.GetLastAccessUsec(),
				})
			}
		}
		return res, nil
	}, sender.WithConsistencyMode(rfpb.Header_RANGELEASE))
	if err != nil {
		return nil, err
	}

	// Combine the partial responses from each shard.
	allFound := make(map[*sgpb.FileRecord]bool, len(req.GetFileRecords()))
	for _, rsp := range rsps {
		res, ok := rsp.(findMetadataResult)
		if !ok {
			return nil, status.InternalError("response not of type findResult")
		}

		for k, v := range res.found {
			allFound[k] = v
		}

		for _, p := range res.atimeUpdates {
			rc.sendAccessTimeUpdate(p.key, p.lastAccessUsec)
		}
	}

	// Assemble the response proto.
	rsp := &mdpb.FindResponse{
		FindResponses: make([]*mdpb.FindResponse_FindOperationResponse, len(req.GetFileRecords())),
	}

	for i, fileRecord := range req.GetFileRecords() {
		present, ok := allFound[fileRecord]
		rsp.FindResponses[i] = &mdpb.FindResponse_FindOperationResponse{
			Present: ok && present,
		}
	}
	return rsp, nil
}

func (rc *Server) setOperationsToKeyMetas(setOperations []*mdpb.SetRequest_SetOperation) ([]*sender.KeyMeta, error) {
	var keys []*sender.KeyMeta
	for _, setOperation := range setOperations {
		fileMetadataKey, err := rc.fileMetadataKey(setOperation.GetFileMetadata().GetFileRecord())
		if err != nil {
			return nil, err
		}
		keys = append(keys, &sender.KeyMeta{Key: fileMetadataKey, Meta: setOperation})
	}
	return keys, nil
}

func (rc *Server) Set(ctx context.Context, req *mdpb.SetRequest) (*mdpb.SetResponse, error) {
	keys, err := rc.setOperationsToKeyMetas(req.GetSetOperations())
	if err != nil {
		return nil, err
	}

	// Shard the query by key and query shards in parallel.
	_, err = rc.sender().RunMultiKey(ctx, keys, func(c rfspb.ApiClient, h *rfpb.Header, keys []*sender.KeyMeta) (interface{}, error) {
		// sender runMultiKeyFuncs require that we return an interface{}
		// and error, but in this case there's no value to return, so
		// always return nil for the interface, even on success.
		batch := rbuilder.NewBatchBuilder()
		for _, k := range keys {
			setOp := k.Meta.(*mdpb.SetRequest_SetOperation)
			fm := setOp.GetFileMetadata()
			fm.LastAccessUsec = rc.clock.Now().UnixMicro()
			batch.Add(&rfpb.SetRequest{
				Key:          k.Key,
				FileMetadata: fm,
			})
		}
		batchProto, err := batch.ToProto()
		if err != nil {
			return nil, err
		}
		rsp, err := c.SyncPropose(ctx, &rfpb.SyncProposeRequest{
			Header: h,
			Batch:  batchProto,
		})
		if err != nil {
			return nil, err
		}
		return nil, rbuilder.NewBatchResponseFromProto(rsp.GetBatch()).AnyError()
	}, sender.WithConsistencyMode(rfpb.Header_RANGELEASE))
	if err != nil {
		return nil, err
	}

	return &mdpb.SetResponse{}, nil
}

func (rc *Server) deleteOperationsToKeyMetas(deleteOperations []*mdpb.DeleteRequest_DeleteOperation) ([]*sender.KeyMeta, error) {
	var keys []*sender.KeyMeta
	for _, deleteOperation := range deleteOperations {
		fileMetadataKey, err := rc.fileMetadataKey(deleteOperation.GetFileRecord())
		if err != nil {
			return nil, err
		}
		keys = append(keys, &sender.KeyMeta{Key: fileMetadataKey, Meta: deleteOperation})
	}
	return keys, nil
}

func (rc *Server) Delete(ctx context.Context, req *mdpb.DeleteRequest) (*mdpb.DeleteResponse, error) {
	keys, err := rc.deleteOperationsToKeyMetas(req.GetDeleteOperations())
	if err != nil {
		return nil, err
	}

	// Shard the query by key and query shards in parallel.
	_, err = rc.sender().RunMultiKey(ctx, keys, func(c rfspb.ApiClient, h *rfpb.Header, keys []*sender.KeyMeta) (interface{}, error) {
		// sender runMultiKeyFuncs require that we return an interface{}
		// and error, but in this case there's no value to return, so
		// always return nil for the interface, even on success.
		batch := rbuilder.NewBatchBuilder()
		for _, k := range keys {
			deleteOp := k.Meta.(*mdpb.DeleteRequest_DeleteOperation)
			batch.Add(&rfpb.DeleteRequest{
				Key:        k.Key,
				MatchAtime: deleteOp.GetMatchAtime(),
			})
		}
		batchProto, err := batch.ToProto()
		if err != nil {
			return nil, err
		}
		rsp, err := c.SyncPropose(ctx, &rfpb.SyncProposeRequest{
			Header: h,
			Batch:  batchProto,
		})
		if err != nil {
			return nil, err
		}
		return nil, rbuilder.NewBatchResponseFromProto(rsp.GetBatch()).AnyError()
	}, sender.WithConsistencyMode(rfpb.Header_RANGELEASE))
	if err != nil {
		return nil, err
	}

	return &mdpb.DeleteResponse{}, nil
}

func (rc *Server) TestingWaitForGC() {
	rc.store.TestingWaitForGC()
}

func (rc *Server) TestingFlush() {
	rc.store.TestingFlush()
}

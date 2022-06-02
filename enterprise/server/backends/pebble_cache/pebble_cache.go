package pebble_cache

import (
	"bytes"
	"context"
	"flag"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
)

var (
	rootDirectory       = flag.String("cache.pebble.root_directory", "", "The root directory to store the database in.")
	blockCacheSizeBytes = flag.Int64("cache.pebble.block_cache_size_bytes", 1000*megabyte, "How much ram to give the block cache")
	partitions          = flagutil.Slice("cache.pebble.partitions", []disk.Partition{}, "")
	partitionMappings   = flagutil.Slice("cache.pebble.partition_mappings", []disk.PartitionMapping{}, "")
)

// TODO:
//  - add a pacer that deletes files as new ones are written when the cache is
//    above a certain size
//  - add a queue that bumps file ttls
//  - add a flag that ingests a disk cache
const (
	// cutoffThreshold is the point above which a janitor thread will run
	// and delete the oldest items from the cache.
	janitorCutoffThreshold = .90

	// janitorCheckPeriod is how often the janitor thread will wake up to
	// check the cache size.
	JanitorCheckPeriod = 1 * time.Second
	megabyte           = 1e6

	defaultPartitionID       = "default"
	partitionDirectoryPrefix = "PT"
)

type Options struct {
	RootDirectory       string
	Partitions          []disk.Partition
	PartitionMappings   []disk.PartitionMapping
	MaxSizeBytes        int64
	BlockCacheSizeBytes int64
}

type PebbleCache struct {
	opts *Options

	env       environment.Env
	db        *pebble.DB
	isolation *rfpb.Isolation

	waitGroup *sync.WaitGroup
	quitChan  chan struct{}
	eg        *errgroup.Group
}

func Register(env environment.Env) error {
	if *rootDirectory == "" {
		return nil
	}
	if env.GetCache() != nil {
		log.Warning("A cache has already been registered, skipping registering pebble_cache.")
		return nil
	}
	opts := &Options{
		RootDirectory:       *rootDirectory,
		Partitions:          *partitions,
		PartitionMappings:   *partitionMappings,
		BlockCacheSizeBytes: *blockCacheSizeBytes,
		MaxSizeBytes:        cache_config.MaxSizeBytes(),
	}
	c, err := NewPebbleCache(env, opts)
	if err != nil {
		return status.InternalErrorf("Error configuring pebble cache: %s", err)
	}
	env.SetCache(c)
	c.Start()
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		return c.Stop()
	})
	return nil
}

// validateOpts validates that each partition mapping points to a partition.
func validateOpts(opts *Options) error {
	if opts.MaxSizeBytes == 0 {
		return status.FailedPreconditionError("Pebble cache size must be greater than 0")
	}

	for _, pm := range opts.PartitionMappings {
		found := false
		for _, p := range opts.Partitions {
			if p.ID == pm.PartitionID {
				found = true
				break
			}
		}
		if !found {
			return status.NotFoundErrorf("Mapping to unknown partition %q", pm.PartitionID)
		}
	}
	return nil
}

func ensureDefaultPartitionExists(opts *Options) {
	foundDefaultPartition := false
	for _, pm := range opts.PartitionMappings {
		if pm.PartitionID == defaultPartitionID {
			foundDefaultPartition = true
		}
	}
	if foundDefaultPartition {
		return
	}
	opts.Partitions = append(opts.Partitions, disk.Partition{
		ID:           defaultPartitionID,
		MaxSizeBytes: opts.MaxSizeBytes,
	})
}

func NewPebbleCache(env environment.Env, opts *Options) (*PebbleCache, error) {
	if err := validateOpts(opts); err != nil {
		return nil, err
	}
	ensureDefaultPartitionExists(opts)

	c := pebble.NewCache(*blockCacheSizeBytes)
	defer c.Unref()

	db, err := pebble.Open(opts.RootDirectory, &pebble.Options{Cache: c})
	if err != nil {
		return nil, err
	}
	pc := &PebbleCache{
		opts:      opts,
		env:       env,
		db:        db,
		quitChan:  make(chan struct{}),
		eg:        &errgroup.Group{},
		waitGroup: &sync.WaitGroup{},
		isolation: &rfpb.Isolation{
			CacheType:   rfpb.Isolation_CAS_CACHE,
			PartitionId: defaultPartitionID,
		},
	}
	if err := disk.EnsureDirectoryExists(pc.blobDir()); err != nil {
		return nil, err
	}
	return pc, nil
}

func (p *PebbleCache) lookupPartitionID(ctx context.Context, remoteInstanceName string) (string, error) {
	auth := p.env.GetAuthenticator()
	if auth == nil {
		return defaultPartitionID, nil
	}
	user, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return defaultPartitionID, nil
	}
	for _, pm := range p.opts.PartitionMappings {
		if pm.GroupID == user.GetGroupID() && strings.HasPrefix(remoteInstanceName, pm.Prefix) {
			return pm.PartitionID, nil
		}
	}
	return defaultPartitionID, nil
}

func (p *PebbleCache) WithIsolation(ctx context.Context, cacheType interfaces.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
	partID, err := p.lookupPartitionID(ctx, remoteInstanceName)
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

	clone := *p
	clone.isolation = newIsolation
	return &clone, nil
}

func (p *PebbleCache) makeFileRecord(ctx context.Context, d *repb.Digest) (*rfpb.FileRecord, error) {
	_, err := digest.Validate(d)
	if err != nil {
		return nil, err
	}

	return &rfpb.FileRecord{
		Isolation: p.isolation,
		Digest:    d,
	}, nil
}

func (p *PebbleCache) blobDir() string {
	partDir := partitionDirectoryPrefix + p.isolation.GetPartitionId()
	return filepath.Join(p.opts.RootDirectory, "blobs", partDir)
}

func hasFileMetadata(iter *pebble.Iterator, fileMetadaKey []byte) bool {
	if iter.SeekGE(fileMetadaKey) && bytes.Compare(iter.Key(), fileMetadaKey) == 0 {
		return true
	}
	return false
}

func (p *PebbleCache) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	iter := p.db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	fileRecord, err := p.makeFileRecord(ctx, d)
	if err != nil {
		return false, err
	}
	fileMetadaKey, err := constants.FileMetadataKey(fileRecord)
	if err != nil {
		return false, err
	}
	return hasFileMetadata(iter, fileMetadaKey), nil
}

func (p *PebbleCache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	iter := p.db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	sort.Slice(digests, func(i, j int) bool {
		return digests[i].GetHash() < digests[j].GetHash()
	})

	var missing []*repb.Digest
	for _, d := range digests {
		fileRecord, err := p.makeFileRecord(ctx, d)
		if err != nil {
			return nil, err
		}
		fileMetadaKey, err := constants.FileMetadataKey(fileRecord)
		if err != nil {
			return nil, err
		}
		if !hasFileMetadata(iter, fileMetadaKey) {
			missing = append(missing, d)
		}
	}
	return missing, nil
}

func (p *PebbleCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	rc, err := p.Reader(ctx, d, 0, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (p *PebbleCache) GetMulti(ctx context.Context, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	foundMap := make(map[*repb.Digest][]byte, len(digests))
	for _, d := range digests {
		data, err := p.Get(ctx, d)
		if status.IsNotFoundError(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		foundMap[d] = data
	}
	return foundMap, nil
}

func (p *PebbleCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	wc, err := p.Writer(ctx, d)
	if err != nil {
		return err
	}
	if _, err := wc.Write(data); err != nil {
		return err
	}
	return wc.Close()
}

func (p *PebbleCache) SetMulti(ctx context.Context, kvs map[*repb.Digest][]byte) error {
	for d, data := range kvs {
		if err := p.Set(ctx, d, data); err != nil {
			return err
		}
	}
	return nil
}

func (p *PebbleCache) Delete(ctx context.Context, d *repb.Digest) error {
	return nil
}

// Low level interface used for seeking and stream-writing.
func (p *PebbleCache) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	iter := p.db.NewIter(nil /*default iterOptions*/)
	defer iter.Close()

	fileRecord, err := p.makeFileRecord(ctx, d)
	if err != nil {
		return nil, err
	}
	fileMetadataKey, err := constants.FileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}

	// First, lookup the FileMetadata. If it's not found, we don't have the file.
	found := iter.SeekGE(fileMetadataKey)
	if !found || bytes.Compare(fileMetadataKey, iter.Key()) != 0 {
		return nil, status.NotFoundErrorf("file %q not found", fileMetadataKey)
	}
	fileMetadata := &rfpb.FileMetadata{}
	if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
		return nil, status.InternalErrorf("error reading file %q metadata", fileMetadataKey)
	}
	if p.isolation.GetCacheType() == rfpb.Isolation_ACTION_CACHE {
		// for AC items, we need to examine the remote_instance_name as
		// well and make sure there is a match.
		if fileMetadata.GetFileRecord().GetIsolation().GetRemoteInstanceName() != p.isolation.GetRemoteInstanceName() {
			return nil, status.NotFoundErrorf("file %q not found", fileMetadataKey)
		}
	}
	return filestore.NewReader(ctx, p.blobDir(), iter, fileMetadata.GetStorageMetadata())
}

type funcCloser struct {
	filestore.WriteCloserMetadata
	closeFn func() error
}

func (dc *funcCloser) Close() error {
	if err := dc.WriteCloserMetadata.Close(); err != nil {
		return err
	}
	return dc.closeFn()
}

func (p *PebbleCache) Writer(ctx context.Context, d *repb.Digest) (io.WriteCloser, error) {
	fileRecord, err := p.makeFileRecord(ctx, d)
	if err != nil {
		return nil, err
	}
	fileMetadataKey, err := constants.FileMetadataKey(fileRecord)
	if err != nil {
		return nil, err
	}
	wcm, err := filestore.NewWriter(ctx, p.blobDir(), p.db.NewBatch(), fileRecord)
	if err != nil {
		return nil, err
	}
	dc := &funcCloser{wcm, func() error {
		md := &rfpb.FileMetadata{
			FileRecord:      fileRecord,
			StorageMetadata: wcm.Metadata(),
		}
		protoBytes, err := proto.Marshal(md)
		if err != nil {
			return err
		}
		err = p.db.Set(fileMetadataKey, protoBytes, &pebble.WriteOptions{Sync: false})
		return err
	}}
	return dc, nil
}

func (p *PebbleCache) TestingWaitForGC() {
	p.waitGroup.Wait()
}

const (
	sampleN        = 10
	samplePoolSize = 16
)

type evictionPoolEntry struct {
	timestamp       int64
	fileRecord      *rfpb.FileRecord
	fileMetadataKey []byte
	filePath        string
}

type partitionEvictor struct {
	part      disk.Partition
	blobDir   string
	reader    pebble.Reader
	writer    pebble.Writer
	waitGroup *sync.WaitGroup

	samplePool     []*evictionPoolEntry
	totalSizeBytes int64
	totalCount     int64
	casCount       int64
	acCount        int64
}

func (e *partitionEvictor) partitionLimits() ([]byte, []byte) {
	start := append([]byte(e.part.ID), constants.MinByte)
	end := append([]byte(e.part.ID), constants.MaxByte)
	return start, end
}

func (e *partitionEvictor) iter() *pebble.Iterator {
	start, end := e.partitionLimits()
	iter := e.reader.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	iter.SeekGE(start)
	return iter
}

func getLastUse(info os.FileInfo) int64 {
	stat := info.Sys().(*syscall.Stat_t)
	// Super Gross! https://github.com/golang/go/issues/31735
	value := reflect.ValueOf(stat)
	var ts syscall.Timespec
	if timeField := value.Elem().FieldByName("Atimespec"); timeField.IsValid() {
		ts = timeField.Interface().(syscall.Timespec)
	} else if timeField := value.Elem().FieldByName("Atim"); timeField.IsValid() {
		ts = timeField.Interface().(syscall.Timespec)
	} else {
		ts = syscall.Timespec{}
	}
	t := time.Unix(ts.Sec, ts.Nsec)
	return t.UnixNano()
}

var digestRunes = []rune("abcdef1234567890")

func (e *partitionEvictor) randomKey(n int) []byte {
	randKey := e.part.ID

	randInt := rand.Int63n(e.totalCount)
	if randInt < e.casCount {
		randKey += "/cas/"
	} else {
		randKey += "/ac/"
	}
	for i := 0; i < n; i++ {
		randKey += string(digestRunes[rand.Intn(len(digestRunes))])
	}
	return []byte(randKey)
}

func (e *partitionEvictor) randomSample(iter *pebble.Iterator, k int) ([]*evictionPoolEntry, error) {
	log.Printf("Sampling")
	samples := make([]*evictionPoolEntry, 0, k)
	fileMetadata := &rfpb.FileMetadata{}

	seen := make(map[string]struct{}, len(e.samplePool))
	for _, entry := range e.samplePool {
		seen[string(entry.fileMetadataKey)] = struct{}{}
	}

	// generate k random digests and for each:
	//   - seek to the next valid key, and return that file record
	for i := 0; i < k*2; i++ {
		randKey := e.randomKey(64)
		valid := iter.SeekGE(randKey)
		if !valid {
			continue
		}
		if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
			return nil, err
		}
		if _, ok := seen[string(iter.Key())]; ok {
			continue
		}
		seen[string(iter.Key())] = struct{}{}

		file, err := constants.FileKey(fileMetadata.GetFileRecord())
		if err != nil {
			return nil, err
		}
		filePath := filepath.Join(e.blobDir, string(file))
		info, err := os.Stat(filePath)
		if err != nil {
			// The file could have already been deleted and our
			// iterator is not yet aware of that.
			continue
		}
		fileMetadataKey := make([]byte, len(iter.Key()))
		copy(fileMetadataKey, iter.Key())
		samples = append(samples, &evictionPoolEntry{
			timestamp:       getLastUse(info),
			fileRecord:      fileMetadata.GetFileRecord(),
			filePath:        filePath,
			fileMetadataKey: fileMetadataKey,
		})
		if len(samples) == k {
			break
		}
	}

	return samples, nil
}

func (e *partitionEvictor) deleteFile(sample *evictionPoolEntry) error {
	if err := e.writer.Delete(sample.fileMetadataKey, &pebble.WriteOptions{Sync: true}); err != nil {
		return err
	}
	return disk.DeleteFile(context.TODO(), sample.filePath)
}

func (e *partitionEvictor) resampleK(k int) error {
	iter := e.iter()
	defer iter.Close()

	for i := 0; i < k; i++ {
		entries, err := e.randomSample(iter, sampleN)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			e.samplePool = append(e.samplePool, entry)
		}
	}

	for _, sample := range e.samplePool {
		info, err := os.Stat(sample.filePath)
		if err != nil {
			continue
		}
		sample.timestamp = getLastUse(info)
	}

	if len(e.samplePool) > 0 {
		sort.Slice(e.samplePool, func(i, j int) bool {
			return e.samplePool[i].timestamp < e.samplePool[j].timestamp
		})
	}

	if len(e.samplePool) > samplePoolSize {
		e.samplePool = e.samplePool[:samplePoolSize]
	}

	return nil
}

func (e *partitionEvictor) evict(count int) error {
	evicted := 0
	for evicted < count {
		lastCount := evicted

		// Resample every time we evict a key
		if err := e.resampleK(1); err != nil {
			return err
		}
		for i, sample := range e.samplePool {
			fileMetadaKey, err := constants.FileMetadataKey(sample.fileRecord)
			if err != nil {
				return err
			}
			_, closer, err := e.reader.Get(fileMetadaKey)
			if err == pebble.ErrNotFound {
				continue
			}
			closer.Close()
			if err := e.deleteFile(sample); err != nil {
				continue
			}
			log.Printf("Deleted file: %q", sample.filePath)
			evicted += 1
			e.samplePool = append(e.samplePool[:i], e.samplePool[i+1:]...)
			break
		}

		// If no candidates were evictable in the whole pool, resample
		// the pool.
		if lastCount == evicted {
			e.samplePool = e.samplePool[:0]
			if err := e.resampleK(samplePoolSize); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *partitionEvictor) computeSize() error {
	iter := e.iter()
	defer iter.Close()

	casPrefix := []byte(e.part.ID + "/cas/")
	acPrefix := []byte(e.part.ID + "/ac/")
	totalCount := int64(0)
	casCount := int64(0)
	acCount := int64(0)
	blobSizeBytes := int64(0)
	metadataSizeBytes := int64(0)
	fileMetadata := &rfpb.FileMetadata{}

	for iter.Next() {
		if err := proto.Unmarshal(iter.Value(), fileMetadata); err != nil {
			return err
		}
		blobSizeBytes += fileMetadata.GetFileRecord().GetDigest().GetSizeBytes()
		metadataSizeBytes += int64(len(iter.Value()))
		totalCount += 1

		// identify and count CAS vs AC files.
		if bytes.Contains(iter.Key(), casPrefix) {
			casCount += 1
		} else if bytes.Contains(iter.Key(), acPrefix) {
			acCount += 1
		} else {
			log.Warningf("Unidentified file (not CAS or AC): %q", iter.Key())
		}
	}

	e.totalCount = totalCount
	e.totalSizeBytes = blobSizeBytes // + metadataSizeBytes
	e.casCount = casCount
	e.acCount = acCount
	return nil
}

func (e *partitionEvictor) ttl() error {
	maxAllowedSize := int64(janitorCutoffThreshold * float64(e.part.MaxSizeBytes))
	e.waitGroup.Add(1)
	defer e.waitGroup.Done()
	for {
		if err := e.computeSize(); err != nil {
			return err
		}
		if e.totalSizeBytes < maxAllowedSize {
			break
		}
		log.Printf("Partition %q has size %d, maxAllowed: %d", e.part.ID, e.totalSizeBytes, maxAllowedSize)

		numToEvict := int(.001 * float64(e.totalCount))
		if numToEvict == 0 {
			numToEvict = 1
		}
		if err := e.evict(numToEvict); err != nil {
			return err
		}
	}
	return nil
}

func (e *partitionEvictor) run(quitChan chan struct{}) error {
	if err := e.computeSize(); err != nil {
		return err
	}
	if err := e.ttl(); err != nil {
		return err
	}

	for {
		select {
		case <-quitChan:
			return nil
		case <-time.After(JanitorCheckPeriod):
			if err := e.ttl(); err != nil {
				return err
			}
		}
	}
}

func (p *PebbleCache) runGC(quitChan chan struct{}) {
	evictors := make([]*partitionEvictor, len(p.opts.Partitions))
	for i, part := range p.opts.Partitions {
		evictors[i] = &partitionEvictor{
			part:       part,
			blobDir:    p.blobDir(),
			samplePool: make([]*evictionPoolEntry, 0, samplePoolSize),
			reader:     p.db,
			writer:     p.db,
			waitGroup:  p.waitGroup,
		}
	}

	for _, evictor := range evictors {
		evictor := evictor
		p.eg.Go(func() error {
			return evictor.run(quitChan)
		})
	}
}

func (p *PebbleCache) Start() error {
	p.quitChan = make(chan struct{}, 0)
	go p.runGC(p.quitChan)
	return nil
}

func (p *PebbleCache) Stop() error {
	close(p.quitChan)
	if err := p.eg.Wait(); err != nil {
		return err
	}
	return p.db.Flush()
}

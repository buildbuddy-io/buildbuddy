package pebble_cache

import (
	"bytes"
	"context"
	"flag"
	"io"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
)

var (
	rootDirectory = flag.String("cache.pebble.root_directory", "", "The root directory to store the database in.")
	blockCacheSizeBytes = flag.Int64("cache.pebble.block_cache_size_bytes", 1000 * megabyte, "How much ram to give the block cache")
)

const (
	megabyte = 1e6
)

type PebbleCache struct {
	db        *pebble.DB
	isolation *rfpb.Isolation

	remoteInstanceName string
	rootDirectory      string

	quitChan           chan struct{}
}

func Register(env environment.Env) error {
	if *rootDirectory == "" {
		return nil
	}
	if env.GetCache() != nil {
		log.Warning("A cache has already been registered, skipping registering pebble_cache.")
		return nil
	}
	maxSizeBytes := cache_config.MaxSizeBytes()
	if maxSizeBytes == 0 {
		return status.FailedPreconditionError("Cache size must be greater than 0 if pebble cache is enabled!")
	}
	c, err := NewPebbleCache(maxSizeBytes)
	if err != nil {
		return status.InternalErrorf("Error configuring pebble cache: %s", err)
	}
	env.SetCache(c)
	return nil
}

func NewPebbleCache(maxSizeBytes int64) (*PebbleCache, error) {
	c := pebble.NewCache(*blockCacheSizeBytes)
	defer c.Unref()

	db, err := pebble.Open(*rootDirectory, &pebble.Options{Cache: c})
	if err != nil {
		return nil, err
	}
	pc := &PebbleCache{
		db:            db,
		rootDirectory: *rootDirectory,
		quitChan:      make(chan struct{}),
	}
	if err := disk.EnsureDirectoryExists(pc.fileDir()); err != nil {
		return nil, err
	}
	return pc, nil
}

func (p *PebbleCache) WithIsolation(ctx context.Context, cacheType interfaces.CacheType, remoteInstanceName string) (interfaces.Cache, error) {
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

	clone := *p
	clone.isolation = newIsolation
	return &clone, nil
}

func (p *PebbleCache) makeFileRecord(ctx context.Context, d *repb.Digest) (*rfpb.FileRecord, error) {
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
		Isolation: p.isolation,
		Digest:    d,
	}, nil
}

func (p *PebbleCache) fileDir() string {
	return filepath.Join(p.rootDirectory, "blobs")
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
	if iter.SeekGE(fileMetadaKey) && bytes.Compare(iter.Key(), fileMetadaKey) == 0 {
		return true, nil
	}
	return false, nil
}

func (p *PebbleCache) FindMissing(ctx context.Context, digests []*repb.Digest) ([]*repb.Digest, error) {
	var missing []*repb.Digest
	// No parallelism here either. Not necessary for an in-memory cache.
	for _, d := range digests {
		ok, err := p.Contains(ctx, d)
		if err != nil {
			return nil, err
		}
		if !ok {
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
	return filestore.NewReader(ctx, p.fileDir(), iter, fileMetadata.GetStorageMetadata())
}

type dbCloser struct {
	filestore.WriteCloserMetadata
	closeFn func() error
}

func (dc *dbCloser) Close() error {
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
	wcm, err := filestore.NewWriter(ctx, p.fileDir(), p.db.NewBatch(), fileRecord)
	if err != nil {
		return nil, err
	}
	dc := &dbCloser{wcm, func() error {
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

func (p *PebbleCache) Start() error {
	return nil
}

func (p *PebbleCache) Stop() error {
	return nil
}

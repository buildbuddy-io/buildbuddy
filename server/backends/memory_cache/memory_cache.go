package memory_cache

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
)

var cacheInMemory = flag.Bool("cache.in_memory", false, "Whether or not to use the in_memory cache.")

const DefaultPartitionID = "default"

type Options struct {
	MaxSizeBytes      int64
	Partitions        []disk.Partition
	PartitionMappings []disk.PartitionMapping
}

type MemoryCache struct {
	authenticator     interfaces.Authenticator
	l                 interfaces.LRU[[]byte]
	lock              *sync.RWMutex
	atimeUpdater      interfaces.DigestOperator
	partitions        []disk.Partition
	partitionMappings []disk.PartitionMapping

	// Partition size tracking - protected by lock
	partitionSizes map[string]int64 // partitionID -> current size in bytes
	// Map from LRU key to partition ID (for eviction callback)
	keyPartitions map[string]string // LRU key -> partitionID
}

func (c *MemoryCache) RegisterAtimeUpdater(updater interfaces.DigestOperator) error {
	c.atimeUpdater = updater
	return nil
}

func sizeFn(value []byte) int64 {
	size := int64(0)
	if value != nil {
		size += int64(len(value))
	}
	return size
}

func Register(env *real_environment.RealEnv) error {
	if !*cacheInMemory {
		return nil
	}
	if env.GetCache() != nil {
		log.Warningf("Overriding configured cache with memory_cache.")
	}
	maxSizeBytes := cache_config.MaxSizeBytes()
	if maxSizeBytes == 0 {
		return status.FailedPreconditionError("Cache size must be greater than 0 if in_memory cache is enabled!")
	}

	c, err := NewMemoryCacheWithOptions(env.GetAuthenticator(), &Options{MaxSizeBytes: maxSizeBytes})
	if err != nil {
		return status.InternalErrorf("Error configuring in-memory cache: %s", err)
	}
	env.SetCache(c)
	return nil
}

func NewMemoryCache(authenticator interfaces.Authenticator, maxSizeBytes int64) (*MemoryCache, error) {
	return NewMemoryCacheWithOptions(authenticator, &Options{MaxSizeBytes: maxSizeBytes})
}

func NewMemoryCacheWithOptions(authenticator interfaces.Authenticator, opts *Options) (*MemoryCache, error) {
	if opts.MaxSizeBytes == 0 {
		return nil, status.FailedPreconditionError("Cache size must be greater than 0")
	}

	// Validate the provided partition config
	for _, pm := range opts.PartitionMappings {
		found := false
		for _, p := range opts.Partitions {
			if p.ID == pm.PartitionID {
				found = true
				break
			}
		}
		if !found {
			return nil, status.NotFoundErrorf("Mapping to unknown partition %q", pm.PartitionID)
		}
	}
	var totalPartitionSize int64
	for _, p := range opts.Partitions {
		totalPartitionSize += p.MaxSizeBytes
	}
	if totalPartitionSize > opts.MaxSizeBytes {
		return nil, status.FailedPreconditionErrorf("Sum of partition sizes (%d bytes) exceeds max cache size (%d bytes)", totalPartitionSize, opts.MaxSizeBytes)
	}

	m := &MemoryCache{
		authenticator:     authenticator,
		lock:              &sync.RWMutex{},
		partitions:        opts.Partitions,
		partitionMappings: opts.PartitionMappings,
		partitionSizes:    make(map[string]int64),
		keyPartitions:     make(map[string]string),
	}

	l, err := lru.NewLRU[[]byte](&lru.Config[[]byte]{
		MaxSize: opts.MaxSizeBytes,
		SizeFn:  sizeFn,
		OnEvict: func(key string, value []byte, reason lru.EvictionReason) {
			// Update partition size tracking when entries are evicted.
			// No locking needed - this callback is called from within LRU
			// operations (Add, Remove), which are always called while holding
			// m.lock.
			if partitionID, ok := m.keyPartitions[key]; ok {
				m.partitionSizes[partitionID] -= int64(len(value))
				delete(m.keyPartitions, key)
			}
		},
	})
	if err != nil {
		return nil, err
	}
	m.l = l
	return m, nil
}

func (m *MemoryCache) key(ctx context.Context, r *rspb.ResourceName) (string, error) {
	rn := digest.ResourceNameFromProto(r)
	if err := rn.Validate(); err != nil {
		return "", err
	}

	// Get partition ID for key isolation
	partitionID, err := m.Partition(ctx, r.GetInstanceName())
	if err != nil {
		return "", err
	}

	hash := rn.GetDigest().GetHash()

	var key string
	if r.GetCacheType() == rspb.CacheType_AC {
		// AC entries are isolated by partition AND group since they are not
		// content-addressed.
		groupID := getGroupID(ctx, m.authenticator)
		key = filepath.Join(partitionID, groupID, "ac", r.GetInstanceName(), hash)
	} else {
		// CAS entries are shared across groups within a partition since they
		// are content-addressed (same hash = same content).
		key = filepath.Join(partitionID, "cas", hash)
	}
	return key, nil
}

func (m *MemoryCache) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	k, err := m.key(ctx, r)
	if err != nil {
		return false, err
	}
	m.lock.Lock()
	contains := m.l.Contains(k)
	m.lock.Unlock()

	m.updateAtime(ctx, r)

	return contains, nil
}

// TODO(buildbuddy-internal#1485) - Add last access and modify time
func (m *MemoryCache) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	d := r.GetDigest()
	k, err := m.key(ctx, r)
	if err != nil {
		return nil, err
	}
	m.lock.Lock()
	v, contains := m.l.Get(k)
	m.lock.Unlock()

	if !contains {
		return nil, status.NotFoundErrorf("Digest '%s/%d' not found in cache", d.GetHash(), d.GetSizeBytes())
	}

	// TODO - Add digest size support for AC
	digestSizeBytes := int64(-1)
	if r.GetCacheType() == rspb.CacheType_CAS {
		digestSizeBytes = int64(len(v))
	}

	return &interfaces.CacheMetadata{
		StoredSizeBytes: int64(len(v)),
		DigestSizeBytes: digestSizeBytes,
	}, nil
}

func (m *MemoryCache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	var missing []*repb.Digest
	// No parallelism here either. Not necessary for an in-memory cache.
	for _, r := range resources {
		ok, err := m.Contains(ctx, r)
		if err != nil {
			return nil, err
		}
		if !ok {
			missing = append(missing, r.GetDigest())
		}
	}
	return missing, nil
}

func (m *MemoryCache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	k, err := m.key(ctx, r)
	if err != nil {
		return nil, err
	}
	m.lock.Lock()
	v, ok := m.l.Get(k)
	m.lock.Unlock()
	if !ok {
		return nil, status.NotFoundErrorf("Key %s not found", r.GetDigest())
	}
	m.updateAtime(ctx, r)
	return v, nil
}

func (m *MemoryCache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	foundMap := make(map[*repb.Digest][]byte, len(resources))
	// No parallelism here either. Not necessary for an in-memory cache.
	for _, r := range resources {
		data, err := m.Get(ctx, r)
		if status.IsNotFoundError(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		foundMap[r.GetDigest()] = data
	}
	return foundMap, nil
}

func (m *MemoryCache) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
	k, err := m.key(ctx, r)
	if err != nil {
		return err
	}
	partitionID, err := m.Partition(ctx, r.GetInstanceName())
	if err != nil {
		return err
	}

	m.lock.Lock()
	// Track partition size. If this key already exists, the eviction callback
	// will subtract the old size when l.Add triggers a ConflictEviction.
	m.keyPartitions[k] = partitionID
	m.partitionSizes[partitionID] += int64(len(data))
	m.l.Add(k, data)
	m.lock.Unlock()
	return nil
}

func (m *MemoryCache) SetMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) error {
	for r, data := range kvs {
		if err := m.Set(ctx, r, data); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryCache) Delete(ctx context.Context, r *rspb.ResourceName) error {
	k, err := m.key(ctx, r)
	if err != nil {
		return err
	}
	m.lock.Lock()
	removed := m.l.Remove(k)
	m.lock.Unlock()
	if !removed {
		d := r.GetDigest()
		return status.NotFoundErrorf("digest %s/%d not found in memory cache", d.GetHash(), d.GetSizeBytes())
	}
	return nil
}

// Low level interface used for seeking and stream-writing.
func (m *MemoryCache) Reader(ctx context.Context, rn *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	// Locking and key prefixing are handled in Get.
	buf, err := m.Get(ctx, rn)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(buf)
	r.Seek(uncompressedOffset, 0)
	length := int64(len(buf))
	if limit != 0 && limit < length {
		length = limit
	}
	if length > 0 {
		return io.NopCloser(io.LimitReader(r, length)), nil
	}
	return io.NopCloser(r), nil
}

func (m *MemoryCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	var buffer bytes.Buffer
	wc := ioutil.NewCustomCommitWriteCloser(&buffer)
	wc.CommitFn = func(int64) error {
		// Locking and key prefixing are handled in SetDeprecated.
		return m.Set(ctx, r, buffer.Bytes())
	}
	return wc, nil
}

func (m *MemoryCache) Start() error {
	return nil
}

func (m *MemoryCache) Stop() error {
	return nil
}

func (m *MemoryCache) Partition(ctx context.Context, remoteInstanceName string) (string, error) {
	if len(m.partitionMappings) == 0 {
		return DefaultPartitionID, nil
	}

	groupID := getGroupID(ctx, m.authenticator)
	return m.lookupPartitionID(groupID, remoteInstanceName), nil
}

func getGroupID(ctx context.Context, authenticator interfaces.Authenticator) string {
	user, err := authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser
	}
	return user.GetGroupID()
}

func (m *MemoryCache) lookupPartitionID(groupID, remoteInstanceName string) string {
	for _, pm := range m.partitionMappings {
		if (pm.GroupID == "" || pm.GroupID == groupID) && strings.HasPrefix(remoteInstanceName, pm.Prefix) {
			return pm.PartitionID
		}
	}
	return DefaultPartitionID
}

func (m *MemoryCache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	return compressor == repb.Compressor_IDENTITY
}

func (m *MemoryCache) updateAtime(ctx context.Context, rn *rspb.ResourceName) {
	if m.atimeUpdater != nil {
		m.atimeUpdater.Enqueue(ctx, rn.GetInstanceName(), []*repb.Digest{rn.GetDigest()}, rn.GetDigestFunction())
	}
}

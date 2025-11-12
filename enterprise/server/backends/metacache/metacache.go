package metacache

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/cache_config"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/gcs"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"

	mdpb "github.com/buildbuddy-io/buildbuddy/proto/metadata"
	mdspb "github.com/buildbuddy-io/buildbuddy/proto/metadata_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

var (
	config = flag.Struct("cache.meta_cache", cache_config.MetaCacheConfig{}, "Config to specify meta cache")
)

const (
	DefaultPartitionID = "default"

	// CompressorBufSizeBytes is the buffer size we use for each chunk when compressing data
	// It should be relatively large to get a good compression ratio bc each chunk is compressed independently
	CompressorBufSizeBytes = 4e6 // 4 MB

	// The max size of the read buffer, used to protect from invalid and malicious requests.
	maxReadBufferSize = 1024 * 1024 * 4

	// Default values for Options
	defaultName                        = "meta_cache"
	defaultMaxInlineFileSizeBytes      = int64(1024)
	defaultMinBytesAutoZstdCompression = int64(100)
)

type Options struct {
	Name           string
	Clock          clockwork.Clock
	MetadataClient mdspb.MetadataServiceClient
	FileStorer     filestore.Store

	PartitionMappings []disk.PartitionMapping

	MinBytesAutoZstdCompression int64
	MaxInlineFileSizeBytes      int64

	MetadataBackend string

	GCSBucket      string
	GCSCredentials string
	GCSProjectID   string
	GCSAppName     string
	GCSTTLDays     int64
}

type Cache struct {
	env        environment.Env
	bufferPool *bytebufferpool.VariableSizePool
	opts       Options
}

// Register creates a new MetaCache from the configured flags and sets it in
// the provided env.
func Register(env *real_environment.RealEnv) error {
	if config.MetadataBackend == "" {
		return nil
	}
	opts, err := GetOptionsFromConfig(env, config)
	if err != nil {
		return err
	}
	mc, err := New(env, opts)
	if err != nil {
		return status.InternalErrorf("Error configuring meta cache: %s", err)
	}
	if env.GetCache() != nil {
		log.Warningf("Overriding configured cache with metacache [%s].", mc.opts.Name)
	}
	env.SetCache(mc)
	return nil

}

func GetOptionsFromConfig(env environment.Env, cfg *cache_config.MetaCacheConfig) (Options, error) {
	emptyOpts := Options{}
	if cfg == nil {
		return emptyOpts, status.FailedPreconditionErrorf("meta cache config is nil")
	}
	if cfg.MetadataBackend == "" {
		return emptyOpts, status.FailedPreconditionError("Meta cache metadata backend must be set")
	}
	opts := Options{
		Name:                        cfg.Name,
		MetadataBackend:             cfg.MetadataBackend,
		PartitionMappings:           cfg.PartitionMappings,
		MaxInlineFileSizeBytes:      cfg.MaxInlineFileSizeBytes,
		MinBytesAutoZstdCompression: cfg.MinBytesAutoZstdCompression,
		GCSBucket:                   cfg.GCSConfig.Bucket,
		GCSCredentials:              cfg.GCSConfig.Credentials,
		GCSProjectID:                cfg.GCSConfig.ProjectID,
		GCSAppName:                  cfg.GCSConfig.AppName,
	}

	if cfg.GCSConfig.TTLDays != nil {
		opts.GCSTTLDays = *cfg.GCSConfig.TTLDays
	}

	return opts, nil
}

func setOptionDefaults(opts *Options) {
	if opts.Name == "" {
		opts.Name = defaultName
	}
	if opts.MaxInlineFileSizeBytes == 0 {
		opts.MaxInlineFileSizeBytes = defaultMaxInlineFileSizeBytes
	}
	if opts.MinBytesAutoZstdCompression == 0 {
		opts.MinBytesAutoZstdCompression = defaultMaxInlineFileSizeBytes
	}
	if opts.Clock == nil {
		opts.Clock = clockwork.NewRealClock()
	}
}

func New(env environment.Env, opts Options) (*Cache, error) {
	localOpts := opts
	setOptionDefaults(&localOpts)
	if localOpts.MetadataClient == nil {
		if localOpts.MetadataBackend == "" {
			return nil, status.FailedPreconditionError("Meta cache metadata client or metadata backend must be set")
		}
		conn, err := grpc_client.DialInternalWithPoolSize(env, opts.MetadataBackend, 2)
		if err != nil {
			return nil, status.UnavailableErrorf("could not dial metadata-server backend %q: %s", opts.MetadataBackend, err)
		}
		client := mdspb.NewMetadataServiceClient(conn)
		localOpts.MetadataClient = client
	}

	if localOpts.FileStorer == nil {
		filestoreOpts := make([]filestore.Option, 0)
		if opts.GCSBucket != "" {
			// Create a new GCS Client with compression disabled. This cache
			// will already compress blobs before storing them, so we don't
			// want the gcs lib to attempt to compress them too.
			ctx := env.GetServerContext()
			gcsBlobstore, err := gcs.NewGCSBlobStore(ctx, opts.GCSBucket, "", opts.GCSCredentials, opts.GCSProjectID, false /*=enableCompression*/)
			if err != nil {
				return nil, err
			}
			// Because eviction is critical to how the cache works, if
			// we're unable to ensure the bucket TTL is set correctly we
			// return an error.
			if err := gcsBlobstore.SetBucketCustomTimeTTL(ctx, opts.GCSTTLDays); err != nil {
				return nil, err
			}
			filestoreOpts = append(filestoreOpts, filestore.WithGCSBlobstore(gcsBlobstore, opts.GCSAppName))
			log.Infof("Meta Cache: GCS TTL is set to %d days", opts.GCSTTLDays)
			localOpts.FileStorer = filestore.New(filestoreOpts...)
		}
	}

	return &Cache{
		env:        env,
		bufferPool: bytebufferpool.VariableSize(CompressorBufSizeBytes),
		opts:       localOpts,
	}, nil
}

// zstdCompressor compresses bytes before writing them to the nested writer
type zstdCompressor struct {
	cacheName string

	interfaces.CommittedWriteCloser
	compressBuf []byte
	bufferPool  *bytebufferpool.VariableSizePool

	numDecompressedBytes int
	numCompressedBytes   int
}

// TODO(tylerw): move to util/compression
func NewZstdCompressor(cacheName string, wc interfaces.CommittedWriteCloser, bp *bytebufferpool.VariableSizePool, digestSize int64) *zstdCompressor {
	compressBuf := bp.Get(digestSize)
	return &zstdCompressor{
		cacheName:            cacheName,
		CommittedWriteCloser: wc,
		compressBuf:          compressBuf,
		bufferPool:           bp,
	}
}

func (z *zstdCompressor) Write(decompressedBytes []byte) (int, error) {
	z.compressBuf = compression.CompressZstd(z.compressBuf, decompressedBytes)
	compressedBytesWritten, err := z.CommittedWriteCloser.Write(z.compressBuf)
	if err != nil {
		return 0, err
	}

	z.numDecompressedBytes += len(decompressedBytes)
	z.numCompressedBytes += compressedBytesWritten

	// Return the size of the original buffer even though a different compressed buffer size may have been written,
	// or clients will return a short write error
	return len(decompressedBytes), nil
}

func (z *zstdCompressor) Close() error {
	metrics.CompressionRatio.
		With(prometheus.Labels{metrics.CompressionType: "zstd", metrics.CacheNameLabel: z.cacheName}).
		Observe(float64(z.numCompressedBytes) / float64(z.numDecompressedBytes))

	z.bufferPool.Put(z.compressBuf)
	return z.CommittedWriteCloser.Close()
}

func (c *Cache) encryptionEnabled(ctx context.Context) (bool, error) {
	if !authutil.EncryptionEnabled(ctx, c.env.GetAuthenticator()) {
		return false, nil
	}
	if c.env.GetCrypter() == nil {
		return false, status.FailedPreconditionError("encryption requested, but crypter not available")
	}
	return true, nil
}

func (c *Cache) userGroupID(ctx context.Context) string {
	user, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser
	}
	return user.GetGroupID()
}

func (c *Cache) lookupGroupAndPartitionID(ctx context.Context, remoteInstanceName string) (string, string) {
	groupID := c.userGroupID(ctx)
	for _, pm := range c.opts.PartitionMappings {
		if pm.GroupID == groupID && strings.HasPrefix(remoteInstanceName, pm.Prefix) {
			return groupID, pm.PartitionID
		}
	}
	return groupID, DefaultPartitionID
}

func (c *Cache) gcsObjectIsPastTTL(gcsMetadata *sgpb.StorageMetadata_GCSMetadata) bool {
	// The GCS TTL is set as an integer number of days. The docs are vague,
	// but it seems plausible that if a file is *ever* marked for deletion,
	// it will be deleted, even if it has changed since. Basically, there is
	// one job marking files for deletion, and another job deleting them,
	// and if the object has changed between those two events, it is still
	// deleted.
	//
	// For this reason, if a GCS object is ever less than 1 hour away from
	// TTL, assume it has already been marked for deletion.
	customTimeUsec := gcsMetadata.GetLastCustomTimeUsec()
	buffer := time.Hour

	return c.opts.Clock.Since(time.UnixMicro(customTimeUsec))+buffer > time.Duration(c.opts.GCSTTLDays*24)*time.Hour
}

func (c *Cache) makeFileRecord(ctx context.Context, pr *rspb.ResourceName) (*sgpb.FileRecord, error) {
	r := digest.ResourceNameFromProto(pr)
	if err := r.Validate(); err != nil {
		return nil, err
	}

	groupID, partID := c.lookupGroupAndPartitionID(ctx, r.GetInstanceName())
	encryptionEnabled, err := c.encryptionEnabled(ctx)
	if err != nil {
		return nil, err
	}

	var encryption *sgpb.Encryption
	if encryptionEnabled {
		ak, err := c.env.GetCrypter().ActiveKey(ctx)
		if err != nil {
			return nil, status.UnavailableErrorf("encryption key not available: %s", err)
		}
		encryption = &sgpb.Encryption{KeyId: ak.GetEncryptionKeyId()}
	}

	return &sgpb.FileRecord{
		Isolation: &sgpb.Isolation{
			CacheType:          r.GetCacheType(),
			RemoteInstanceName: r.GetInstanceName(),
			PartitionId:        partID,
			GroupId:            groupID,
		},
		Digest:         r.GetDigest(),
		DigestFunction: r.GetDigestFunction(),
		Compressor:     r.GetCompressor(),
		Encryption:     encryption,
	}, nil
}

// key is a comparable key for uniquely identifying a ResourceName/FileRecord.
// Note: groupID, partitionID, and encryptionKeyID are deliberately omitted
// because these fields are set by the server based on request context.
type key struct {
	cacheType          rspb.CacheType
	remoteInstanceName string
	hash               string
	sizeBytes          int64
	digestFunction     repb.DigestFunction_Value
}

// newKeyFromFileRecord creates a key from a FileRecord.
func newKeyFromFileRecord(fr *sgpb.FileRecord) key {
	iso := fr.GetIsolation()
	d := fr.GetDigest()
	return key{
		cacheType:          iso.GetCacheType(),
		remoteInstanceName: iso.GetRemoteInstanceName(),
		hash:               string(d.GetHash()),
		sizeBytes:          d.GetSizeBytes(),
		digestFunction:     fr.GetDigestFunction(),
	}
}

// newKeyFromResourceName creates a key from a ResourceName.
func newKeyFromResourceName(r *rspb.ResourceName) key {
	d := r.GetDigest()
	return key{
		cacheType:          r.GetCacheType(),
		remoteInstanceName: r.GetInstanceName(),
		hash:               string(d.GetHash()),
		sizeBytes:          d.GetSizeBytes(),
		digestFunction:     r.GetDigestFunction(),
	}
}

func (c *Cache) lookupMetadatas(ctx context.Context, records ...*sgpb.FileRecord) ([]*sgpb.FileMetadata, error) {
	req := &mdpb.GetRequest{
		FileRecords: records,
	}
	rsp, err := c.opts.MetadataClient.Get(ctx, req)
	if err != nil {
		return nil, err
	}
	return rsp.GetFileMetadatas(), nil
}

func (c *Cache) setMetadatas(ctx context.Context, metadatas ...*sgpb.FileMetadata) error {
	req := &mdpb.SetRequest{
		SetOperations: make([]*mdpb.SetRequest_SetOperation, len(metadatas)),
	}
	for i, md := range metadatas {
		req.SetOperations[i] = &mdpb.SetRequest_SetOperation{
			FileMetadata: md,
		}
	}
	_, err := c.opts.MetadataClient.Set(ctx, req)
	return err
}

func (c *Cache) deleteFileAndMetadata(ctx context.Context, fileRecord *sgpb.FileRecord, atimeToMatch int64) error {
	req := &mdpb.DeleteRequest{
		DeleteOperations: []*mdpb.DeleteRequest_DeleteOperation{{
			FileRecord: fileRecord,
			MatchAtime: atimeToMatch,
		}},
	}
	_, err := c.opts.MetadataClient.Delete(ctx, req)
	return err
}

func (c *Cache) handleMetadataMismatch(ctx context.Context, causeErr error, md *sgpb.FileMetadata) bool {
	if !status.IsNotFoundError(causeErr) && !os.IsNotExist(causeErr) {
		return false
	}
	storageMetadata := md.GetStorageMetadata()

	switch {
	case storageMetadata.GetInlineMetadata() != nil:
		log.Errorf("Impossible metadata mismatch: something is broken")
		return false
	case storageMetadata.GetGcsMetadata() != nil:
		break
	default:
		log.Errorf("Unexpected metadata (not gcs or inline): %v", md)
		return false
	}

	log.Warningf("[%s] Metadata record found (%+v) but blob was not: %s", c.opts.Name, md, causeErr)
	if err := c.deleteFileAndMetadata(ctx, md.GetFileRecord(), md.GetLastAccessUsec()); err != nil {
		if status.IsNotFoundError(err) {
			return false // already deleted
		}
		log.Warningf("[%s] Error deleting metadata: %s", c.opts.Name, err)
		return false
	}
	return true
}

func (c *Cache) readerForStorage(ctx context.Context, storage *sgpb.StorageMetadata, offset, limit int64) (io.ReadCloser, error) {
	switch {
	case storage.GetInlineMetadata() != nil:
		return c.opts.FileStorer.InlineReader(storage.GetInlineMetadata(), offset, limit)
	case storage.GetGcsMetadata() != nil:
		return c.opts.FileStorer.BlobReader(ctx, storage.GetGcsMetadata(), offset, limit)
	default:
		return nil, status.InvalidArgumentErrorf("Unsupported storage metadata: %+v", storage)
	}
}
func (c *Cache) writerForRecord(ctx context.Context, fileRecord *sgpb.FileRecord, sizeHint int64) (interfaces.MetadataWriteCloser, error) {
	if sizeHint < c.opts.MaxInlineFileSizeBytes {
		// This returns a interfaces.MetadataWriteCloser, so
		// Commit() will not be called.
		return c.opts.FileStorer.InlineWriter(ctx, sizeHint), nil
	} else {
		// This returns a interfaces.CommittedMetadataWriteCloser, but
		// Commit() will still be called.
		return c.opts.FileStorer.BlobWriter(ctx, fileRecord)
	}
}

// newWrappedWriter returns an interfaces.CommittedWriteCloser that on Write
// will:
// (1) compress the data if shouldCompress is true; and then
// (2) encrypt the data if encryption is enabled
// (3) write the data using input wcm's Write method.
// On Commit, it will write the metadata for fileRecord.
func (c *Cache) newWrappedWriter(ctx context.Context, fileRecord *sgpb.FileRecord, sizeHint int64, fileType sgpb.FileMetadata_FileType) (interfaces.CommittedWriteCloser, error) {
	wcm, err := c.writerForRecord(ctx, fileRecord, sizeHint)
	if err != nil {
		return nil, err
	}

	var encryptionMetadata *sgpb.EncryptionMetadata
	cwc := ioutil.NewCustomCommitWriteCloser(wcm)
	cwc.CommitFn = func(bytesWritten int64) error {
		now := c.opts.Clock.Now().UnixMicro()
		md := &sgpb.FileMetadata{
			FileRecord:         fileRecord,
			StorageMetadata:    wcm.Metadata(),
			EncryptionMetadata: encryptionMetadata,
			StoredSizeBytes:    bytesWritten,
			LastAccessUsec:     now,
			LastModifyUsec:     now,
			FileType:           fileType,
		}
		if bytesWritten == 0 {
			log.Infof("Rejecting zero-length write. Metadata: %+v", md)
			return status.UnavailableError("zero-length writes are not allowed")
		}
		return c.setMetadatas(ctx, md)
	}

	wc := interfaces.CommittedWriteCloser(cwc)
	shouldEncrypt, err := c.encryptionEnabled(ctx)
	if err != nil {
		_ = wc.Close()
		return nil, err
	}
	if shouldEncrypt {
		ewc, err := c.env.GetCrypter().NewEncryptor(ctx, fileRecord.GetDigest(), wc)
		if err != nil {
			_ = wc.Close()
			return nil, status.UnavailableErrorf("encryptor not available: %s", err)
		}
		encryptionMetadata = ewc.Metadata()
		wc = ewc
	}

	return wc, nil
}

func (c *Cache) writerWithSizeHint(ctx context.Context, r *rspb.ResourceName, sizeHint int64) (interfaces.CommittedWriteCloser, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	if spn.IsRecording() {
		spn.SetAttributes(
			attribute.String("digest_hash", r.GetDigest().GetHash()),
			attribute.Int64("digest_size", r.GetDigest().GetSizeBytes()))
	}
	// If data is not already compressed, return a writer that will compress
	// it before writing.
	// N.B. We only compress data *over* a given size, because compressing
	// very small blobs can often inflate their size.
	shouldCompress := r.GetCompressor() == repb.Compressor_IDENTITY && sizeHint >= c.opts.MinBytesAutoZstdCompression
	if shouldCompress {
		r = &rspb.ResourceName{
			Digest:         r.GetDigest(),
			DigestFunction: r.GetDigestFunction(),
			InstanceName:   r.GetInstanceName(),
			Compressor:     repb.Compressor_ZSTD,
			CacheType:      r.GetCacheType(),
		}
	}

	fileRecord, err := c.makeFileRecord(ctx, r)
	if err != nil {
		return nil, err
	}

	wc, err := c.newWrappedWriter(ctx, fileRecord, sizeHint, sgpb.FileMetadata_COMPLETE_FILE_TYPE)
	if err != nil {
		return nil, err
	}
	if shouldCompress {
		return NewZstdCompressor(c.opts.Name, wc, c.bufferPool, fileRecord.GetDigest().GetSizeBytes()), nil
	}
	return wc, nil
}

// Normal cache-like operations
func (c *Cache) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	missing, err := c.FindMissing(ctx, []*rspb.ResourceName{r})
	if err != nil {
		return false, err
	}
	return len(missing) == 0, nil
}

func (c *Cache) Metadata(ctx context.Context, r *rspb.ResourceName) (*interfaces.CacheMetadata, error) {
	fileRecord, err := c.makeFileRecord(ctx, r)
	if err != nil {
		return nil, err
	}
	mds, err := c.lookupMetadatas(ctx, fileRecord)
	if err != nil {
		return nil, err
	}
	if len(mds) != 1 {
		return nil, status.NotFoundErrorf("Found %d records for metadata: %+v", len(mds), r)
	}
	md := mds[0]
	return &interfaces.CacheMetadata{
		StoredSizeBytes:    md.GetStoredSizeBytes(),
		DigestSizeBytes:    md.GetFileRecord().GetDigest().GetSizeBytes(),
		LastModifyTimeUsec: md.GetLastModifyUsec(),
		LastAccessTimeUsec: md.GetLastAccessUsec(),
	}, nil
}

func (c *Cache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	req := &mdpb.FindRequest{
		FileRecords: make([]*sgpb.FileRecord, len(resources)),
	}
	for i, r := range resources {
		fileRecord, err := c.makeFileRecord(ctx, r)
		if err != nil {
			return nil, err
		}
		req.FileRecords[i] = fileRecord
	}
	rsp, err := c.opts.MetadataClient.Find(ctx, req)
	if err != nil {
		return nil, err
	}
	missing := make([]*repb.Digest, 0)
	for i, findRsp := range rsp.GetFindResponses() {
		if !findRsp.GetPresent() {
			missing = append(missing, resources[i].GetDigest())
		}
	}
	return missing, nil
}

func (c *Cache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	rc, err := c.Reader(ctx, r, 0, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	buf := bytes.NewBuffer(make([]byte, 0, digest.SafeBufferSize(r, maxReadBufferSize)))
	_, err = io.Copy(buf, rc)
	return buf.Bytes(), err
}

func (c *Cache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	// Convert resources to fileRecords
	fileRecords := make([]*sgpb.FileRecord, 0, len(resources))
	for _, r := range resources {
		fileRecord, err := c.makeFileRecord(ctx, r)
		if err != nil {
			return nil, err
		}
		fileRecords = append(fileRecords, fileRecord)
	}

	mds, err := c.lookupMetadatas(ctx, fileRecords...)
	if err != nil {
		return nil, err
	}

	keyToMetadata := make(map[key]*sgpb.FileMetadata, len(mds))
	for _, md := range mds {
		k := newKeyFromFileRecord(md.GetFileRecord())
		keyToMetadata[k] = md
	}

	foundMap := make(map[*repb.Digest][]byte, len(resources))
	for _, r := range resources {
		k := newKeyFromResourceName(r)
		md, ok := keyToMetadata[k]
		if !ok {
			continue
		}
		rc, err := c.reader(ctx, md, r, 0, 0)
		if err != nil {
			if status.IsNotFoundError(err) || os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		buf := bytes.NewBuffer(make([]byte, 0, digest.SafeBufferSize(r, maxReadBufferSize)))
		_, copyErr := io.Copy(buf, rc)
		closeErr := rc.Close()
		if copyErr != nil {
			log.Warningf("[%s] GetMulti encountered error when copying %s: %s", c.opts.Name, r.GetDigest().GetHash(), copyErr)
			continue
		}
		if closeErr != nil {
			log.Warningf("[%s] GetMulti cannot close reader when copying %s: %s", c.opts.Name, r.GetDigest().GetHash(), closeErr)
			continue
		}
		foundMap[r.GetDigest()] = buf.Bytes()
	}
	return foundMap, nil
}

func (c *Cache) Set(ctx context.Context, r *rspb.ResourceName, data []byte) error {
	wc, err := c.writerWithSizeHint(ctx, r, int64(len(data)))
	if err != nil {
		return err
	}
	defer wc.Close()
	if _, err := wc.Write(data); err != nil {
		return err
	}
	return wc.Commit()
}

func (c *Cache) SetMulti(ctx context.Context, kvs map[*rspb.ResourceName][]byte) error {
	// TODO(tylerw): make this more efficient, possibly by batching
	// the metadata update operations and running the GCS uploads in
	// parallel.
	for r, data := range kvs {
		if err := c.Set(ctx, r, data); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) Delete(ctx context.Context, r *rspb.ResourceName) error {
	fileRecord, err := c.makeFileRecord(ctx, r)
	if err != nil {
		return err
	}

	return c.deleteFileAndMetadata(ctx, fileRecord, 0 /*=matchAtime*/)
}

func (c *Cache) reader(ctx context.Context, md *sgpb.FileMetadata, r *rspb.ResourceName, uncompressedOffset, uncompressedLimit int64) (io.ReadCloser, error) {
	// If this object was not chunked, and is somehow stored as a zero-
	// length file, pretend it does not exist.
	storageMetadata := md.GetStorageMetadata()

	if chunkedMD := storageMetadata.GetChunkedMetadata(); chunkedMD == nil && md.GetStoredSizeBytes() == 0 {
		log.Infof("Ignoring zero-length file. md: %+v", md)
		return nil, status.NotFoundError("object not found (zero-length)")
	}

	// If this is a GCS object, ensure the custom time is relatively recent
	// so that we avoid saying something exists when it's been deleted by
	// a GCS lifecycle rule.
	if gcsMetadata := md.GetStorageMetadata().GetGcsMetadata(); gcsMetadata != nil {
		if c.gcsObjectIsPastTTL(gcsMetadata) {
			return nil, status.NotFoundError("backing object may have expired")
		}
	}

	requestedCompression := r.GetCompressor()
	cachedCompression := md.GetFileRecord().GetCompressor()
	if requestedCompression == cachedCompression &&
		requestedCompression != repb.Compressor_IDENTITY &&
		(uncompressedOffset != 0 || uncompressedLimit != 0) {
		return nil, status.FailedPreconditionError("passthrough compression does not support offset/limit")
	}

	shouldDecrypt := md.EncryptionMetadata != nil
	if shouldDecrypt {
		encryptionEnabled, err := c.encryptionEnabled(ctx)
		if err != nil {
			return nil, err
		}
		if !encryptionEnabled {
			return nil, status.NotFoundError("decryption key not available")
		}
	}

	// If the data is stored uncompressed/unencrypted, we can use the offset/limit directly
	// otherwise we need to decompress/decrypt first.
	offset := int64(0)
	limit := int64(0)
	rawStorage := cachedCompression == repb.Compressor_IDENTITY && !shouldDecrypt
	if rawStorage {
		offset = uncompressedOffset
		limit = uncompressedLimit
	}

	shouldDecompress := cachedCompression == repb.Compressor_ZSTD && requestedCompression == repb.Compressor_IDENTITY

	reader, err := c.readerForStorage(ctx, md.GetStorageMetadata(), offset, limit)
	if err != nil {
		if status.IsNotFoundError(err) || os.IsNotExist(err) {
			c.handleMetadataMismatch(ctx, err, md)
		}
		return nil, err
	}

	if !rawStorage {
		if shouldDecrypt {
			d, err := c.env.GetCrypter().NewDecryptor(ctx, md.GetFileRecord().GetDigest(), reader, md.GetEncryptionMetadata())
			if err != nil {
				return nil, status.UnavailableErrorf("decryptor not available: %s", err)
			}
			reader = d
		}
		if shouldDecompress && storageMetadata.GetChunkedMetadata() == nil {
			// We don't need to decompress the chunked reader's content since
			// it already returns decompressed content from its children.
			dr, err := compression.NewZstdDecompressingReader(reader)
			if err != nil {
				return nil, err
			}
			reader = dr
		}
		if uncompressedOffset != 0 {
			if _, err := io.CopyN(io.Discard, reader, uncompressedOffset); err != nil {
				_ = reader.Close()
				return nil, err
			}
		}
		if uncompressedLimit != 0 {
			reader = ioutil.LimitReadCloser(reader, uncompressedLimit)
		}
	}

	if requestedCompression == repb.Compressor_ZSTD && cachedCompression == repb.Compressor_IDENTITY {
		bufSize := digest.SafeBufferSize(r, CompressorBufSizeBytes)
		return compression.NewBufferedZstdCompressingReader(reader, c.bufferPool, int64(bufSize))
	}

	return reader, nil
}

// Low level interface used for seeking and stream-writing.
func (c *Cache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, uncompressedLimit int64) (io.ReadCloser, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	if spn.IsRecording() {
		spn.SetAttributes(
			attribute.String("digest_hash", r.GetDigest().GetHash()),
			attribute.Int64("digest_size", r.GetDigest().GetSizeBytes()))
	}
	fileRecord, err := c.makeFileRecord(ctx, r)
	if err != nil {
		return nil, err
	}

	mds, err := c.lookupMetadatas(ctx, fileRecord)
	if err != nil {
		return nil, err
	}
	if len(mds) != 1 {
		log.Errorf("File record %v found multiple metadatas: %v", fileRecord, mds)
		return nil, status.InternalError("only one metadata record should match query")
	}
	md := mds[0]

	return c.reader(ctx, md, r, uncompressedOffset, uncompressedLimit)

}

func (c *Cache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	return c.writerWithSizeHint(ctx, r, r.GetDigest().GetSizeBytes())
}

// SupportsCompressor returns whether the cache supports storing data compressed
// with the given compressor.
func (c *Cache) SupportsCompressor(compressor repb.Compressor_Value) bool {
	switch compressor {
	case repb.Compressor_IDENTITY, repb.Compressor_ZSTD:
		return true
	default:
		return false
	}
}

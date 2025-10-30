package metacache

import (
	"bytes"
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/filestore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"

	mdpb "github.com/buildbuddy-io/buildbuddy/proto/metadata"
	mdspb "github.com/buildbuddy-io/buildbuddy/proto/metadata_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

const (
	DefaultPartitionID = "default"

	// CompressorBufSizeBytes is the buffer size we use for each chunk when compressing data
	// It should be relatively large to get a good compression ratio bc each chunk is compressed independently
	CompressorBufSizeBytes = 4e6 // 4 MB
)

type Options struct {
	Name           string
	Clock          clockwork.Clock
	MetadataClient mdspb.MetadataServiceClient
	FileStorer     filestore.Store

	PartitionMappings []disk.PartitionMapping

	MinBytesAutoZstdCompression int64
	MaxInlineFileSizeBytes      int64

	GCSTTLDays int
}

type Cache struct {
	env        environment.Env
	bufferPool *bytebufferpool.VariableSizePool
	opts       Options
}

func New(env environment.Env, opts Options) (*Cache, error) {
	localOpts := opts
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

// TODO(tylerw): move to util/compression
// compressionReader helps manage resources associated with a compression.NewZstdCompressingReader
type compressionReader struct {
	io.ReadCloser
	readBuf     []byte
	compressBuf []byte
	bufferPool  *bytebufferpool.VariableSizePool
}

func (r *compressionReader) Close() error {
	err := r.ReadCloser.Close()
	r.bufferPool.Put(r.readBuf)
	r.bufferPool.Put(r.compressBuf)
	return err
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

// Below, in Get(), this value is the max initial allocatable buffer size.
// Set it somewhat conservatively so that we're not DOSed by someone crafting
// remote_instance_names that match this just to use memory.
const maxInitialByteBufferSize = (1024 * 1024 * 4)

func (c *Cache) Get(ctx context.Context, r *rspb.ResourceName) ([]byte, error) {
	rc, err := c.Reader(ctx, r, 0, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	var buf *bytes.Buffer

	// TODO(tylerw): move this function to cachetools or something?
	if r.GetCacheType() == rspb.CacheType_CAS {
		// If this is a CAS object, size the buffer to fit exactly.
		buf = bytes.NewBuffer(make([]byte, 0, int(r.GetDigest().GetSizeBytes())))
	} else if strings.HasPrefix(r.GetInstanceName(), content_addressable_storage_server.TreeCacheRemoteInstanceName) {
		// If this is a TreeCache entry that we wrote; pull the size
		// from the remote instance name.
		parts := strings.Split(r.GetInstanceName(), "/")
		if s, err := strconv.Atoi(parts[len(parts)-1]); err == nil {
			buf = bytes.NewBuffer(make([]byte, 0, min(s, maxInitialByteBufferSize)))
		} else {
			buf = new(bytes.Buffer)
		}
	} else {
		buf = new(bytes.Buffer)
	}
	_, err = io.Copy(buf, rc)
	return buf.Bytes(), err
}

func (c *Cache) GetMulti(ctx context.Context, resources []*rspb.ResourceName) (map[*repb.Digest][]byte, error) {
	return nil, status.UnimplementedError("not yet")
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

// Low level interface used for seeking and stream-writing.
func (c *Cache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, uncompressedLimit int64) (io.ReadCloser, error) {
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
		return nil, status.InternalErrorf("only one metadata record should match query")
	}
	md := mds[0]

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
			return nil, status.NotFoundErrorf("decryption key not available")
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
		bufSize := int64(CompressorBufSizeBytes)
		resourceSize := r.GetDigest().GetSizeBytes()
		if resourceSize > 0 && resourceSize < bufSize {
			bufSize = resourceSize
		}

		readBuf := c.bufferPool.Get(bufSize)
		compressBuf := c.bufferPool.Get(bufSize)

		cr, err := compression.NewZstdCompressingReader(reader, readBuf, compressBuf)
		if err != nil {
			c.bufferPool.Put(readBuf)
			c.bufferPool.Put(compressBuf)
			return nil, err
		}
		return &compressionReader{
			ReadCloser:  cr,
			readBuf:     readBuf,
			compressBuf: compressBuf,
			bufferPool:  c.bufferPool,
		}, err
	}

	return reader, nil
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

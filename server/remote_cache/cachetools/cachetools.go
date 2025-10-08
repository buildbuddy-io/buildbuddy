package cachetools

import (
	"bytes"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/rpcutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

const (
	// uploadBufSizeBytes controls the size of the buffers used for uploading
	// to bytestream.Write. This means it also controls the payload size for
	// each WriteRequest. https://github.com/grpc/grpc.github.io/issues/371
	// that 16KiB-64KiB payloads work best, but our experiments and benchmarks
	// show that 128KiB works best. Values bigger and slower than that are both
	// slower. Values bigger than that allocate more bytes, and values smaller
	// than that allocate the same number of bytes but with more allocations.
	uploadBufSizeBytes = 128 * 1024
	// Matches https://github.com/bazelbuild/bazel/blob/9c22032c8dc0eb2ec20d8b5a5c73d1f5f075ae37/src/main/java/com/google/devtools/build/lib/remote/options/RemoteOptions.java#L461-L464
	minSizeBytesToCompress = 100
	// batchUploadLimitBytes controls how big an object or batch can be in a
	// BatchUploadBlobs RPC. In experiments, 2MiB blobs are 5-10% faster to
	// upload using the bytestream.Write api.
	batchUploadLimitBytes = min(2*1024*1024, rpcutil.GRPCMaxSizeBytes)
)

var (
	enableUploadCompression     = flag.Bool("cache.client.enable_upload_compression", true, "If true, enable compression of uploads to remote caches")
	casRPCTimeout               = flag.Duration("cache.client.cas_rpc_timeout", 1*time.Minute, "Maximum time a single batch RPC or a single ByteStream chunk read can take.")
	acRPCTimeout                = flag.Duration("cache.client.ac_rpc_timeout", 15*time.Second, "Maximum time a single Action Cache RPC can take.")
	filecacheTreeSalt           = flag.String("cache.filecache_tree_salt", "20250304", "A salt to invalidate filecache tree hashes, if/when needed.")
	requestCachedSubtreeDigests = flag.Bool("cache.request_cached_subtree_digests", true, "If true, GetTree requests will set send_cached_subtree_digests.")

	uploadBufPool = bytebufferpool.FixedSize(uploadBufSizeBytes)
)

func retryOptions(name string) *retry.Options {
	opts := retry.DefaultOptions()
	opts.MaxRetries = 3
	opts.Name = name
	return opts
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

func getBlob(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.CASResourceName, out io.Writer) error {
	if bsClient == nil {
		return status.FailedPreconditionError("ByteStreamClient not configured")
	}
	if r.IsEmpty() {
		return nil
	}

	req := &bspb.ReadRequest{
		ResourceName: r.DownloadString(),
	}
	stream, err := bsClient.Read(ctx, req)
	if err != nil {
		if gstatus.Code(err) == gcodes.NotFound {
			return digest.MissingDigestError(r.GetDigest())
		}
		return err
	}
	checksum, err := digest.HashForDigestType(r.GetDigestFunction())
	if err != nil {
		return err
	}
	w := io.MultiWriter(checksum, out)

	close := func() error { return nil }
	if r.GetCompressor() == repb.Compressor_ZSTD {
		decompressor, err := compression.NewZstdDecompressor(w)
		if err != nil {
			return err
		}
		w = decompressor
		close = sync.OnceValue(decompressor.Close)
	}
	defer close()

	receiver := rpcutil.NewReceiver[*bspb.ReadResponse](ctx, stream)
	for {
		rsp, err := receiver.RecvWithTimeoutCause(*casRPCTimeout, status.DeadlineExceededError("timed out waiting for Read response"))
		if err == io.EOF {
			// Close before returning from this loop to make sure all bytes are
			// flushed from the decompressor to the output/checksum writers.
			// Note: this is safe even though we also defer close() above, since we
			// wrap decompressor.Close with sync.OnceValue.
			if err := close(); err != nil {
				return err
			}
			break
		}
		if err != nil {
			return err
		}
		if _, err := w.Write(rsp.Data); err != nil {
			return err
		}
	}
	computedDigest := hex.EncodeToString(checksum.Sum(nil))
	if computedDigest != r.GetDigest().GetHash() {
		return status.DataLossErrorf("Downloaded content (hash %q) did not match expected (hash %q)", computedDigest, r.GetDigest().GetHash())
	}
	return nil
}

func GetBlob(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.CASResourceName, out io.Writer) error {
	// We can only retry if we can rewind the writer back to the beginning.
	seeker, retryable := out.(io.Seeker)
	if retryable {
		return retry.DoVoid(ctx, retryOptions("ByteStream.Read"), func(ctx context.Context) error {
			if _, err := seeker.Seek(0, io.SeekStart); err != nil {
				return retry.NonRetryableError(err)
			}
			ctx, cancel := context.WithTimeout(ctx, *casRPCTimeout)
			defer cancel()
			err := getBlob(ctx, bsClient, r, out)
			if status.IsNotFoundError(err) {
				return retry.NonRetryableError(err)
			}
			return err
		})
	} else {
		return getBlob(ctx, bsClient, r, out)
	}
}

// BlobResponse is a response to an individual blob in a BatchReadBlobs request.
type BlobResponse struct {
	// Digest identifies the blob that was requested.
	Digest *repb.Digest

	// Data contains the blob contents if it was fetched successfully.
	Data []byte
	// Err holds any error encountered when fetching the blob.
	Err error
}

// BatchReadBlobs issues a BatchReadBlobs request and returns a mapping from
// digest hash to byte payload.
//
// It validates the response so that if the returned err is nil, then all
// digests in the request are guaranteed to have a corresponding map entry.
func BatchReadBlobs(ctx context.Context, casClient repb.ContentAddressableStorageClient, req *repb.BatchReadBlobsRequest) ([]*BlobResponse, error) {
	return retry.Do(ctx, retryOptions("BatchReadBlobs"), func(ctx context.Context) ([]*BlobResponse, error) {
		ctx, cancel := context.WithTimeout(ctx, *casRPCTimeout)
		defer cancel()
		return batchReadBlobs(ctx, casClient, req)
	})
}

func batchReadBlobs(ctx context.Context, casClient repb.ContentAddressableStorageClient, req *repb.BatchReadBlobsRequest) ([]*BlobResponse, error) {
	res, err := casClient.BatchReadBlobs(ctx, req)
	if err != nil {
		return nil, err
	}
	expected := map[string]struct{}{}
	for _, d := range req.GetDigests() {
		expected[d.GetHash()] = struct{}{}
	}
	// Validate that the response doesn't contain any unexpected digests.
	for _, res := range res.Responses {
		if _, ok := expected[res.GetDigest().GetHash()]; !ok {
			return nil, status.UnknownErrorf("unexpected digest in batch response: %q", digest.String(res.GetDigest()))
		}
	}
	// Build the results map, decompressing if needed and validating digests.
	results := make([]*BlobResponse, 0, len(res.Responses))
	for _, res := range res.Responses {
		delete(expected, res.GetDigest().GetHash())

		err := gstatus.ErrorProto(res.GetStatus())
		if err != nil {
			results = append(results, &BlobResponse{
				Digest: res.GetDigest(),
				Err:    err,
			})
			continue
		}
		// TODO: parallel decompression
		// TODO: accept decompression buffer map as optional arg
		data, err := decompressBytes(res.Data, res.GetDigest(), res.GetCompressor())
		if err != nil {
			return nil, status.WrapError(err, "decompress blob")
		}
		// Validate digest
		downloadedContentDigest, err := digest.Compute(bytes.NewReader(data), req.GetDigestFunction())
		if err != nil {
			return nil, err
		}
		if downloadedContentDigest.GetHash() != res.GetDigest().GetHash() || downloadedContentDigest.GetSizeBytes() != res.GetDigest().GetSizeBytes() {
			return nil, status.UnknownErrorf("digest validation failed: expected %q, got %q", digest.String(res.GetDigest()), digest.String(downloadedContentDigest))
		}
		results = append(results, &BlobResponse{
			Digest: res.GetDigest(),
			Data:   data,
		})
	}
	if len(expected) > 0 {
		return nil, status.UnknownErrorf("missing digests in response: %s", slices.Collect(maps.Keys(expected)))
	}
	return results, nil
}

func computeDigest(in io.ReadSeeker, instanceName string, digestFunction repb.DigestFunction_Value) (*digest.CASResourceName, error) {
	d, err := digest.Compute(in, digestFunction)
	if err != nil {
		return nil, err
	}
	return digest.NewCASResourceName(d, instanceName, digestFunction), nil
}

func ComputeFileDigest(fullFilePath, instanceName string, digestFunction repb.DigestFunction_Value) (*digest.CASResourceName, error) {
	f, err := os.Open(fullFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return computeDigest(f, instanceName, digestFunction)
}

func uploadFromReader(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.CASResourceName, in io.Reader) (*repb.Digest, int64, error) {
	if bsClient == nil {
		return nil, 0, status.FailedPreconditionError("ByteStreamClient not configured")
	}
	if r.IsEmpty() {
		return r.GetDigest(), 0, nil
	}
	stream, err := bsClient.Write(ctx)
	if err != nil {
		return nil, 0, err
	}

	var rc io.ReadCloser = io.NopCloser(in)
	if r.GetCompressor() == repb.Compressor_ZSTD {
		rbuf, cbuf := uploadBufPool.Get(), uploadBufPool.Get()
		defer func() {
			uploadBufPool.Put(rbuf)
			uploadBufPool.Put(cbuf)
		}()
		reader, err := compression.NewZstdCompressingReader(rc, rbuf, cbuf)
		if err != nil {
			return nil, 0, status.InternalErrorf("Failed to compress blob: %s", err)
		}
		rc = reader
	}
	defer rc.Close()

	buf := uploadBufPool.Get()
	defer uploadBufPool.Put(buf)
	bytesUploaded := int64(0)
	sender := rpcutil.NewSender(ctx, stream)
	resourceName := r.NewUploadString()
	for {
		n, err := ioutil.ReadTryFillBuffer(rc, buf)
		if err != nil && err != io.EOF {
			return nil, bytesUploaded, err
		}
		readDone := err == io.EOF

		req := &bspb.WriteRequest{
			Data:         buf[:n],
			ResourceName: resourceName,
			WriteOffset:  bytesUploaded,
			FinishWrite:  readDone,
		}

		err = sender.SendWithTimeoutCause(req, *casRPCTimeout, status.DeadlineExceededError("Timed out sending Write request"))
		if err != nil {
			// If the blob already exists in the CAS, the server will respond EOF.
			// It is safe to stop sending writes.
			if err == io.EOF {
				break
			}
			return nil, bytesUploaded, err
		}
		bytesUploaded += int64(len(req.Data))
		if readDone {
			break
		}

	}
	rsp, err := stream.CloseAndRecv()
	if err != nil {
		// If there is a hash mismatch and the reader supports seeking, re-hash
		// to check whether a concurrent mutation has occurred.
		if rs, ok := in.(io.ReadSeeker); ok && status.IsDataLossError(err) {
			if err := checkConcurrentMutation(r.GetDigest(), r.GetDigestFunction(), rs); err != nil {
				return nil, 0, retry.NonRetryableError(status.WrapError(err, "check for concurrent mutation during upload"))
			}
		}
		return nil, bytesUploaded, err
	}

	remoteSize := rsp.GetCommittedSize()
	if r.GetCompressor() == repb.Compressor_IDENTITY {
		// Either the write succeeded or was short-circuited, but in
		// either case, the remoteSize for uncompressed uploads should
		// match the file size.
		if remoteSize != r.GetDigest().GetSizeBytes() {
			return nil, bytesUploaded, status.DataLossErrorf("Remote size (%d) != uploaded size: (%d)", remoteSize, r.GetDigest().GetSizeBytes())
		}
	} else {
		// -1 is returned if the blob already exists, otherwise the
		// remoteSize should agree with what we uploaded.
		if remoteSize != bytesUploaded && remoteSize != -1 {
			return nil, bytesUploaded, status.DataLossErrorf("Remote size (%d) != uploaded size: (%d)", remoteSize, r.GetDigest().GetSizeBytes())
		}
	}

	return r.GetDigest(), bytesUploaded, nil
}

type uploadRetryResult = struct {
	digest        *repb.Digest
	uploadedBytes int64
}

// UploadFromReader attempts to read all bytes from the `in` `Reader` until encountering an EOF
// and write all those bytes to the CAS.
// If the input Reader is also a Seeker, UploadFromReader will retry the upload until success.
//
// On success, it returns the digest of the uploaded blob and the number of bytes confirmed uploaded.
// If the blob already exists, this call will succeed and return the number of bytes uploaded before the server short-circuited the upload.
// On error, it returns the number of bytes uploaded before the error (and the error).
// UploadFromReader confirms that the expected number of bytes have been written to the CAS
// and returns a DataLossError if not.
func UploadFromReader(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.CASResourceName, in io.Reader) (*repb.Digest, int64, error) {
	// We can only retry if we can rewind the reader back to the beginning.
	seeker, retryable := in.(io.Seeker)
	if retryable {
		result, err := retry.Do(ctx, retryOptions("ByteStream.Write"), func(ctx context.Context) (uploadRetryResult, error) {
			if _, err := seeker.Seek(0, io.SeekStart); err != nil {
				return uploadRetryResult{digest: nil, uploadedBytes: 0}, retry.NonRetryableError(err)
			}
			d, u, err := uploadFromReader(ctx, bsClient, r, in)
			return uploadRetryResult{
				digest:        d,
				uploadedBytes: u,
			}, err
		})
		return result.digest, result.uploadedBytes, err
	} else {
		return uploadFromReader(ctx, bsClient, r, in)
	}
}

func GetActionResult(ctx context.Context, acClient repb.ActionCacheClient, ar *digest.ACResourceName) (*repb.ActionResult, error) {
	if acClient == nil {
		return nil, status.FailedPreconditionError("ActionCacheClient not configured")
	}
	req := &repb.GetActionResultRequest{
		ActionDigest:   ar.GetDigest(),
		InstanceName:   ar.GetInstanceName(),
		DigestFunction: ar.GetDigestFunction(),
	}
	return retry.Do(ctx, retryOptions("GetActionResult"), func(ctx context.Context) (*repb.ActionResult, error) {
		ctx, cancel := context.WithTimeout(ctx, *acRPCTimeout)
		defer cancel()
		rsp, err := acClient.GetActionResult(ctx, req)
		if status.IsNotFoundError(err) {
			return nil, retry.NonRetryableError(err)
		}
		return rsp, err
	})
}

func UploadActionResult(ctx context.Context, acClient repb.ActionCacheClient, r *digest.ACResourceName, ar *repb.ActionResult) error {
	if acClient == nil {
		return status.FailedPreconditionError("ActionCacheClient not configured")
	}

	req := &repb.UpdateActionResultRequest{
		InstanceName:   r.GetInstanceName(),
		ActionDigest:   r.GetDigest(),
		ActionResult:   ar,
		DigestFunction: r.GetDigestFunction(),
	}
	_, err := retry.Do(ctx, retryOptions("UpdateActionResult"), func(ctx context.Context) (*repb.ActionResult, error) {
		ctx, cancel := context.WithTimeout(ctx, *acRPCTimeout)
		defer cancel()
		return acClient.UpdateActionResult(ctx, req)
	})
	return err
}

func UploadProto(ctx context.Context, bsClient bspb.ByteStreamClient, instanceName string, digestFunction repb.DigestFunction_Value, in proto.Message) (*repb.Digest, error) {
	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(data)
	resourceName, err := computeDigest(reader, instanceName, digestFunction)
	if err != nil {
		return nil, err
	}
	maybeSetCompressor(resourceName)
	// Go back to the beginning so we can re-read the file contents as we upload.
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	result, _, err := UploadFromReader(ctx, bsClient, resourceName, reader)
	return result, err
}

func UploadBlob(ctx context.Context, bsClient bspb.ByteStreamClient, instanceName string, digestFunction repb.DigestFunction_Value, in io.ReadSeeker) (*repb.Digest, error) {
	resourceName, err := computeDigest(in, instanceName, digestFunction)
	if err != nil {
		return nil, err
	}
	maybeSetCompressor(resourceName)
	// Go back to the beginning so we can re-read the file contents as we upload.
	if _, err := in.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	result, _, err := UploadFromReader(ctx, bsClient, resourceName, in)
	return result, err
}

func UploadFile(ctx context.Context, bsClient bspb.ByteStreamClient, instanceName string, digestFunction repb.DigestFunction_Value, fullFilePath string) (*repb.Digest, error) {
	f, err := os.Open(fullFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	resourceName, err := computeDigest(f, instanceName, digestFunction)
	if err != nil {
		return nil, err
	}
	maybeSetCompressor(resourceName)
	// Go back to the beginning so we can re-read the file contents as we upload.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	result, _, err := UploadFromReader(ctx, bsClient, resourceName, f)
	return result, err
}

// byteWriteSeeker implements an io.WriterAt with a []byte array. In turn, this
// allows using io.OffsetWriter to implement a Writer + Seeker that can be
// passed to GetBlob, which allows retrying failed downloads. We don't use a
// bytes.Buffer because it does not implement the io.Seeker interface.
type byteWriteSeeker []byte

func (ws byteWriteSeeker) WriteAt(p []byte, off int64) (int, error) {
	if len(p)+int(off) > len(ws) {
		return 0, status.FailedPreconditionError("Write off end of byte array")
	}
	return copy(ws[off:], p), nil
}

func GetBlobAsProto(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.CASResourceName, out proto.Message) error {
	buf := byteWriteSeeker(make([]byte, r.GetDigest().GetSizeBytes()))
	bufWriter := io.NewOffsetWriter(buf, 0)

	if err := GetBlob(ctx, bsClient, r, bufWriter); err != nil {
		return err
	}
	return proto.Unmarshal([]byte(buf), out)
}

func readProtoFromCache(ctx context.Context, cache interfaces.Cache, r *rspb.ResourceName, out proto.Message) error {
	data, err := cache.Get(ctx, r)
	if err != nil {
		return err
	}
	return proto.Unmarshal(data, out)
}

func ReadProtoFromCAS(ctx context.Context, cache interfaces.Cache, r *digest.CASResourceName, out proto.Message) error {
	return readProtoFromCache(ctx, cache, r.ToProto(), out)
}

func ReadProtoFromAC(ctx context.Context, cache interfaces.Cache, r *digest.ACResourceName, out proto.Message) error {
	return readProtoFromCache(ctx, cache, r.ToProto(), out)
}

func UploadBytesToCache(ctx context.Context, cache interfaces.Cache, cacheType rspb.CacheType, remoteInstanceName string, digestFunction repb.DigestFunction_Value, in io.ReadSeeker) (*repb.Digest, error) {
	d, err := digest.Compute(in, digestFunction)
	if err != nil {
		return nil, err
	}
	resourceName := digest.NewResourceName(d, remoteInstanceName, cacheType, digestFunction)
	if resourceName.IsEmpty() {
		return d, nil
	}
	// Go back to the beginning so we can re-read the file contents as we upload.
	if _, err := in.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	wc, err := cache.Writer(ctx, resourceName.ToProto())
	if err != nil {
		return nil, err
	}
	defer wc.Close()
	_, err = io.Copy(wc, in)
	if err != nil {
		return nil, err
	}
	return d, wc.Commit()
}

func uploadProtoToCache(ctx context.Context, cache interfaces.Cache, cacheType rspb.CacheType, instanceName string, digestFunction repb.DigestFunction_Value, in proto.Message) (*repb.Digest, error) {
	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(data)
	return UploadBytesToCache(ctx, cache, cacheType, instanceName, digestFunction, reader)
}

func UploadBlobToCAS(ctx context.Context, bsClient bspb.ByteStreamClient, instanceName string, digestFunction repb.DigestFunction_Value, blob []byte) (*repb.Digest, error) {
	reader := bytes.NewReader(blob)
	d, err := digest.Compute(reader, digestFunction)
	if err != nil {
		return nil, err
	}
	resourceName := digest.NewCASResourceName(d, instanceName, digestFunction)
	if resourceName.IsEmpty() {
		return d, nil
	}
	maybeSetCompressor(resourceName)
	result, _, err := UploadFromReader(ctx, bsClient, resourceName, reader)
	return result, err
}

func UploadProtoToCAS(ctx context.Context, cache interfaces.Cache, instanceName string, digestFunction repb.DigestFunction_Value, in proto.Message) (*repb.Digest, error) {
	return uploadProtoToCache(ctx, cache, rspb.CacheType_CAS, instanceName, digestFunction, in)
}

func SupportsCompression(ctx context.Context, capabilitiesClient repb.CapabilitiesClient) (bool, error) {
	rsp, err := capabilitiesClient.GetCapabilities(ctx, &repb.GetCapabilitiesRequest{})
	if err != nil {
		return false, err
	}
	supportsBytestreamCompression := false
	for _, compressorValue := range rsp.GetCacheCapabilities().GetSupportedCompressors() {
		if compressorValue == repb.Compressor_ZSTD {
			supportsBytestreamCompression = true
			break
		}
	}

	supportsBatchUpdateCompression := false
	for _, compressorValue := range rsp.GetCacheCapabilities().GetSupportedBatchUpdateCompressors() {
		if compressorValue == repb.Compressor_ZSTD {
			supportsBatchUpdateCompression = true
			break
		}
	}
	return supportsBytestreamCompression && supportsBatchUpdateCompression, nil
}

// BatchCASUploader uploads many files to CAS concurrently, batching small
// uploads together and falling back to bytestream uploads for large files.
type BatchCASUploader struct {
	ctx             context.Context
	env             environment.Env
	once            *sync.Once
	compress        bool
	eg              *errgroup.Group
	unsentBatchReq  *repb.BatchUpdateBlobsRequest
	uploads         map[digest.Key]struct{}
	instanceName    string
	digestFunction  repb.DigestFunction_Value
	unsentBatchSize int64
	stats           UploadStats
}

// NewBatchCASUploader returns an uploader to be used only for the given request
// context (it should not be used outside the lifecycle of the request).
func NewBatchCASUploader(ctx context.Context, env environment.Env, instanceName string, digestFunction repb.DigestFunction_Value) *BatchCASUploader {
	eg, ctx := errgroup.WithContext(ctx)
	return &BatchCASUploader{
		ctx:             ctx,
		env:             env,
		once:            &sync.Once{},
		compress:        false,
		eg:              eg,
		unsentBatchReq:  &repb.BatchUpdateBlobsRequest{InstanceName: instanceName, DigestFunction: digestFunction},
		unsentBatchSize: 0,
		instanceName:    instanceName,
		digestFunction:  digestFunction,
		uploads:         make(map[digest.Key]struct{}),
	}
}

func (ul *BatchCASUploader) supportsCompression() bool {
	ul.once.Do(func() {
		if !*enableUploadCompression {
			return
		}
		capabilitiesClient := ul.env.GetCapabilitiesClient()
		if capabilitiesClient == nil {
			log.Warningf("Upload compression was enabled but no capabilities client found. Cannot verify cache server capabilities")
			return
		}
		enabled, err := SupportsCompression(ul.ctx, capabilitiesClient)
		if err != nil {
			log.Errorf("Error determinining if cache server supports compression: %s", err)
		}
		if enabled {
			ul.compress = true
		} else {
			log.Debugf("Upload compression was enabled but remote server did not support compression")
		}
	})
	return ul.compress
}

// Upload adds the given content to the current batch or begins a streaming
// upload if it exceeds the maximum batch size. It closes r when it is no
// longer needed.
func (ul *BatchCASUploader) Upload(d *repb.Digest, rsc io.ReadSeekCloser) error {
	// De-dupe uploads by digest.
	dk := digest.NewKey(d)
	if _, ok := ul.uploads[dk]; ok {
		ul.stats.DuplicateBytes += d.GetSizeBytes()
		return rsc.Close()
	}
	ul.uploads[dk] = struct{}{}
	ul.stats.UploadedObjects++
	ul.stats.UploadedBytes += d.GetSizeBytes()

	rsc.Seek(0, 0)
	r := io.ReadCloser(rsc)

	compressor := repb.Compressor_IDENTITY
	if ul.supportsCompression() && d.GetSizeBytes() >= minSizeBytesToCompress {
		compressor = repb.Compressor_ZSTD
	}

	if d.GetSizeBytes() > batchUploadLimitBytes {
		resourceName := digest.NewCASResourceName(d, ul.instanceName, ul.digestFunction)
		resourceName.SetCompressor(compressor)

		byteStreamClient := ul.env.GetByteStreamClient()
		if byteStreamClient == nil {
			return status.InvalidArgumentError("missing bytestream client")
		}
		ul.eg.Go(func() error {
			defer r.Close()
			_, _, err := UploadFromReader(ul.ctx, byteStreamClient, resourceName, r)
			return err
		})
		return nil
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := r.Close(); err != nil {
		return err
	}

	if compressor == repb.Compressor_ZSTD {
		b = compression.CompressZstd(nil, b)
	}
	additionalSize := int64(len(b))
	if ul.unsentBatchSize+additionalSize > batchUploadLimitBytes {
		ul.flushCurrentBatch()
	}
	ul.unsentBatchReq.Requests = append(ul.unsentBatchReq.Requests, &repb.BatchUpdateBlobsRequest_Request{
		Digest:     d,
		Data:       b,
		Compressor: compressor,
	})
	ul.unsentBatchSize += additionalSize
	return nil
}

func (ul *BatchCASUploader) UploadProto(in proto.Message) (*repb.Digest, error) {
	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}
	d, err := digest.Compute(bytes.NewReader(data), ul.digestFunction)
	if err != nil {
		return nil, err
	}
	if err := ul.Upload(d, NewBytesReadSeekCloser(data)); err != nil {
		return nil, err
	}
	return d, nil
}

func (ul *BatchCASUploader) UploadFile(path string) (*repb.Digest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	d, err := digest.Compute(f, ul.digestFunction)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	// Add output files to the filecache.
	if ul.env.GetFileCache() != nil {
		if err := ul.env.GetFileCache().AddFile(ul.ctx, &repb.FileNode{Digest: d, IsExecutable: isExecutable(info)}, path); err != nil {
			log.Warningf("Error adding file to filecache: %s", err)
		}
	}

	// Note: uploader.Upload will close the file.
	if err := ul.Upload(d, f); err != nil {
		return nil, err
	}
	return d, nil
}

func (ul *BatchCASUploader) flushCurrentBatch() error {
	casClient := ul.env.GetContentAddressableStorageClient()
	if casClient == nil {
		return status.InvalidArgumentError("missing CAS client")
	}

	req := ul.unsentBatchReq
	ul.unsentBatchReq = &repb.BatchUpdateBlobsRequest{
		InstanceName:   ul.instanceName,
		DigestFunction: ul.digestFunction,
	}
	ul.unsentBatchSize = 0
	ul.eg.Go(func() error {
		rsp, err := retry.Do(ul.ctx, retryOptions("BatchUpdateBlobs"), func(ctx context.Context) (*repb.BatchUpdateBlobsResponse, error) {
			ctx, cancel := context.WithTimeout(ctx, *casRPCTimeout)
			defer cancel()
			return casClient.BatchUpdateBlobs(ctx, req)
		})
		if err != nil {
			return err
		}
		for i, fileResponse := range rsp.GetResponses() {
			if fileResponse.GetStatus().GetCode() == int32(gcodes.DataLoss) && i < len(req.GetRequests()) {
				// If there is a hash mismatch, re-hash the uncompressed payload
				// to check whether a concurrent mutation occurred after we
				// computed the original digest.
				ri := req.GetRequests()[i]
				b, err := decompressBytes(ri.GetData(), ri.GetDigest(), ri.GetCompressor())
				if err != nil {
					log.CtxWarningf(ul.ctx, "Error decompressing blob while checking for concurrent mutation: %s", err)
				} else {
					if err := checkConcurrentMutation(fileResponse.GetDigest(), ul.digestFunction, bytes.NewReader(b)); err != nil {
						return status.WrapError(err, "check for concurrent mutation during upload")
					}
				}
			}
			if fileResponse.GetStatus().GetCode() != int32(gcodes.OK) {
				return gstatus.Error(gcodes.Code(fileResponse.GetStatus().GetCode()), fmt.Sprintf("Error uploading file: %v", fileResponse.GetDigest()))
			}
		}
		return nil
	})
	return nil
}

func (ul *BatchCASUploader) Wait() error {
	if len(ul.unsentBatchReq.GetRequests()) > 0 {
		if err := ul.flushCurrentBatch(); err != nil {
			return err
		}
	}
	return ul.eg.Wait()
}

func decompressBytes(b []byte, d *repb.Digest, compressor repb.Compressor_Value) ([]byte, error) {
	if compressor == repb.Compressor_ZSTD {
		buf := make([]byte, 0, d.GetSizeBytes())
		return compression.DecompressZstd(buf, b)
	}
	return b, nil
}

// Re-computes the digest for the given reader and compares it to a digest
// computed previously. Returns an error if the digests do not match or if
// there is an error re-computing the digest.
func checkConcurrentMutation(originalDigest *repb.Digest, digestFunction repb.DigestFunction_Value, r io.ReadSeeker) error {
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return status.DataLossErrorf("seek: %s", err)
	}
	computedDigest, err := digest.Compute(r, digestFunction)
	if err != nil {
		return status.DataLossErrorf("recompute digest: %s", err)
	}
	if !digest.Equal(computedDigest, originalDigest) {
		return status.DataLossErrorf("possible concurrent mutation detected: digest changed from %s to %s", digest.String(originalDigest), digest.String(computedDigest))
	}
	return nil
}

// UploadStats contains the statistics for a batch of uploads.
type UploadStats struct {
	UploadedObjects, UploadedBytes, DuplicateBytes int64
}

// Stats returns information about all the uploads in this BatchCASUploader.
// It's only correct to call it after Wait.
func (ul *BatchCASUploader) Stats() UploadStats {
	return ul.stats
}

type bytesReadSeekCloser struct {
	io.ReadSeeker
}

func NewBytesReadSeekCloser(b []byte) io.ReadSeekCloser {
	return &bytesReadSeekCloser{bytes.NewReader(b)}
}
func (*bytesReadSeekCloser) Close() error { return nil }

// UploadDirectoryToCAS uploads all the files in a given directory to the CAS
// as well as the directory structure, and returns the digest of the root
// Directory proto that can be used to fetch the uploaded contents.
func UploadDirectoryToCAS(ctx context.Context, env environment.Env, instanceName string, digestFunction repb.DigestFunction_Value, rootDirPath string) (*repb.Digest, *repb.Digest, error) {
	ul := NewBatchCASUploader(ctx, env, instanceName, digestFunction)

	// Recursively find and upload all descendant dirs.
	visited, rootDirectoryDigest, err := uploadDir(ul, rootDirPath, nil /*=visited*/)
	if err != nil {
		return nil, nil, err
	}
	if len(visited) == 0 {
		return nil, nil, status.InternalError("empty directory list after uploading directory tree; this should never happen")
	}
	// Upload the tree, which consists of the root dir as well as all descendant
	// dirs.
	rootTree := &repb.Tree{Root: visited[0], Children: visited[1:]}
	treeDigest, err := ul.UploadProto(rootTree)
	if err != nil {
		return nil, nil, err
	}
	if err := ul.Wait(); err != nil {
		return nil, nil, err
	}
	return rootDirectoryDigest, treeDigest, nil
}

func uploadDir(ul *BatchCASUploader, dirPath string, visited []*repb.Directory) ([]*repb.Directory, *repb.Digest, error) {
	dir := &repb.Directory{}
	// Append the directory before doing any other work, so that the root
	// directory is located at visited[0] at the end of recursion.
	visited = append(visited, dir)
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, nil, err
	}
	for _, entry := range entries {
		name := entry.Name()
		path := filepath.Join(dirPath, name)

		if entry.IsDir() {
			var d *repb.Digest
			visited, d, err = uploadDir(ul, path, visited)
			if err != nil {
				return nil, nil, err
			}
			dir.Directories = append(dir.Directories, &repb.DirectoryNode{
				Name:   name,
				Digest: d,
			})
		} else if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return nil, nil, err
			}
			d, err := ul.UploadFile(path)
			if err != nil {
				return nil, nil, err
			}
			dir.Files = append(dir.Files, &repb.FileNode{
				Name:         name,
				Digest:       d,
				IsExecutable: isExecutable(info),
			})
		} else if entry.Type()&os.ModeSymlink == os.ModeSymlink {
			target, err := os.Readlink(path)
			if err != nil {
				return nil, nil, err
			}
			dir.Symlinks = append(dir.Symlinks, &repb.SymlinkNode{
				Name:   name,
				Target: target,
			})
		}
	}
	digest, err := ul.UploadProto(dir)
	if err != nil {
		return nil, nil, err
	}
	return visited, digest, nil
}

func isExecutable(info os.FileInfo) bool {
	return info.Mode()&0100 != 0
}

type tree struct {
	dirs     []*repb.Directory
	subtrees []*digest.CASResourceName
}

func streamTreeWithRetries(ctx context.Context, casClient repb.ContentAddressableStorageClient, root *digest.CASResourceName, sendCachedSubtreeDigests bool) (*tree, error) {
	return retry.Do(ctx, retryOptions("StreamTree"), func(ctx context.Context) (*tree, error) {
		t, err := streamTree(ctx, casClient, root, sendCachedSubtreeDigests)
		if status.IsNotFoundError(err) {
			return nil, retry.NonRetryableError(err)
		}
		return t, err
	})
}

func streamTree(ctx context.Context, casClient repb.ContentAddressableStorageClient, root *digest.CASResourceName, sendCachedSubtreeDigests bool) (*tree, error) {
	var dirs []*repb.Directory
	var subtrees []*digest.CASResourceName
	nextPageToken := ""
	for {
		stream, err := casClient.GetTree(ctx, &repb.GetTreeRequest{
			RootDigest:               root.GetDigest(),
			InstanceName:             root.GetInstanceName(),
			PageToken:                nextPageToken,
			DigestFunction:           root.GetDigestFunction(),
			SendCachedSubtreeDigests: sendCachedSubtreeDigests,
		})
		if err != nil {
			return nil, err
		}
		for {
			rsp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			nextPageToken = rsp.GetNextPageToken()
			dirs = append(dirs, rsp.GetDirectories()...)
			for _, st := range rsp.GetSubtrees() {
				subtrees = append(subtrees, digest.NewCASResourceName(st.GetDigest(), st.GetInstanceName(), st.GetDigestFunction()))
			}
		}
		if nextPageToken == "" {
			break
		}
	}
	if !sendCachedSubtreeDigests && len(subtrees) > 0 {
		return nil, status.InternalError("Received subtrees even though they weren't requested!")
	}
	return &tree{dirs: dirs, subtrees: subtrees}, nil
}

// Makes a salted pointer to the specified file in the filecache - we add in
// the salt so that we can invalidate the entire filecache-based treecache
// if needed.
// Exposed for testing--no reason to use outside of this package.
func MakeFileNode(r *digest.CASResourceName) (*repb.FileNode, error) {
	buf := strings.NewReader(fmt.Sprintf("%s/%d", r.GetDigest().GetHash(), r.GetDigest().GetSizeBytes()) + r.GetInstanceName() + "_treecache_" + *filecacheTreeSalt)
	d, err := digest.Compute(buf, r.GetDigestFunction())
	if err != nil {
		return nil, err
	}
	return &repb.FileNode{Digest: &repb.Digest{Hash: d.GetHash(), SizeBytes: 0}}, nil
}

func getTreeCacheFromFilecache(ctx context.Context, r *digest.CASResourceName, fc interfaces.FileCache) (*capb.TreeCache, int, error) {
	file, err := MakeFileNode(r)
	if err != nil {
		return nil, 0, err
	}
	buf, err := fc.Read(ctx, file)
	if err != nil {
		return nil, 0, err
	}
	out := &capb.TreeCache{}
	if err := proto.Unmarshal(buf, out); err != nil {
		return nil, 0, err
	}

	return out, len(buf), nil
}

func writeTreeCacheToFilecache(ctx context.Context, r *digest.CASResourceName, data *capb.TreeCache, fc interfaces.FileCache) (int, error) {
	file, err := MakeFileNode(r)
	if err != nil {
		return 0, err
	}
	contents, err := proto.Marshal(data)
	if err != nil {
		return 0, err
	}

	return fc.Write(ctx, file, contents)
}

func getSubtree(ctx context.Context, subtree *digest.CASResourceName, fc interfaces.FileCache, bs bspb.ByteStreamClient) ([]*capb.DirectoryWithDigest, error) {
	// First, check the filecache.
	treeCache, bytesRead, err := getTreeCacheFromFilecache(ctx, subtree, fc)
	if err == nil {
		// We found the file in the filecache.
		metrics.GetTreeFilecacheBytesRead.Add(float64(bytesRead))
		metrics.GetTreeFilecacheTreesRead.Add(1)
		metrics.GetTreeDirectoryLookupCount.With(prometheus.Labels{
			metrics.GetTreeLookupLocation: "filecache",
		}).Add(float64(len(treeCache.GetChildren())))
	} else if status.IsNotFoundError(err) {
		// Not in the filecache--fetch from bytestream instead.
		treeCache = &capb.TreeCache{}
		err := GetBlobAsProto(ctx, bs, subtree, treeCache)
		if err != nil {
			// Shouldn't happen! The remote server just told us about the tree.
			return nil, err
		}
		bytesWritten, err := writeTreeCacheToFilecache(ctx, subtree, treeCache, fc)
		if err != nil {
			return nil, err
		}
		metrics.GetTreeFilecacheBytesWritten.Add(float64(bytesWritten))
		metrics.GetTreeFilecacheTreesWritten.Add(1)
		metrics.GetTreeDirectoryLookupCount.With(prometheus.Labels{
			metrics.GetTreeLookupLocation: "remote",
		}).Add(float64(len(treeCache.GetChildren())))
	} else {
		// Filecache read failed for some more nefarious reason.
		return nil, err
	}

	// Fetch any treecache splits.  This looks recursive, but we currently
	// guarantee that splits will only be one level deep.  This splitting
	// exists to save storage space for large node_modules dirs, etc.
	if len(treeCache.GetTreeCacheChildren()) > 0 {
		subtreeEG, subtreeCtx := errgroup.WithContext(ctx)
		var stMutex sync.Mutex
		for _, child := range treeCache.GetTreeCacheChildren() {
			subtreeEG.Go(func() error {
				r, err := digest.CASResourceNameFromProto(child)
				if err != nil {
					return err
				}
				childDirs, err := getSubtree(subtreeCtx, r, fc, bs)
				if err != nil {
					return err
				}

				stMutex.Lock()
				defer stMutex.Unlock()
				treeCache.Children = append(treeCache.Children, childDirs...)
				return nil
			})
		}
		if err := subtreeEG.Wait(); err != nil {
			return nil, err
		}
	}

	return treeCache.Children, nil
}

func getAndCacheTreeFromRootDirectoryDigest(ctx context.Context, casClient repb.ContentAddressableStorageClient, r *digest.CASResourceName, fc interfaces.FileCache, bs bspb.ByteStreamClient) ([]*repb.Directory, error) {
	tree, err := streamTreeWithRetries(ctx, casClient, r, true && fc != nil)
	if err != nil {
		return nil, err
	}

	uncachedDirCount := len(tree.dirs)
	metrics.GetTreeDirectoryLookupCount.With(prometheus.Labels{
		metrics.GetTreeLookupLocation: "uncached",
	}).Add(float64(uncachedDirCount))

	allStDirs := make([]*repb.Directory, 0)

	// Fetch each subtree digest.
	if len(tree.subtrees) > 0 {
		var stMutex sync.Mutex
		subtreeEG, subtreeCtx := errgroup.WithContext(ctx)
		for _, st := range tree.subtrees {
			subtreeEG.Go(func() error {
				stDirs, err := getSubtree(subtreeCtx, st, fc, bs)
				if err != nil {
					return err
				}

				stMutex.Lock()
				defer stMutex.Unlock()
				for _, dwd := range stDirs {
					allStDirs = append(allStDirs, dwd.GetDirectory())
				}
				return nil
			})
		}
		if err := subtreeEG.Wait(); err != nil {
			return nil, err
		}
	}

	tree.dirs = append(tree.dirs, allStDirs...)

	// TODO(jdhollen): if we want, we can dedupe the directories here using the
	// DirectoryWithDigest protos above.  Doesn't seem critical for now, though.
	return tree.dirs, nil
}

func GetAndMaybeCacheTreeFromRootDirectoryDigest(ctx context.Context, casClient repb.ContentAddressableStorageClient, r *digest.CASResourceName, fc interfaces.FileCache, bs bspb.ByteStreamClient) (*repb.Tree, error) {
	var dirs []*repb.Directory
	if fc != nil && bs != nil && *requestCachedSubtreeDigests {
		out, err := getAndCacheTreeFromRootDirectoryDigest(ctx, casClient, r, fc, bs)
		if err != nil {
			return nil, err
		}
		dirs = out
	} else {
		tree, err := streamTreeWithRetries(ctx, casClient, r, false)
		if err != nil {
			return nil, err
		}
		if len(tree.subtrees) > 0 {
			return nil, status.InternalError("GetTree received a tree with subtrees, but subtrees are disabled.")
		}
		dirs = tree.dirs
	}

	if len(dirs) == 0 {
		return &repb.Tree{Root: &repb.Directory{}}, nil
	}

	return &repb.Tree{
		Root:     dirs[0],
		Children: dirs[1:],
	}, nil
}

func GetTreeFromRootDirectoryDigest(ctx context.Context, casClient repb.ContentAddressableStorageClient, r *digest.CASResourceName) (*repb.Tree, error) {
	return GetAndMaybeCacheTreeFromRootDirectoryDigest(ctx, casClient, r, nil, nil)
}

func maybeSetCompressor(rn *digest.CASResourceName) {
	if *enableUploadCompression && rn.GetDigest().GetSizeBytes() >= minSizeBytesToCompress {
		rn.SetCompressor(repb.Compressor_ZSTD)
	}
}

type UploadWriter struct {
	ctx          context.Context
	stream       bspb.ByteStream_WriteClient
	sender       rpcutil.Sender[*bspb.WriteRequest]
	uploadString string

	bytesUploaded int64
	committedSize int64

	sendErr   error
	committed bool
	closed    bool

	buf           []byte
	bytesBuffered int

	useZstd bool
	cbuf    []byte
}

// Write copies the input bytes to an internal buffer and may send some or all of the bytes to the CAS.
// Bytes are not guaranteed to be uploaded to the CAS until a call to Commit() succeeds.
// Returning status.AlreadyExists indicates that the blob already exists in the CAS and no further writes are necessary.
func (uw *UploadWriter) Write(p []byte) (int, error) {
	if uw.closed {
		return 0, status.FailedPreconditionError("Cannot write to UploadWriter after it is closed")
	}
	if uw.committed {
		return 0, status.FailedPreconditionError("Cannot write to UploadWriter after it is committed")
	}
	if uw.sendErr != nil {
		return 0, status.WrapError(uw.sendErr, "UploadWriter already encountered send error, cannot write")
	}
	written := 0
	for len(p) > 0 {
		n := copy(uw.buf[uw.bytesBuffered:], p)
		uw.bytesBuffered += n
		written += n
		if err := uw.flush(false /* finish */); err != nil {
			return written, err
		}
		p = p[n:]
	}
	return written, nil
}

// flush sends a WriteRequest to the CAS if the internal buffer is full
// or if this is the last write (finish=true).
func (uw *UploadWriter) flush(finish bool) error {
	if !finish && uw.bytesBuffered < uploadBufSizeBytes {
		return nil
	}
	data := uw.buf[:uw.bytesBuffered]
	if uw.useZstd {
		// If CompressZstd allocates a new buffer for the compressed bytes,
		// use it to send the request and then let it be garbaged collected.
		// We will put uw.cbuf back into the pool on Close().
		data = compression.CompressZstd(uw.cbuf[:0], uw.buf[:uw.bytesBuffered])
	}
	req := &bspb.WriteRequest{
		Data:         data,
		ResourceName: uw.uploadString,
		WriteOffset:  uw.bytesUploaded,
		FinishWrite:  finish,
	}
	uw.sendErr = uw.sender.SendWithTimeoutCause(req, *casRPCTimeout, status.DeadlineExceededError("Timed out sending Write request"))
	if uw.sendErr != nil {
		if uw.sendErr == io.EOF {
			// The server closed the stream, so we need to call CloseAndRecv
			// to find the error.
			rsp, err := uw.stream.CloseAndRecv()
			if err == nil {
				uw.committedSize = rsp.GetCommittedSize()
				// byte_stream_server.go doesn't return an error when the
				// resource already exists, but this is the only case where
				// send returns EOF and recv returns nil.
				err = status.AlreadyExistsError(req.GetResourceName())
			}
			uw.sendErr = err
		}
		return uw.sendErr
	}
	uw.bytesUploaded += int64(len(data))
	uw.bytesBuffered = 0
	return nil
}

// Commit sends any bytes remaining in the internal buffer to the CAS
// and tells the server that the stream is done sending writes.
func (uw *UploadWriter) Commit() error {
	if uw.closed {
		return status.FailedPreconditionError("UploadWriter already closed, cannot commit")
	}
	if uw.committed {
		return nil
	}
	if uw.sendErr != nil {
		if status.IsAlreadyExistsError(uw.sendErr) {
			return nil
		}
		return status.WrapError(uw.sendErr, "UploadWriter already encountered send error, cannot commit")
	}

	uw.committed = true
	err := uw.flush(true /* finish */)
	if err != nil {
		if status.IsAlreadyExistsError(err) {
			// If we know the resource already exists, flush already called
			// CloseAndRecv and populated uw.committedSize. Don't return an
			// error because from the clients point of view, the write
			// succeeded. Also, sends are not guaranteed to return an EOF, so
			// it's possible the server didn't let us know that the object
			// exists. For consistency, it's better to always return nil instead
			// of sometimes returning AlreadyExists here.
			return nil
		}
		return err
	}
	rsp, err := uw.stream.CloseAndRecv()
	if err != nil {
		return err
	}
	uw.committedSize = rsp.GetCommittedSize()
	return nil
}

// Close closes the underlying stream and returns an internal buffer to the pool.
// It is expected (and safe) to call Close even if Commit fails.
func (uw *UploadWriter) Close() error {
	if uw.closed {
		return status.FailedPreconditionError("UploadWriter already closed, cannot close again")
	}
	uw.closed = true
	uploadBufPool.Put(uw.buf)
	if uw.useZstd {
		uploadBufPool.Put(uw.cbuf)
	}
	return nil
}

func (uw *UploadWriter) GetCommittedSize() int64 {
	return uw.committedSize
}

func (uw *UploadWriter) GetBytesUploaded() int64 {
	return uw.bytesUploaded
}

// Assert that UploadWriter implements CommittedWriteCloser
var _ interfaces.CommittedWriteCloser = (*UploadWriter)(nil)

// NewUploadWriter returns an UploadWriter that writes to the CAS for the specific resource name.
// The blob is guaranteed to be written to the CAS only if all Write(...) calls and the Commit() calls succeed.
// The caller is responsible for checking data integrity using GetCommittedSize() and GetBytesUploaded().
//
// You must call Close() on an UploadWriter.
// You also must either
//   - call Commit() on the UploadWriter or
//   - encounter a non-EOF error in a call to Write(...)
func NewUploadWriter(ctx context.Context, bsClient bspb.ByteStreamClient, r *digest.CASResourceName) (*UploadWriter, error) {
	if bsClient == nil {
		return nil, status.FailedPreconditionError("ByteStreamClient not configured")
	}
	stream, err := bsClient.Write(ctx)
	if err != nil {
		return nil, err
	}
	sender := rpcutil.NewSender[*bspb.WriteRequest](ctx, stream)
	if r.GetCompressor() == repb.Compressor_ZSTD {
		return &UploadWriter{
			ctx:          ctx,
			stream:       stream,
			sender:       sender,
			uploadString: r.NewUploadString(),
			buf:          uploadBufPool.Get(),

			useZstd: true,
			cbuf:    uploadBufPool.Get(),
		}, nil
	}
	return &UploadWriter{
		ctx:          ctx,
		stream:       stream,
		sender:       sender,
		uploadString: r.NewUploadString(),
		buf:          uploadBufPool.Get(),
	}, nil
}

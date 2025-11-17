package byte_stream_server_proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/cdc_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/chunk_metadata_store"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_crypter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

var (
	// CDC (Content-Defined Chunking) configuration for proxy layer
	enableCDC             = flag.Bool("cache_proxy.enable_cdc", false, "If true, enable content-defined chunking at the proxy layer to optimize bandwidth to remote cache")
	minBlobSizeToChunk    = flag.Int64("cache_proxy.min_blob_size_to_chunk", 256*1024*1024, "Minimum blob size in bytes to trigger chunking at proxy layer. Defaults to 256MB.")
	averageChunkSizeBytes = flag.Int("cache_proxy.average_chunk_size_bytes", 256*1024, "Average size of chunks for CDC at proxy layer. Must be a power of 2 between 256B and 256MB. Defaults to 256KB.")
)

type ByteStreamServerProxy struct {
	supportsEncryption func(context.Context) bool
	atimeUpdater       interfaces.AtimeUpdater
	authenticator      interfaces.Authenticator
	local              interfaces.ByteStreamServer
	remote             bspb.ByteStreamClient
	localCache         interfaces.Cache // For CDC chunk operations
	remoteCASmClient   repb.ContentAddressableStorageClient
	chunkMetadataStore *chunk_metadata_store.ChunkMetadataStore
}

func Register(env *real_environment.RealEnv) error {
	proxy, err := New(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ByteStreamServerProxy: %s", err)
	}
	env.SetByteStreamServer(proxy)
	return nil
}

func New(env environment.Env) (*ByteStreamServerProxy, error) {
	atimeUpdater := env.GetAtimeUpdater()
	if atimeUpdater == nil {
		return nil, fmt.Errorf("An AtimeUpdater is required to enable ByteStreamServerProxy")
	}
	authenticator := env.GetAuthenticator()
	if authenticator == nil {
		return nil, fmt.Errorf("An Authenticator is required to enable ByteStreamServerProxy")
	}
	remote := env.GetByteStreamClient()
	if remote == nil {
		return nil, fmt.Errorf("A remote ByteStreamClient is required to enable ByteStreamServerProxy")
	}
	local := env.GetLocalByteStreamServer()
	if local == nil {
		return nil, fmt.Errorf("A local ByteStreamServer is required to enable ByteStreamServerProxy")
	}

	proxy := &ByteStreamServerProxy{
		supportsEncryption: remote_crypter.SupportsEncryption(env),
		atimeUpdater:       atimeUpdater,
		authenticator:      authenticator,
		local:              local,
		remote:             remote,
	}

	// Initialize CDC components if enabled
	if *enableCDC {
		localCache := env.GetCache()
		if localCache == nil {
			return nil, fmt.Errorf("CDC enabled but no local cache available")
		}
		remoteCAS := env.GetContentAddressableStorageClient()
		if remoteCAS == nil {
			return nil, fmt.Errorf("CDC enabled but no remote CAS client available")
		}
		proxy.localCache = localCache
		proxy.remoteCASmClient = remoteCAS
		proxy.chunkMetadataStore = chunk_metadata_store.NewChunkMetadataStore(localCache)
		log.Infof("ByteStreamServerProxy: CDC enabled (minBlobSize=%d, avgChunkSize=%d)",
			*minBlobSizeToChunk, *averageChunkSizeBytes)
	}

	return proxy, nil
}

// Wrapper around a ByteStream_ReadServer that counts the number of frames
// and bytes read through it.
type meteredReadServerStream struct {
	bytes  int64
	frames int64
	bspb.ByteStream_ReadServer
}

func (s *meteredReadServerStream) Send(message *bspb.ReadResponse) error {
	s.bytes += int64(len(message.GetData()))
	s.frames++
	return s.ByteStream_ReadServer.Send(message)
}

func (s *ByteStreamServerProxy) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	requestTypeLabel := proxy_util.RequestTypeLabelFromContext(ctx)
	meteredStream := &meteredReadServerStream{ByteStream_ReadServer: stream}
	stream = meteredStream
	cacheStatus, err := s.read(ctx, req, meteredStream)
	recordReadMetrics(cacheStatus, requestTypeLabel, err, int(meteredStream.bytes))
	return err
}

func (s *ByteStreamServerProxy) read(ctx context.Context, req *bspb.ReadRequest, stream *meteredReadServerStream) (string, error) {
	if authutil.EncryptionEnabled(ctx, s.authenticator) && !s.supportsEncryption(ctx) {
		return metrics.UncacheableStatusLabel, s.readRemoteOnly(ctx, req, stream)
	}

	// Store auth headers in context so they can be reused between the
	// atime_updater and the hit_tracker_client.
	ctx = authutil.ContextWithCachedAuthHeaders(ctx, s.authenticator)

	if proxy_util.SkipRemote(ctx) {
		if err := s.readLocalOnly(req, stream); err != nil {
			log.CtxInfof(ctx, "Error reading local: %v", err)
			return metrics.MissStatusLabel, err
		}
		return metrics.HitStatusLabel, nil
	}

	rn, err := digest.ParseDownloadResourceName(req.GetResourceName())
	if err != nil {
		return metrics.HitStatusLabel, nil
	}

	// CDC READ PATH: Check if blob is stored as chunks locally
	if *enableCDC && s.chunkMetadataStore != nil {
		instanceName := rn.GetInstanceName()
		blobDigest := rn.GetDigest()
		digestFunction := rn.GetDigestFunction()

		// Check for chunk metadata
		chunkMetadata, err := s.chunkMetadataStore.Get(ctx, instanceName, blobDigest, digestFunction)
		if err != nil {
			log.CtxWarningf(ctx, "CDC Read: error checking chunk metadata: %s", err)
		} else if chunkMetadata != nil && len(chunkMetadata.GetResource()) > 0 {
			// Blob is chunked! Reconstruct from local chunks
			log.CtxDebugf(ctx, "CDC Read: blob %s has %d chunks, reconstructing",
				blobDigest.GetHash()[:8], len(chunkMetadata.GetResource()))

			err := s.readChunkedBlobFromLocal(ctx, chunkMetadata, instanceName, digestFunction, stream)
			if err == nil {
				s.atimeUpdater.EnqueueByResourceName(ctx, rn)
				return metrics.HitStatusLabel, nil
			}
			log.CtxWarningf(ctx, "CDC Read: failed to read chunks, falling back: %s", err)
			// Fall through to try whole blob or remote
		}
	}

	// Try reading as whole blob from local cache
	localErr := s.local.ReadCASResource(ctx, rn, req.GetReadOffset(), req.GetReadLimit(), stream)
	// If some responses were streamed to the client, just return the
	// error. Otherwise, fall-back to remote. We might be able to continue
	// streaming to the client by doing an offset read from the remote
	// cache, but keep it simple for now.
	if localErr != nil && stream.frames == 0 {
		// Recover from local error if no frames have been sent
		return metrics.MissStatusLabel, s.readRemoteWriteLocal(req, stream)
	} else {
		s.atimeUpdater.EnqueueByResourceName(ctx, rn)
		return metrics.HitStatusLabel, localErr
	}
}

func (s *ByteStreamServerProxy) readRemoteOnly(ctx context.Context, req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	remoteReadStream, err := s.remote.Read(ctx, req)
	if err != nil {
		log.CtxInfof(ctx, "error reading from remote: %s", err)
		return err
	}
	return flushToClient(ctx, remoteReadStream, stream)
}

func flushToClient(ctx context.Context, remoteReadStream bspb.ByteStream_ReadClient, stream bspb.ByteStream_ReadServer) error {
	for {
		message, err := remoteReadStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.CtxInfof(ctx, "Error streaming from remote for read: %s", err)
			return err
		}
		if err = stream.Send(message); err != nil {
			return err
		}
	}
}

func (s *ByteStreamServerProxy) readLocalOnly(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	return s.local.Read(req, stream)
}

func writeRequest(resourceName string, data []byte, offset int64, finishWrite bool) *bspb.WriteRequest {
	return &bspb.WriteRequest{
		ResourceName: resourceName,
		Data:         data,
		WriteOffset:  offset,
		FinishWrite:  finishWrite,
	}
}

type readThroughCacheStream struct {
	// This is just here so we can pass this to local.Write
	bspb.ByteStream_WriteServer

	ctx      context.Context
	remote   bspb.ByteStream_ReadClient
	client   bspb.ByteStream_ReadServer
	uploadRN string

	localWriteOffset   int64
	remoteRecvErr      error
	clientSendErr      error
	localWriteFinished bool
}

// Recv turns a ReadResponse from a remote read into a WriteRequest that gets
// returned to a local write.
func (r *readThroughCacheStream) Recv() (*bspb.WriteRequest, error) {
	resp, err := r.remote.Recv()
	if err != nil {
		r.remoteRecvErr = err
		if err == io.EOF {
			return writeRequest(r.uploadRN, nil, r.localWriteOffset, true), nil
		}
		return nil, err
	}

	if err := r.client.Send(resp); err != nil {
		// If the client isn't listening any more, quit here and don't write to
		// local
		r.clientSendErr = err
		return nil, err
	}
	req := writeRequest(r.uploadRN, resp.GetData(), r.localWriteOffset, false)
	r.localWriteOffset += int64(len(resp.GetData()))
	return req, nil
}

func (r *readThroughCacheStream) SendAndClose(resp *bspb.WriteResponse) error {
	// Ignore the local write response
	r.localWriteFinished = true
	return nil
}

func (r *readThroughCacheStream) Context() context.Context { return r.ctx }

func (s *ByteStreamServerProxy) readRemoteWriteLocal(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	// Rewrite the resource name so we can write to the local server
	rn, err := digest.ParseDownloadResourceName(req.GetResourceName())
	if err != nil {
		return err
	}
	remoteReadStream, err := s.remote.Read(ctx, req)
	if err != nil {
		log.CtxInfof(ctx, "error reading from remote: %s", err)
		return err
	}

	readThrough := &readThroughCacheStream{
		ctx:      ctx,
		remote:   remoteReadStream,
		client:   stream,
		uploadRN: rn.NewUploadString(),
	}
	localErr := s.local.Write(readThrough)

	if localErr != nil {
		log.CtxDebugf(ctx, "Error writing to local ByteStreamServer: %v", err)
	}
	if readThrough.clientSendErr != nil {
		return readThrough.clientSendErr
	}
	if readThrough.remoteRecvErr == io.EOF {
		return nil
	} else if readThrough.remoteRecvErr != nil {
		log.CtxInfof(ctx, "Error streaming from remote for read through: %s", err)
		return readThrough.remoteRecvErr
	}
	// The local write returned but remoteRecvErr != EOF, which means the read
	// isn't done, so flush the rest.
	if err := flushToClient(ctx, remoteReadStream, stream); err != nil {
		return err
	}
	if localErr == nil && !readThrough.localWriteFinished {
		// Only log this if the local write didn't fail.
		log.CtxInfo(ctx, "remote read done but local write is not")
	}
	return nil
}

func recordReadMetrics(cacheStatus string, proxyRequestType string, err error, bytesRead int) {
	labels := prometheus.Labels{
		metrics.StatusLabel:           strconv.Itoa(int(gstatus.Code(err))),
		metrics.CacheHitMissStatus:    cacheStatus,
		metrics.CacheProxyRequestType: proxyRequestType,
	}
	metrics.ByteStreamProxiedReadRequests.With(labels).Inc()
	metrics.ByteStreamProxiedReadBytes.With(labels).Add(float64(bytesRead))
}

// Wrapper around a ByteStream_WriteServer that counts the number of bytes
// written through it.
type meteredServerSideClientStream struct {
	bytes int64
	bspb.ByteStream_WriteServer
}

func (s *meteredServerSideClientStream) Recv() (*bspb.WriteRequest, error) {
	message, err := s.ByteStream_WriteServer.Recv()
	s.bytes += int64(len(message.GetData()))
	return message, err
}

func (s *ByteStreamServerProxy) Write(stream bspb.ByteStream_WriteServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	requestTypeLabel := proxy_util.RequestTypeLabelFromContext(stream.Context())
	meteredStream := &meteredServerSideClientStream{ByteStream_WriteServer: stream}
	stream = meteredStream
	var err error
	if authutil.EncryptionEnabled(ctx, s.authenticator) && !s.supportsEncryption(ctx) {
		err = s.writeRemoteOnly(ctx, stream)
	} else if proxy_util.SkipRemote(ctx) {
		err = s.writeLocalOnly(stream)
	} else {
		err = s.dualWrite(ctx, stream)
	}
	recordWriteMetrics(meteredStream.bytes, err, requestTypeLabel)
	return err
}

func (s *ByteStreamServerProxy) writeRemoteOnly(ctx context.Context, stream bspb.ByteStream_WriteServer) error {
	remoteStream, err := s.remote.Write(ctx)
	if err != nil {
		return err
	}
	if err := flushToRemote(stream, remoteStream); err != nil {
		return err
	}
	resp, err := remoteStream.CloseAndRecv()
	if err != nil {
		return err
	}
	return stream.SendAndClose(resp)
}

func (s *ByteStreamServerProxy) writeLocalOnly(stream bspb.ByteStream_WriteServer) error {
	return s.local.Write(stream)
}

type forwardingWriteStream struct {
	bspb.ByteStream_WriteServer
	remote          bspb.ByteStream_WriteClient
	recvErr         error
	remoteSendErr   error
	finishWriteSent bool
	localFinished   bool
}

func (s *forwardingWriteStream) Recv() (*bspb.WriteRequest, error) {
	req, err := s.ByteStream_WriteServer.Recv()
	if err != nil {
		s.recvErr = err
		return nil, err
	}
	s.remoteSendErr = s.remote.Send(req)
	if s.remoteSendErr != nil {
		// Tell the local server to abandon the request since the remote request
		// finished or failed. We could instead let local finish when remote
		// finishes early (with EOF). This might take longer, but we would have
		// the blob locally. For now, we rely on the fact that if someone needs
		// this blob, we'll then read it from remote and write it locally.
		return nil, io.EOF
	}
	if req.GetFinishWrite() {
		s.finishWriteSent = true
	}
	return req, nil
}

func (s *forwardingWriteStream) SendAndClose(resp *bspb.WriteResponse) error {
	// Ignore the local response
	s.localFinished = true
	return nil
}

func (s *ByteStreamServerProxy) dualWrite(ctx context.Context, stream bspb.ByteStream_WriteServer) error {
	// If CDC is not enabled, use the simple forwarding approach
	// if !*enableCDC || s.chunkMetadataStore == nil {
	// 	return s.dualWriteSimple(ctx, stream)
	// }

	// CDC enabled: use CDC-aware write path
	return s.dualWriteWithCDC(ctx, stream)
}

// dualWriteSimple is the original dualWrite implementation without CDC
func (s *ByteStreamServerProxy) dualWriteSimple(ctx context.Context, stream bspb.ByteStream_WriteServer) error {
	remoteStream, err := s.remote.Write(ctx)
	if err != nil {
		return err
	}
	forwarding := &forwardingWriteStream{ByteStream_WriteServer: stream, remote: remoteStream}
	localErr := s.local.Write(forwarding)

	if forwarding.recvErr != nil {
		return forwarding.recvErr
	}
	if localErr != nil {
		log.CtxInfof(ctx, "error writing to local bytestream server for write: %s", localErr)
	}
	if forwarding.remoteSendErr != nil && forwarding.remoteSendErr != io.EOF {
		// Remote failed, so fail the whole request
		return forwarding.remoteSendErr
	}
	if !forwarding.finishWriteSent && forwarding.remoteSendErr == nil {
		// Local write returned, but remoteSendErr != EOF so we need to forward
		// the rest.
		if localErr == nil {
			log.CtxInfo(ctx, "local write done but remote write is not")
		}
		if err := flushToRemote(stream, remoteStream); err != nil {
			return err
		}
	}
	if localErr == nil && !forwarding.localFinished {
		// Only log this if the local write didn't fail.
		log.CtxInfo(ctx, "remote write done but local write is not")
	}
	resp, err := remoteStream.CloseAndRecv()
	if err != nil {
		return err
	}
	return stream.SendAndClose(resp)
}

// dualWriteWithCDC implements CDC-aware dual write with streaming:
// 1. Streams data through decompressor (if compressed) → chunker → compressor
// 2. Stores chunks to local cache (always chunked, never full blob)
// 3. Uploads only missing chunks to remote
// 4. Stores chunk metadata locally
func (s *ByteStreamServerProxy) dualWriteWithCDC(ctx context.Context, stream bspb.ByteStream_WriteServer) error {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()

	// STEP 1: Parse resource name from first message to get blob info
	firstReq, err := stream.Recv()
	if err != nil {
		return err
	}

	resourceName := firstReq.GetResourceName()
	if resourceName == "" {
		return status.InvalidArgumentError("resource name is required")
	}

	rn, err := digest.ParseUploadResourceName(resourceName)
	if err != nil {
		return err
	}

	blobDigest := rn.GetDigest()
	instanceName := rn.GetInstanceName()
	digestFunction := rn.GetDigestFunction()

	compressor := rn.GetCompressor()
	log.CtxDebugf(ctx, "CDC Write: blob=%s size=%d compressor=%s", blobDigest.GetHash(), blobDigest.GetSizeBytes(), compressor)

	// STEP 2: Create a reader from the WriteServer stream
	// This will stream data as it arrives, not buffer everything
	streamReader := &writeStreamReader{
		stream:    stream,
		firstReq:  firstReq,
		firstUsed: false,
	}

	// STEP 3: Set up decompression pipeline if needed
	// CDC works best on uncompressed data to find meaningful content boundaries.
	// If the blob is compressed, we decompress it before chunking, then compress
	// each chunk individually.
	var uncompressedReader io.Reader = streamReader
	var decompressor io.ReadCloser

	if compressor == repb.Compressor_ZSTD {
		log.CtxDebugf(ctx, "CDC Write: setting up ZSTD decompression stream")
		pr, pw := io.Pipe()
		decompressor, err = compression.NewZstdDecompressingReader(pr)
		if err != nil {
			pw.Close()
			return status.InternalErrorf("failed to create decompressor: %s", err)
		}

		// Start goroutine to read compressed data and write to decompressor pipe
		// Errors will propagate through the pipe to the decompressor reader
		go func() {
			defer pw.Close()
			if _, err := io.Copy(pw, streamReader); err != nil {
				pw.CloseWithError(err)
			}
		}()

		uncompressedReader = decompressor
	}

	// STEP 4: Track chunks as they're created
	var chunks []*cdc_proxy.ChunkInfo
	var chunkMetadata *sgpb.StorageMetadata_ChunkedMetadata
	var mu sync.Mutex

	// STEP 5: Stream through chunker, compressing and storing chunks as they're created
	writeChunk := func(chunkInfo *cdc_proxy.ChunkInfo) error {
		// Compress chunk if original was compressed
		var chunkDataToStore []byte
		var chunkRN *digest.CASResourceName
		if compressor == repb.Compressor_ZSTD {
			chunkDataToStore = compression.CompressZstd(nil, chunkInfo.Data)
			chunkRN = digest.NewCASResourceName(chunkInfo.Digest, instanceName, digestFunction)
			chunkRN.SetCompressor(repb.Compressor_ZSTD)
		} else {
			chunkDataToStore = chunkInfo.Data
			chunkRN = digest.NewCASResourceName(chunkInfo.Digest, instanceName, digestFunction)
		}

		// Store chunk to local cache (always chunk, never full blob)
		if err := s.localCache.Set(ctx, chunkRN.ToProto(), chunkDataToStore); err != nil {
			log.CtxWarningf(ctx, "CDC Write: failed to write chunk %s to local: %s",
				chunkInfo.Digest.GetHash(), err)
			// Continue with other chunks
		}

		// Track chunk for later remote upload
		mu.Lock()
		chunks = append(chunks, &cdc_proxy.ChunkInfo{
			Digest: chunkInfo.Digest,
			Data:   chunkDataToStore, // Store compressed data for upload
		})
		mu.Unlock()

		return nil
	}

	// STEP 6: Stream data through chunker (always chunk, no size check)
	log.CtxDebugf(ctx, "CDC Write: streaming through chunker")
	chunkMetadata, err = cdc_proxy.ChunkBlobStream(ctx, uncompressedReader, *averageChunkSizeBytes, digestFunction, writeChunk)
	if err != nil {
		if decompressor != nil {
			decompressor.Close()
		}
		return status.InternalErrorf("failed to chunk blob: %s", err)
	}

	if decompressor != nil {
		if err := decompressor.Close(); err != nil {
			log.CtxWarningf(ctx, "CDC Write: error closing decompressor: %s", err)
		}
	}

	mu.Lock()
	totalChunks := len(chunks)
	mu.Unlock()

	log.CtxDebugf(ctx, "CDC Write: created %d chunks", totalChunks)

	// STEP 7: Update chunk metadata with compressor info
	// The metadata ResourceNames need compressor set for proper reconstruction
	if compressor == repb.Compressor_ZSTD {
		for _, chunkRN := range chunkMetadata.Resource {
			chunkRN.Compressor = repb.Compressor_ZSTD
		}
	}

	// STEP 8: Store chunk metadata locally
	// This allows us to reconstruct the blob from local chunks on read.
	// The metadata maps the original blob digest to the list of chunk digests.
	if err := s.chunkMetadataStore.Set(ctx, instanceName, blobDigest, digestFunction, chunkMetadata); err != nil {
		log.CtxWarningf(ctx, "CDC Write: failed to store local chunk metadata: %s", err)
		// This is serious - without metadata we can't reconstruct the blob locally
		// But continue with remote upload
	}

	// STEP 9: Check which chunks are missing from remote
	chunkDigests := cdc_proxy.GetChunkDigests(chunkMetadata)
	findMissingReq := &repb.FindMissingBlobsRequest{
		InstanceName:   instanceName,
		BlobDigests:    chunkDigests,
		DigestFunction: digestFunction,
	}

	findMissingResp, err := s.remoteCASmClient.FindMissingBlobs(ctx, findMissingReq)
	if err != nil {
		log.CtxWarningf(ctx, "CDC Write: FindMissingBlobs failed, uploading all chunks: %s", err)
		findMissingResp = &repb.FindMissingBlobsResponse{MissingBlobDigests: chunkDigests}
	}

	missingCount := len(findMissingResp.GetMissingBlobDigests())
	log.CtxInfof(ctx, "CDC Write: %d/%d chunks missing from remote (%.1f%% dedup)",
		missingCount, totalChunks, 100.0*(1.0-float64(missingCount)/float64(totalChunks)))

	// STEP 10: Upload only missing chunks to remote (compressed if original was compressed)
	// This is the key bandwidth optimization - we only upload chunks that don't
	// already exist on the remote cache. Chunks are compressed to match local storage.
	if missingCount > 0 {
		// Build map of missing digests for quick lookup
		missingMap := make(map[string]bool)
		for _, d := range findMissingResp.GetMissingBlobDigests() {
			missingMap[d.GetHash()] = true
		}

		// Upload missing chunks (already compressed from writeChunk)
		mu.Lock()
		for _, chunk := range chunks {
			if missingMap[chunk.Digest.GetHash()] {
				if err := s.writeChunkToRemote(ctx, chunk, instanceName, digestFunction); err != nil {
					log.CtxWarningf(ctx, "CDC Write: failed to upload chunk %s: %s",
						chunk.Digest.GetHash(), err)
					// Continue with other chunks
				}
			}
		}
		mu.Unlock()
	}

	// STEP 11: Upload chunk metadata to remote
	// The metadata allows the remote cache to reconstruct the blob from chunks.
	// We store both the metadata blob and pointer to remote.
	if err := s.uploadChunkMetadataToRemote(ctx, instanceName, blobDigest, digestFunction, chunkMetadata); err != nil {
		log.CtxWarningf(ctx, "CDC Write: failed to upload chunk metadata to remote: %s", err)
		// This is critical - remote won't be able to reconstruct the blob
		// But we've already uploaded the chunks, so continue
	}

	log.CtxInfof(ctx, "CDC Write: completed (chunks=%d, uploaded=%d, saved=%.1f%% bandwidth)",
		totalChunks, missingCount, 100.0*(1.0-float64(missingCount)/float64(totalChunks)))

	// STEP 10: Send success response to client
	// Use original blob size from digest (uncompressed size)
	resp := &bspb.WriteResponse{
		CommittedSize: blobDigest.GetSizeBytes(),
	}
	return stream.SendAndClose(resp)
}

// writeStreamReader converts a ByteStream_WriteServer into an io.Reader
// to enable streaming without buffering all data.
type writeStreamReader struct {
	stream    bspb.ByteStream_WriteServer
	firstReq  *bspb.WriteRequest
	firstUsed bool
	buf       []byte
}

func (r *writeStreamReader) Read(p []byte) (int, error) {
	// Use first request if not yet consumed
	if !r.firstUsed {
		r.firstUsed = true
		data := r.firstReq.GetData()
		if len(data) == 0 {
			// First request had no data, continue to next request
		} else {
			n := copy(p, data)
			if n < len(data) {
				// Store remaining data in buffer
				r.buf = data[n:]
			}
			return n, nil
		}
	}

	// Use buffered data if available
	if len(r.buf) > 0 {
		n := copy(p, r.buf)
		r.buf = r.buf[n:]
		return n, nil
	}

	// Read next request from stream
	req, err := r.stream.Recv()
	if err == io.EOF {
		return 0, io.EOF
	}
	if err != nil {
		return 0, err
	}

	data := req.GetData()
	if len(data) == 0 {
		// Empty request, try next one
		return r.Read(p)
	}

	n := copy(p, data)
	if n < len(data) {
		// Store remaining data in buffer
		r.buf = data[n:]
	}

	return n, nil
}

// writeToLocal writes a blob to the local cache via ByteStream
func (s *ByteStreamServerProxy) writeToLocal(ctx context.Context, rn *digest.CASResourceName, data []byte) error {
	// Create a simple write stream for local
	localStream := &localWriteStream{
		ctx:          ctx,
		resourceName: rn.NewUploadString(),
		data:         data,
	}
	return s.local.Write(localStream)
}

// writeWholeBlobToRemote uploads a complete blob to remote cache
func (s *ByteStreamServerProxy) writeWholeBlobToRemote(ctx context.Context, rn *digest.CASResourceName, data []byte, clientStream bspb.ByteStream_WriteServer) error {
	remoteStream, err := s.remote.Write(ctx)
	if err != nil {
		return err
	}

	// Send data to remote
	req := &bspb.WriteRequest{
		ResourceName: rn.NewUploadString(),
		Data:         data,
		WriteOffset:  0,
		FinishWrite:  true,
	}
	if err := remoteStream.Send(req); err != nil {
		return err
	}

	resp, err := remoteStream.CloseAndRecv()
	if err != nil {
		return err
	}

	return clientStream.SendAndClose(resp)
}

// writeChunkToRemote uploads a single chunk to remote cache via CAS
func (s *ByteStreamServerProxy) writeChunkToRemote(ctx context.Context, chunk *cdc_proxy.ChunkInfo, instanceName string, digestFunction repb.DigestFunction_Value) error {
	// Use BatchUpdateBlobs for chunks
	req := &repb.BatchUpdateBlobsRequest{
		InstanceName:   instanceName,
		DigestFunction: digestFunction,
		Requests: []*repb.BatchUpdateBlobsRequest_Request{
			{
				Digest: chunk.Digest,
				Data:   chunk.Data,
			},
		},
	}

	resp, err := s.remoteCASmClient.BatchUpdateBlobs(ctx, req)
	if err != nil {
		return err
	}

	// Check if upload succeeded
	if len(resp.GetResponses()) > 0 && resp.GetResponses()[0].GetStatus().GetCode() != 0 {
		return status.InternalErrorf("chunk upload failed: %s", resp.GetResponses()[0].GetStatus().GetMessage())
	}

	return nil
}

// uploadChunkMetadataToRemote uploads chunk metadata to remote cache
// This uses the same two-level storage as ChunkMetadataStore:
// 1. ChunkedMetadata proto -> stored in remote CAS with its own digest
// 2. Blob digest -> Metadata digest pointer (stored in remote cache)
func (s *ByteStreamServerProxy) uploadChunkMetadataToRemote(ctx context.Context, instanceName string, blobDigest *repb.Digest, digestFunction repb.DigestFunction_Value, chunkMetadata *sgpb.StorageMetadata_ChunkedMetadata) error {
	// Marshal chunk metadata
	metadataBlob, err := proto.Marshal(chunkMetadata)
	if err != nil {
		return status.InternalErrorf("marshalling chunk metadata: %s", err)
	}

	// Compute digest for metadata blob
	metadataDigest, err := digest.Compute(bytes.NewReader(metadataBlob), digestFunction)
	if err != nil {
		return status.InternalErrorf("computing chunk metadata digest: %s", err)
	}

	// Upload metadata blob to remote CAS
	metadataReq := &repb.BatchUpdateBlobsRequest{
		InstanceName:   instanceName,
		DigestFunction: digestFunction,
		Requests: []*repb.BatchUpdateBlobsRequest_Request{
			{
				Digest: metadataDigest,
				Data:   metadataBlob,
			},
		},
	}

	metadataResp, err := s.remoteCASmClient.BatchUpdateBlobs(ctx, metadataReq)
	if err != nil {
		return status.InternalErrorf("uploading chunk metadata to remote CAS: %s", err)
	}

	if len(metadataResp.GetResponses()) > 0 && metadataResp.GetResponses()[0].GetStatus().GetCode() != 0 {
		return status.InternalErrorf("chunk metadata upload failed: %s", metadataResp.GetResponses()[0].GetStatus().GetMessage())
	}

	// Create and upload pointer to metadata
	pointer := &sgpb.StorageMetadata_ChunkedMetadataPointer{
		MetadataDigest: metadataDigest,
	}
	pointerBlob, err := proto.Marshal(pointer)
	if err != nil {
		return status.InternalErrorf("marshalling chunk metadata pointer: %s", err)
	}

	// Create pointer key (same method as ChunkMetadataStore)
	pointerKey := s.makeMetadataPointerKey(instanceName, blobDigest, digestFunction)

	// Upload pointer via ByteStream (since it's small and we need to use the cache interface)
	// We'll use the remote ByteStream client to write the pointer
	uploadRN := digest.NewCASResourceName(pointerKey.GetDigest(), instanceName, digestFunction)
	uploadRN.SetCompressor(repb.Compressor_IDENTITY)

	remoteStream, err := s.remote.Write(ctx)
	if err != nil {
		return status.InternalErrorf("creating remote write stream for pointer: %s", err)
	}

	pointerReq := &bspb.WriteRequest{
		ResourceName: uploadRN.NewUploadString(),
		Data:         pointerBlob,
		WriteOffset:  0,
		FinishWrite:  true,
	}

	if err := remoteStream.Send(pointerReq); err != nil {
		return status.InternalErrorf("sending pointer to remote: %s", err)
	}

	if _, err := remoteStream.CloseAndRecv(); err != nil {
		return status.InternalErrorf("closing pointer upload stream: %s", err)
	}

	log.CtxDebugf(ctx, "CDC Write: uploaded chunk metadata to remote (metadata=%s, pointer=%s)",
		metadataDigest.GetHash()[:8], pointerKey.GetDigest().GetHash()[:8])

	return nil
}

// makeMetadataPointerKey creates a cache key for storing the metadata pointer
// This duplicates the logic from ChunkMetadataStore to maintain consistency
func (s *ByteStreamServerProxy) makeMetadataPointerKey(instanceName string, blobDigest *repb.Digest, digestFunction repb.DigestFunction_Value) *rspb.ResourceName {
	// Use the same logic as ChunkMetadataStore
	return s.chunkMetadataStore.MakeMetadataPointerKey(instanceName, blobDigest, digestFunction)
}

// localWriteStream implements ByteStream_WriteServer for writing to local cache
type localWriteStream struct {
	bspb.ByteStream_WriteServer
	ctx          context.Context
	resourceName string
	data         []byte
	offset       int
}

func (s *localWriteStream) Context() context.Context {
	return s.ctx
}

func (s *localWriteStream) Recv() (*bspb.WriteRequest, error) {
	if s.offset >= len(s.data) {
		return nil, io.EOF
	}

	// Send data in chunks
	chunkSize := 1024 * 1024 // 1MB chunks
	end := s.offset + chunkSize
	finishWrite := false
	if end >= len(s.data) {
		end = len(s.data)
		finishWrite = true
	}

	req := &bspb.WriteRequest{
		ResourceName: s.resourceName,
		Data:         s.data[s.offset:end],
		WriteOffset:  int64(s.offset),
		FinishWrite:  finishWrite,
	}

	s.offset = end
	return req, nil
}

func (s *localWriteStream) SendAndClose(resp *bspb.WriteResponse) error {
	// Ignore response
	return nil
}

// readChunkedBlobFromLocal reconstructs a blob from its chunks stored locally
// and streams it to the client.
func (s *ByteStreamServerProxy) readChunkedBlobFromLocal(ctx context.Context, chunkMetadata *sgpb.StorageMetadata_ChunkedMetadata, instanceName string, digestFunction repb.DigestFunction_Value, stream bspb.ByteStream_ReadServer) error {
	// Verify all chunks exist before attempting to read
	chunkResources := chunkMetadata.GetResource()
	if len(chunkResources) == 0 {
		return status.NotFoundError("no chunks in metadata")
	}

	log.CtxDebugf(ctx, "CDC Read: reconstructing from %d chunks", len(chunkResources))

	// Read and stream each chunk in order
	// Chunks may be stored compressed, so we need to decompress them before streaming
	for i, chunkRN := range chunkResources {
		chunkDigest := chunkRN.GetDigest()
		if chunkDigest == nil {
			return status.InternalErrorf("chunk %d has no digest", i)
		}

		// Read chunk from local cache (may be compressed)
		chunkData, err := s.localCache.Get(ctx, chunkRN)
		if err != nil {
			return status.NotFoundErrorf("chunk %d (%s) not found in local cache: %s",
				i, chunkDigest.GetHash()[:8], err)
		}

		// Decompress chunk if it was stored compressed
		// We need to send uncompressed data to client for concatenation
		var chunkDataToSend []byte
		if chunkRN.GetCompressor() == repb.Compressor_ZSTD {
			decompressed, err := compression.DecompressZstd(nil, chunkData)
			if err != nil {
				return status.InternalErrorf("chunk %d (%s) decompression failed: %s",
					i, chunkDigest.GetHash()[:8], err)
			}
			chunkDataToSend = decompressed
		} else {
			chunkDataToSend = chunkData
		}

		// Stream uncompressed chunk data to client
		// The chunks will be concatenated to reconstruct the original uncompressed blob
		resp := &bspb.ReadResponse{
			Data: chunkDataToSend,
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	log.CtxDebugf(ctx, "CDC Read: successfully reconstructed blob from %d chunks", len(chunkResources))
	return nil
}

func flushToRemote(stream bspb.ByteStream_WriteServer, remoteStream bspb.ByteStream_WriteClient) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		err = remoteStream.Send(req)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if req.GetFinishWrite() {
			return nil
		}
	}
}

func recordWriteMetrics(bytesWritten int64, err error, proxyRequestType string) {
	labels := prometheus.Labels{
		metrics.StatusLabel:           strconv.Itoa(int(gstatus.Code(err))),
		metrics.CacheHitMissStatus:    metrics.MissStatusLabel,
		metrics.CacheProxyRequestType: proxyRequestType,
	}
	metrics.ByteStreamProxiedWriteRequests.With(labels).Inc()
	if bytesWritten > 0 {
		metrics.ByteStreamProxiedWriteBytes.With(labels).Add(float64(bytesWritten))
	}
}

func (s *ByteStreamServerProxy) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	if proxy_util.SkipRemote(ctx) {
		return nil, status.UnimplementedError("Skip remote not implemented")
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	return s.remote.QueryWriteStatus(ctx, req)
}

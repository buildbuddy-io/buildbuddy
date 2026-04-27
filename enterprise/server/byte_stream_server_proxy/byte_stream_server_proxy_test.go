package byte_stream_server_proxy

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/atime_updater"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunking"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/byte_stream"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/pkg/openfeature/memprovider"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

var (
	benchRTT         = flag.Duration("bench_rtt", 30*time.Millisecond, "Simulated network RTT for benchmark remote RPCs (e.g. SplitBlob, ByteStream.Read)")
	benchRTTJitter   = flag.Duration("bench_rtt_jitter", 6*time.Millisecond, "Jitter added to the simulated benchmark RTT")
	benchUploadDelay = flag.Duration("bench_upload_delay", 16*time.Millisecond, "Simulated time to upload 1MB to the remote cache")
)

type remoteReadExpectation int

const (
	never remoteReadExpectation = iota
	once
	always
)

type noOpCAS struct {
	t testing.TB
}

func (c *noOpCAS) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	return &repb.FindMissingBlobsResponse{}, nil
}

func (c *noOpCAS) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	c.t.Fatal("Unexpected call to BatchUpdateBlobs")
	return nil, status.InternalError("Unexpected call to BatchUpdateBlobs")
}

func (c *noOpCAS) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	c.t.Fatal("Unexpected call to BatchReadBlobs")
	return nil, status.InternalError("Unexpected call to BatchReadBlobs")
}

func (c *noOpCAS) GetTree(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	c.t.Fatal("Unexpected call to GetTree")
	return status.InternalError("Unexpected call to GetTree")
}

func (c *noOpCAS) SpliceBlob(ctx context.Context, req *repb.SpliceBlobRequest) (*repb.SpliceBlobResponse, error) {
	c.t.Fatal("Unexpected call to SpliceBlob")
	return nil, status.InternalError("SpliceBlob RPC is not currently implemented")
}

func (c *noOpCAS) SplitBlob(ctx context.Context, req *repb.SplitBlobRequest) (*repb.SplitBlobResponse, error) {
	c.t.Fatal("Unexpected call to SplitBlob")
	return nil, status.InternalError("SplitBlob RPC is not currently implemented")
}

type casRPCRecorder struct {
	mu           sync.Mutex
	fmbGroups    [][]string
	uploadGroups [][]string
	uploadSizes  []int64
}

func recordCASUnaryInterceptor(rec *casRPCRecorder) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		rec.mu.Lock()
		switch {
		case strings.HasSuffix(method, "/FindMissingBlobs"):
			fmbReq, ok := req.(*repb.FindMissingBlobsRequest)
			if ok {
				group := make([]string, 0, len(fmbReq.GetBlobDigests()))
				for _, d := range fmbReq.GetBlobDigests() {
					group = append(group, d.GetHash())
				}
				rec.fmbGroups = append(rec.fmbGroups, group)
			}
		case strings.HasSuffix(method, "/BatchUpdateBlobs"):
			uploadReq, ok := req.(*repb.BatchUpdateBlobsRequest)
			if ok {
				group := make([]string, 0, len(uploadReq.GetRequests()))
				size := int64(0)
				for _, r := range uploadReq.GetRequests() {
					group = append(group, r.GetDigest().GetHash())
					size += int64(len(r.GetData()))
				}
				rec.uploadGroups = append(rec.uploadGroups, group)
				rec.uploadSizes = append(rec.uploadSizes, size)
			}
		}
		rec.mu.Unlock()
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func requestCountingUnaryInterceptor(count *atomic.Int32) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		count.Add(1)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func requestCountingStreamInterceptor(count *atomic.Int32) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		count.Add(1)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func TestChunkUploaderGroupsFindMissingAndDedupesWithinBlob(t *testing.T) {
	ctx := testContext()
	env := testenv.GetTestEnv(t)
	rec := &casRPCRecorder{}
	cas, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	require.NoError(t, err)
	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	repb.RegisterContentAddressableStorageServer(grpcServer, cas)
	go runFunc()

	conn, err := testenv.LocalGRPCConn(ctx, lis, grpc.WithUnaryInterceptor(recordCASUnaryInterceptor(rec)))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	s := &ByteStreamServerProxy{
		remoteCAS: repb.NewContentAddressableStorageClient(conn),
		bufPool:   bytebufferpool.VariableSize(int(chunking.MaxChunkSizeBytes())),
		efp:       fp,
	}
	uploader, err := newChunkUploader(context.Background(), s, "instance", repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	type chunkData struct {
		digest *repb.Digest
		data   []byte
	}
	uniqueChunks := make([]chunkData, 9)
	for i := range uniqueChunks {
		_, rawData := testdigest.RandomCASResourceBuf(t, 700_000)
		d, err := digest.Compute(bytes.NewReader(rawData), repb.DigestFunction_BLAKE3)
		require.NoError(t, err)
		uniqueChunks[i] = chunkData{
			digest: d,
			data:   rawData,
		}
	}
	inputDigests := []*repb.Digest{
		uniqueChunks[0].digest,
		uniqueChunks[1].digest,
		uniqueChunks[2].digest,
		uniqueChunks[3].digest,
		uniqueChunks[4].digest,
		uniqueChunks[5].digest,
		uniqueChunks[6].digest,
		uniqueChunks[7].digest,
		uniqueChunks[0].digest,
		uniqueChunks[8].digest,
		uniqueChunks[1].digest,
		uniqueChunks[0].digest,
	}
	for _, d := range inputDigests {
		var rawData []byte
		for _, chunk := range uniqueChunks {
			if digest.Equal(chunk.digest, d) {
				rawData = chunk.data
				break
			}
		}
		require.NotNil(t, rawData)
		buf := s.bufPool.Get(int64(len(rawData)))
		compressedData := compression.CompressZstd(buf, rawData)
		uploader.addChunk(compressedData, buf, d)
	}

	require.NoError(t, uploader.flush())

	rec.mu.Lock()
	defer rec.mu.Unlock()

	fmbSizes := make([]int, 0, len(rec.fmbGroups))
	fmbDigests := make(map[string]struct{})
	for _, group := range rec.fmbGroups {
		fmbSizes = append(fmbSizes, len(group))
		for _, hash := range group {
			fmbDigests[hash] = struct{}{}
		}
	}
	sort.Ints(fmbSizes)
	require.Equal(t, []int{9}, fmbSizes)
	require.Len(t, fmbDigests, len(uniqueChunks), "expected only unique digests to hit FindMissingBlobs")

	uploadedDigests := make(map[string]struct{})
	for i, group := range rec.uploadGroups {
		for _, hash := range group {
			uploadedDigests[hash] = struct{}{}
		}
		require.LessOrEqual(t, rec.uploadSizes[i], int64(cachetools.BatchUploadLimitBytes))
	}
	require.Len(t, uploadedDigests, len(uniqueChunks), "expected only unique digests to be uploaded")
}

func runRemoteServices(ctx context.Context, env *testenv.TestEnv, t testing.TB) (bspb.ByteStreamClient, repb.ContentAddressableStorageClient, *atomic.Int32, *atomic.Int32) {
	server, err := byte_stream_server.NewByteStreamServer(env)
	cas := noOpCAS{t: t}
	require.NoError(t, err)
	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	bspb.RegisterByteStreamServer(grpcServer, server)
	repb.RegisterContentAddressableStorageServer(grpcServer, &cas)
	go runFunc()
	unaryRequestCounter := atomic.Int32{}
	streamRequestCounter := atomic.Int32{}
	conn, err := testenv.LocalGRPCConn(ctx, lis,
		grpc.WithUnaryInterceptor(requestCountingUnaryInterceptor(&unaryRequestCounter)),
		grpc.WithStreamInterceptor(requestCountingStreamInterceptor(&streamRequestCounter)))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return bspb.NewByteStreamClient(conn), repb.NewContentAddressableStorageClient(conn), &unaryRequestCounter, &streamRequestCounter
}

func runBSProxy(ctx context.Context, client bspb.ByteStreamClient, env *testenv.TestEnv, t testing.TB) bspb.ByteStreamClient {
	env.SetByteStreamClient(client)
	bss, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	env.SetLocalByteStreamServer(bss)
	byteStreamServer, err := New(env)
	require.NoError(t, err)
	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()
	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return bspb.NewByteStreamClient(conn)
}

func waitContains(ctx context.Context, env *testenv.TestEnv, rn *rspb.ResourceName) error {
	for i := 1; i <= 100; i++ {
		found, err := env.GetCache().Contains(ctx, rn)
		if err != nil {
			return err
		}
		if found {
			return nil
		}
		time.Sleep(time.Duration(i) * time.Millisecond)
	}
	casrn, err := digest.CASResourceNameFromProto(rn)
	if err != nil {
		return err
	}
	s := casrn.DownloadString()
	return status.NotFoundErrorf("Timed out waiting for cache to contain %s", s)
}

func testContext() context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs(authutil.ClientIdentityHeaderName, "fakeheader"))
}

func TestRead(t *testing.T) {
	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)
	userWithEncryption := testauth.User("user", "group")
	userWithEncryption.CacheEncryptionEnabled = true
	users := map[string]interfaces.UserInfo{"user": userWithEncryption}
	ta := testauth.NewTestAuthenticator(t, users)
	proxyEnv.SetAuthenticator(ta)
	bs, _, _, requestCounter := runRemoteServices(ctx, remoteEnv, t)
	proxy := runBSProxy(ctx, bs, proxyEnv, t)

	anonCtx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)
	encryptedUserCtx, err := ta.WithAuthenticatedUser(anonCtx, "user")
	require.NoError(t, err)

	cases := []struct {
		ctx              context.Context
		wantError        error
		size             int64
		offset           int64
		remotes          remoteReadExpectation
		proxyShouldCache bool
	}{
		{ // Simple Read
			ctx:              anonCtx,
			wantError:        nil,
			size:             1234,
			offset:           0,
			remotes:          once,
			proxyShouldCache: true,
		},
		{
			// Encrypted Read -- contents for users/organizations with
			// encryption enabled should not be cached in the proxy.
			ctx:              encryptedUserCtx,
			wantError:        nil,
			size:             1234,
			offset:           0,
			remotes:          always,
			proxyShouldCache: false,
		},
		{ // Large Read
			ctx:              anonCtx,
			wantError:        nil,
			size:             1000 * 1000 * 100,
			offset:           0,
			remotes:          once,
			proxyShouldCache: true,
		},
		{ // 0 length read
			ctx:              anonCtx,
			wantError:        nil,
			size:             0,
			offset:           0,
			remotes:          never,
			proxyShouldCache: false,
		},
		{ // Offset
			ctx:              anonCtx,
			wantError:        nil,
			size:             1234,
			offset:           1,
			remotes:          always,
			proxyShouldCache: false,
		},
		{ // Max offset
			ctx:              anonCtx,
			wantError:        nil,
			size:             1234,
			offset:           1234,
			remotes:          always,
			proxyShouldCache: false,
		},
	}

	for _, tc := range cases {
		rn, data := testdigest.NewRandomResourceAndBuf(t, tc.size, rspb.CacheType_CAS, "")
		// Set the value in the remote cache.
		err := remoteEnv.GetCache().Set(tc.ctx, rn, data)
		require.NoError(t, err)

		// Read it through the proxied bytestream API several times, ensuring
		// it's only read from the remote byestream server when expected.
		for i := 1; i < 4; i++ {
			var buf bytes.Buffer
			r, err := digest.CASResourceNameFromProto(rn)
			require.NoError(t, err)
			gotErr := byte_stream.ReadBlob(tc.ctx, proxy, r, &buf, tc.offset)
			if gstatus.Code(gotErr) != gstatus.Code(tc.wantError) {
				t.Errorf("got %v; want %v", gotErr, tc.wantError)
			}
			got := buf.String()
			if got != string(data[tc.offset:]) {
				t.Errorf("got %.100s; want %.100s", got, data)
			}

			switch tc.remotes {
			case never:
				require.Equal(t, int32(0), requestCounter.Load())
			case once:
				require.Equal(t, int32(1), requestCounter.Load())
			case always:
				require.Equal(t, int32(i), requestCounter.Load())
			}

			if tc.proxyShouldCache {
				require.NoError(t, waitContains(tc.ctx, proxyEnv, rn))
			}
		}
		requestCounter.Store(0)
	}
}

func TestRead_RemoteAtimeUpdated(t *testing.T) {
	rn, blob := testdigest.RandomCompressibleCASResourceBuf(t, 5e4, "" /*instanceName*/)
	d := rn.Digest

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	bs, cas, unaryRequestCounter, streamRequestCounter := runRemoteServices(ctx, remoteEnv, t)

	proxyEnv := testenv.GetTestEnv(t)
	clock := clockwork.NewFakeClock()
	proxyEnv.SetClock(clock)
	proxyEnv.SetContentAddressableStorageClient(cas)
	require.NoError(t, atime_updater.Register(proxyEnv))
	proxy := runBSProxy(ctx, bs, proxyEnv, t)

	// Upload the test blob to the proxy
	ctx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)
	uploadRn := fmt.Sprintf("uploads/%s/blobs/%s/%d", uuid.New(), d.Hash, d.SizeBytes)
	bazelVersion := "5.0.0"
	byte_stream.MustUploadChunked(t, ctx, proxy, bazelVersion, uploadRn, blob, true)
	require.NoError(t, waitContains(ctx, proxyEnv, rn))
	streamRequestCounter.Store(0)

	// Read the blob back from the proxy
	downloadBuf := []byte{}
	downloadRn := fmt.Sprintf("blobs/%s/%d", d.Hash, d.SizeBytes)
	downloadStream, err := proxy.Read(ctx, &bspb.ReadRequest{ResourceName: downloadRn})
	require.NoError(t, err)
	for {
		res, err := downloadStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		downloadBuf = append(downloadBuf, res.Data...)
	}
	require.Equal(t, blob, downloadBuf)
	require.Equal(t, int32(0), unaryRequestCounter.Load())
	require.Equal(t, int32(0), streamRequestCounter.Load())

	// Tick the atime updater and verify a unary FindMissingBlob RPC was sent
	clock.Advance(time.Minute)
	time.Sleep(25 * time.Millisecond)
	require.Equal(t, int32(1), unaryRequestCounter.Load())
	require.Equal(t, int32(0), streamRequestCounter.Load())
}

func TestWrite(t *testing.T) {
	// Make blob big enough to require multiple chunks to upload
	rn, blob := testdigest.RandomCompressibleCASResourceBuf(t, 5e6, "" /*instanceName*/)
	compressedBlob := compression.CompressZstd(nil, blob)
	require.NotEqual(t, blob, compressedBlob, "sanity check: blob != compressedBlob")

	// Note: Digest is of uncompressed contents
	d := rn.GetDigest()

	testCases := []struct {
		name                        string
		writeToRemote               bool
		uploadResourceName          string
		uploadBlob                  []byte
		downloadResourceName        string
		expectedDownloadCompression repb.Compressor_Value
		bazelVersion                string
	}{
		{
			name:                        "Write compressed, read compressed",
			uploadResourceName:          fmt.Sprintf("uploads/%s/compressed-blobs/zstd/%s/%d", uuid.New(), d.Hash, d.SizeBytes),
			uploadBlob:                  compressedBlob,
			downloadResourceName:        fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes),
			expectedDownloadCompression: repb.Compressor_ZSTD,
		},
		{
			name:                        "Write compressed, read decompressed",
			uploadResourceName:          fmt.Sprintf("uploads/%s/compressed-blobs/zstd/%s/%d", uuid.New(), d.Hash, d.SizeBytes),
			uploadBlob:                  compressedBlob,
			downloadResourceName:        fmt.Sprintf("blobs/%s/%d", d.Hash, d.SizeBytes),
			expectedDownloadCompression: repb.Compressor_IDENTITY,
		},
		{
			name:                        "Write decompressed, read decompressed",
			uploadResourceName:          fmt.Sprintf("uploads/%s/blobs/%s/%d", uuid.New(), d.Hash, d.SizeBytes),
			uploadBlob:                  blob,
			downloadResourceName:        fmt.Sprintf("blobs/%s/%d", d.Hash, d.SizeBytes),
			expectedDownloadCompression: repb.Compressor_IDENTITY,
		},
		{
			name:                        "Write decompressed, read compressed",
			uploadResourceName:          fmt.Sprintf("uploads/%s/blobs/%s/%d", uuid.New(), d.Hash, d.SizeBytes),
			uploadBlob:                  blob,
			downloadResourceName:        fmt.Sprintf("compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes),
			expectedDownloadCompression: repb.Compressor_ZSTD,
		},
		{
			name:                        "Compressed with instance name",
			uploadResourceName:          fmt.Sprintf("instance/uploads/%s/compressed-blobs/zstd/%s/%d", uuid.New(), d.Hash, d.SizeBytes),
			uploadBlob:                  compressedBlob,
			downloadResourceName:        fmt.Sprintf("instance/compressed-blobs/zstd/%s/%d", d.Hash, d.SizeBytes),
			expectedDownloadCompression: repb.Compressor_ZSTD,
		},
	}
	for _, tc := range testCases {
		run := func(t *testing.T) {
			remoteEnv := testenv.GetTestEnv(t)
			proxyEnv := testenv.GetTestEnv(t)
			ctx := byte_stream.WithBazelVersion(t, testContext(), tc.bazelVersion)

			// Enable compression
			flags.Set(t, "cache.zstd_transcoding_enabled", true)
			proxyEnv.SetCache(&testcompression.CompressionCache{Cache: proxyEnv.GetCache()})

			userWithEncryption := testauth.User("user", "group")
			userWithEncryption.CacheEncryptionEnabled = true
			users := map[string]interfaces.UserInfo{"user": userWithEncryption}
			ta := testauth.NewTestAuthenticator(t, users)
			proxyEnv.SetAuthenticator(ta)
			ctx, err := prefix.AttachUserPrefixToContext(ctx, ta)
			require.NoError(t, err)
			encryptedUserCtx, err := ta.WithAuthenticatedUser(ctx, "user")
			require.NoError(t, err)
			remote, _, _, requestCounter := runRemoteServices(ctx, remoteEnv, t)
			proxy := runBSProxy(ctx, remote, proxyEnv, t)

			// Upload the blob
			writeDest := proxy
			writeEnv := proxyEnv
			if tc.writeToRemote {
				writeDest = remote
				writeEnv = remoteEnv
			}
			byte_stream.MustUploadChunked(t, ctx, writeDest, tc.bazelVersion, tc.uploadResourceName, tc.uploadBlob, true)
			require.NoError(t, waitContains(ctx, writeEnv, rn))
			requestCounter.Store(0)

			// Verify we can read the blob from both caches.
			sources := []bspb.ByteStreamClient{remote, proxy}
			for i, source := range sources {
				downloadBuf := []byte{}
				downloadStream, err := source.Read(ctx, &bspb.ReadRequest{
					ResourceName: tc.downloadResourceName,
				})
				require.NoError(t, err, tc.name)
				for {
					res, err := downloadStream.Recv()
					if err == io.EOF {
						break
					}
					require.NoError(t, err, tc.name)
					downloadBuf = append(downloadBuf, res.Data...)
				}

				if tc.expectedDownloadCompression == repb.Compressor_IDENTITY {
					require.Equal(t, blob, downloadBuf, tc.name)
				} else if tc.expectedDownloadCompression == repb.Compressor_ZSTD {
					decompressedDownloadBuf, err := compression.DecompressZstd(nil, downloadBuf)
					require.NoError(t, err, tc.name)
					require.Equal(t, blob, decompressedDownloadBuf, tc.name)
				}

				// If we originally wrote the blob to the remote, we expect 1
				// more request to read it from the proxy.
				if tc.writeToRemote {
					require.Equal(t, int32(i+1), requestCounter.Load())
				} else {
					require.Equal(t, int32(1), requestCounter.Load())
				}
			}

			// Now try uploading a duplicate to the proxy. The duplicate upload
			// shouldn't fail and we should still be able to read the blob.
			byte_stream.MustUploadChunked(t, ctx, proxy, tc.bazelVersion, tc.uploadResourceName, tc.uploadBlob, false)
			requestCounter.Store(0)

			// Uploading the same blob via the remote-only path should succeed.
			byte_stream.MustUploadChunked(t, encryptedUserCtx, proxy, tc.bazelVersion, tc.uploadResourceName, tc.uploadBlob, false)
			require.Equal(t, int32(1), requestCounter.Load())
			requestCounter.Store(0)

			for _, source := range sources {
				downloadBuf := []byte{}
				downloadStream, err := source.Read(ctx, &bspb.ReadRequest{
					ResourceName: tc.downloadResourceName,
				})
				require.NoError(t, err, tc.name)
				for {
					res, err := downloadStream.Recv()
					if err == io.EOF {
						break
					}
					require.NoError(t, err, tc.name)
					downloadBuf = append(downloadBuf, res.Data...)
				}

				if tc.expectedDownloadCompression == repb.Compressor_IDENTITY {
					require.Equal(t, blob, downloadBuf, tc.name)
				} else if tc.expectedDownloadCompression == repb.Compressor_ZSTD {
					decompressedDownloadBuf, err := compression.DecompressZstd(nil, downloadBuf)
					require.NoError(t, err, tc.name)
					require.Equal(t, blob, decompressedDownloadBuf, tc.name)
				}

				require.Equal(t, int32(1), requestCounter.Load())
			}
		}

		// Run all tests for both bazel 5.0.0 (which introduced compression) and
		// 5.1.0 (which added support for short-circuiting duplicate compressed
		// uploads)
		tc.bazelVersion = "5.0.0"
		tc.writeToRemote = false
		t.Run(tc.name+", bazel "+tc.bazelVersion+" proxyfirst", run)
		tc.bazelVersion = "5.0.0"
		tc.writeToRemote = true
		t.Run(tc.name+", bazel "+tc.bazelVersion+" remotefirst", run)
		tc.bazelVersion = "5.1.0"
		tc.writeToRemote = false
		t.Run(tc.name+", bazel "+tc.bazelVersion+" proxyfirst", run)
		tc.bazelVersion = "5.1.0"
		tc.writeToRemote = true
		t.Run(tc.name+", bazel "+tc.bazelVersion+" remotefirst", run)
	}
}

func TestSkipRemote(t *testing.T) {
	ctx := testContext()
	ctx = metadata.AppendToOutgoingContext(ctx, proxy_util.SkipRemoteKey, "true")

	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)
	bs, _, _, requestCounter := runRemoteServices(ctx, remoteEnv, t)
	proxy := runBSProxy(ctx, bs, proxyEnv, t)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	// Write blob big enough to require multiple chunks to upload.
	rn, data := testdigest.NewRandomResourceAndBuf(t, 5e6, rspb.CacheType_CAS, "")
	byte_stream.MustUploadChunked(t, ctx, proxy, "5.0.0", fmt.Sprintf("uploads/%s/blobs/%s/%d", uuid.New(), rn.Digest.Hash, rn.Digest.SizeBytes), data, true)
	require.NoError(t, waitContains(ctx, proxyEnv, rn))

	// Check that no data was written to the remote cache.
	require.Equal(t, int32(0), requestCounter.Load())

	// Test read.
	var buf bytes.Buffer
	r, err := digest.CASResourceNameFromProto(rn)
	require.NoError(t, err)
	err = byte_stream.ReadBlob(ctx, proxy, r, &buf, 0)
	require.NoError(t, err)
	require.Equal(t, data, buf.Bytes())
	require.NoError(t, waitContains(ctx, proxyEnv, rn))
	requestCounter.Store(0)
}

func TestSkipRemoteEncryptedRemoteOnly(t *testing.T) {
	ctx := testContext()
	ctx = metadata.AppendToOutgoingContext(ctx, proxy_util.SkipRemoteKey, "true")

	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)

	userWithEncryption := testauth.User("user", "group")
	userWithEncryption.CacheEncryptionEnabled = true
	ta := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{"user": userWithEncryption})
	proxyEnv.SetAuthenticator(ta)

	bs, _, _, requestCounter := runRemoteServices(ctx, remoteEnv, t)
	proxy := runBSProxy(ctx, bs, proxyEnv, t)

	anonCtx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)
	encryptedUserCtx, err := ta.WithAuthenticatedUser(anonCtx, "user")
	require.NoError(t, err)

	rn, data := testdigest.NewRandomResourceAndBuf(t, 1234, rspb.CacheType_CAS, "")
	casRN, err := digest.CASResourceNameFromProto(rn)
	require.NoError(t, err)

	readStream, err := proxy.Read(encryptedUserCtx, &bspb.ReadRequest{ResourceName: casRN.DownloadString()})
	if err == nil {
		_, err = readStream.Recv()
	}
	require.True(t, status.IsFailedPreconditionError(err), "err = %v", err)
	require.Equal(t, int32(0), requestCounter.Load())

	writeStream, err := proxy.Write(encryptedUserCtx)
	require.NoError(t, err)
	err = writeStream.Send(&bspb.WriteRequest{
		ResourceName: casRN.NewUploadString(),
		Data:         data,
		FinishWrite:  true,
	})
	if err == nil || err == io.EOF {
		_, err = writeStream.CloseAndRecv()
	}
	require.True(t, status.IsFailedPreconditionError(err), "err = %v", err)
	require.Equal(t, int32(0), requestCounter.Load())
}

func BenchmarkReadAlwaysPresent(b *testing.B) {
	*log.LogLevel = "error"
	log.Configure()

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(b)
	proxyEnv := testenv.GetTestEnv(b)
	bs, _, _, requestCounter := runRemoteServices(ctx, remoteEnv, b)
	proxy := runBSProxy(ctx, bs, proxyEnv, b)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	if err != nil {
		b.Errorf("error attaching user prefix: %v", err)
	}

	dataString := strings.Repeat("abcdefghijklmnopqrstuvwxyz", 1_000)
	d := repb.Digest{
		Hash:      "9bae04df8ee130bb0a94596b84b4ab161a2abf8a94c4e30523249fad5754ae27",
		SizeBytes: int64(len(dataString)),
	}
	rn := digest.NewCASResourceName(&d, "", repb.DigestFunction_SHA256)
	writeStream, err := proxy.Write(ctx)
	require.NoError(b, err)
	require.NoError(b, writeStream.Send(&bspb.WriteRequest{
		ResourceName: rn.NewUploadString(),
		WriteOffset:  0,
		Data:         []byte(dataString),
		FinishWrite:  true,
	}))
	resp, err := writeStream.CloseAndRecv()
	require.NoError(b, err)
	require.Equal(b, int64(len(dataString)), resp.GetCommittedSize())

	b.ReportAllocs()
	requestCounter.Store(0)

	for b.Loop() {
		downloadBuf := make([]byte, 0, len(dataString))
		downloadStream, err := proxy.Read(ctx, &bspb.ReadRequest{ResourceName: rn.DownloadString()})
		require.NoError(b, err)
		for {
			res, err := downloadStream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(b, err)
			downloadBuf = append(downloadBuf, res.Data...)
		}
		require.Equal(b, dataString, string(downloadBuf))
		require.Equal(b, int32(0), requestCounter.Load())
	}
}

func BenchmarkReadThroughCache(b *testing.B) {
	*log.LogLevel = "error"
	log.Configure()
	// Disable the atime updater as it can interfere with the request counter.
	flags.Set(b, "cache_proxy.remote_atime_max_digests_per_group", 0)

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(b)
	proxyEnv := testenv.GetTestEnv(b)
	bs, _, _, requestCounter := runRemoteServices(ctx, remoteEnv, b)
	proxy := runBSProxy(ctx, bs, proxyEnv, b)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	if err != nil {
		b.Errorf("error attaching user prefix: %v", err)
	}

	for _, zstd := range []bool{false, true} {
		// The largest size is such after compression, it's bigger than 5MiB,
		// just to be bigger than 4MiB, which we often use as max buffer size.
		for _, size := range []int64{10_000, 3_000_000, 20_000_000} {
			b.Run(fmt.Sprintf("zstd=%v/size=%v", zstd, size), func(b *testing.B) {
				b.ReportAllocs()
				requestCounter.Store(0)
				i := 0
				for b.Loop() {
					i++
					b.StopTimer()
					rnProto, data := testdigest.RandomCASResourceBuf(b, size)
					if zstd {
						data = compression.CompressZstd(nil, data)
						rnProto.Compressor = repb.Compressor_ZSTD
					}
					rn, err := digest.CASResourceNameFromProto(rnProto)
					require.NoError(b, err)
					writeStream, err := bs.Write(ctx)
					require.NoError(b, err)
					require.NoError(b, writeStream.Send(&bspb.WriteRequest{
						ResourceName: rn.NewUploadString(),
						WriteOffset:  0,
						Data:         data,
						FinishWrite:  true,
					}))
					resp, err := writeStream.CloseAndRecv()
					require.NoError(b, err)
					if !zstd {
						require.Equal(b, int64(len(data)), resp.GetCommittedSize())
					} else {
						// With compression, the committed size won't be equal
						// to the input size. It can even be bigger when the
						// input is small.
						require.Less(b, int64(0), resp.GetCommittedSize())
					}
					b.StartTimer()

					downloadBuf := make([]byte, 0, len(data))
					downloadStream, err := proxy.Read(ctx, &bspb.ReadRequest{ResourceName: rn.DownloadString()})
					require.NoError(b, err)
					for {
						res, err := downloadStream.Recv()
						if err == io.EOF {
							break
						}
						require.NoError(b, err)
						downloadBuf = append(downloadBuf, res.Data...)
					}
					b.StopTimer()
					if zstd {
						data, err = compression.DecompressZstd(nil, data)
						require.NoError(b, err)
						downloadBuf, err = compression.DecompressZstd(nil, downloadBuf)
						require.NoError(b, err)
					}
					require.Equal(b, data, downloadBuf)
					require.Equal(b, int32(i*2), requestCounter.Load())
					b.StartTimer()

				}
			})
		}
	}
}

func BenchmarkWriteAlreadyExists(b *testing.B) {
	*log.LogLevel = "error"
	log.Configure()
	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(b)
	proxyEnv := testenv.GetTestEnv(b)
	bs, _, _, requestCounter := runRemoteServices(ctx, remoteEnv, b)
	proxy := runBSProxy(ctx, bs, proxyEnv, b)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	if err != nil {
		b.Errorf("error attaching user prefix: %v", err)
	}

	i := 1
	dataString := strings.Repeat("abcdefghijklmnopqrstuvwxyz", 1_000)
	d := repb.Digest{
		Hash:      "9bae04df8ee130bb0a94596b84b4ab161a2abf8a94c4e30523249fad5754ae27",
		SizeBytes: int64(len(dataString)),
	}
	b.ReportAllocs()
	for b.Loop() {
		rn := digest.NewCASResourceName(&d, fmt.Sprintf("%d", i), repb.DigestFunction_SHA256)
		writeStream, err := proxy.Write(ctx)
		require.NoError(b, err)
		require.NoError(b, writeStream.Send(&bspb.WriteRequest{
			ResourceName: rn.NewUploadString(),
			WriteOffset:  0,
			Data:         []byte(dataString),
			FinishWrite:  true,
		}))
		resp, err := writeStream.CloseAndRecv()
		require.NoError(b, err)
		require.Equal(b, int64(len(dataString)), resp.GetCommittedSize())
		require.Equal(b, int32(i), requestCounter.Load())
		i++
	}
}

func BenchmarkWriteUnique(b *testing.B) {
	*log.LogLevel = "error"
	log.Configure()
	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(b)
	proxyEnv := testenv.GetTestEnv(b)
	bs, _, _, requestCounter := runRemoteServices(ctx, remoteEnv, b)
	proxy := runBSProxy(ctx, bs, proxyEnv, b)

	ctx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	if err != nil {
		b.Errorf("error attaching user prefix: %v", err)
	}

	i := 1
	for _, zstd := range []bool{false, true} {
		// The largest size is such after compression, it's bigger than 5MiB,
		// which is the batchSize below.
		for _, size := range []int64{10_000, 3_000_000, 20_000_000} {
			b.Run(fmt.Sprintf("zstd=%v/size=%v", zstd, size), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					rnProto, data := testdigest.NewRandomResourceAndBuf(b, size, rspb.CacheType_CAS, strconv.Itoa(i))
					if zstd {
						data = compression.CompressZstd(nil, data)
						rnProto.Compressor = repb.Compressor_ZSTD
					}
					rn, err := digest.CASResourceNameFromProto(rnProto)
					require.NoError(b, err)
					uploadString := rn.NewUploadString()
					b.StartTimer()

					writeStream, err := proxy.Write(ctx)
					require.NoError(b, err)

					// Use a batch size bigger than 4MiB, which we often use
					// for the max buffer size,
					batchSize := 5_000_000
					written := int64(0)
					for len(data) > 0 {
						batch := min(len(data), batchSize)
						require.NoError(b, writeStream.Send(&bspb.WriteRequest{
							ResourceName: uploadString,
							WriteOffset:  written,
							Data:         data[:batch],
							FinishWrite:  batch == len(data),
						}))
						written += int64(batch)
						data = data[batch:]
					}
					resp, err := writeStream.CloseAndRecv()
					require.NoError(b, err)
					require.Equal(b, int32(i), requestCounter.Load())
					i++
					if !zstd {
						require.Equal(b, int64(size), resp.GetCommittedSize())
					} else {
						// With compression, the committed size won't be equal
						// to the input size. It can even be bigger when the
						// input is small.
						require.Less(b, int64(0), resp.GetCommittedSize())
					}
				}
			})
		}
	}
}

func TestReadChunked(t *testing.T) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.attempt_chunked_reads": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	remoteEnv.SetExperimentFlagProvider(fp)
	proxyEnv.SetExperimentFlagProvider(fp)

	remoteBSS, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(t, err)
	remoteCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(t, err)
	remoteGRPC, remoteRun, remoteLis := testenv.RegisterLocalGRPCServer(t, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPC, remoteBSS)
	repb.RegisterContentAddressableStorageServer(remoteGRPC, remoteCAS)
	go remoteRun()
	remoteConn, err := testenv.LocalGRPCConn(ctx, remoteLis)
	require.NoError(t, err)
	t.Cleanup(func() { remoteConn.Close() })
	bsClient := bspb.NewByteStreamClient(remoteConn)
	casClient := repb.NewContentAddressableStorageClient(remoteConn)

	proxyEnv.SetByteStreamClient(bsClient)
	proxyEnv.SetContentAddressableStorageClient(casClient)
	proxyBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(t, err)
	proxyEnv.SetLocalByteStreamServer(proxyBSS)
	proxyServer, err := New(proxyEnv)
	require.NoError(t, err)
	proxyGRPC, proxyRun, proxyLis := testenv.RegisterLocalGRPCServer(t, proxyEnv)
	bspb.RegisterByteStreamServer(proxyGRPC, proxyServer)
	go proxyRun()
	proxyConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(t, err)
	t.Cleanup(func() { proxyConn.Close() })
	proxy := bspb.NewByteStreamClient(proxyConn)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)

	_, originalData := testdigest.RandomCASResourceBuf(t, 3*1024*1024)
	blobDigest, err := digest.Compute(bytes.NewReader(originalData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	var chunkDigests []*repb.Digest
	writeChunkFn := func(chunkData []byte) error {
		chunkDataCopy := make([]byte, len(chunkData))
		copy(chunkDataCopy, chunkData)
		chunkDigest, err := digest.Compute(bytes.NewReader(chunkDataCopy), repb.DigestFunction_BLAKE3)
		if err != nil {
			return err
		}
		chunkDigests = append(chunkDigests, chunkDigest)
		chunkRN := digest.NewCASResourceName(chunkDigest, "", repb.DigestFunction_BLAKE3)
		return remoteEnv.GetCache().Set(ctx, chunkRN.ToProto(), chunkDataCopy)
	}
	cdcChunker, err := chunking.NewChunker(ctx, 64*1024, writeChunkFn)
	require.NoError(t, err)
	_, err = cdcChunker.Write(originalData)
	require.NoError(t, err)
	require.NoError(t, cdcChunker.Close())
	require.Greater(t, len(chunkDigests), 1)

	_, err = casClient.SpliceBlob(ctx, &repb.SpliceBlobRequest{
		BlobDigest:     blobDigest,
		ChunkDigests:   chunkDigests,
		DigestFunction: repb.DigestFunction_BLAKE3,
	})
	require.NoError(t, err)

	readLabels := prometheus.Labels{
		metrics.StatusLabel:     "OK",
		metrics.CompressionType: "IDENTITY",
	}
	proxiedReadMissLabels := prometheus.Labels{
		metrics.StatusLabel:           "OK",
		metrics.CacheHitMissStatus:    metrics.MissStatusLabel,
		metrics.CacheProxyRequestType: metrics.DefaultCacheProxyRequestLabel,
		metrics.CompressionType:       "IDENTITY",
		metrics.ChunkedLabel:          "true",
		metrics.GroupID:               groupIDForMetrics(ctx),
	}
	proxiedReadHitLabels := prometheus.Labels{
		metrics.StatusLabel:           "OK",
		metrics.CacheHitMissStatus:    metrics.HitStatusLabel,
		metrics.CacheProxyRequestType: metrics.DefaultCacheProxyRequestLabel,
		metrics.CompressionType:       "IDENTITY",
		metrics.ChunkedLabel:          "true",
		metrics.GroupID:               groupIDForMetrics(ctx),
	}
	readRequestsBefore := testutil.ToFloat64(metrics.ByteStreamChunkedReadRequests.With(readLabels))
	readBlobBytesBefore := testutil.ToFloat64(metrics.ByteStreamChunkedReadBlobBytes.With(readLabels))
	readChunksTotalBefore := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksTotal.With(readLabels))
	readChunksLocalBefore := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksLocal.With(readLabels))
	readChunksRemoteBefore := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksRemote.With(readLabels))
	readBytesLocalBefore := testutil.ToFloat64(metrics.ByteStreamChunkedReadBytesLocal.With(readLabels))
	readBytesRemoteBefore := testutil.ToFloat64(metrics.ByteStreamChunkedReadBytesRemote.With(readLabels))
	proxiedReadMissBytesBefore := testutil.ToFloat64(metrics.ByteStreamProxiedReadBytes.With(proxiedReadMissLabels))
	proxiedReadHitBytesBefore := testutil.ToFloat64(metrics.ByteStreamProxiedReadBytes.With(proxiedReadHitLabels))

	downloadCASRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	readBlob := func() []byte {
		downloadStream, err := proxy.Read(ctx, &bspb.ReadRequest{ResourceName: downloadCASRN.DownloadString()})
		require.NoError(t, err)

		var reconstructedData []byte
		for {
			res, err := downloadStream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			reconstructedData = append(reconstructedData, res.Data...)
		}
		return reconstructedData
	}

	require.Equal(t, originalData, readBlob())

	readRequestsAfter := testutil.ToFloat64(metrics.ByteStreamChunkedReadRequests.With(readLabels))
	readBlobBytesAfter := testutil.ToFloat64(metrics.ByteStreamChunkedReadBlobBytes.With(readLabels))
	readChunksTotalAfter := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksTotal.With(readLabels))
	readChunksLocalAfter := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksLocal.With(readLabels))
	readChunksRemoteAfter := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksRemote.With(readLabels))
	readBytesLocalAfter := testutil.ToFloat64(metrics.ByteStreamChunkedReadBytesLocal.With(readLabels))
	readBytesRemoteAfter := testutil.ToFloat64(metrics.ByteStreamChunkedReadBytesRemote.With(readLabels))

	require.Equal(t, float64(1), readRequestsAfter-readRequestsBefore, "one chunked read request for the blob")
	require.Equal(t, float64(len(originalData)), readBlobBytesAfter-readBlobBytesBefore, "blob bytes = original uncompressed size")
	require.Equal(t, float64(len(chunkDigests)), readChunksTotalAfter-readChunksTotalBefore, "total chunks = number of chunks in manifest")
	require.Equal(t, float64(0), readChunksLocalAfter-readChunksLocalBefore, "first read: no chunks in local cache yet")
	require.Equal(t, float64(len(chunkDigests)), readChunksRemoteAfter-readChunksRemoteBefore, "first read: all chunks fetched from remote")
	require.Equal(t, float64(0), readBytesLocalAfter-readBytesLocalBefore, "first read: no bytes served from local cache")
	require.Equal(t, float64(len(originalData)), readBytesRemoteAfter-readBytesRemoteBefore, "first read: all bytes fetched from remote")
	require.Equal(t, float64(len(originalData)), testutil.ToFloat64(metrics.ByteStreamProxiedReadBytes.With(proxiedReadMissLabels))-proxiedReadMissBytesBefore)
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.ByteStreamProxiedReadBytes.With(proxiedReadHitLabels))-proxiedReadHitBytesBefore)

	for _, chunkDigest := range chunkDigests {
		chunkRN := digest.NewCASResourceName(chunkDigest, "", repb.DigestFunction_BLAKE3)
		require.NoError(t, waitContains(ctx, proxyEnv, chunkRN.ToProto()))
	}

	readRequestsBefore = testutil.ToFloat64(metrics.ByteStreamChunkedReadRequests.With(readLabels))
	readBlobBytesBefore = testutil.ToFloat64(metrics.ByteStreamChunkedReadBlobBytes.With(readLabels))
	readChunksTotalBefore = testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksTotal.With(readLabels))
	readChunksLocalBefore = testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksLocal.With(readLabels))
	readChunksRemoteBefore = testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksRemote.With(readLabels))
	readBytesLocalBefore = testutil.ToFloat64(metrics.ByteStreamChunkedReadBytesLocal.With(readLabels))
	readBytesRemoteBefore = testutil.ToFloat64(metrics.ByteStreamChunkedReadBytesRemote.With(readLabels))
	proxiedReadMissBytesBefore = testutil.ToFloat64(metrics.ByteStreamProxiedReadBytes.With(proxiedReadMissLabels))
	proxiedReadHitBytesBefore = testutil.ToFloat64(metrics.ByteStreamProxiedReadBytes.With(proxiedReadHitLabels))

	require.Equal(t, originalData, readBlob())

	readRequestsAfter = testutil.ToFloat64(metrics.ByteStreamChunkedReadRequests.With(readLabels))
	readBlobBytesAfter = testutil.ToFloat64(metrics.ByteStreamChunkedReadBlobBytes.With(readLabels))
	readChunksTotalAfter = testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksTotal.With(readLabels))
	readChunksLocalAfter = testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksLocal.With(readLabels))
	readChunksRemoteAfter = testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksRemote.With(readLabels))
	readBytesLocalAfter = testutil.ToFloat64(metrics.ByteStreamChunkedReadBytesLocal.With(readLabels))
	readBytesRemoteAfter = testutil.ToFloat64(metrics.ByteStreamChunkedReadBytesRemote.With(readLabels))

	require.Equal(t, float64(1), readRequestsAfter-readRequestsBefore, "one chunked read request for the blob")
	require.Equal(t, float64(len(originalData)), readBlobBytesAfter-readBlobBytesBefore, "blob bytes = original uncompressed size")
	require.Equal(t, float64(len(chunkDigests)), readChunksTotalAfter-readChunksTotalBefore, "total chunks = number of chunks in manifest")
	require.Equal(t, float64(len(chunkDigests)), readChunksLocalAfter-readChunksLocalBefore, "second read: all chunks served from local cache")
	require.Equal(t, float64(0), readChunksRemoteAfter-readChunksRemoteBefore, "second read: no remote chunk fetches")
	require.Equal(t, float64(len(originalData)), readBytesLocalAfter-readBytesLocalBefore, "second read: all bytes served from local cache")
	require.Equal(t, float64(0), readBytesRemoteAfter-readBytesRemoteBefore, "second read: no bytes fetched from remote")
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.ByteStreamProxiedReadBytes.With(proxiedReadMissLabels))-proxiedReadMissBytesBefore)
	require.Equal(t, float64(len(originalData)), testutil.ToFloat64(metrics.ByteStreamProxiedReadBytes.With(proxiedReadHitLabels))-proxiedReadHitBytesBefore)
}

func TestReadChunkedEncryptedRemoteOnly(t *testing.T) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.attempt_chunked_reads": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)

	userWithEncryption := testauth.User("user", "group")
	userWithEncryption.CacheEncryptionEnabled = true
	users := map[string]interfaces.UserInfo{"user": userWithEncryption}
	ta := testauth.NewTestAuthenticator(t, users)
	proxyEnv.SetAuthenticator(ta)

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	remoteEnv.SetExperimentFlagProvider(fp)
	proxyEnv.SetExperimentFlagProvider(fp)

	remoteBSS, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(t, err)
	remoteCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(t, err)
	remoteGRPC, remoteRun, remoteLis := testenv.RegisterLocalGRPCServer(t, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPC, remoteBSS)
	repb.RegisterContentAddressableStorageServer(remoteGRPC, remoteCAS)
	go remoteRun()

	var splitBlobCalls atomic.Int32
	remoteConn, err := testenv.LocalGRPCConn(ctx, remoteLis, grpc.WithUnaryInterceptor(func(
		ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		if strings.HasSuffix(method, "/SplitBlob") {
			splitBlobCalls.Add(1)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}))
	require.NoError(t, err)
	t.Cleanup(func() { remoteConn.Close() })

	proxyEnv.SetByteStreamClient(bspb.NewByteStreamClient(remoteConn))
	casClient := repb.NewContentAddressableStorageClient(remoteConn)
	proxyEnv.SetContentAddressableStorageClient(casClient)
	proxyBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(t, err)
	proxyEnv.SetLocalByteStreamServer(proxyBSS)
	proxyServer, err := New(proxyEnv)
	require.NoError(t, err)
	proxyGRPC, proxyRun, proxyLis := testenv.RegisterLocalGRPCServer(t, proxyEnv)
	bspb.RegisterByteStreamServer(proxyGRPC, proxyServer)
	go proxyRun()
	proxyConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(t, err)
	t.Cleanup(func() { proxyConn.Close() })
	proxy := bspb.NewByteStreamClient(proxyConn)

	anonCtx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)
	encryptedUserCtx, err := ta.WithAuthenticatedUser(anonCtx, "user")
	require.NoError(t, err)

	_, originalData := testdigest.RandomCASResourceBuf(t, 3*1024*1024)
	blobDigest, err := digest.Compute(bytes.NewReader(originalData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	var chunkDigests []*repb.Digest
	writeChunkFn := func(chunkData []byte) error {
		chunkDataCopy := make([]byte, len(chunkData))
		copy(chunkDataCopy, chunkData)
		chunkDigest, err := digest.Compute(bytes.NewReader(chunkDataCopy), repb.DigestFunction_BLAKE3)
		if err != nil {
			return err
		}
		chunkDigests = append(chunkDigests, chunkDigest)
		chunkRN := digest.NewCASResourceName(chunkDigest, "", repb.DigestFunction_BLAKE3)
		return remoteEnv.GetCache().Set(anonCtx, chunkRN.ToProto(), chunkDataCopy)
	}
	cdcChunker, err := chunking.NewChunker(anonCtx, 64*1024, writeChunkFn)
	require.NoError(t, err)
	_, err = cdcChunker.Write(originalData)
	require.NoError(t, err)
	require.NoError(t, cdcChunker.Close())
	require.Greater(t, len(chunkDigests), 1)

	_, err = casClient.SpliceBlob(anonCtx, &repb.SpliceBlobRequest{
		BlobDigest:     blobDigest,
		ChunkDigests:   chunkDigests,
		DigestFunction: repb.DigestFunction_BLAKE3,
	})
	require.NoError(t, err)
	splitBlobCalls.Store(0)

	downloadRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	readBlob := func() []byte {
		downloadStream, err := proxy.Read(encryptedUserCtx, &bspb.ReadRequest{ResourceName: downloadRN.DownloadString()})
		require.NoError(t, err)

		var reconstructedData []byte
		for {
			res, err := downloadStream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			reconstructedData = append(reconstructedData, res.Data...)
		}
		return reconstructedData
	}

	require.Equal(t, originalData, readBlob())
	require.Equal(t, int32(1), splitBlobCalls.Load())
	for _, chunkDigest := range chunkDigests {
		chunkRN := digest.NewCASResourceName(chunkDigest, "", repb.DigestFunction_BLAKE3)
		found, err := proxyEnv.GetCache().Contains(anonCtx, chunkRN.ToProto())
		require.NoError(t, err)
		require.False(t, found)
	}

	require.Equal(t, originalData, readBlob())
	require.Equal(t, int32(2), splitBlobCalls.Load())
	for _, chunkDigest := range chunkDigests {
		chunkRN := digest.NewCASResourceName(chunkDigest, "", repb.DigestFunction_BLAKE3)
		found, err := proxyEnv.GetCache().Contains(anonCtx, chunkRN.ToProto())
		require.NoError(t, err)
		require.False(t, found)
	}
}

func TestReadChunkedEncryptedRemoteOnlyFallsBackToFullBlob(t *testing.T) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.attempt_chunked_reads": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)

	userWithEncryption := testauth.User("user", "group")
	userWithEncryption.CacheEncryptionEnabled = true
	users := map[string]interfaces.UserInfo{"user": userWithEncryption}
	ta := testauth.NewTestAuthenticator(t, users)
	proxyEnv.SetAuthenticator(ta)

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	remoteEnv.SetExperimentFlagProvider(fp)
	proxyEnv.SetExperimentFlagProvider(fp)

	remoteBSS, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(t, err)
	remoteCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(t, err)
	remoteGRPC, remoteRun, remoteLis := testenv.RegisterLocalGRPCServer(t, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPC, remoteBSS)
	repb.RegisterContentAddressableStorageServer(remoteGRPC, remoteCAS)
	go remoteRun()

	var splitBlobCalls atomic.Int32
	remoteConn, err := testenv.LocalGRPCConn(ctx, remoteLis, grpc.WithUnaryInterceptor(func(
		ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		if strings.HasSuffix(method, "/SplitBlob") {
			splitBlobCalls.Add(1)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}))
	require.NoError(t, err)
	t.Cleanup(func() { remoteConn.Close() })

	proxyEnv.SetByteStreamClient(bspb.NewByteStreamClient(remoteConn))
	proxyEnv.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(remoteConn))
	proxyBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(t, err)
	proxyEnv.SetLocalByteStreamServer(proxyBSS)
	proxyServer, err := New(proxyEnv)
	require.NoError(t, err)
	proxyGRPC, proxyRun, proxyLis := testenv.RegisterLocalGRPCServer(t, proxyEnv)
	bspb.RegisterByteStreamServer(proxyGRPC, proxyServer)
	go proxyRun()
	proxyConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(t, err)
	t.Cleanup(func() { proxyConn.Close() })
	proxy := bspb.NewByteStreamClient(proxyConn)

	anonCtx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)
	encryptedUserCtx, err := ta.WithAuthenticatedUser(anonCtx, "user")
	require.NoError(t, err)
	remoteCtx, err := prefix.AttachUserPrefixToContext(ctx, remoteEnv.GetAuthenticator())
	require.NoError(t, err)

	_, originalData := testdigest.RandomCASResourceBuf(t, 3*1024*1024)
	blobDigest, err := digest.Compute(bytes.NewReader(originalData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	blobRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	require.NoError(t, remoteEnv.GetCache().Set(remoteCtx, blobRN.ToProto(), originalData))

	readBlob := func() []byte {
		downloadStream, err := proxy.Read(encryptedUserCtx, &bspb.ReadRequest{ResourceName: blobRN.DownloadString()})
		require.NoError(t, err)

		var got []byte
		for {
			res, err := downloadStream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			got = append(got, res.Data...)
		}
		return got
	}

	require.Equal(t, originalData, readBlob())
	require.Equal(t, int32(1), splitBlobCalls.Load())
	found, err := proxyEnv.GetCache().Contains(anonCtx, blobRN.ToProto())
	require.NoError(t, err)
	require.False(t, found)

	require.Equal(t, originalData, readBlob())
	require.Equal(t, int32(2), splitBlobCalls.Load())
	found, err = proxyEnv.GetCache().Contains(anonCtx, blobRN.ToProto())
	require.NoError(t, err)
	require.False(t, found)
}

func TestReadChunkedCompressedWarmLocal(t *testing.T) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.attempt_chunked_reads": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.intercept_and_chunk_large_writes": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	uploadProxyEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	remoteEnv.SetExperimentFlagProvider(fp)
	uploadProxyEnv.SetExperimentFlagProvider(fp)
	proxyEnv.SetExperimentFlagProvider(fp)

	remoteBSS, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(t, err)
	remoteCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(t, err)
	remoteGRPC, remoteRun, remoteLis := testenv.RegisterLocalGRPCServer(t, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPC, remoteBSS)
	repb.RegisterContentAddressableStorageServer(remoteGRPC, remoteCAS)
	go remoteRun()
	remoteConn, err := testenv.LocalGRPCConn(ctx, remoteLis)
	require.NoError(t, err)
	t.Cleanup(func() { remoteConn.Close() })
	bsClient := bspb.NewByteStreamClient(remoteConn)
	casClient := repb.NewContentAddressableStorageClient(remoteConn)

	uploadProxyEnv.SetByteStreamClient(bsClient)
	uploadProxyEnv.SetContentAddressableStorageClient(casClient)
	uploadProxyBSS, err := byte_stream_server.NewByteStreamServer(uploadProxyEnv)
	require.NoError(t, err)
	uploadProxyEnv.SetLocalByteStreamServer(uploadProxyBSS)
	uploadProxyServer, err := New(uploadProxyEnv)
	require.NoError(t, err)

	proxyEnv.SetByteStreamClient(bsClient)
	proxyEnv.SetContentAddressableStorageClient(casClient)
	proxyBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(t, err)
	proxyEnv.SetLocalByteStreamServer(proxyBSS)
	proxyServer, err := New(proxyEnv)
	require.NoError(t, err)
	proxyGRPC, proxyRun, proxyLis := testenv.RegisterLocalGRPCServer(t, proxyEnv)
	bspb.RegisterByteStreamServer(proxyGRPC, proxyServer)
	go proxyRun()
	proxyConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(t, err)
	t.Cleanup(func() { proxyConn.Close() })
	proxy := bspb.NewByteStreamClient(proxyConn)

	uploadCtx, err := prefix.AttachUserPrefixToContext(ctx, uploadProxyEnv.GetAuthenticator())
	require.NoError(t, err)
	readCtx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)

	_, originalData := testdigest.RandomCASResourceBuf(t, 3*1024*1024)
	blobDigest, err := digest.Compute(bytes.NewReader(originalData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	compressedData := compression.CompressZstd(nil, originalData)
	blobRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	blobRN.SetCompressor(repb.Compressor_ZSTD)
	require.NoError(t, uploadProxyServer.Write(&rawWriteStream{
		ctx:          uploadCtx,
		resourceName: blobRN.NewUploadString(),
		data:         compressedData,
	}))
	splitResp, err := casClient.SplitBlob(readCtx, &repb.SplitBlobRequest{
		BlobDigest:     blobDigest,
		DigestFunction: repb.DigestFunction_BLAKE3,
	})
	require.NoError(t, err)
	chunkDigests := splitResp.GetChunkDigests()
	require.Greater(t, len(chunkDigests), 1)

	readLabels := prometheus.Labels{
		metrics.StatusLabel:     "OK",
		metrics.CompressionType: "ZSTD",
	}
	readRequestsBefore := testutil.ToFloat64(metrics.ByteStreamChunkedReadRequests.With(readLabels))
	readChunksTotalBefore := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksTotal.With(readLabels))
	readChunksLocalBefore := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksLocal.With(readLabels))
	readChunksRemoteBefore := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksRemote.With(readLabels))

	downloadCASRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	downloadCASRN.SetCompressor(repb.Compressor_ZSTD)
	readBlob := func() []byte {
		downloadStream, err := proxy.Read(readCtx, &bspb.ReadRequest{ResourceName: downloadCASRN.DownloadString()})
		require.NoError(t, err)

		var compressedBlob []byte
		for {
			res, err := downloadStream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			compressedBlob = append(compressedBlob, res.Data...)
		}
		return compressedBlob
	}

	gotCompressed := readBlob()
	got, err := compression.DecompressZstd(nil, gotCompressed)
	require.NoError(t, err)
	require.Equal(t, originalData, got)

	readRequestsAfter := testutil.ToFloat64(metrics.ByteStreamChunkedReadRequests.With(readLabels))
	readChunksTotalAfter := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksTotal.With(readLabels))
	readChunksLocalAfter := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksLocal.With(readLabels))
	readChunksRemoteAfter := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksRemote.With(readLabels))

	require.Equal(t, float64(1), readRequestsAfter-readRequestsBefore, "one chunked read request for the blob")
	require.Equal(t, float64(len(chunkDigests)), readChunksTotalAfter-readChunksTotalBefore, "total chunks = number of chunks in manifest")
	require.Equal(t, float64(0), readChunksLocalAfter-readChunksLocalBefore, "first read: no chunks in local cache yet")
	require.Equal(t, float64(len(chunkDigests)), readChunksRemoteAfter-readChunksRemoteBefore, "first read: all chunks fetched from remote")

	for _, chunkDigest := range chunkDigests {
		chunkRN := digest.NewCASResourceName(chunkDigest, "", repb.DigestFunction_BLAKE3)
		chunkRN.SetCompressor(downloadCASRN.GetCompressor())
		require.NoError(t, waitContains(readCtx, proxyEnv, chunkRN.ToProto()))
	}

	readRequestsBefore = testutil.ToFloat64(metrics.ByteStreamChunkedReadRequests.With(readLabels))
	readChunksTotalBefore = testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksTotal.With(readLabels))
	readChunksLocalBefore = testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksLocal.With(readLabels))
	readChunksRemoteBefore = testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksRemote.With(readLabels))

	gotCompressed = readBlob()
	got, err = compression.DecompressZstd(nil, gotCompressed)
	require.NoError(t, err)
	require.Equal(t, originalData, got)

	readRequestsAfter = testutil.ToFloat64(metrics.ByteStreamChunkedReadRequests.With(readLabels))
	readChunksTotalAfter = testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksTotal.With(readLabels))
	readChunksLocalAfter = testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksLocal.With(readLabels))
	readChunksRemoteAfter = testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksRemote.With(readLabels))

	require.Equal(t, float64(1), readRequestsAfter-readRequestsBefore, "one chunked read request for the blob")
	require.Equal(t, float64(len(chunkDigests)), readChunksTotalAfter-readChunksTotalBefore, "total chunks = number of chunks in manifest")
	require.Equal(t, float64(len(chunkDigests)), readChunksLocalAfter-readChunksLocalBefore, "second read: all chunks served from local cache")
	require.Equal(t, float64(0), readChunksRemoteAfter-readChunksRemoteBefore, "second read: no remote chunk fetches")
}

type faultyCache struct {
	interfaces.Cache
	faultDigest string
	failAfter   int64
	failErr     error
}

func (c *faultyCache) Reader(ctx context.Context, r *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	reader, err := c.Cache.Reader(ctx, r, uncompressedOffset, limit)
	if err != nil {
		return nil, err
	}
	shouldFault := c.faultDigest == r.GetDigest().GetHash()
	failAfter := c.failAfter
	failErr := c.failErr
	if shouldFault {
		return &faultyReader{ReadCloser: reader, failAfter: failAfter, failErr: failErr}, nil
	}
	return reader, nil
}

type faultyReader struct {
	io.ReadCloser
	bytesRead int64
	failAfter int64
	failErr   error
}

func (r *faultyReader) Read(p []byte) (int, error) {
	remaining := r.failAfter - r.bytesRead
	if remaining <= 0 {
		return 0, r.failErr
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.ReadCloser.Read(p)
	r.bytesRead += int64(n)
	return n, err
}

func TestReadChunkedWithOffset(t *testing.T) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.attempt_chunked_reads": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	remoteEnv.SetExperimentFlagProvider(fp)
	proxyEnv.SetExperimentFlagProvider(fp)

	// Start the backing remote cache services.
	remoteBSS, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(t, err)
	remoteCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(t, err)
	remoteGRPC, remoteRun, remoteLis := testenv.RegisterLocalGRPCServer(t, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPC, remoteBSS)
	repb.RegisterContentAddressableStorageServer(remoteGRPC, remoteCAS)
	go remoteRun()
	remoteConn, err := testenv.LocalGRPCConn(ctx, remoteLis)
	require.NoError(t, err)
	t.Cleanup(func() { remoteConn.Close() })
	bsClient := bspb.NewByteStreamClient(remoteConn)
	casClient := repb.NewContentAddressableStorageClient(remoteConn)

	// Start the proxy under test and point it at the remote cache.
	proxyEnv.SetByteStreamClient(bsClient)
	proxyEnv.SetContentAddressableStorageClient(casClient)
	proxyBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(t, err)
	proxyEnv.SetLocalByteStreamServer(proxyBSS)
	proxyServer, err := New(proxyEnv)
	require.NoError(t, err)
	proxyGRPC, proxyRun, proxyLis := testenv.RegisterLocalGRPCServer(t, proxyEnv)
	bspb.RegisterByteStreamServer(proxyGRPC, proxyServer)
	go proxyRun()
	proxyConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(t, err)
	t.Cleanup(func() { proxyConn.Close() })
	proxy := bspb.NewByteStreamClient(proxyConn)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)

	// Write the blob as CDC chunks and publish the manifest remotely.
	_, originalData := testdigest.RandomCASResourceBuf(t, 3*1024*1024)
	blobDigest, err := digest.Compute(bytes.NewReader(originalData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	var chunkDigests []*repb.Digest
	var chunkDataCopies [][]byte
	writeChunkFn := func(chunkData []byte) error {
		chunkDataCopy := make([]byte, len(chunkData))
		copy(chunkDataCopy, chunkData)
		chunkDigest, err := digest.Compute(bytes.NewReader(chunkDataCopy), repb.DigestFunction_BLAKE3)
		if err != nil {
			return err
		}
		chunkDigests = append(chunkDigests, chunkDigest)
		chunkDataCopies = append(chunkDataCopies, chunkDataCopy)
		chunkRN := digest.NewCASResourceName(chunkDigest, "", repb.DigestFunction_BLAKE3)
		return remoteEnv.GetCache().Set(ctx, chunkRN.ToProto(), chunkDataCopy)
	}
	cdcChunker, err := chunking.NewChunker(ctx, 64*1024, writeChunkFn)
	require.NoError(t, err)
	_, err = cdcChunker.Write(originalData)
	require.NoError(t, err)
	require.NoError(t, cdcChunker.Close())
	require.Greater(t, len(chunkDigests), 1)

	_, err = casClient.SpliceBlob(ctx, &repb.SpliceBlobRequest{
		BlobDigest:     blobDigest,
		ChunkDigests:   chunkDigests,
		DigestFunction: repb.DigestFunction_BLAKE3,
	})
	require.NoError(t, err)

	downloadCASRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)

	t.Run("MidFirstChunkOffsetWarmLocal", func(t *testing.T) {
		for i, chunkDigest := range chunkDigests {
			chunkRN := digest.NewCASResourceName(chunkDigest, "", repb.DigestFunction_BLAKE3)
			require.NoError(t, proxyEnv.GetCache().Set(ctx, chunkRN.ToProto(), chunkDataCopies[i]))
		}

		readLabels := prometheus.Labels{
			metrics.StatusLabel:     "OK",
			metrics.CompressionType: "IDENTITY",
		}
		readChunksLocalBefore := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksLocal.With(readLabels))
		readChunksRemoteBefore := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksRemote.With(readLabels))

		offset := chunkDigests[0].GetSizeBytes() / 2
		downloadStream, err := proxy.Read(ctx, &bspb.ReadRequest{
			ResourceName: downloadCASRN.DownloadString(),
			ReadOffset:   offset,
		})
		require.NoError(t, err)

		var got []byte
		for {
			res, err := downloadStream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			got = append(got, res.Data...)
		}
		require.Equal(t, originalData[offset:], got)

		readChunksLocalAfter := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksLocal.With(readLabels))
		readChunksRemoteAfter := testutil.ToFloat64(metrics.ByteStreamChunkedReadChunksRemote.With(readLabels))
		require.Equal(t, float64(len(chunkDigests)), readChunksLocalAfter-readChunksLocalBefore)
		require.Equal(t, float64(0), readChunksRemoteAfter-readChunksRemoteBefore)
	})

	t.Run("MidChunkOffset", func(t *testing.T) {
		offset := chunkDigests[0].GetSizeBytes() + chunkDigests[1].GetSizeBytes()/2
		downloadStream, err := proxy.Read(ctx, &bspb.ReadRequest{
			ResourceName: downloadCASRN.DownloadString(),
			ReadOffset:   offset,
		})
		require.NoError(t, err)

		var got []byte
		for {
			res, err := downloadStream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			got = append(got, res.Data...)
		}
		require.Equal(t, originalData[offset:], got)
	})

	t.Run("ChunkBoundaryOffset", func(t *testing.T) {
		offset := chunkDigests[0].GetSizeBytes()
		downloadStream, err := proxy.Read(ctx, &bspb.ReadRequest{
			ResourceName: downloadCASRN.DownloadString(),
			ReadOffset:   offset,
		})
		require.NoError(t, err)

		var got []byte
		for {
			res, err := downloadStream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			got = append(got, res.Data...)
		}
		require.Equal(t, originalData[offset:], got)
	})
}

func TestReadChunkedFallsBackToLocalBlob(t *testing.T) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.attempt_chunked_reads": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	remoteEnv.SetExperimentFlagProvider(fp)
	proxyEnv.SetExperimentFlagProvider(fp)

	remoteBSS, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(t, err)
	remoteCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(t, err)
	remoteGRPC, remoteRun, remoteLis := testenv.RegisterLocalGRPCServer(t, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPC, remoteBSS)
	repb.RegisterContentAddressableStorageServer(remoteGRPC, remoteCAS)
	go remoteRun()

	var splitBlobCalls atomic.Int32
	remoteConn, err := testenv.LocalGRPCConn(ctx, remoteLis, grpc.WithUnaryInterceptor(func(
		ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		if strings.HasSuffix(method, "/SplitBlob") {
			splitBlobCalls.Add(1)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}))
	require.NoError(t, err)
	t.Cleanup(func() { remoteConn.Close() })

	proxyEnv.SetByteStreamClient(bspb.NewByteStreamClient(remoteConn))
	proxyEnv.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(remoteConn))
	proxyBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(t, err)
	proxyEnv.SetLocalByteStreamServer(proxyBSS)
	proxyServer, err := New(proxyEnv)
	require.NoError(t, err)
	proxyGRPC, proxyRun, proxyLis := testenv.RegisterLocalGRPCServer(t, proxyEnv)
	bspb.RegisterByteStreamServer(proxyGRPC, proxyServer)
	go proxyRun()
	proxyConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(t, err)
	t.Cleanup(func() { proxyConn.Close() })
	proxy := bspb.NewByteStreamClient(proxyConn)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)

	_, originalData := testdigest.RandomCASResourceBuf(t, 3*1024*1024)
	blobDigest, err := digest.Compute(bytes.NewReader(originalData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	blobRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	require.NoError(t, proxyEnv.GetCache().Set(ctx, blobRN.ToProto(), originalData))

	downloadStream, err := proxy.Read(ctx, &bspb.ReadRequest{ResourceName: blobRN.DownloadString()})
	require.NoError(t, err)

	var got []byte
	for {
		res, err := downloadStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, res.Data...)
	}

	require.Equal(t, originalData, got)
	require.Equal(t, int32(1), splitBlobCalls.Load(), "chunked-eligible reads should try SplitBlob first")
}

func TestReadChunkedPartialLocalFailure(t *testing.T) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.attempt_chunked_reads": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	remoteEnv.SetExperimentFlagProvider(fp)
	proxyEnv.SetExperimentFlagProvider(fp)

	// Start the backing remote cache services.
	remoteBSS, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(t, err)
	remoteCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(t, err)
	remoteGRPC, remoteRun, remoteLis := testenv.RegisterLocalGRPCServer(t, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPC, remoteBSS)
	repb.RegisterContentAddressableStorageServer(remoteGRPC, remoteCAS)
	go remoteRun()
	remoteConn, err := testenv.LocalGRPCConn(ctx, remoteLis)
	require.NoError(t, err)
	t.Cleanup(func() { remoteConn.Close() })
	bsClient := bspb.NewByteStreamClient(remoteConn)
	casClient := repb.NewContentAddressableStorageClient(remoteConn)

	proxyEnv.SetByteStreamClient(bsClient)
	proxyEnv.SetContentAddressableStorageClient(casClient)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)

	// Write the blob as CDC chunks and publish the manifest remotely.
	_, originalData := testdigest.RandomCASResourceBuf(t, 3*1024*1024)
	blobDigest, err := digest.Compute(bytes.NewReader(originalData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	var chunkDigests []*repb.Digest
	writeChunkFn := func(chunkData []byte) error {
		chunkDataCopy := make([]byte, len(chunkData))
		copy(chunkDataCopy, chunkData)
		chunkDigest, err := digest.Compute(bytes.NewReader(chunkDataCopy), repb.DigestFunction_BLAKE3)
		if err != nil {
			return err
		}
		chunkDigests = append(chunkDigests, chunkDigest)
		chunkRN := digest.NewCASResourceName(chunkDigest, "", repb.DigestFunction_BLAKE3)
		return remoteEnv.GetCache().Set(ctx, chunkRN.ToProto(), chunkDataCopy)
	}
	cdcChunker, err := chunking.NewChunker(ctx, 64*1024, writeChunkFn)
	require.NoError(t, err)
	_, err = cdcChunker.Write(originalData)
	require.NoError(t, err)
	require.NoError(t, cdcChunker.Close())
	require.Greater(t, len(chunkDigests), 1)

	_, err = casClient.SpliceBlob(ctx, &repb.SpliceBlobRequest{
		BlobDigest:     blobDigest,
		ChunkDigests:   chunkDigests,
		DigestFunction: repb.DigestFunction_BLAKE3,
	})
	require.NoError(t, err)

	chunk1RN := digest.NewCASResourceName(chunkDigests[1], "", repb.DigestFunction_BLAKE3)
	chunk1Data, err := remoteEnv.GetCache().Get(ctx, chunk1RN.ToProto())
	require.NoError(t, err)

	fc := &faultyCache{
		Cache:       proxyEnv.GetCache(),
		faultDigest: chunkDigests[1].GetHash(),
		failAfter:   int64(len(chunk1Data) / 3),
		failErr:     status.UnavailableError("injected fault"),
	}
	proxyEnv.SetCache(fc)
	require.NoError(t, fc.Cache.Set(ctx, chunk1RN.ToProto(), chunk1Data))

	// Rebuild the proxy so local chunk reads go through the fault-injecting cache.
	proxyBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(t, err)
	proxyEnv.SetLocalByteStreamServer(proxyBSS)
	proxyServer, err := New(proxyEnv)
	require.NoError(t, err)
	proxyGRPC, proxyRun, proxyLis := testenv.RegisterLocalGRPCServer(t, proxyEnv)
	bspb.RegisterByteStreamServer(proxyGRPC, proxyServer)
	go proxyRun()
	proxyConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(t, err)
	t.Cleanup(func() { proxyConn.Close() })
	proxy := bspb.NewByteStreamClient(proxyConn)

	downloadCASRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	downloadStream, err := proxy.Read(ctx, &bspb.ReadRequest{ResourceName: downloadCASRN.DownloadString()})
	require.NoError(t, err)

	var got []byte
	for {
		res, err := downloadStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, res.Data...)
	}
	require.Equal(t, originalData, got)
}

func TestReadRemoteChunkRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		name         string
		err          error
		wantAttempts int
	}{
		{name: "not_found", err: status.NotFoundError("missing"), wantAttempts: 1},
		{name: "permission_denied", err: status.PermissionDeniedError("denied"), wantAttempts: 1},
		{name: "unauthenticated", err: status.UnauthenticatedError("unauthenticated"), wantAttempts: 1},
		{name: "invalid_argument", err: status.InvalidArgumentError("invalid"), wantAttempts: 1},
		{name: "failed_precondition", err: status.FailedPreconditionError("bad state"), wantAttempts: 1},
		{name: "out_of_range", err: status.OutOfRangeError("bad offset"), wantAttempts: 1},
		{name: "unimplemented", err: status.UnimplementedError("unsupported"), wantAttempts: 1},
		{name: "short_buffer", err: io.ErrShortBuffer, wantAttempts: 1},
		{name: "unavailable", err: status.UnavailableError("transient"), wantAttempts: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := chunkReadRetryOptions()
			opts.DontLogFailedAttempts = true

			attempts := 0
			_, err := retry.Do(context.Background(), opts, func(ctx context.Context) (struct{}, error) {
				attempts++
				if tc.err == nil {
					return struct{}{}, nil
				}
				if tc.err == io.ErrShortBuffer ||
					status.IsNotFoundError(tc.err) ||
					status.IsPermissionDeniedError(tc.err) ||
					status.IsUnauthenticatedError(tc.err) ||
					status.IsInvalidArgumentError(tc.err) ||
					status.IsFailedPreconditionError(tc.err) ||
					status.IsOutOfRangeError(tc.err) ||
					status.IsUnimplementedError(tc.err) {
					return struct{}{}, retry.NonRetryableError(tc.err)
				}
				return struct{}{}, tc.err
			})
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.wantAttempts, attempts)
		})
	}
}

func TestWriteChunked(t *testing.T) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.intercept_and_chunk_large_writes": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	flags.Set(t, "cache.zstd_transcoding_enabled", true)

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	remoteEnv.SetExperimentFlagProvider(fp)
	proxyEnv.SetExperimentFlagProvider(fp)

	pc, err := pebble_cache.NewPebbleCache(proxyEnv, &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(t),
		MaxSizeBytes:  1_000_000_000,
	})
	require.NoError(t, err)
	err = pc.Start()
	require.NoError(t, err)
	t.Cleanup(func() { pc.Stop() })
	proxyEnv.SetCache(pc)

	remoteBSS, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(t, err)
	remoteCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(t, err)
	remoteGRPC, remoteRun, remoteLis := testenv.RegisterLocalGRPCServer(t, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPC, remoteBSS)
	repb.RegisterContentAddressableStorageServer(remoteGRPC, remoteCAS)
	go remoteRun()
	remoteConn, err := testenv.LocalGRPCConn(ctx, remoteLis)
	require.NoError(t, err)
	t.Cleanup(func() { remoteConn.Close() })
	bsClient := bspb.NewByteStreamClient(remoteConn)
	casClient := repb.NewContentAddressableStorageClient(remoteConn)

	proxyEnv.SetByteStreamClient(bsClient)
	proxyEnv.SetContentAddressableStorageClient(casClient)
	proxyBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(t, err)
	proxyEnv.SetLocalByteStreamServer(proxyBSS)
	proxyServer, err := New(proxyEnv)
	require.NoError(t, err)
	proxyGRPC, proxyRun, proxyLis := testenv.RegisterLocalGRPCServer(t, proxyEnv)
	bspb.RegisterByteStreamServer(proxyGRPC, proxyServer)
	go proxyRun()
	proxyConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(t, err)
	t.Cleanup(func() { proxyConn.Close() })
	proxy := bspb.NewByteStreamClient(proxyConn)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)

	_, originalData := testdigest.RandomCASResourceBuf(t, 10*1024*1024)
	blobDigest, err := digest.Compute(bytes.NewReader(originalData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	compressedData := compression.CompressZstd(nil, originalData)

	blobRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	blobRN.SetCompressor(repb.Compressor_ZSTD)

	// Capture metric values before the write
	writeLabels := prometheus.Labels{
		metrics.StatusLabel:     "OK",
		metrics.CompressionType: "ZSTD",
	}
	writeLabelsWithGroup := prometheus.Labels{
		metrics.StatusLabel:     "OK",
		metrics.CompressionType: "ZSTD",
		metrics.GroupID:         interfaces.AuthAnonymousUser,
	}
	writeBlobBytesBefore := testutil.ToFloat64(metrics.ByteStreamChunkedWriteBlobBytes.With(writeLabels))
	writeChunksTotalBefore := testutil.ToFloat64(metrics.ByteStreamChunkedWriteChunksTotal.With(writeLabelsWithGroup))
	writeChunksDedupedBefore := testutil.ToFloat64(metrics.ByteStreamChunkedWriteChunksDeduped.With(writeLabelsWithGroup))
	writeChunkBytesBefore := testutil.ToFloat64(metrics.ByteStreamChunkedWriteChunkBytes.With(writeLabels))
	writeDedupedBytesBefore := testutil.ToFloat64(metrics.ByteStreamChunkedWriteDedupedChunkBytes.With(writeLabels))

	uploadStream, err := proxy.Write(ctx)
	require.NoError(t, err)
	remaining := compressedData
	for len(remaining) > 0 {
		chunkSize := 1_000_000
		if chunkSize > len(remaining) {
			chunkSize = len(remaining)
		}
		err = uploadStream.Send(&bspb.WriteRequest{
			ResourceName: blobRN.NewUploadString(),
			WriteOffset:  int64(len(compressedData) - len(remaining)),
			Data:         remaining[:chunkSize],
			FinishWrite:  chunkSize == len(remaining),
		})
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		remaining = remaining[chunkSize:]
	}
	_, err = uploadStream.CloseAndRecv()
	require.NoError(t, err)

	writeBlobBytesAfter := testutil.ToFloat64(metrics.ByteStreamChunkedWriteBlobBytes.With(writeLabels))
	writeChunksTotalAfter := testutil.ToFloat64(metrics.ByteStreamChunkedWriteChunksTotal.With(writeLabelsWithGroup))
	writeChunksDedupedAfter := testutil.ToFloat64(metrics.ByteStreamChunkedWriteChunksDeduped.With(writeLabelsWithGroup))
	writeChunkBytesAfter := testutil.ToFloat64(metrics.ByteStreamChunkedWriteChunkBytes.With(writeLabels))
	writeDedupedBytesAfter := testutil.ToFloat64(metrics.ByteStreamChunkedWriteDedupedChunkBytes.With(writeLabels))

	require.Equal(t, float64(len(originalData)), writeBlobBytesAfter-writeBlobBytesBefore, "blob bytes = original uncompressed size")
	require.Greater(t, writeChunksTotalAfter-writeChunksTotalBefore, float64(0), "should have produced multiple chunks")
	require.Equal(t, float64(0), writeChunksDedupedAfter-writeChunksDedupedBefore, "first write: no chunks existed on remote yet")
	require.Greater(t, writeChunkBytesAfter-writeChunkBytesBefore, float64(0), "chunk bytes = sum of all chunk sizes")
	require.Equal(t, float64(0), writeDedupedBytesAfter-writeDedupedBytesBefore, "first write: no bytes deduped")

	downloadRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	downloadRN.SetCompressor(repb.Compressor_ZSTD)
	downloadStream, err := proxy.Read(ctx, &bspb.ReadRequest{ResourceName: downloadRN.DownloadString()})
	require.NoError(t, err)

	var downloadedCompressedData []byte
	for {
		res, err := downloadStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		downloadedCompressedData = append(downloadedCompressedData, res.Data...)
	}

	decompressedData, err := compression.DecompressZstd(nil, downloadedCompressedData)
	require.NoError(t, err)
	require.Equal(t, originalData, decompressedData)

	downloadRN.SetCompressor(repb.Compressor_IDENTITY)
	downloadStream, err = proxy.Read(ctx, &bspb.ReadRequest{ResourceName: downloadRN.DownloadString()})
	require.NoError(t, err)

	var reconstructedData []byte
	for {
		res, err := downloadStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		reconstructedData = append(reconstructedData, res.Data...)
	}

	require.Equal(t, originalData, reconstructedData)
}

func TestWriteChunkedEncryptedRemoteOnly(t *testing.T) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.intercept_and_chunk_large_writes": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	flags.Set(t, "cache.zstd_transcoding_enabled", true)

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)

	userWithEncryption := testauth.User("user", "group")
	userWithEncryption.CacheEncryptionEnabled = true
	users := map[string]interfaces.UserInfo{"user": userWithEncryption}
	ta := testauth.NewTestAuthenticator(t, users)
	proxyEnv.SetAuthenticator(ta)

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	remoteEnv.SetExperimentFlagProvider(fp)
	proxyEnv.SetExperimentFlagProvider(fp)

	proxyPC, err := pebble_cache.NewPebbleCache(proxyEnv, &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(t),
		MaxSizeBytes:  1_000_000_000,
	})
	require.NoError(t, err)
	require.NoError(t, proxyPC.Start())
	t.Cleanup(func() { proxyPC.Stop() })
	proxyEnv.SetCache(proxyPC)

	remotePC, err := pebble_cache.NewPebbleCache(remoteEnv, &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(t),
		MaxSizeBytes:  1_000_000_000,
	})
	require.NoError(t, err)
	require.NoError(t, remotePC.Start())
	t.Cleanup(func() { remotePC.Stop() })
	remoteEnv.SetCache(remotePC)

	remoteBSS, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(t, err)
	remoteCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(t, err)
	remoteGRPC, remoteRun, remoteLis := testenv.RegisterLocalGRPCServer(t, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPC, remoteBSS)
	repb.RegisterContentAddressableStorageServer(remoteGRPC, remoteCAS)
	go remoteRun()

	remoteConn, err := testenv.LocalGRPCConn(ctx, remoteLis)
	require.NoError(t, err)
	t.Cleanup(func() { remoteConn.Close() })

	proxyEnv.SetByteStreamClient(bspb.NewByteStreamClient(remoteConn))
	casClient := repb.NewContentAddressableStorageClient(remoteConn)
	proxyEnv.SetContentAddressableStorageClient(casClient)
	proxyBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(t, err)
	proxyEnv.SetLocalByteStreamServer(proxyBSS)
	proxyServer, err := New(proxyEnv)
	require.NoError(t, err)
	proxyGRPC, proxyRun, proxyLis := testenv.RegisterLocalGRPCServer(t, proxyEnv)
	bspb.RegisterByteStreamServer(proxyGRPC, proxyServer)
	go proxyRun()

	proxyConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(t, err)
	t.Cleanup(func() { proxyConn.Close() })
	proxy := bspb.NewByteStreamClient(proxyConn)

	anonCtx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)
	encryptedUserCtx, err := ta.WithAuthenticatedUser(anonCtx, "user")
	require.NoError(t, err)
	remoteCtx, err := prefix.AttachUserPrefixToContext(ctx, remoteEnv.GetAuthenticator())
	require.NoError(t, err)

	_, originalData := testdigest.RandomCASResourceBuf(t, 10*1024*1024)
	blobDigest, err := digest.Compute(bytes.NewReader(originalData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	compressedData := compression.CompressZstd(nil, originalData)
	blobRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	blobRN.SetCompressor(repb.Compressor_ZSTD)

	uploadStream, err := proxy.Write(encryptedUserCtx)
	require.NoError(t, err)
	remaining := compressedData
	written := int64(0)
	for len(remaining) > 0 {
		chunkSize := min(1_000_000, len(remaining))
		require.NoError(t, uploadStream.Send(&bspb.WriteRequest{
			ResourceName: blobRN.NewUploadString(),
			WriteOffset:  written,
			Data:         remaining[:chunkSize],
			FinishWrite:  chunkSize == len(remaining),
		}))
		written += int64(chunkSize)
		remaining = remaining[chunkSize:]
	}
	_, err = uploadStream.CloseAndRecv()
	require.NoError(t, err)

	splitResp, err := casClient.SplitBlob(remoteCtx, &repb.SplitBlobRequest{
		BlobDigest:     blobDigest,
		InstanceName:   "",
		DigestFunction: repb.DigestFunction_BLAKE3,
	})
	require.NoError(t, err)
	require.Greater(t, len(splitResp.GetChunkDigests()), 1)

	for _, chunkDigest := range splitResp.GetChunkDigests() {
		chunkRN := digest.NewCASResourceName(chunkDigest, "", repb.DigestFunction_BLAKE3)
		chunkRN.SetCompressor(repb.Compressor_ZSTD)

		foundInProxy, err := proxyEnv.GetCache().Contains(anonCtx, chunkRN.ToProto())
		require.NoError(t, err)
		require.False(t, foundInProxy)

		foundInRemote, err := remoteEnv.GetCache().Contains(remoteCtx, chunkRN.ToProto())
		require.NoError(t, err)
		require.True(t, foundInRemote)
	}

	downloadStream, err := proxy.Read(encryptedUserCtx, &bspb.ReadRequest{ResourceName: blobRN.DownloadString()})
	require.NoError(t, err)
	var downloadedCompressedData []byte
	for {
		res, err := downloadStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		downloadedCompressedData = append(downloadedCompressedData, res.Data...)
	}

	decompressedData, err := compression.DecompressZstd(nil, downloadedCompressedData)
	require.NoError(t, err)
	require.Equal(t, originalData, decompressedData)
}

func TestWriteChunkedGroupsFindMissingAndBatchesUploads(t *testing.T) {
	flags.Set(t, "cache.avg_chunk_size_bytes", 64*1024)
	flags.Set(t, "cache.zstd_transcoding_enabled", true)

	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.intercept_and_chunk_large_writes": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	rec := &casRPCRecorder{}
	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	remoteEnv.SetExperimentFlagProvider(fp)
	proxyEnv.SetExperimentFlagProvider(fp)

	proxyPC, err := pebble_cache.NewPebbleCache(proxyEnv, &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(t),
		MaxSizeBytes:  1_000_000_000,
	})
	require.NoError(t, err)
	require.NoError(t, proxyPC.Start())
	t.Cleanup(func() { proxyPC.Stop() })
	proxyEnv.SetCache(proxyPC)

	remotePC, err := pebble_cache.NewPebbleCache(remoteEnv, &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(t),
		MaxSizeBytes:  1_000_000_000,
	})
	require.NoError(t, err)
	require.NoError(t, remotePC.Start())
	t.Cleanup(func() { remotePC.Stop() })
	remoteEnv.SetCache(remotePC)

	remoteBSS, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(t, err)
	remoteCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(t, err)
	remoteGRPC, remoteRun, remoteLis := testenv.RegisterLocalGRPCServer(t, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPC, remoteBSS)
	repb.RegisterContentAddressableStorageServer(remoteGRPC, remoteCAS)
	go remoteRun()

	remoteConn, err := testenv.LocalGRPCConn(ctx, remoteLis, grpc.WithUnaryInterceptor(recordCASUnaryInterceptor(rec)))
	require.NoError(t, err)
	t.Cleanup(func() { remoteConn.Close() })

	proxyEnv.SetByteStreamClient(bspb.NewByteStreamClient(remoteConn))
	casClient := repb.NewContentAddressableStorageClient(remoteConn)
	proxyEnv.SetContentAddressableStorageClient(casClient)
	proxyBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(t, err)
	proxyEnv.SetLocalByteStreamServer(proxyBSS)
	proxyServer, err := New(proxyEnv)
	require.NoError(t, err)
	proxyGRPC, proxyRun, proxyLis := testenv.RegisterLocalGRPCServer(t, proxyEnv)
	bspb.RegisterByteStreamServer(proxyGRPC, proxyServer)
	go proxyRun()

	proxyConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(t, err)
	t.Cleanup(func() { proxyConn.Close() })

	ctx, err = prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)
	proxy := bspb.NewByteStreamClient(proxyConn)

	_, originalData := testdigest.RandomCASResourceBuf(t, 10*1024*1024)
	blobDigest, err := digest.Compute(bytes.NewReader(originalData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	compressedData := compression.CompressZstd(nil, originalData)
	blobRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	blobRN.SetCompressor(repb.Compressor_ZSTD)

	uploadStream, err := proxy.Write(ctx)
	require.NoError(t, err)
	remaining := compressedData
	written := int64(0)
	for len(remaining) > 0 {
		chunkSize := min(1_000_000, len(remaining))
		require.NoError(t, uploadStream.Send(&bspb.WriteRequest{
			ResourceName: blobRN.NewUploadString(),
			WriteOffset:  written,
			Data:         remaining[:chunkSize],
			FinishWrite:  chunkSize == len(remaining),
		}))
		written += int64(chunkSize)
		remaining = remaining[chunkSize:]
	}
	_, err = uploadStream.CloseAndRecv()
	require.NoError(t, err)

	splitResp, err := casClient.SplitBlob(ctx, &repb.SplitBlobRequest{
		BlobDigest:     blobDigest,
		InstanceName:   "",
		DigestFunction: repb.DigestFunction_BLAKE3,
	})
	require.NoError(t, err)
	require.Greater(t, len(splitResp.GetChunkDigests()), 32)

	downloadRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	downloadStream, err := proxy.Read(ctx, &bspb.ReadRequest{ResourceName: downloadRN.DownloadString()})
	require.NoError(t, err)

	var reconstructedData []byte
	for {
		res, err := downloadStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		reconstructedData = append(reconstructedData, res.Data...)
	}
	require.Equal(t, originalData, reconstructedData)

	rec.mu.Lock()
	defer rec.mu.Unlock()

	totalChunks := len(splitResp.GetChunkDigests())
	expectedFullGroups := totalChunks / 32
	remainder := totalChunks % 32
	expectedGroupCount := expectedFullGroups
	if remainder > 0 {
		expectedGroupCount++
	}
	require.Equal(t, expectedGroupCount, len(rec.fmbGroups))

	var fmbDigestCount int
	for i, group := range rec.fmbGroups {
		if i < expectedFullGroups {
			require.Equal(t, 32, len(group))
		} else {
			require.Equal(t, remainder, len(group))
		}
		fmbDigestCount += len(group)
	}
	require.Equal(t, totalChunks, fmbDigestCount)

	var uploadedDigestCount int
	for i, group := range rec.uploadGroups {
		require.LessOrEqual(t, rec.uploadSizes[i], int64(cachetools.BatchUploadLimitBytes))
		uploadedDigestCount += len(group)
	}
	require.Greater(t, len(rec.uploadGroups), 1)
	require.Equal(t, len(splitResp.GetChunkDigests()), uploadedDigestCount)
}

func TestWriteChunkedFallbackBelowThreshold(t *testing.T) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.intercept_and_chunk_large_writes": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))

	flags.Set(t, "cache.zstd_transcoding_enabled", true)

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)

	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	remoteEnv.SetExperimentFlagProvider(fp)
	proxyEnv.SetExperimentFlagProvider(fp)

	pc, err := pebble_cache.NewPebbleCache(proxyEnv, &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(t),
		MaxSizeBytes:  1_000_000_000,
	})
	require.NoError(t, err)
	err = pc.Start()
	require.NoError(t, err)
	t.Cleanup(func() { pc.Stop() })
	proxyEnv.SetCache(pc)

	remoteBSS, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(t, err)
	remoteCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(t, err)
	remoteGRPC, remoteRun, remoteLis := testenv.RegisterLocalGRPCServer(t, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPC, remoteBSS)
	repb.RegisterContentAddressableStorageServer(remoteGRPC, remoteCAS)
	go remoteRun()
	remoteConn, err := testenv.LocalGRPCConn(ctx, remoteLis)
	require.NoError(t, err)
	t.Cleanup(func() { remoteConn.Close() })
	bsClient := bspb.NewByteStreamClient(remoteConn)
	casClient := repb.NewContentAddressableStorageClient(remoteConn)

	proxyEnv.SetByteStreamClient(bsClient)
	proxyEnv.SetContentAddressableStorageClient(casClient)
	proxyBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(t, err)
	proxyEnv.SetLocalByteStreamServer(proxyBSS)
	proxyServer, err := New(proxyEnv)
	require.NoError(t, err)
	proxyGRPC, proxyRun, proxyLis := testenv.RegisterLocalGRPCServer(t, proxyEnv)
	bspb.RegisterByteStreamServer(proxyGRPC, proxyServer)
	go proxyRun()
	proxyConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(t, err)
	t.Cleanup(func() { proxyConn.Close() })
	proxy := bspb.NewByteStreamClient(proxyConn)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(t, err)

	// Create a blob smaller than the threshold (1MB vs 2MB default max)
	_, originalData := testdigest.RandomCASResourceBuf(t, 1*1024*1024)
	blobDigest, err := digest.Compute(bytes.NewReader(originalData), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)

	blobRN := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
	uploadString := blobRN.NewUploadString()

	writeLabels := prometheus.Labels{
		metrics.StatusLabel:     "OK",
		metrics.CompressionType: "IDENTITY",
	}
	chunkedBlobBytesBefore := testutil.ToFloat64(metrics.ByteStreamChunkedWriteBlobBytes.With(writeLabels))
	chunkedChunkBytesBefore := testutil.ToFloat64(metrics.ByteStreamChunkedWriteChunkBytes.With(writeLabels))

	uploadStream, err := proxy.Write(ctx)
	require.NoError(t, err)
	remaining := originalData
	for len(remaining) > 0 {
		chunkSize := 100_000
		if chunkSize > len(remaining) {
			chunkSize = len(remaining)
		}
		err = uploadStream.Send(&bspb.WriteRequest{
			ResourceName: uploadString,
			WriteOffset:  int64(len(originalData) - len(remaining)),
			Data:         remaining[:chunkSize],
			FinishWrite:  chunkSize == len(remaining),
		})
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		remaining = remaining[chunkSize:]
	}
	_, err = uploadStream.CloseAndRecv()
	require.NoError(t, err)

	chunkedBlobBytesAfter := testutil.ToFloat64(metrics.ByteStreamChunkedWriteBlobBytes.With(writeLabels))
	chunkedChunkBytesAfter := testutil.ToFloat64(metrics.ByteStreamChunkedWriteChunkBytes.With(writeLabels))
	require.Equal(t, chunkedBlobBytesBefore, chunkedBlobBytesAfter)
	require.Equal(t, chunkedChunkBytesBefore, chunkedChunkBytesAfter)
	downloadStream, err := proxy.Read(ctx, &bspb.ReadRequest{ResourceName: blobRN.DownloadString()})
	require.NoError(t, err)

	var downloadedData []byte
	for {
		res, err := downloadStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		downloadedData = append(downloadedData, res.Data...)
	}

	require.Equal(t, originalData, downloadedData)
}

func networkLatencyUnaryInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	time.Sleep(simulatedBenchRTT())
	return invoker(ctx, method, req, reply, cc, opts...)
}

type delayedRecvClientStream struct {
	grpc.ClientStream
	method    string
	recvCount int
}

func (s *delayedRecvClientStream) RecvMsg(m interface{}) error {
	if strings.HasSuffix(s.method, "/Read") {
		if s.recvCount == 0 {
			time.Sleep(simulatedBenchRTT())
		}
		s.recvCount++
	} else if strings.HasSuffix(s.method, "/Write") {
		time.Sleep(*benchUploadDelay)
	}
	return s.ClientStream.RecvMsg(m)
}

func networkSimStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	cs, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return nil, err
	}
	return &delayedRecvClientStream{ClientStream: cs, method: method}, nil
}

var benchDelayCount atomic.Uint64

func simulatedBenchRTT() time.Duration {
	delay := *benchRTT
	if jitter := *benchRTTJitter; jitter > 0 {
		step := int64(benchDelayCount.Add(1)%5) - 2
		delay += time.Duration(step) * jitter / 2
	}
	if delay < 0 {
		return 0
	}
	return delay
}

type chunkedReadBenchmarkEnv struct {
	ctx       context.Context
	proxyEnv  *testenv.TestEnv
	remoteEnv *testenv.TestEnv
	proxy     bspb.ByteStreamClient
	casClient repb.ContentAddressableStorageClient
}

func setupChunkedBenchmarkEnv(b *testing.B) (bspb.ByteStreamClient, context.Context) {
	*log.LogLevel = "error"
	log.Configure()

	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.intercept_and_chunk_large_writes": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(b, openfeature.SetNamedProviderAndWait(b.Name(), testProvider))

	flags.Set(b, "cache.zstd_transcoding_enabled", true)

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(b)
	proxyEnv := testenv.GetTestEnv(b)

	fp, err := experiments.NewFlagProvider(b.Name())
	require.NoError(b, err)
	remoteEnv.SetExperimentFlagProvider(fp)
	proxyEnv.SetExperimentFlagProvider(fp)

	// Use pebble cache for the proxy (required for chunking)
	pc, err := pebble_cache.NewPebbleCache(proxyEnv, &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(b),
		MaxSizeBytes:  100_000_000_000,
	})
	require.NoError(b, err)
	require.NoError(b, pc.Start())
	b.Cleanup(func() { pc.Stop() })
	proxyEnv.SetCache(pc)

	remotePC, err := pebble_cache.NewPebbleCache(remoteEnv, &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(b),
		MaxSizeBytes:  100_000_000_000,
	})
	require.NoError(b, err)
	require.NoError(b, remotePC.Start())
	b.Cleanup(func() { remotePC.Stop() })
	remoteEnv.SetCache(remotePC)

	remoteBSS, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(b, err)
	remoteCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(b, err)
	remoteGRPC, remoteRun, remoteLis := testenv.RegisterLocalGRPCServer(b, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPC, remoteBSS)
	repb.RegisterContentAddressableStorageServer(remoteGRPC, remoteCAS)
	go remoteRun()
	remoteConn, err := testenv.LocalGRPCConn(ctx, remoteLis,
		grpc.WithChainUnaryInterceptor(networkLatencyUnaryInterceptor),
		grpc.WithChainStreamInterceptor(networkSimStreamInterceptor),
	)
	require.NoError(b, err)
	b.Cleanup(func() { remoteConn.Close() })
	bsClient := bspb.NewByteStreamClient(remoteConn)
	casClient := repb.NewContentAddressableStorageClient(remoteConn)

	proxyEnv.SetByteStreamClient(bsClient)
	proxyEnv.SetContentAddressableStorageClient(casClient)
	proxyBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(b, err)
	proxyEnv.SetLocalByteStreamServer(proxyBSS)
	proxyServer, err := New(proxyEnv)
	require.NoError(b, err)
	proxyGRPC, proxyRun, proxyLis := testenv.RegisterLocalGRPCServer(b, proxyEnv)
	bspb.RegisterByteStreamServer(proxyGRPC, proxyServer)
	go proxyRun()
	proxyConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(b, err)
	b.Cleanup(func() { proxyConn.Close() })
	proxy := bspb.NewByteStreamClient(proxyConn)

	ctx, err = prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(b, err)

	return proxy, ctx
}

func setupChunkedReadBenchmarkEnv(b *testing.B) *chunkedReadBenchmarkEnv {
	*log.LogLevel = "error"
	log.Configure()

	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"cache.chunking_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
		"cache_proxy.attempt_chunked_reads": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(b, openfeature.SetNamedProviderAndWait(b.Name(), testProvider))

	flags.Set(b, "cache.zstd_transcoding_enabled", true)

	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(b)
	proxyEnv := testenv.GetTestEnv(b)

	fp, err := experiments.NewFlagProvider(b.Name())
	require.NoError(b, err)
	remoteEnv.SetExperimentFlagProvider(fp)
	proxyEnv.SetExperimentFlagProvider(fp)

	pc, err := pebble_cache.NewPebbleCache(proxyEnv, &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(b),
		MaxSizeBytes:  100_000_000_000,
	})
	require.NoError(b, err)
	require.NoError(b, pc.Start())
	b.Cleanup(func() { pc.Stop() })
	proxyEnv.SetCache(pc)

	remotePC, err := pebble_cache.NewPebbleCache(remoteEnv, &pebble_cache.Options{
		RootDirectory: testfs.MakeTempDir(b),
		MaxSizeBytes:  100_000_000_000,
	})
	require.NoError(b, err)
	require.NoError(b, remotePC.Start())
	b.Cleanup(func() { remotePC.Stop() })
	remoteEnv.SetCache(remotePC)

	remoteBSS, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(b, err)
	remoteCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(b, err)
	remoteGRPC, remoteRun, remoteLis := testenv.RegisterLocalGRPCServer(b, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPC, remoteBSS)
	repb.RegisterContentAddressableStorageServer(remoteGRPC, remoteCAS)
	go remoteRun()
	remoteConn, err := testenv.LocalGRPCConn(ctx, remoteLis,
		grpc.WithChainUnaryInterceptor(networkLatencyUnaryInterceptor),
		grpc.WithChainStreamInterceptor(networkSimStreamInterceptor),
	)
	require.NoError(b, err)
	b.Cleanup(func() { remoteConn.Close() })

	proxyEnv.SetByteStreamClient(bspb.NewByteStreamClient(remoteConn))
	casClient := repb.NewContentAddressableStorageClient(remoteConn)
	proxyEnv.SetContentAddressableStorageClient(casClient)
	proxyBSS, err := byte_stream_server.NewByteStreamServer(proxyEnv)
	require.NoError(b, err)
	proxyEnv.SetLocalByteStreamServer(proxyBSS)
	proxyServer, err := New(proxyEnv)
	require.NoError(b, err)
	proxyGRPC, proxyRun, proxyLis := testenv.RegisterLocalGRPCServer(b, proxyEnv)
	bspb.RegisterByteStreamServer(proxyGRPC, proxyServer)
	go proxyRun()
	proxyConn, err := testenv.LocalGRPCConn(ctx, proxyLis)
	require.NoError(b, err)
	b.Cleanup(func() { proxyConn.Close() })

	ctx, err = prefix.AttachUserPrefixToContext(ctx, proxyEnv.GetAuthenticator())
	require.NoError(b, err)

	return &chunkedReadBenchmarkEnv{
		ctx:       ctx,
		proxyEnv:  proxyEnv,
		remoteEnv: remoteEnv,
		proxy:     bspb.NewByteStreamClient(proxyConn),
		casClient: casClient,
	}
}

type benchmarkChunk struct {
	digest         *repb.Digest
	compressedData []byte
}

func benchmarkLocalChunkSet(chunks []benchmarkChunk, localPercent int) map[digest.Key]struct{} {
	if localPercent <= 0 {
		return map[digest.Key]struct{}{}
	}
	localSet := make(map[digest.Key]struct{}, len(chunks))
	if localPercent >= 100 {
		for _, chunk := range chunks {
			localSet[digest.NewKey(chunk.digest)] = struct{}{}
		}
		return localSet
	}

	indexes := make([]int, len(chunks))
	for i := range chunks {
		indexes[i] = i
	}
	sort.Slice(indexes, func(i, j int) bool {
		return chunks[indexes[i]].digest.GetHash() < chunks[indexes[j]].digest.GetHash()
	})

	localCount := len(chunks) * localPercent / 100
	for _, idx := range indexes[:localCount] {
		localSet[digest.NewKey(chunks[idx].digest)] = struct{}{}
	}
	return localSet
}

func prepareChunkedReadBenchmarkData(b *testing.B, ctx context.Context, size int64) (*repb.Digest, []benchmarkChunk) {
	_, originalData := testdigest.RandomCASResourceBuf(b, size)
	blobDigest, err := digest.Compute(bytes.NewReader(originalData), repb.DigestFunction_BLAKE3)
	require.NoError(b, err)

	chunks := make([]benchmarkChunk, 0)
	writeChunkFn := func(chunkData []byte) error {
		chunkDigest, err := digest.Compute(bytes.NewReader(chunkData), repb.DigestFunction_BLAKE3)
		if err != nil {
			return err
		}
		chunks = append(chunks, benchmarkChunk{
			digest:         chunkDigest,
			compressedData: compression.CompressZstd(nil, chunkData),
		})
		return nil
	}
	cdcChunker, err := chunking.NewChunker(ctx, int(chunking.AvgChunkSizeBytes()), writeChunkFn)
	require.NoError(b, err)
	_, err = cdcChunker.Write(originalData)
	require.NoError(b, err)
	require.NoError(b, cdcChunker.Close())
	return blobDigest, chunks
}

func publishChunkedReadBenchmarkBlob(b *testing.B, env *chunkedReadBenchmarkEnv, instanceName string, blobDigest *repb.Digest, chunks []benchmarkChunk, localSet map[digest.Key]struct{}) string {
	chunkDigests := make([]*repb.Digest, 0, len(chunks))
	for _, chunk := range chunks {
		chunkDigests = append(chunkDigests, chunk.digest)
		chunkRN := digest.NewCASResourceName(chunk.digest, instanceName, repb.DigestFunction_BLAKE3)
		chunkRN.SetCompressor(repb.Compressor_ZSTD)
		require.NoError(b, env.remoteEnv.GetCache().Set(env.ctx, chunkRN.ToProto(), chunk.compressedData))
		if _, ok := localSet[digest.NewKey(chunk.digest)]; ok {
			require.NoError(b, env.proxyEnv.GetCache().Set(env.ctx, chunkRN.ToProto(), chunk.compressedData))
		}
	}
	_, err := env.casClient.SpliceBlob(env.ctx, &repb.SpliceBlobRequest{
		BlobDigest:     blobDigest,
		ChunkDigests:   chunkDigests,
		InstanceName:   instanceName,
		DigestFunction: repb.DigestFunction_BLAKE3,
	})
	require.NoError(b, err)

	downloadRN := digest.NewCASResourceName(blobDigest, instanceName, repb.DigestFunction_BLAKE3)
	downloadRN.SetCompressor(repb.Compressor_ZSTD)
	return downloadRN.DownloadString()
}

func BenchmarkWriteChunkedUnique(b *testing.B) {
	proxy, ctx := setupChunkedBenchmarkEnv(b)

	iter := 0
	for _, size := range []int64{100_000_000, 500_000_000, 1_000_000_000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				iter++
				_, rawData := testdigest.NewRandomResourceAndBuf(b, size, rspb.CacheType_CAS, strconv.Itoa(iter))
				blobDigest, err := digest.Compute(bytes.NewReader(rawData), repb.DigestFunction_BLAKE3)
				require.NoError(b, err)
				compressedData := compression.CompressZstd(nil, rawData)

				rn := digest.NewCASResourceName(blobDigest, "", repb.DigestFunction_BLAKE3)
				rn.SetCompressor(repb.Compressor_ZSTD)
				uploadString := rn.NewUploadString()

				b.StartTimer()

				writeStream, err := proxy.Write(ctx)
				require.NoError(b, err)

				batchSize := 1_000_000
				written := int64(0)
				data := compressedData
				for len(data) > 0 {
					batch := min(len(data), batchSize)
					require.NoError(b, writeStream.Send(&bspb.WriteRequest{
						ResourceName: uploadString,
						WriteOffset:  written,
						Data:         data[:batch],
						FinishWrite:  batch == len(data),
					}))
					written += int64(batch)
					data = data[batch:]
				}
				_, err = writeStream.CloseAndRecv()
				require.NoError(b, err)

				b.StopTimer()
			}
		})
	}
}

func BenchmarkWriteChunkedWithDedup(b *testing.B) {
	proxy, ctx := setupChunkedBenchmarkEnv(b)

	baseSize := int64(10_000_000)
	_, baseData := testdigest.RandomCASResourceBuf(b, baseSize)

	iter := 0
	for _, overlapPercent := range []int{100, 75, 50, 25} {
		b.Run(fmt.Sprintf("overlap=%d%%", overlapPercent), func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				iter++
				// Create data with specified overlap with base
				data := make([]byte, baseSize)
				overlapBytes := int(baseSize) * overlapPercent / 100
				copy(data[:overlapBytes], baseData[:overlapBytes])
				if overlapBytes < int(baseSize) {
					_, randomPart := testdigest.RandomCASResourceBuf(b, baseSize-int64(overlapBytes))
					copy(data[overlapBytes:], randomPart)
				}

				blobDigest, err := digest.Compute(bytes.NewReader(data), repb.DigestFunction_BLAKE3)
				require.NoError(b, err)
				compressedData := compression.CompressZstd(nil, data)

				rn := digest.NewCASResourceName(blobDigest, strconv.Itoa(iter), repb.DigestFunction_BLAKE3)
				rn.SetCompressor(repb.Compressor_ZSTD)
				uploadString := rn.NewUploadString()

				b.StartTimer()

				writeStream, err := proxy.Write(ctx)
				require.NoError(b, err)

				batchSize := 1_000_000
				written := int64(0)
				for len(compressedData) > 0 {
					batch := min(len(compressedData), batchSize)
					require.NoError(b, writeStream.Send(&bspb.WriteRequest{
						ResourceName: uploadString,
						WriteOffset:  written,
						Data:         compressedData[:batch],
						FinishWrite:  batch == len(compressedData),
					}))
					written += int64(batch)
					compressedData = compressedData[batch:]
				}
				_, err = writeStream.CloseAndRecv()
				require.NoError(b, err)

				b.StopTimer()
			}
		})
	}
}

func BenchmarkReadChunkedMixedLocalRemote(b *testing.B) {
	ctx := testContext()

	for _, size := range []int64{10_000_000, 50_000_000, 100_000_000, 500_000_000, 1_000_000_000} {
		blobDigest, chunks := prepareChunkedReadBenchmarkData(b, ctx, size)
		for _, localPercent := range []int{0, 25, 50, 75, 100} {
			localSet := benchmarkLocalChunkSet(chunks, localPercent)

			b.Run(fmt.Sprintf("size=%d/local=%d%%", size, localPercent), func(b *testing.B) {
				env := setupChunkedReadBenchmarkEnv(b)
				b.ReportAllocs()
				b.StopTimer()
				for i := 0; i < b.N; i++ {
					instanceName := fmt.Sprintf("bench/%d/%d/%d", size, localPercent, i)
					downloadRN := publishChunkedReadBenchmarkBlob(b, env, instanceName, blobDigest, chunks, localSet)

					b.StartTimer()

					downloadStream, err := env.proxy.Read(env.ctx, &bspb.ReadRequest{ResourceName: downloadRN})
					require.NoError(b, err)

					downloadedSize := int64(0)
					for {
						res, err := downloadStream.Recv()
						if err == io.EOF {
							break
						}
						require.NoError(b, err)
						downloadedSize += int64(len(res.Data))
					}

					b.StopTimer()

					require.Greater(b, downloadedSize, int64(0))
				}
			})
		}
	}
}

package byte_stream_server_proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/atime_updater"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/byte_stream"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
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

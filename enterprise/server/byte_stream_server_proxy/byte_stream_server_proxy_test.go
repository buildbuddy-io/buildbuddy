package byte_stream_server_proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/atime_updater"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/byte_stream"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
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

// The real AtimeUpdater makes RPCs which may interfere with tests.
type noOpAtimeUpdater struct{}

func (a *noOpAtimeUpdater) Enqueue(_ context.Context, _ string, _ []*repb.Digest, _ repb.DigestFunction_Value) {
}
func (a *noOpAtimeUpdater) EnqueueByResourceName(_ context.Context, _ string) {}
func (a *noOpAtimeUpdater) EnqueueByFindMissingRequest(_ context.Context, _ *repb.FindMissingBlobsRequest) {
}

type noOpCAS struct {
	t *testing.T
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

func runRemoteServices(ctx context.Context, env *testenv.TestEnv, t *testing.T) (bspb.ByteStreamClient, repb.ContentAddressableStorageClient, *atomic.Int32, *atomic.Int32) {
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

func runLocalBSS(ctx context.Context, env *testenv.TestEnv, t *testing.T) bspb.ByteStreamClient {
	server, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	grpcServer, runFunc, lis := testenv.RegisterLocalInternalGRPCServer(t, env)
	bspb.RegisterByteStreamServer(grpcServer, server)
	go runFunc()
	conn, err := testenv.LocalInternalGRPCConn(ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return bspb.NewByteStreamClient(conn)
}

func runBSProxy(ctx context.Context, client bspb.ByteStreamClient, env *testenv.TestEnv, t *testing.T) bspb.ByteStreamClient {
	if env.GetAtimeUpdater() == nil {
		env.SetAtimeUpdater(&noOpAtimeUpdater{})
	}
	env.SetByteStreamClient(client)
	env.SetLocalByteStreamClient(runLocalBSS(ctx, env, t))
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
	s, err := digest.ResourceNameFromProto(rn).DownloadString()
	if err != nil {
		return err
	}
	return status.NotFoundErrorf("Timed out waiting for cache to contain %s", s)
}

func testContext() context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs(authutil.ClientIdentityHeaderName, "fakeheader"))
}

func TestRead(t *testing.T) {
	ctx := testContext()
	remoteEnv := testenv.GetTestEnv(t)
	proxyEnv := testenv.GetTestEnv(t)
	bs, _, _, requestCounter := runRemoteServices(ctx, remoteEnv, t)
	proxy := runBSProxy(ctx, bs, proxyEnv, t)

	cases := []struct {
		wantError error
		cacheType rspb.CacheType
		size      int64
		offset    int64
		remotes   remoteReadExpectation
	}{
		{ // Simple Read
			wantError: nil,
			cacheType: rspb.CacheType_CAS,
			size:      1234,
			offset:    0,
			remotes:   once,
		},
		{ // Large Read
			wantError: nil,
			cacheType: rspb.CacheType_CAS,
			size:      1000 * 1000 * 100,
			offset:    0,
			remotes:   once,
		},
		{ // 0 length read
			wantError: nil,
			cacheType: rspb.CacheType_CAS,
			size:      0,
			offset:    0,
			remotes:   never,
		},
		{ // Offset
			wantError: nil,
			cacheType: rspb.CacheType_CAS,
			size:      1234,
			offset:    1,
			remotes:   always,
		},
		{ // Max offset
			wantError: nil,
			cacheType: rspb.CacheType_CAS,
			size:      1234,
			offset:    1234,
			remotes:   always,
		},
	}

	ctx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv)
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}

	for _, tc := range cases {
		rn, data := testdigest.NewRandomResourceAndBuf(t, tc.size, tc.cacheType, "")
		// Set the value in the remote cache.
		err := remoteEnv.GetCache().Set(ctx, rn, data)
		require.NoError(t, err)

		// Read it through the proxied bytestream API several times, ensuring
		// it's only read from the remote byestream server the first time.
		for i := 1; i < 4; i++ {
			var buf bytes.Buffer
			gotErr := byte_stream.ReadBlob(ctx, proxy, digest.ResourceNameFromProto(rn), &buf, tc.offset)
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

			// offset and 0-sized reads are not cached in the proxy.
			if tc.offset == 0 && tc.size > 0 {
				require.NoError(t, waitContains(ctx, proxyEnv, rn))
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
	ctx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv)
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

			ctx, err := prefix.AttachUserPrefixToContext(ctx, proxyEnv)
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

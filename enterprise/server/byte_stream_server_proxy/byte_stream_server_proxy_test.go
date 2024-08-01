package byte_stream_server_proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/byte_stream"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcompression"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

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

func requestCountingInterceptor(count *atomic.Int32) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		count.Add(1)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func runRemoteBSS(ctx context.Context, env *testenv.TestEnv, t *testing.T) (bspb.ByteStreamClient, *atomic.Int32) {
	server, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	grpcServer, runFunc := testenv.RegisterLocalGRPCServer(t, env)
	bspb.RegisterByteStreamServer(grpcServer, server)
	go runFunc()
	streamRequestCounter := atomic.Int32{}
	conn, err := testenv.LocalGRPCConn(ctx, env,
		grpc.WithStreamInterceptor(requestCountingInterceptor(&streamRequestCounter)))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return bspb.NewByteStreamClient(conn), &streamRequestCounter
}

func runLocalBSS(ctx context.Context, env *testenv.TestEnv, t *testing.T) bspb.ByteStreamClient {
	server, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	grpcServer, runFunc := testenv.RegisterLocalInternalGRPCServer(t, env)
	bspb.RegisterByteStreamServer(grpcServer, server)
	go runFunc()
	conn, err := testenv.LocalInternalGRPCConn(ctx, env)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return bspb.NewByteStreamClient(conn)
}

func runBSProxy(ctx context.Context, client bspb.ByteStreamClient, env *testenv.TestEnv, t *testing.T) bspb.ByteStreamClient {
	env.SetByteStreamClient(client)
	env.SetLocalByteStreamClient(runLocalBSS(ctx, env, t))
	byteStreamServer, err := New(env)
	require.NoError(t, err)
	grpcServer, runFunc := testenv.RegisterLocalGRPCServer(t, env)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()
	conn, err := testenv.LocalGRPCConn(ctx, env)
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

func TestRead(t *testing.T) {
	ctx := context.Background()
	remoteEnv := testenv.GetTestEnv(t)
	localEnv := testenv.GetTestEnv(t)
	bs, requestCounter := runRemoteBSS(ctx, remoteEnv, t)
	proxy := runBSProxy(ctx, bs, localEnv, t)

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

	ctx, err := prefix.AttachUserPrefixToContext(ctx, localEnv)
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
				require.NoError(t, waitContains(ctx, localEnv, rn))
			}
		}
		requestCounter.Store(0)
	}
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
	}
	for _, tc := range testCases {
		run := func(t *testing.T) {
			remoteEnv := testenv.GetTestEnv(t)
			localEnv := testenv.GetTestEnv(t)
			ctx := byte_stream.WithBazelVersion(t, context.Background(), tc.bazelVersion)

			// Enable compression
			flags.Set(t, "cache.zstd_transcoding_enabled", true)
			localEnv.SetCache(&testcompression.CompressionCache{Cache: localEnv.GetCache()})

			ctx, err := prefix.AttachUserPrefixToContext(ctx, localEnv)
			require.NoError(t, err)
			bs, requestCounter := runRemoteBSS(ctx, remoteEnv, t)
			proxy := runBSProxy(ctx, bs, localEnv, t)

			// Upload the blob
			byte_stream.MustUploadChunked(t, ctx, proxy, tc.bazelVersion, tc.uploadResourceName, tc.uploadBlob, true)
			require.NoError(t, waitContains(ctx, localEnv, rn))

			// Read back the blob we just uploaded
			downloadBuf := []byte{}
			downloadStream, err := proxy.Read(ctx, &bspb.ReadRequest{
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

			// There should've been 1 request from the proxy to the backing cache.
			require.Equal(t, int32(1), requestCounter.Load())

			// Now try uploading a duplicate. The duplicate upload should not fail,
			// and we should still be able to read the blob.
			byte_stream.MustUploadChunked(t, ctx, proxy, tc.bazelVersion, tc.uploadResourceName, tc.uploadBlob, false)

			downloadBuf = []byte{}
			downloadStream, err = proxy.Read(ctx, &bspb.ReadRequest{
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

			// There should've been 1 more request from the proxy to the backing cache.
			require.Equal(t, int32(2), requestCounter.Load())

		}

		// Run all tests for both bazel 5.0.0 (which introduced compression) and
		// 5.1.0 (which added support for short-circuiting duplicate compressed
		// uploads)
		tc.bazelVersion = "5.0.0"
		t.Run(tc.name+", bazel "+tc.bazelVersion, run)

		tc.bazelVersion = "5.1.0"
		t.Run(tc.name+", bazel "+tc.bazelVersion, run)
	}
}

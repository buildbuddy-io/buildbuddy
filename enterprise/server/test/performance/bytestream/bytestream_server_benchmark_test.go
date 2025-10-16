// Benchmarks for byte_stream_server.go. They live in enterprise because they
// depend on the distributed cache, which is enterprise-only.
package bytestream_server_benchmark_test

import (
	"context"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

// Mostly copied from bytestream_server_test.go
func runByteStreamServer(ctx context.Context, t testing.TB, env *testenv.TestEnv) *grpc.ClientConn {
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	clientConn, err := testenv.LocalGRPCConn(ctx, lis, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4*1024*1024)))
	require.NoError(t, err)
	return clientConn
}

// slowCache wraps a Cache, but makes each write and read operation sleep for
// 1us per 100 bytes (or 10ms per 1MB).
type slowCache struct {
	interfaces.Cache
}

func (c *slowCache) Writer(ctx context.Context, r *rspb.ResourceName) (interfaces.CommittedWriteCloser, error) {
	w, err := c.Cache.Writer(ctx, r)
	if err != nil {
		return nil, err
	}
	if rf, ok := w.(io.ReaderFrom); ok {
		return &slowWriterReaderFrom{
			slowWriter: slowWriter{CommittedWriteCloser: w},
			ReaderFrom: rf,
		}, nil
	}
	return &slowWriter{
		CommittedWriteCloser: w,
	}, nil
}

func (c *slowCache) Reader(ctx context.Context, rn *rspb.ResourceName, uncompressedOffset, limit int64) (io.ReadCloser, error) {
	r, err := c.Cache.Reader(ctx, rn, uncompressedOffset, limit)
	if err != nil {
		return nil, err
	}
	if writerTo, ok := r.(io.WriterTo); ok {
		return &slowReaderWriterTo{
			slowReader: slowReader{r},
			WriterTo:   writerTo,
		}, nil
	}
	return &slowReader{r}, nil
}

type slowWriter struct {
	interfaces.CommittedWriteCloser
}

func (w *slowWriter) Write(p []byte) (int, error) {
	proportionalSleep(len(p))
	return w.CommittedWriteCloser.Write(p)
}

type slowWriterReaderFrom struct {
	slowWriter
	io.ReaderFrom
}

func (w *slowWriterReaderFrom) ReadFrom(r io.Reader) (int64, error) {
	n, err := w.ReaderFrom.ReadFrom(r)
	proportionalSleep(int(n))
	return n, err
}

type slowReader struct {
	io.ReadCloser
}

func (r *slowReader) Read(buf []byte) (int, error) {
	n, err := r.ReadCloser.Read(buf)
	proportionalSleep(n)
	return n, err
}

type slowReaderWriterTo struct {
	slowReader
	io.WriterTo
}

func (r *slowReaderWriterTo) WriteTo(w io.Writer) (int64, error) {
	n, err := r.WriterTo.WriteTo(w)
	proportionalSleep(int(n))
	return n, err
}

func proportionalSleep(n int) {
	busyLoop(time.Duration(n/100) * time.Microsecond)
}

// time.Sleep can't reliably sleep for less than 1ms, so use a busy loop
// instead.
func busyLoop(dur time.Duration) {
	start := time.Now()
	for time.Since(start) < dur {
	}
}

func getDistributedCache(t testing.TB, te environment.Env, c interfaces.Cache, lookasideCacheSizeBytes int64) interfaces.Cache {
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(t))
	conf := distributed.CacheConfig{
		ListenAddr:                   listenAddr,
		GroupName:                    "default",
		ReplicationFactor:            1,
		Nodes:                        []string{listenAddr},
		DisableLocalLookup:           true,
		LookasideCacheSizeBytes:      lookasideCacheSizeBytes,
		EnableLocalCompressionLookup: true,
	}
	dc, err := distributed.NewDistributedCache(te, c, conf, te.GetHealthChecker())
	require.NoError(t, err)
	dc.StartListening()
	t.Cleanup(func() { dc.Shutdown(context.Background()) })
	return dc
}

func getPebbleCache(t testing.TB, te environment.Env) interfaces.Cache {
	testRootDir := testfs.MakeTempDir(t)
	pc, err := pebble_cache.NewPebbleCache(te, &pebble_cache.Options{
		Name:          testRootDir,
		RootDirectory: testRootDir,
		MaxSizeBytes:  10_000_000_000,
	})
	require.NoError(t, err)
	pc.Start()
	t.Cleanup(func() { pc.Stop() })
	return pc
}

func BenchmarkWrite(b *testing.B) {
	*log.LogLevel = "error"
	log.Configure()

	// Bazel uses 16KiB chunks. cachetools.UploadFromReader uses 128KiB chunks,
	// and this shows that's the sweetspot.
	chunkSizes := []int{16 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1_000_000}

	for _, test := range []struct {
		cacheType string
		cacheFunc func(b *testing.B, env environment.Env) interfaces.Cache
	}{
		{
			"fast",
			func(b *testing.B, env environment.Env) interfaces.Cache {
				return getDistributedCache(b, env, getPebbleCache(b, env), 0)
			},
		},
		{
			"slow",
			func(b *testing.B, env environment.Env) interfaces.Cache {
				return getDistributedCache(b, env, &slowCache{getPebbleCache(b, env)}, 0)
			},
		},
	} {
		b.Run(fmt.Sprintf("cache_type=%v", test.cacheType), func(b *testing.B) {
			env := testenv.GetTestEnv(b)
			ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env.GetAuthenticator())
			require.NoError(b, err)
			cache := test.cacheFunc(b, env)
			env.SetCache(cache)
			conn := runByteStreamServer(ctx, b, env)
			client := bspb.NewByteStreamClient(conn)
			for _, objectSize := range []int64{1000, 100_000, 5_000_001, 15_000_001} {
				b.Run(fmt.Sprintf("object_size=%d", objectSize), func(b *testing.B) {
					// We only need one chunk size >= object size. All chunk
					// sizes bigger than that will perform the same since they
					// will all send the whole object in one chunk.
					lastChunkSizeIndex := 0
					for i, chunkSize := range chunkSizes {
						lastChunkSizeIndex = i
						if chunkSize >= int(objectSize) {
							break
						}
					}
					for _, chunkSize := range chunkSizes[:lastChunkSizeIndex+1] {
						b.Run(fmt.Sprintf("chunk_size=%d", chunkSize), func(b *testing.B) {
							for _, compressor := range []repb.Compressor_Value{repb.Compressor_IDENTITY, repb.Compressor_ZSTD} {
								b.Run(fmt.Sprintf("compressor=%s", compressor), func(b *testing.B) {
									benchmarkWrite(b, ctx, objectSize, chunkSize, compressor, cache, client)
								})
							}
						})
					}
				})
			}
		})
	}
}

func benchmarkWrite(b *testing.B, ctx context.Context, objectSize int64, chunkSize int, compressor repb.Compressor_Value, cache interfaces.Cache, client bspb.ByteStreamClient) {
	b.ReportAllocs()
	b.SetBytes(objectSize)
	i := 0
	for b.Loop() {
		i++
		b.StopTimer()
		r, buf := testdigest.RandomCASResourceBuf(b, objectSize)
		r.Compressor = compressor
		rn, err := digest.CASResourceNameFromProto(r)
		require.NoError(b, err)
		uploadString := rn.NewUploadString()
		compressedBuf := buf
		if compressor == repb.Compressor_ZSTD {
			compressedBuf = compression.CompressZstd(nil, buf)
		}
		// Don't include dial latency in the time.
		stream, err := client.Write(ctx)
		require.NoError(b, err)
		b.StartTimer()

		offset := 0
		for chunk := range slices.Chunk(compressedBuf, chunkSize) {
			require.NoError(b, stream.Send(&bspb.WriteRequest{
				ResourceName: uploadString,
				Data:         chunk,
				WriteOffset:  int64(offset),
			}))
			fmt.Println(uploadString)
			uploadString = ""
			offset += len(chunk)
		}
		require.NoError(b, stream.Send(&bspb.WriteRequest{
			ResourceName: uploadString,
			FinishWrite:  true,
			WriteOffset:  int64(offset),
		}))
		resp, err := stream.CloseAndRecv()
		require.NoError(b, err)
		require.Equal(b, int64(len(compressedBuf)), resp.GetCommittedSize())

		b.StopTimer()
		if i == 1 {
			// Only test data validity once
			actual, err := cache.Get(ctx, r)
			require.NoError(b, err)
			if compressor == repb.Compressor_ZSTD {
				actual, err = compression.DecompressZstd(nil, actual)
				require.NoError(b, err)
			}
			require.Equal(b, buf, actual, "Resource %q: data mismatch. offset =%v", r, offset)
		}
		// Delete to guarantee that later writes won't be short-circuited.
		require.NoError(b, cache.Delete(ctx, r))
		if i == 1 {
			// Only verify Delete once
			cont, err := cache.Contains(ctx, r)
			require.NoError(b, err)
			require.False(b, cont)
		}
		b.StartTimer()
	}
}

func BenchmarkRead(b *testing.B) {
	*log.LogLevel = "error"
	log.Configure()

	for _, test := range []struct {
		cacheType string
		cacheFunc func(b *testing.B, env environment.Env) interfaces.Cache
	}{
		{
			"fast",
			func(b *testing.B, env environment.Env) interfaces.Cache {
				return getDistributedCache(b, env, getPebbleCache(b, env), 0)
			},
		},
		{
			"slow",
			func(b *testing.B, env environment.Env) interfaces.Cache {
				return getDistributedCache(b, env, &slowCache{getPebbleCache(b, env)}, 0)
			},
		},
		{
			"non-distributed", // benchmark reads which land on the correct app
			func(b *testing.B, env environment.Env) interfaces.Cache {
				return getPebbleCache(b, env)
			},
		},
	} {
		b.Run(fmt.Sprintf("cache_type=%v", test.cacheType), func(b *testing.B) {
			env := testenv.GetTestEnv(b)
			ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env.GetAuthenticator())
			require.NoError(b, err)
			cache := test.cacheFunc(b, env)
			env.SetCache(cache)
			conn := runByteStreamServer(ctx, b, env)
			client := bspb.NewByteStreamClient(conn)
			for _, objectSize := range []int64{1000, 100_000, 5_000_001, 15_000_001} {
				b.Run(fmt.Sprintf("object_size=%d", objectSize), func(b *testing.B) {
					for _, compressor := range []repb.Compressor_Value{repb.Compressor_IDENTITY, repb.Compressor_ZSTD} {
						b.Run(fmt.Sprintf("compressor=%s", compressor), func(b *testing.B) {
							benchmarkRead(b, ctx, objectSize, compressor, cache, client)
						})
					}
				})
			}
		})
	}
}

func benchmarkRead(b *testing.B, ctx context.Context, objectSize int64, compressor repb.Compressor_Value, cache interfaces.Cache, client bspb.ByteStreamClient) {
	r, buf := testdigest.RandomCASResourceBuf(b, objectSize)
	r.Compressor = compressor
	rn, err := digest.CASResourceNameFromProto(r)
	require.NoError(b, err)
	compressedBuf := buf
	if compressor == repb.Compressor_ZSTD {
		compressedBuf = compression.CompressZstd(nil, buf)
	}
	writeClient, err := client.Write(ctx)
	require.NoError(b, err)
	err = writeClient.Send(&bspb.WriteRequest{Data: compressedBuf, ResourceName: rn.NewUploadString(), FinishWrite: true})
	require.NoError(b, err)
	resp, err := writeClient.CloseAndRecv()
	require.NoError(b, err)
	require.Less(b, int64(0), resp.GetCommittedSize())

	b.ReportAllocs()
	b.SetBytes(objectSize)
	i := 0
	for b.Loop() {
		i++
		stream, err := client.Read(ctx, &bspb.ReadRequest{ResourceName: rn.DownloadString()})
		require.NoError(b, err)
		var readBytes []byte
		if i == 1 {
			readBytes = make([]byte, 0, len(compressedBuf))
		}
		for {
			reply, err := stream.Recv()
			if i == 1 {
				// Only test data validity once
				b.StopTimer()
				readBytes = append(readBytes, reply.GetData()...)
				b.StartTimer()
			}
			if err == io.EOF {
				break
			}
			require.NoError(b, err)
		}
		if i == 1 {
			b.StopTimer()
			require.Equal(b, compressedBuf, readBytes)
			b.StartTimer()
		}
	}
}

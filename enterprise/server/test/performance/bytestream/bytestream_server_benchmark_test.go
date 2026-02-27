// Benchmarks for byte_stream_server.go. They live in enterprise because they
// depend on the distributed cache, which is enterprise-only.
package bytestream_server_benchmark_test

import (
	"context"
	"flag"
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
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/config"
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

var (
	benchAll = flag.Bool("all", false, "Run all benchmarks, or just a sample if this is false. Defaults to false to reduce CI time.")
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
	conf := distributed.Options{
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

// alwaysMissCache is a wrapper around a Cache that always reports misses. This
// makes it so that writes are never short-circuited in the bytestream server or
// in the distributed cache.
type alwaysMissCache struct {
	interfaces.Cache
}

func (c *alwaysMissCache) Contains(ctx context.Context, r *rspb.ResourceName) (bool, error) {
	return false, nil
}

func (c *alwaysMissCache) FindMissing(ctx context.Context, resources []*rspb.ResourceName) ([]*repb.Digest, error) {
	missing := make([]*repb.Digest, 0, len(resources))
	for _, r := range resources {
		missing = append(missing, r.GetDigest())
	}
	return missing, nil
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
	return &alwaysMissCache{pc}
}

func BenchmarkWrite(b *testing.B) {
	*log.LogLevel = "error"
	log.Configure()

	// Bazel uses 16KiB chunks. cachetools.UploadFromReader uses 256KB chunks,
	// and this shows that's the sweetspot.
	chunkSizes := chunkSizes()
	for _, parallel := range []bool{false, true} {
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
				"non-distributed", // benchmark writes which land on the correct app
				func(b *testing.B, env environment.Env) interfaces.Cache {
					return getPebbleCache(b, env)
				},
			},
		} {
			for _, compressor := range []repb.Compressor_Value{repb.Compressor_IDENTITY, repb.Compressor_ZSTD} {
				if !*benchAll && (!parallel || compressor == repb.Compressor_IDENTITY || test.cacheType == "slow") {
					continue
				}
				b.Run(fmt.Sprintf("parallel=%v/cache_type=%v/compressor=%v", parallel, test.cacheType, compressor), func(b *testing.B) {
					env := testenv.GetTestEnv(b)
					ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env.GetAuthenticator())
					require.NoError(b, err)
					cache := test.cacheFunc(b, env)
					env.SetCache(cache)
					conn := runByteStreamServer(ctx, b, env)
					client := bspb.NewByteStreamClient(conn)
					for _, objectSize := range []int64{1000, 100_000, 5_000_001, 15_000_001} {
						// We only need one chunk size >= object size. All chunk
						// sizes bigger than that will perform the same since they
						// will all send the whole object in one chunk.
						lastChunkSizeIndex := 0
						firstChunkSizeIndex := 0
						for i, chunkSize := range chunkSizes {
							if objectSize >= 15_000_001 && chunkSize <= 128*1000 {
								// Don't benchmark small chunk sizes for 15MB objects.
								// They are already tested with 5MB objects.
								firstChunkSizeIndex = i
							}
							lastChunkSizeIndex = i
							if chunkSize >= int(objectSize) {
								break
							}
						}
						for _, chunkSize := range chunkSizes[firstChunkSizeIndex : lastChunkSizeIndex+1] {
							b.Run(fmt.Sprintf("object_size=%d/chunk_size=%d", objectSize, chunkSize), func(b *testing.B) {
								benchmarkWrite(b, ctx, parallel, objectSize, chunkSize, compressor, cache, client)
							})
						}
					}
				})
			}
		}
	}
}

func benchmarkWrite(b *testing.B, ctx context.Context, parallel bool, objectSize int64, chunkSize int, compressor repb.Compressor_Value, cache interfaces.Cache, client bspb.ByteStreamClient) {
	// Write once to verify correctness.
	buf, compressedBuf, rn := createBlob(b, objectSize, compressor)
	write(ctx, b, client, compressedBuf, chunkSize, rn.NewUploadString())
	actual, err := cache.Get(ctx, rn.ToProto())
	require.NoError(b, err)
	if compressor == repb.Compressor_ZSTD {
		actual, err = compression.DecompressZstd(nil, actual)
		require.NoError(b, err)
	}
	require.Equal(b, buf, actual, "Resource %q: data mismatch", rn)

	b.ReportAllocs()
	b.SetBytes(objectSize)
	if !parallel {
		for b.Loop() {
			write(ctx, b, client, compressedBuf, chunkSize, rn.NewUploadString())
		}
	} else {
		b.ResetTimer()
		b.SetParallelism(10) // 10 times more parallelism than GOMAXPROCS.
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				write(ctx, b, client, compressedBuf, chunkSize, rn.NewUploadString())
			}
		})
	}
}

func createBlob(b *testing.B, objectSize int64, compressor repb.Compressor_Value) (buf, compressedBuf []byte, rn *digest.CASResourceName) {
	var r *rspb.ResourceName
	r, buf = testdigest.RandomCASResourceBuf(b, objectSize)
	r.Compressor = compressor
	rn, err := digest.CASResourceNameFromProto(r)
	require.NoError(b, err)
	compressedBuf = buf
	if compressor == repb.Compressor_ZSTD {
		compressedBuf = compression.CompressZstd(nil, buf)
	}
	return buf, compressedBuf, rn
}

func write(ctx context.Context, b *testing.B, client bspb.ByteStreamClient, compressedBuf []byte, chunkSize int, uploadString string) {
	stream, err := client.Write(ctx)
	require.NoError(b, err)
	offset := 0
	for chunk := range slices.Chunk(compressedBuf, chunkSize) {
		require.NoError(b, stream.Send(&bspb.WriteRequest{
			ResourceName: uploadString,
			Data:         chunk,
			WriteOffset:  int64(offset),
		}))
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
	require.Equal(b, len(compressedBuf), offset)
}

func BenchmarkRead(b *testing.B) {
	*log.LogLevel = "error"
	log.Configure()

	bufSizes := chunkSizes()
	for _, parallel := range []bool{false, true} {
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
			for _, compressor := range []repb.Compressor_Value{repb.Compressor_IDENTITY, repb.Compressor_ZSTD} {
				if !*benchAll && (!parallel || compressor == repb.Compressor_IDENTITY || test.cacheType == "slow") {
					continue
				}
				b.Run(fmt.Sprintf("parallel=%v/cache_type=%v/compressor=%v", parallel, test.cacheType, compressor), func(b *testing.B) {
					env := testenv.GetTestEnv(b)
					ctx, err := prefix.AttachUserPrefixToContext(context.Background(), env.GetAuthenticator())
					require.NoError(b, err)
					cache := test.cacheFunc(b, env)
					env.SetCache(cache)
					conn := runByteStreamServer(ctx, b, env)
					client := bspb.NewByteStreamClient(conn)
					for _, objectSize := range []int64{1000, 100_000, 5_000_001, 15_000_001} {
						lastBufSizeIndex := 0
						firstBufSizeIndex := 0
						for i, chunkSize := range bufSizes {
							if objectSize >= 15_000_001 && chunkSize <= 128*1000 {
								// Don't benchmark small chunk sizes for 15MB objects.
								// They are already tested with 5MB objects.
								firstBufSizeIndex = i
							}
							lastBufSizeIndex = i
							if chunkSize >= int(objectSize) {
								break
							}
						}
						for _, bufSize := range bufSizes[firstBufSizeIndex : lastBufSizeIndex+1] {
							*config.ReadBufSizeBytes = bufSize
							b.Run(fmt.Sprintf("object_size=%v/buf_size=%v", objectSize, bufSize), func(b *testing.B) {
								benchmarkRead(b, ctx, parallel, objectSize, compressor, cache, client)
							})
						}
					}
				})
			}
		}
	}
}

func benchmarkRead(b *testing.B, ctx context.Context, parallel bool, objectSize int64, compressor repb.Compressor_Value, cache interfaces.Cache, client bspb.ByteStreamClient) {
	_, compressedBuf, rn := createBlob(b, objectSize, compressor)
	write(ctx, b, client, compressedBuf, 1*1024*1024, rn.NewUploadString())

	// Check that we can read the data back correctly before starting the benchmark.
	stream, err := client.Read(ctx, &bspb.ReadRequest{ResourceName: rn.DownloadString()})
	require.NoError(b, err)
	readBytes := make([]byte, 0, len(compressedBuf))
	for {
		reply, err := stream.Recv()
		readBytes = append(readBytes, reply.GetData()...)
		if err == io.EOF {
			break
		}
		require.NoError(b, err)
	}
	require.Equal(b, compressedBuf, readBytes)

	b.ReportAllocs()
	b.SetBytes(objectSize)
	downloadString := rn.DownloadString()
	if parallel {
		b.ResetTimer()
		b.SetParallelism(10) // 10 times more parallelism than GOMAXPROCS.
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				read(ctx, b, client, downloadString, compressedBuf)
			}
		})
	} else {
		for b.Loop() {
			read(ctx, b, client, downloadString, compressedBuf)
		}
	}
}

func read(ctx context.Context, b *testing.B, client bspb.ByteStreamClient, resourceName string, compressedBuf []byte) {
	stream, err := client.Read(ctx, &bspb.ReadRequest{ResourceName: resourceName})
	require.NoError(b, err)
	readBytes := 0
	for {
		reply, err := stream.Recv()
		readBytes += len(reply.GetData())
		if err == io.EOF {
			break
		}
		require.NoError(b, err)
	}
	require.Equal(b, len(compressedBuf), readBytes)
}

func chunkSizes() []int {
	if *benchAll {
		return []int{
			16 * 1000,
			16 * 1024,
			32 * 1000,
			32 * 1024,
			64 * 1000,
			64 * 1024,
			128 * 1000,
			128 * 1024,
			256 * 1000,
			256 * 1024,
			512 * 1000,
			512 * 1024,
			1000 * 1000,
			1024 * 1024,
			4 * 1000 * 1000,
		}
	}
	return []int{
		16 * 1000,
		16 * 1024,
		32 * 1000,
		128 * 1000,
		256 * 1000,
		256 * 1024,
		512 * 1000,
		1000 * 1000,
	}
}

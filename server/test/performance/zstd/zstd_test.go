package zstd_test

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	// "github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"

	datadog "github.com/DataDog/zstd"
	klauspost "github.com/klauspost/compress/zstd"
	valyala "github.com/valyala/gozstd"
)

var (
	checkCorrectness = flag.Bool("check_correctness", false, "")
	blobPath         = flag.String("blob_path", "/home/b/bb-cache-datasets/buildbuddy/c49028706190c8c28f97f2b81757736b58834f416aaa7ea2ea9259a284a60c28", "path to blob to compress")
	// blobPath = flag.String("blob_path", "/home/b/bb-cache-datasets/buildbuddy/20839d0640f256bcd552bed9a1a4a2c9079d486d8351fd30c2e196ef09d364ab", "path to blob to compress")
	runParallel = flag.Bool("run_parallel", false, "whether to RunParallel")

	klauspostDecompressor *klauspost.Decoder

	valyalaCompressorPool = NewValyalaWriterPool()
)

const (
	readBufSizeBytes = (1024 * 1024 * 4) - (1024 * 256)
)

func init() {
	d, err := klauspost.NewReader(nil)
	if err != nil {
		panic(err)
	}
	klauspostDecompressor = d
}

func TestMain(m *testing.M) {
	flag.Parse()
	s, err := os.Stat(*blobPath)
	if err != nil {
		panic(err)
	}
	fmt.Println("blob_path: " + *blobPath)
	fmt.Printf("blob_size: %.2f KB\n", float64(s.Size())/1e3)
	m.Run()
}

func BenchmarkDataDogStreamingCompression(t *testing.B) {
	benchmarkCompressionWithReader(t, func(w io.Writer, r io.Reader, size int) {
		c := datadog.NewWriter(w)
		// c := NewDataDogZstdCompressor(r)
		_, err := io.Copy(c, r)
		require.NoError(t, err)
		err = c.Close()
		require.NoError(t, err)
	})
}

func BenchmarkDataDogChunkedStreamingCompression(t *testing.B) {
	benchmarkCompressionWithReader(t, func(w io.Writer, r io.Reader, size int) {
		bufLen := size
		if bufLen > readBufSizeBytes {
			bufLen = readBufSizeBytes
		}
		buf := make([]byte, bufLen)
		cbuf := make([]byte, bufLen)

		for {
			n, err := r.Read(buf)
			if n > 0 {
				var err error
				cbuf, err = datadog.Compress(cbuf[:0], buf[:n])
				require.NoError(t, err)
				_, err = w.Write(cbuf)
				require.NoError(t, err)
			}
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
	})
}

func BenchmarkDataDogSingleCompression(t *testing.B) {
	benchmarkCompressionWithBytes(t, func(p []byte) []byte {
		b, err := datadog.Compress(nil, p)
		require.NoError(t, err)
		return b
	})
}

func BenchmarkKlauspostStreamingCompression(t *testing.B) {
	benchmarkCompressionWithReader(t, func(w io.Writer, r io.Reader, size int) {
		c, err := klauspost.NewWriter(w)
		require.NoError(t, err)
		_, err = c.ReadFrom(r)
		require.NoError(t, err)
		err = c.Close()
		require.NoError(t, err)
	})
}

func BenchmarkKlauspostChunkedStreamingCompression(t *testing.B) {
	enc, err := klauspost.NewWriter(nil)
	require.NoError(t, err)

	benchmarkCompressionWithReader(t, func(w io.Writer, r io.Reader, size int) {
		bufLen := size
		if bufLen > readBufSizeBytes {
			bufLen = readBufSizeBytes
		}
		buf := make([]byte, bufLen)
		cbuf := make([]byte, bufLen)

		for {
			n, err := r.Read(buf)
			if n > 0 {
				cbuf = enc.EncodeAll(buf[:n], cbuf[:0])
				_, err := w.Write(cbuf)
				require.NoError(t, err)
			}
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
	})
}

func BenchmarkKlauspostSingleCompression(t *testing.B) {
	enc, err := klauspost.NewWriter(nil)
	require.NoError(t, err)

	benchmarkCompressionWithBytes(t, func(b []byte) []byte {
		// c, err := klauspost_zstd.NewWriter(nil)
		// require.NoError(t, err)
		c := enc.EncodeAll(b, nil)
		enc.Reset(nil)
		return c
	})
}

func BenchmarkValyalaStreamingCompression(t *testing.B) {
	benchmarkCompressionWithReader(t, func(w io.Writer, r io.Reader, size int) {
		// Non-pooled
		// c := valyala.NewWriter(w)

		// Pooled
		c := valyalaCompressorPool.Get()
		c.Reset(w, nil, valyala.DefaultCompressionLevel)

		// _, err := c.ReadFrom(r)
		_, err := io.Copy(c, r)
		require.NoError(t, err)
		err = c.Close()
		require.NoError(t, err)
	})
}

func BenchmarkValyalaChunkedStreamingCompression(t *testing.B) {
	benchmarkCompressionWithReader(t, func(w io.Writer, r io.Reader, size int) {
		bufLen := size
		if bufLen > readBufSizeBytes {
			bufLen = readBufSizeBytes
		}
		buf := make([]byte, bufLen)
		cbuf := make([]byte, bufLen)

		for {
			n, err := r.Read(buf)
			if n > 0 {
				cbuf = valyala.Compress(cbuf[:0], buf[:n])
				_, err := w.Write(cbuf)
				require.NoError(t, err)
			}
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
	})
}

func BenchmarkValyalaSingleCompression(t *testing.B) {
	benchmarkCompressionWithBytes(t, func(b []byte) []byte {
		return valyala.Compress(nil, b)
	})
}

// DECOMPRESSION BELOW

func BenchmarkValyalaStreamingDecompression(t *testing.B) {
	benchmarkDecompressionWithReader(t, func(w io.Writer, r io.Reader) {
		d := valyala.NewReader(r)
		_, err := d.WriteTo(w)
		require.NoError(t, err)
	})
}

func BenchmarkValyalaSingleDecompression(t *testing.B) {
	benchmarkDecompressionWithBytes(t, func(dst []byte, src []byte) []byte {
		out, err := valyala.Decompress(dst, src)
		require.NoError(t, err)
		return out
	})
}

func BenchmarkDataDogStreamingDecompression(t *testing.B) {
	benchmarkDecompressionWithReader(t, func(w io.Writer, r io.Reader) {
		d := datadog.NewReader(r)
		_, err := io.Copy(w, d)
		require.NoError(t, err)
	})
}

func BenchmarkDataDogSingleDecompression(t *testing.B) {
	benchmarkDecompressionWithBytes(t, func(dst []byte, src []byte) []byte {
		out, err := datadog.Decompress(dst, src)
		require.NoError(t, err)
		return out
	})
}

func BenchmarkKlauspostStreamingDecompression(t *testing.B) {
	benchmarkDecompressionWithReader(t, func(w io.Writer, r io.Reader) {
		d, err := klauspost.NewReader(r)
		require.NoError(t, err)
		_, err = d.WriteTo(w)
		require.NoError(t, err)
	})
}

func BenchmarkKlauspostSingleDecompression(t *testing.B) {
	benchmarkDecompressionWithBytes(t, func(dst []byte, src []byte) []byte {
		out, err := klauspostDecompressor.DecodeAll(src, dst)
		require.NoError(t, err)
		return out
	})
}

func benchmarkDecompressionWithReader(t *testing.B, testFunc func(io.Writer, io.Reader)) {
	benchmarkDecompressionWithBytes(t, func(dst []byte, src []byte) []byte {
		buf := bytes.NewBuffer(dst)
		r := bytes.NewReader(src)
		testFunc(buf, r)
		return buf.Bytes()
	})
}

func benchmarkDecompressionWithBytes(t *testing.B, testFunc func(dst []byte, src []byte) []byte) {
	b := mustReadAll(t, *blobPath)
	t.SetBytes(int64(len(b)))
	src := valyala.Compress(nil, b)
	dst := make([]byte, 0, len(b))

	run := func() {
		out := testFunc(dst, src)
		if !*runParallel {
			ratio := float64(len(src)) / float64(len(b))
			t.ReportMetric(ratio, "comp")
		}
		if *checkCorrectness {
			require.Equal(t, b, out)
		}
	}

	t.ResetTimer()
	if *runParallel {
		t.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				run()
			}
		})
	} else {
		for i := 0; i < t.N; i++ {
			run()
		}
	}
}

func benchmarkCompressionWithReader(t *testing.B, testFunc func(w io.Writer, r io.Reader, size int)) {
	benchmarkCompressionWithBytes(t, func(p []byte) []byte {
		buf := &bytes.Buffer{}
		testFunc(buf, bytes.NewReader(p), len(p))
		return buf.Bytes()
	})
}

func benchmarkCompressionWithBytes(t *testing.B, testFunc func([]byte) []byte) {
	t.SetBytes(fileSize(t, *blobPath))
	b := mustReadAll(t, *blobPath)

	run := func() {
		c := testFunc(b)
		if !*runParallel {
			ratio := float64(len(c)) / float64(len(b))
			t.ReportMetric(ratio, "comp")
		}
		if *checkCorrectness {
			dbuf, err := datadog.Decompress(nil, c)

			// var dbuf []byte
			// var err error
			// dbuf, err = valyala.Decompress(dbuf[:0], c)
			require.NoError(t, err)
			require.Equal(t, b, dbuf)
		}
	}

	t.ResetTimer()
	if *runParallel {
		t.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				run()
			}
		})
	} else {
		for i := 0; i < t.N; i++ {
			run()
		}
	}
}

func fileSize(t testing.TB, path string) int64 {
	s, err := os.Stat(path)
	require.NoError(t, err)
	return s.Size()
}

func mustReadAll(t testing.TB, path string) []byte {
	f, err := os.Open(*blobPath)
	require.NoError(t, err)
	b, err := io.ReadAll(f)
	require.NoError(t, err)
	return b
}

type VZstdWriterPool struct {
	pool sync.Pool
}

func NewValyalaWriterPool() *VZstdWriterPool {
	return &VZstdWriterPool{
		pool: sync.Pool{
			New: func() interface{} {
				return valyala.NewWriter(nil)
			},
		},
	}
}

func (p *VZstdWriterPool) Get() *valyala.Writer {
	zw := p.pool.Get().(*valyala.Writer)
	return zw
}

func (p *VZstdWriterPool) Put(w *valyala.Writer) {
	p.pool.Put(w)
}

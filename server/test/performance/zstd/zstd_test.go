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
	t.Cleanup(func() {
		err := enc.Close()
		require.NoError(t, err)
	})
	benchmarkCompressionWithBytes(t, func(b []byte, ctx interface{}) ([]byte, interface{}) {
		c := enc.EncodeAll(b, nil)
		return c, nil
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
	benchmarkCompressionWithBytes(t, func(b []byte, _ interface{}) ([]byte, interface{}) {
		return valyala.Compress(nil, b), nil
	})
}

// DECOMPRESSION BELOW

func BenchmarkValyalaStreamingDecompression(t *testing.B) {
	benchmarkDecompressionWithReader(t, func(w io.Writer, r io.Reader) {
		d := valyala.NewReader(r)
		defer d.Release()
		_, err := d.WriteTo(w)
		require.NoError(t, err)
	})
}

func BenchmarkValyalaSingleDecompression(t *testing.B) {
	benchmarkDecompressionWithBytes(t, func(dst []byte, src []byte, _ interface{}) ([]byte, interface{}) {
		out, err := valyala.Decompress(dst, src)
		require.NoError(t, err)
		return out, nil
	})
}

func BenchmarkKlauspostStreamingDecompression(t *testing.B) {
	pool := NewDecoderPool()
	benchmarkDecompressionWithReader(t, func(w io.Writer, r io.Reader) {
		d, err := pool.Get()
		require.NoError(t, err)

		err = d.Reset(r)
		require.NoError(t, err)

		_, err = d.WriteTo(w)
		require.NoError(t, err)

		d.Reset(nil)
		pool.Put(d)
	})
}

func BenchmarkKlauspostSingleDecompression(t *testing.B) {
	benchmarkDecompressionWithBytes(t, func(dst []byte, src []byte, _ interface{}) ([]byte, interface{}) {
		out, err := klauspostDecompressor.DecodeAll(src, dst)
		require.NoError(t, err)
		return out, nil
	})
}

func benchmarkDecompressionWithReader(t *testing.B, testFunc func(io.Writer, io.Reader)) {
	benchmarkDecompressionWithBytes(t, func(dst []byte, src []byte, _ interface{}) ([]byte, interface{}) {
		buf := bytes.NewBuffer(dst)
		r := bytes.NewReader(src)
		testFunc(buf, r)
		return buf.Bytes(), nil
	})
}

func benchmarkDecompressionWithBytes(t *testing.B, testFunc func([]byte, []byte, interface{}) ([]byte, interface{})) {
	b := mustReadAll(t, *blobPath)
	t.SetBytes(int64(len(b)))
	src := valyala.Compress(nil, b)
	dst := make([]byte, 0, len(b))
	// Uncomment to use compressed bytes (i.e. incoming bytes) for reported throughput
	// t.SetBytes(int64(len(src)))

	run := func(data interface{}) interface{} {
		out, data := testFunc(dst, src, data)
		if !*runParallel {
			ratio := float64(len(src)) / float64(len(b))
			t.ReportMetric(ratio, "comp")
		}
		if *checkCorrectness {
			require.Equal(t, b, out)
		}
		return data
	}

	t.ResetTimer()
	if *runParallel {
		t.RunParallel(func(pb *testing.PB) {
			var data interface{}
			for pb.Next() {
				data = run(data)
			}
		})
	} else {
		var data interface{}
		for i := 0; i < t.N; i++ {
			data = run(data)
		}
	}
}

func benchmarkCompressionWithReader(t *testing.B, testFunc func(w io.Writer, r io.Reader, size int)) {
	benchmarkCompressionWithBytes(t, func(p []byte, data interface{}) ([]byte, interface{}) {
		buf := &bytes.Buffer{}
		testFunc(buf, bytes.NewReader(p), len(p))
		return buf.Bytes(), nil
	})
}

func benchmarkCompressionWithBytes(t *testing.B, testFunc func([]byte, interface{}) ([]byte, interface{})) {
	t.SetBytes(fileSize(t, *blobPath))
	b := mustReadAll(t, *blobPath)

	run := func(data interface{}) interface{} {
		c, data := testFunc(b, data)
		if !*runParallel {
			ratio := float64(len(c)) / float64(len(b))
			t.ReportMetric(ratio, "comp")
		}
		if *checkCorrectness {
			dbuf, err := valyala.Decompress(nil, c)

			// var dbuf []byte
			// var err error
			// dbuf, err = valyala.Decompress(dbuf[:0], c)
			require.NoError(t, err)
			require.Equal(t, b, dbuf)
		}
		return data
	}

	t.ResetTimer()
	if *runParallel {
		t.RunParallel(func(pb *testing.PB) {
			var data interface{}
			for pb.Next() {
				data = run(data)
			}
		})
	} else {
		var data interface{}
		for i := 0; i < t.N; i++ {
			data = run(data)
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

type DecoderPool struct {
	pool sync.Pool
}

func NewDecoderPool() *DecoderPool {
	return &DecoderPool{
		pool: sync.Pool{
			New: func() interface{} {
				dc, err := klauspost.NewReader(nil)
				if err != nil {
					return err
				}
				return dc
			},
		},
	}
}

func (p *DecoderPool) Get() (*klauspost.Decoder, error) {
	val := p.pool.Get()
	if err, ok := val.(error); ok {
		return nil, err
	}
	return val.(*klauspost.Decoder), nil
}

func (p *DecoderPool) Put(d *klauspost.Decoder) {
	d.Reset(nil)
	p.pool.Put(d)
}

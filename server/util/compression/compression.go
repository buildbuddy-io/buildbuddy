package compression

import (
	"bytes"
	"compress/flate"
	"io"
	"runtime"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// zstdEncoder can be shared across goroutines to compress chunks of data
	// using EncodeAll. Streaming functions such as encoder.ReadFrom or io.Copy
	// *must not* be used. The encoder *must not* be closed.
	zstdEncoder = mustGetZstdEncoder()

	// zstdDecoderPool can be used across goroutines to retrieve ZSTD decoders,
	// either for streaming decompression using ReadFrom or batch decompression
	// using DecodeAll. The returned decoders *must not* be closed.
	zstdDecoderPool = NewZstdDecoderPool()
)

func mustGetZstdEncoder() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		panic(err)
	}
	return enc
}

// CompressZstd compresses a chunk of data into dst using zstd compression at
// the default level. If dst is not big enough, then a new buffer will be
// allocated.
func CompressZstd(dst []byte, src []byte) []byte {
	metrics.BytesCompressed.With(prometheus.Labels{metrics.CompressionType: "zstd"}).Add(float64(len(src)))
	return zstdEncoder.EncodeAll(src, dst[:0])
}

// DecompressZstd decompresses a full chunk of zstd data into dst. If dst is
// not big enough then a new buffer will be allocated.
func DecompressZstd(dst []byte, src []byte) ([]byte, error) {
	dec, err := zstdDecoderPool.Get(nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := zstdDecoderPool.Put(dec); err != nil {
			log.Errorf("Failed to return zstd decoder to pool: %s", err)
		}
	}()
	buf, err := dec.DecodeAll(src, dst[:0])
	metrics.BytesDecompressed.With(prometheus.Labels{metrics.CompressionType: "zstd"}).Add(float64(len(buf)))
	return buf, err
}

type zstdDecompressor struct {
	pw   *io.PipeWriter
	done chan error
}

// NewZstdDecompressor returns a WriteCloser that accepts zstd-compressed bytes,
// and streams the decompressed bytes to the given writer.
//
// Note that writes are not matched one-to-one, since the compression scheme may
// require more than one chunk of compressed data in order to write a single
// chunk of decompressed data.
func NewZstdDecompressor(writer io.Writer) (io.WriteCloser, error) {
	pr, pw := io.Pipe()
	decoder, err := zstdDecoderPool.Get(pr)
	if err != nil {
		return nil, err
	}
	d := &zstdDecompressor{
		pw:   pw,
		done: make(chan error, 1),
	}
	go func() {
		defer func() {
			if err := zstdDecoderPool.Put(decoder); err != nil {
				log.Errorf("Failed to return zstd decoder to pool: %s", err.Error())
			}
		}()
		defer pr.Close()
		n, err := decoder.WriteTo(writer)
		metrics.BytesDecompressed.With(prometheus.Labels{metrics.CompressionType: "zstd"}).Add(float64(n))
		d.done <- err
		close(d.done)
	}()
	return d, nil
}

func (d *zstdDecompressor) Write(p []byte) (int, error) {
	return d.pw.Write(p)
}

func (d *zstdDecompressor) Close() error {
	var lastErr error
	if err := d.pw.Close(); err != nil {
		lastErr = err
	}

	// NOTE: We don't close the decompressor here since it cannot be reused once
	// closed. The decompressor will be closed when finalized.

	// Wait for the remaining bytes to be decompressed by the goroutine. Note that
	// since we just closed the write-end of the pipe, the decoder will see an
	// EOF from the read-end and the goroutine should exit.
	err, ok := <-d.done
	if ok {
		lastErr = err
	}
	return lastErr
}

type wrappedReader struct {
	pr          *io.PipeReader
	inputReader io.ReadCloser
}

func (r *wrappedReader) Read(p []byte) (int, error) {
	return r.pr.Read(p)
}

func (r *wrappedReader) Close() error {
	defer r.inputReader.Close()
	return r.pr.Close()
}

// NewZstdCompressingReader returns a reader that reads chunks from the given
// reader into the read buffer, and makes the zstd-compressed chunks available
// on the output reader. Each chunk read into the read buffer is immediately
// compressed, independently of other chunks, and piped to the output reader.
// The default compression level is used.
//
// The read buffer must have a non-zero length, and should have a relatively
// large length in order to get a good compression ratio, since chunks are
// compressed independently. If the length of the byte stream provided by the
// given reader is known, and is relatively small, then it is recommended to
// provide a read buffer that can exactly fit the full contents of the stream.
//
// The compression buffer is optional and is used as a staging buffer for
// compressed contents before sending to the output reader. It is recommended to
// set this to a buffer that has a capacity equal to the read buffer. If any
// compressed chunk's size is greater than the uncompressed chunk, then a new
// compression buffer is allocated internally. This scenario should be rare if
// the data is even modestly compressible and the compression buffer capacity is
// at least a few hundred bytes.
func NewZstdCompressingReader(reader io.ReadCloser, readBuf []byte, compressBuf []byte) (io.ReadCloser, error) {
	pr, pw := io.Pipe()
	go func() {
		for {
			n, err := reader.Read(readBuf)
			if n > 0 {
				compressBuf = CompressZstd(compressBuf[:0], readBuf[:n])
				if _, err := pw.Write(compressBuf); err != nil {
					pw.CloseWithError(err)
					return
				}
			}
			if err != nil {
				pw.CloseWithError(err)
				return
			}
		}
	}()
	return &wrappedReader{
		pr:          pr,
		inputReader: reader,
	}, nil
}

// NewZstdDecompressingReader reads zstd-compressed data from the input
// reader and makes the decompressed data available on the output reader
func NewZstdDecompressingReader(reader io.ReadCloser) (io.ReadCloser, error) {
	// Stream data from reader to decoder
	decoder, err := zstdDecoderPool.Get(reader)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() {
		// Write decoded bytes to pw and pipe to pr
		n, err := decoder.WriteTo(pw)
		pw.CloseWithError(err)
		metrics.BytesDecompressed.With(prometheus.Labels{metrics.CompressionType: "zstd"}).Add(float64(n))

		if err := zstdDecoderPool.Put(decoder); err != nil {
			log.Errorf("Failed to return zstd decoder to pool: %s", err.Error())
		}
	}()

	return &wrappedReader{
		pr:          pr,
		inputReader: reader,
	}, nil
}

// DecoderRef wraps a *zstd.Decoder. Since it does not directly start any
// goroutines, it can be garbage collected before the wrapped decoder can.
// When garbage collected, a finalizer automatically closes the wrapped decoder,
// thus allowing the decoder to be garbage collected as well.
type DecoderRef struct{ *zstd.Decoder }

// ZstdDecoderPool allows reusing zstd decoders to avoid excessive allocations.
type ZstdDecoderPool struct {
	pool sync.Pool
}

func NewZstdDecoderPool() *ZstdDecoderPool {
	return &ZstdDecoderPool{
		pool: sync.Pool{
			New: func() interface{} {
				dc, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
				if err != nil {
					return err
				}
				ref := &DecoderRef{dc}
				runtime.SetFinalizer(ref, func(ref *DecoderRef) {
					ref.Decoder.Close()
				})
				return ref
			},
		},
	}
}

// Get returns a decoder from the pool. The returned decoder must be returned
// back to the pool with Put. The returned decoders *must not* be closed.
//
// If the returned decoder will only be used to decode chunks via DecodeAll, a
// nil reader can be passed.
func (p *ZstdDecoderPool) Get(reader io.Reader) (*DecoderRef, error) {
	val := p.pool.Get()
	if err, ok := val.(error); ok {
		return nil, err
	}
	// No need to check this type assertion since Put can only accept decoders.
	decoder := val.(*DecoderRef)
	if err := decoder.Reset(reader); err != nil {
		return nil, err
	}
	return decoder, nil
}

func (p *ZstdDecoderPool) Put(ref *DecoderRef) error {
	// Release reference to enclosed reader before adding back to the pool.
	if err := ref.Reset(nil); err != nil {
		return err
	}
	p.pool.Put(ref)
	return nil
}

func CompressFlate(data []byte, level int) ([]byte, error) {
	var b bytes.Buffer
	w, err := flate.NewWriter(&b, level)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func DecompressFlate(data []byte) ([]byte, error) {
	dataReader := bytes.NewReader(data)
	rc := flate.NewReader(dataReader)

	buf, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

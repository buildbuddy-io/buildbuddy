package compression

import (
	"bufio"
	"errors"
	"io"
	"runtime"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/bazel-buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
)

const compressChunkSize = 4 * 1024 * 1024 // 4MB

var (
	// zstdEncoder can be shared across goroutines to compress chunks of data
	// using EncodeAll. Streaming functions such as encoder.ReadFrom or io.Copy
	// *must not* be used. The encoder *must not* be closed.
	zstdEncoder = mustGetZstdEncoder()

	// zstdDecoderPool can be used across goroutines to retrieve ZSTD decoders,
	// either for streaming decompression using ReadFrom or batch decompression
	// using DecodeAll. The returned decoders *must not* be closed.
	zstdDecoderPool = NewZstdDecoderPool()

	compressBufPool = bytebufferpool.FixedSize(compressChunkSize)
	bufWriterPool   = sync.Pool{
		New: func() any {
			return bufio.NewWriterSize(io.Discard, compressChunkSize)
		},
	}
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

type compressingReader struct {
	inputReader io.ReadCloser
	readBuf     []byte
	compressBuf []byte
	leftover    []byte
	readErr     error
}

func (r *compressingReader) Read(p []byte) (int, error) {
	var n int
	if len(r.leftover) == 0 && r.readErr == nil {
		n, r.readErr = r.inputReader.Read(r.readBuf)
		if n > 0 {
			r.compressBuf = CompressZstd(r.compressBuf[:0], r.readBuf[:n])
			r.leftover = r.compressBuf
		}
	}
	n = copy(p, r.leftover)
	// Save the rest for the next read
	r.leftover = r.leftover[n:]
	if len(r.leftover) > 0 {
		// Don't return errors until we've drained the leftover buffer.
		return n, nil
	}
	return n, r.readErr
}

func (r *compressingReader) Close() error {
	return r.inputReader.Close()
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
	if len(readBuf) == 0 {
		return nil, io.ErrShortBuffer
	}
	return &compressingReader{
		inputReader: reader,
		readBuf:     readBuf,
		compressBuf: compressBuf,
	}, nil
}

type compressingWriter struct {
	w               io.Writer
	compressBuf     []byte
	poolCompressBuf []byte
	closed          bool
}

func (c *compressingWriter) Write(p []byte) (int, error) {
	if c.closed {
		return 0, status.FailedPreconditionError("compressingWriter already closed, cannot receive writes")
	}
	totalWritten := 0
	for len(p) > 0 {
		chunkSize := min(len(p), cap(c.compressBuf))
		chunk := p[:chunkSize]
		c.compressBuf = CompressZstd(c.compressBuf[:0], chunk)

		written, err := c.w.Write(c.compressBuf)
		if err != nil {
			return totalWritten, err
		}
		if written < len(c.compressBuf) {
			return totalWritten, io.ErrShortWrite
		}

		totalWritten += chunkSize
		p = p[chunkSize:]
	}
	return totalWritten, nil
}

// Close puts the compression buffer back into the pool.
// If the underlying Writer is also a Closer, it closes it.
func (c *compressingWriter) Close() error {
	if c.closed {
		return status.FailedPreconditionError("compressingWriter already closed, cannot close again")
	}
	c.closed = true
	compressBufPool.Put(c.poolCompressBuf)
	if closer, ok := c.w.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// NewZstdCompressingWriter returns a writer that compresses each chunk of the
// input using zstd and writes the compressed data to the underlying writer.
// The writer uses a fixed-size 4MB buffer for compression.
func NewZstdCompressingWriter(w io.Writer) interfaces.CommittedWriteCloser {
	compressBuf := compressBufPool.Get()
	compressor := &compressingWriter{
		w:               w,
		compressBuf:     compressBuf,
		poolCompressBuf: compressBuf,
	}
	bw := bufWriterPool.Get().(*bufio.Writer)
	bw.Reset(compressor)
	cwc := ioutil.NewCustomCommitWriteCloser(bw)
	cwc.CommitFn = func(_ int64) error {
		return bw.Flush()
	}
	cwc.CloseFn = func() error {
		if compressor.closed {
			return status.FailedPreconditionError("Writer already closed, cannot close again")
		}
		err := compressor.Close()
		bufWriterPool.Put(bw)
		return err
	}
	return cwc
}

// NewZstdDecompressingReader reads zstd-compressed data from the input
// reader and makes the decompressed data available on the output reader. The
// output reader is also an io.WriterTo, which can often prevent allocations
// when used with io.Copy to write into a bytes.Buffer. If you wrap the output
// reader, you probably want to maintain that property.
func NewZstdDecompressingReader(reader io.ReadCloser) (io.ReadCloser, error) {
	// Stream data from reader to decoder
	decoder, err := zstdDecoderPool.Get(reader)
	if err != nil {
		return nil, err
	}
	return &decoderReader{
		decoder:     decoder,
		inputReader: reader,
	}, nil
}

type decoderReader struct {
	decoder     *DecoderRef
	inputReader io.ReadCloser
	read        int
	closed      bool
}

func (r *decoderReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, errors.New("decoderReader.Read used after Close")
	}
	n, err := r.decoder.Read(p)
	r.read += n
	return n, err
}

func (r *decoderReader) WriteTo(w io.Writer) (int64, error) {
	if r.closed {
		return 0, errors.New("decoderReader.WriteTo used after Close")
	}
	n, err := r.decoder.WriteTo(w)
	r.read += int(n)
	return n, err
}

func (r *decoderReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	metrics.BytesDecompressed.With(prometheus.Labels{metrics.CompressionType: "zstd"}).Add(float64(r.read))
	if err := zstdDecoderPool.Put(r.decoder); err != nil {
		log.Errorf("Failed to return zstd decoder to pool: %s", err.Error())
	}
	return r.inputReader.Close()
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
				runtime.AddCleanup(ref, (*zstd.Decoder).Close, dc)
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

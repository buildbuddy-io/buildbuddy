package compression

import (
	"errors"
	"io"
	"runtime"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool"
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

	bufPool = bytebufferpool.VariableSize(4e6) // 4MB

	// These are used a bunch and the labels are constant so just do it once.
	zstdCompressedBytesMetric   = metrics.BytesCompressed.With(prometheus.Labels{metrics.CompressionType: "zstd"})
	zstdDecompressedBytesMetric = metrics.BytesDecompressed.With(prometheus.Labels{metrics.CompressionType: "zstd"})
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
	zstdCompressedBytesMetric.Add(float64(len(src)))
	return zstdEncoder.EncodeAll(src, dst[:0])
}

// ZstdCompressingWriter is an io.WriteCloser that accepts uncompressed bytes
// and writes zstd-compressed bytes to the underlying writer.
type ZstdCompressingWriter struct {
	// The number of compressed bytes written to the underlying writer, so far.
	CompressedBytesWritten int

	writer         io.Writer
	buffer         []byte
	buffered       int
	compressBuffer []byte
	returnBuffers  func()
	err            error
	closed         bool
}

// Flush compresses any buffered data and writes it to the underlying writer. It
// is generally not required since Close will flush any remaining data, but
// Flush can be used to flush data before Close.
func (c *ZstdCompressingWriter) Flush() error {
	if c.closed {
		return errors.New("ZstdCompressingWriter.Flush used after Close")
	}
	if c.err != nil {
		return c.err
	}
	if c.buffered == 0 {
		return nil
	}
	err := c.writeCompressed(c.buffer[:c.buffered])
	c.buffered = 0
	return err
}

func (c *ZstdCompressingWriter) writeCompressed(p []byte) error {
	c.compressBuffer = CompressZstd(c.compressBuffer, p)
	n, err := c.writer.Write(c.compressBuffer)
	c.CompressedBytesWritten += n
	if err == nil && n < len(c.compressBuffer) {
		err = io.ErrShortWrite
	}
	c.err = err
	return c.err
}

func (c *ZstdCompressingWriter) Write(p []byte) (int, error) {
	if c.closed {
		return 0, errors.New("ZstdCompressingWriter.Write used after Close")
	}
	if c.err != nil {
		return 0, c.err
	}
	total := 0
	for len(p) > 0 {
		if c.buffered == 0 && len(p) >= len(c.buffer) {
			// If the input is larger than the buffer, and the buffer is empty,
			// then just compress and write a buffer-sized chunk directly. Don't
			// write the whole input at once since that might force us to
			// allocate a much larger compression buffer.
			n := len(c.buffer)
			if err := c.writeCompressed(p[:n]); err != nil {
				return total, err
			}
			p = p[n:]
			total += n
			continue
		}
		n := copy(c.buffer[c.buffered:], p)
		p = p[n:]
		c.buffered += n
		if c.buffered == len(c.buffer) {
			if err := c.Flush(); err != nil {
				return total, err
			}
		}
		total += n
	}
	return total, nil
}

func (c *ZstdCompressingWriter) ReadFrom(r io.Reader) (int64, error) {
	if c.closed {
		return 0, errors.New("ZstdCompressingWriter.ReadFrom used after Close")
	}
	if c.err != nil {
		return 0, c.err
	}
	var total int64
	for {
		n, err := r.Read(c.buffer[c.buffered:])
		if n > 0 {
			c.buffered += n
			total += int64(n)
			if c.buffered == len(c.buffer) {
				if err := c.Flush(); err != nil {
					return total, err
				}
			}
		}
		if err == io.EOF {
			return total, c.Flush()
		}
		if err != nil {
			c.err = err
			return total, err
		}
	}
}

// Close must be called when done writing to ensure that all bytes are flushed
// and resources are released. It can be called multiple times. No other methods
// may be called after Close.
func (c *ZstdCompressingWriter) Close() error {
	if c.closed {
		return nil
	}
	err := c.Flush()
	c.returnBuffers()
	c.closed = true
	return err
}

// NewZstdCompressingWriter returns a new ZstdCompressingWriter. bufSize must be
// > 0 and determines how much data is buffered before being compressed and
// written out. Larger buffer sizes generally result in better compression
// ratios, but will be capped to an implementation-defined maximum.
func NewZstdCompressingWriter(writer io.Writer, bufSize int64) (*ZstdCompressingWriter, error) {
	if bufSize <= 0 {
		return nil, errors.New("bufSize must be > 0")
	}
	a, b := bufPool.Get(bufSize), bufPool.Get(bufSize)
	return &ZstdCompressingWriter{
		writer:         writer,
		buffer:         a,
		compressBuffer: b,
		returnBuffers: func() {
			bufPool.Put(a)
			bufPool.Put(b)
		},
	}, nil
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
	zstdDecompressedBytesMetric.Add(float64(len(buf)))
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
		zstdDecompressedBytesMetric.Add(float64(n))
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

// newZstdCompressingReader returns a reader that reads chunks from the given
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
func newZstdCompressingReader(reader io.ReadCloser, readBuf []byte, compressBuf []byte) (io.ReadCloser, error) {
	if len(readBuf) == 0 {
		return nil, io.ErrShortBuffer
	}
	return &compressingReader{
		inputReader: reader,
		readBuf:     readBuf,
		compressBuf: compressBuf,
	}, nil
}

// bufPoolCompressingReader helps manage resources associated with a compression.NewZstdCompressingReader
type bufPoolCompressingReader struct {
	io.ReadCloser
	readBuf     []byte
	compressBuf []byte
	bufferPool  *bytebufferpool.VariableSizePool
}

func (r *bufPoolCompressingReader) Close() error {
	err := r.ReadCloser.Close()
	r.bufferPool.Put(r.readBuf)
	r.bufferPool.Put(r.compressBuf)
	return err
}

func NewBufferedZstdCompressingReader(reader io.ReadCloser, bufferPool *bytebufferpool.VariableSizePool, bufSize int64) (io.ReadCloser, error) {
	readBuf := bufferPool.Get(bufSize)
	compressBuf := bufferPool.Get(bufSize)
	cr, err := newZstdCompressingReader(reader, readBuf, compressBuf)
	if err != nil {
		bufferPool.Put(readBuf)
		bufferPool.Put(compressBuf)
		return nil, err
	}
	return &bufPoolCompressingReader{
		ReadCloser:  cr,
		readBuf:     readBuf,
		compressBuf: compressBuf,
		bufferPool:  bufferPool,
	}, nil
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
	zstdDecompressedBytesMetric.Add(float64(r.read))
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

package ctxio

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/span"
)

// Interfaces to allow for unwrapping

type unwrapReader interface {
	unwrapReader() io.Reader
}
type unwrapCtxReader interface {
	unwrapReader() Reader
}

type unwrapCloser interface {
	unwrapCloser() io.Closer
}
type unwrapCtxCloser interface {
	unwrapCloser() Closer
}

type unwrapSeeker interface {
	unwrapSeeker() io.Seeker
}
type unwrapCtxSeeker interface {
	unwrapSeeker() Seeker
}

type unwrapWriter interface {
	unwrapWriter() io.Writer
}
type unwrapCtxWriter interface {
	unwrapWriter() Writer
}

type unwrapReadCloser interface {
	unwrapReadCloser() io.ReadCloser
}
type unwrapCtxReadCloser interface {
	unwrapReadCloser() ReadCloser
}

type unwrapReadSeeker interface {
	unwrapReadSeeker() io.ReadSeeker
}
type unwrapCtxReadSeeker interface {
	unwrapReadSeeker() ReadSeeker
}

type unwrapReadWriter interface {
	unwrapReadWriter() io.ReadWriter
}
type unwrapCtxReadWriter interface {
	unwrapReadWriter() ReadWriter
}

type unwrapReadSeekCloser interface {
	unwrapReadSeekCloser() io.ReadSeekCloser
}
type unwrapCtxReadSeekCloser interface {
	unwrapReadSeekCloser() ReadSeekCloser
}

type unwrapReadWriteCloser interface {
	unwrapReadWriteCloser() io.ReadWriteCloser
}
type unwrapCtxReadWriteCloser interface {
	unwrapReadWriteCloser() ReadWriteCloser
}

type unwrapReadWriteSeeker interface {
	unwrapReadWriteSeeker() io.ReadWriteSeeker
}
type unwrapCtxReadWriteSeeker interface {
	unwrapReadWriteSeeker() ReadWriteSeeker
}

type unwrapWriteCloser interface {
	unwrapWriteCloser() io.WriteCloser
}
type unwrapCtxWriteCloser interface {
	unwrapWriteCloser() WriteCloser
}

type unwrapWriteSeeker interface {
	unwrapWriteSeeker() io.WriteSeeker
}
type unwrapCtxWriteSeeker interface {
	unwrapWriteSeeker() WriteSeeker
}

type unwrapByteReader interface {
	unwrapByteReader() io.ByteReader
}
type unwrapCtxByteReader interface {
	unwrapByteReader() ByteReader
}

type unwrapByteScanner interface {
	unwrapByteScanner() io.ByteScanner
}
type unwrapCtxByteScanner interface {
	unwrapByteScanner() ByteScanner
}

type unwrapByteWriter interface {
	unwrapByteWriter() io.ByteWriter
}
type unwrapCtxByteWriter interface {
	unwrapByteWriter() ByteWriter
}

type unwrapRuneReader interface {
	unwrapRuneReader() io.RuneReader
}
type unwrapCtxRuneReader interface {
	unwrapRuneReader() RuneReader
}

type unwrapRuneScanner interface {
	unwrapRuneScanner() io.RuneScanner
}
type unwrapCtxRuneScanner interface {
	unwrapRuneScanner() RuneScanner
}

type unwrapStringWriter interface {
	unwrapStringWriter() io.StringWriter
}
type unwrapCtxStringWriter interface {
	unwrapStringWriter() StringWriter
}

type unwrapReaderAt interface {
	unwrapReaderAt() io.ReaderAt
}
type unwrapCtxReaderAt interface {
	unwrapReaderAt() ReaderAt
}

type unwrapWriterAt interface {
	unwrapWriterAt() io.WriterAt
}
type unwrapCtxWriterAt interface {
	unwrapWriterAt() WriterAt
}

// Wrappers to allow interoperability of Readers between io and ctxio

type Reader interface {
	Read(ctx context.Context, b []byte) (n int, err error)
}

func ReaderWrapper(ctx context.Context, r Reader) io.Reader {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapReader:
		if innerReader := r.unwrapReader(); innerReader != nil {
			return innerReader
		}
	}
	return &readerWrapper{r, ctx}
}

type readerWrapper struct {
	Reader
	ctx context.Context
}

func (r *readerWrapper) Read(b []byte) (n int, err error) {
	return r.Reader.Read(r.ctx, b)
}
func (r *readerWrapper) unwrapReader() Reader {
	return r.Reader
}

func CtxReaderWrapper(r io.Reader) Reader {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReader:
		if innerReader := r.unwrapReader(); innerReader != nil {
			return innerReader
		}
	}
	return &ctxReaderWrapper{r}
}

type ctxReaderWrapper struct {
	io.Reader
}

func (r *ctxReaderWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.Reader.Read(b)
}
func (r *ctxReaderWrapper) unwrapReader() io.Reader {
	return r.Reader
}

func NoTraceCtxReaderWrapper(r io.Reader) Reader {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReader:
		if innerReader := r.unwrapReader(); innerReader != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerReader.(unwrapReader); ok {
				if wrapped := wrapper.unwrapReader(); wrapped != nil {
					return &noTraceCtxReaderWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxReaderWrapper{r}
}

type noTraceCtxReaderWrapper struct {
	io.Reader
}

func (r *noTraceCtxReaderWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	return r.Reader.Read(b)
}
func (r *noTraceCtxReaderWrapper) unwrapReader() io.Reader {
	return r.Reader
}

// Wrappers to allow interoperability of Writers between io and ctxio

type Writer interface {
	Write(ctx context.Context, b []byte) (n int, err error)
}

func WriterWrapper(ctx context.Context, w Writer) io.Writer {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapWriter:
		if innerWriter := w.unwrapWriter(); innerWriter != nil {
			if wrapper, ok := innerWriter.(unwrapCtxWriter); ok {
				if wrapped := wrapper.unwrapWriter(); wrapped != nil {
					return &writerWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &writerWrapper{w, ctx}
}

type writerWrapper struct {
	Writer
	ctx context.Context
}

func (w *writerWrapper) Write(b []byte) (n int, err error) {
	return w.Writer.Write(w.ctx, b)
}
func (w *writerWrapper) unwrapWriter() Writer {
	return w.Writer
}

func CtxWriterWrapper(w io.Writer) Writer {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapCtxWriter:
		if innerWriter := w.unwrapWriter(); innerWriter != nil {
			return innerWriter
		}
	}
	return &ctxWriterWrapper{w}
}

type ctxWriterWrapper struct {
	io.Writer
}

func (w *ctxWriterWrapper) Write(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return w.Writer.Write(b)
}
func (w *ctxWriterWrapper) unwrapWriter() io.Writer {
	return w.Writer
}

func NoTraceCtxWriterWrapper(w io.Writer) Writer {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapCtxWriter:
		if innerWriter := w.unwrapWriter(); innerWriter != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerWriter.(unwrapWriter); ok {
				if wrapped := wrapper.unwrapWriter(); wrapped != nil {
					return &noTraceCtxWriterWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxWriterWrapper{w}
}

type noTraceCtxWriterWrapper struct {
	io.Writer
}

func (w *noTraceCtxWriterWrapper) Write(ctx context.Context, b []byte) (n int, err error) {
	return w.Writer.Write(b)
}
func (w *noTraceCtxWriterWrapper) unwrapWriter() io.Writer {
	return w.Writer
}

// Wrappers to allow interoperability of Seekers between io and ctxio

type Seeker interface {
	Seek(ctx context.Context, offset int64, whence int) (ret int64, err error)
}

func SeekerWrapper(ctx context.Context, s Seeker) io.Seeker {
	switch s := s.(type) {
	case nil:
		return nil
	case unwrapSeeker:
		if innerSeeker := s.unwrapSeeker(); innerSeeker != nil {
			if wrapper, ok := innerSeeker.(unwrapCtxSeeker); ok {
				if wrapped := wrapper.unwrapSeeker(); wrapped != nil {
					return &seekerWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &seekerWrapper{s, ctx}
}

type seekerWrapper struct {
	Seeker
	ctx context.Context
}

func (s *seekerWrapper) Seek(offset int64, whence int) (n int64, err error) {
	return s.Seeker.Seek(s.ctx, offset, whence)
}
func (s *seekerWrapper) unwrapSeeker() Seeker {
	return s.Seeker
}

func CtxSeekerWrapper(s io.Seeker) Seeker {
	switch s := s.(type) {
	case nil:
		return nil
	case unwrapCtxSeeker:
		if innerSeeker := s.unwrapSeeker(); innerSeeker != nil {
			return innerSeeker
		}
	}
	return &ctxSeekerWrapper{s}
}

type ctxSeekerWrapper struct {
	io.Seeker
}

func (s *ctxSeekerWrapper) Seek(ctx context.Context, offset int64, whence int) (n int64, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return s.Seeker.Seek(offset, whence)
}
func (s *ctxSeekerWrapper) unwrapSeeker() io.Seeker {
	return s.Seeker
}

func NoTraceCtxSeekerWrapper(s io.Seeker) Seeker {
	switch s := s.(type) {
	case nil:
		return nil
	case unwrapCtxSeeker:
		if innerSeeker := s.unwrapSeeker(); innerSeeker != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerSeeker.(unwrapSeeker); ok {
				if wrapped := wrapper.unwrapSeeker(); wrapped != nil {
					return &noTraceCtxSeekerWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxSeekerWrapper{s}
}

type noTraceCtxSeekerWrapper struct {
	io.Seeker
}

func (s *noTraceCtxSeekerWrapper) Seek(ctx context.Context, offset int64, whence int) (n int64, err error) {
	return s.Seeker.Seek(offset, whence)
}
func (s *noTraceCtxSeekerWrapper) unwrapSeeker() io.Seeker {
	return s.Seeker
}

// Wrappers to allow interoperability of Closers between io and ctxio

type Closer interface {
	Close(ctx context.Context) error
}

func CloserWrapper(ctx context.Context, c Closer) io.Closer {
	switch c := c.(type) {
	case nil:
		return nil
	case unwrapCloser:
		if innerCloser := c.unwrapCloser(); innerCloser != nil {
			if wrapper, ok := innerCloser.(unwrapCtxCloser); ok {
				if wrapped := wrapper.unwrapCloser(); wrapped != nil {
					return &closerWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &closerWrapper{c, ctx}
}

type closerWrapper struct {
	Closer
	ctx context.Context
}

func (c *closerWrapper) Close() error {
	return c.Closer.Close(c.ctx)
}
func (c *closerWrapper) unwrapCloser() Closer {
	return c.Closer
}

func CtxCloserWrapper(c io.Closer) Closer {
	switch c := c.(type) {
	case nil:
		return nil
	case unwrapCtxCloser:
		if innerCloser := c.unwrapCloser(); innerCloser != nil {
			return innerCloser
		}
	}
	return &ctxCloserWrapper{c}
}

type ctxCloserWrapper struct {
	io.Closer
}

func (c *ctxCloserWrapper) Close(ctx context.Context) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return c.Closer.Close()
}
func (c *ctxCloserWrapper) unwrapCloser() io.Closer {
	return c.Closer
}

func NoTraceCtxCloserWrapper(c io.Closer) Closer {
	switch c := c.(type) {
	case nil:
		return nil
	case unwrapCtxCloser:
		if innerCloser := c.unwrapCloser(); innerCloser != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerCloser.(unwrapCloser); ok {
				if wrapped := wrapper.unwrapCloser(); wrapped != nil {
					return &noTraceCtxCloserWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxCloserWrapper{c}
}

type noTraceCtxCloserWrapper struct {
	io.Closer
}

func (c *noTraceCtxCloserWrapper) Close(ctx context.Context) error {
	return c.Closer.Close()
}
func (c *noTraceCtxCloserWrapper) unwrapCloser() io.Closer {
	return c.Closer
}

// Functions that wrap common io functions with ss for span

func Copy(ctx context.Context, dst Writer, src Reader) (written int64, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return io.Copy(WriterWrapper(ctx, dst), ReaderWrapper(ctx, src))
}
func CopyBuffer(ctx context.Context, dst Writer, src Reader, buf []byte) (written int64, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return io.CopyBuffer(WriterWrapper(ctx, dst), ReaderWrapper(ctx, src), buf)
}
func CopyN(ctx context.Context, dst Writer, src Reader, n int64) (written int64, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return io.CopyN(WriterWrapper(ctx, dst), ReaderWrapper(ctx, src), n)
}
func Pipe() (*PipeReader, *PipeWriter) {
	r, w := io.Pipe()
	return &PipeReader{r}, &PipeWriter{w}
}
func ReadAll(ctx context.Context, r Reader) ([]byte, error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return io.ReadAll(ReaderWrapper(ctx, r))
}
func ReadAtLeast(ctx context.Context, r Reader, buf []byte, min int) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return io.ReadAtLeast(ReaderWrapper(ctx, r), buf, min)
}
func ReadFull(ctx context.Context, r Reader, buf []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return io.ReadFull(ReaderWrapper(ctx, r), buf)
}
func WriteString(ctx context.Context, w Writer, s string) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return io.WriteString(WriterWrapper(ctx, w), s)
}

// Wrappers for PipeReader and PipeWriter to be ctxio compliant

type PipeReader struct {
	*io.PipeReader
}

func (r *PipeReader) Read(ctx context.Context, data []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.PipeReader.Read(data)
}
func (r *PipeReader) Close(ctx context.Context) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.PipeReader.Close()
}
func (r *PipeReader) CloseWithError(ctx context.Context, err error) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.PipeReader.CloseWithError(err)
}

type PipeWriter struct {
	*io.PipeWriter
}

func (w *PipeWriter) Write(ctx context.Context, data []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return w.PipeWriter.Write(data)
}
func (r *PipeWriter) Close(ctx context.Context) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.PipeWriter.Close()
}
func (r *PipeWriter) CloseWithError(ctx context.Context, err error) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.PipeWriter.CloseWithError(err)
}

// Wrappers to allow interoperability of ReadClosers between io and ctxio

type ReadCloser interface {
	Reader
	Closer
}

func ReadCloserWrapper(ctx context.Context, r ReadCloser) io.ReadCloser {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapReadCloser:
		if innerReadCloser := r.unwrapReadCloser(); innerReadCloser != nil {
			if wrapper, ok := innerReadCloser.(unwrapCtxReadCloser); ok {
				if wrapped := wrapper.unwrapReadCloser(); wrapped != nil {
					return &readCloserWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &readCloserWrapper{r, ctx}
}

type readCloserWrapper struct {
	ReadCloser
	ctx context.Context
}

func (r *readCloserWrapper) Read(b []byte) (n int, err error) {
	return r.ReadCloser.Read(r.ctx, b)
}
func (r *readCloserWrapper) Close() error {
	return r.ReadCloser.Close(r.ctx)
}
func (r *readCloserWrapper) unwrapReadCloser() ReadCloser {
	return r.ReadCloser
}
func (r *readCloserWrapper) unwrapReader() Reader {
	return r.ReadCloser
}
func (r *readCloserWrapper) unwrapCloser() Closer {
	return r.ReadCloser
}

func CtxReadCloserWrapper(r io.ReadCloser) ReadCloser {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReadCloser:
		if innerReadCloser := r.unwrapReadCloser(); innerReadCloser != nil {
			return innerReadCloser
		}
	}
	return &ctxReadCloserWrapper{r}
}

type ctxReadCloserWrapper struct {
	io.ReadCloser
}

func (r *ctxReadCloserWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadCloser.Read(b)
}
func (r *ctxReadCloserWrapper) Close(ctx context.Context) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadCloser.Close()
}
func (r *ctxReadCloserWrapper) unwrapReadCloser() io.ReadCloser {
	return r.ReadCloser
}
func (r *ctxReadCloserWrapper) unwrapReader() io.Reader {
	return r.ReadCloser
}
func (r *ctxReadCloserWrapper) unwrapCloser() io.Closer {
	return r.ReadCloser
}

func NoTraceCtxReadCloserWrapper(r io.ReadCloser) ReadCloser {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReadCloser:
		if innerReadCloser := r.unwrapReadCloser(); innerReadCloser != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerReadCloser.(unwrapReadCloser); ok {
				if wrapped := wrapper.unwrapReadCloser(); wrapped != nil {
					return &noTraceCtxReadCloserWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxReadCloserWrapper{r}
}

type noTraceCtxReadCloserWrapper struct {
	io.ReadCloser
}

func (r *noTraceCtxReadCloserWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	return r.ReadCloser.Read(b)
}
func (r *noTraceCtxReadCloserWrapper) Close(ctx context.Context) error {
	return r.ReadCloser.Close()
}
func (r *noTraceCtxReadCloserWrapper) unwrapReadCloser() io.ReadCloser {
	return r.ReadCloser
}
func (r *noTraceCtxReadCloserWrapper) unwrapReader() io.Reader {
	return r.ReadCloser
}
func (r *noTraceCtxReadCloserWrapper) unwrapCloser() io.Closer {
	return r.ReadCloser
}

type nopReadCloser struct {
	Reader
	*ctxCloserWrapper
}

func (r *nopReadCloser) unwrapReadCloser() io.ReadCloser {
	if innerReader, ok := r.Reader.(unwrapReader); ok {
		return io.NopCloser(innerReader.unwrapReader())
	}
	return nil
}
func (r *nopReadCloser) unwrapReader() io.Reader {
	if innerReader, ok := r.Reader.(unwrapReader); ok {
		return innerReader.unwrapReader()
	}
	return nil
}
func (r *nopReadCloser) unwrapCloser() io.Closer {
	return r.ctxCloserWrapper.Closer
}

func NopCloser(r Reader) ReadCloser {
	if r == nil {
		return nil
	}
	return &nopReadCloser{Reader: r, ctxCloserWrapper: &ctxCloserWrapper{io.NopCloser(nil)}}
}

// Wrappers to allow interoperability of ReadSeekClosers between io and ctxio

type ReadSeekCloser interface {
	Reader
	Seeker
	Closer
}

func ReadSeekCloserWrapper(ctx context.Context, r ReadSeekCloser) io.ReadCloser {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapReadSeekCloser:
		if innerReadSeekCloser := r.unwrapReadSeekCloser(); innerReadSeekCloser != nil {
			if wrapper, ok := innerReadSeekCloser.(unwrapCtxReadSeekCloser); ok {
				if wrapped := wrapper.unwrapReadSeekCloser(); wrapped != nil {
					return &readSeekCloserWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &readSeekCloserWrapper{r, ctx}
}

type readSeekCloserWrapper struct {
	ReadSeekCloser
	ctx context.Context
}

func (r *readSeekCloserWrapper) Read(b []byte) (n int, err error) {
	return r.ReadSeekCloser.Read(r.ctx, b)
}
func (r *readSeekCloserWrapper) Seek(offset int64, whence int) (n int64, err error) {
	return r.ReadSeekCloser.Seek(r.ctx, offset, whence)
}
func (r *readSeekCloserWrapper) Close() error {
	return r.ReadSeekCloser.Close(r.ctx)
}
func (r *readSeekCloserWrapper) unwrapReadSeekCloser() ReadSeekCloser {
	return r.ReadSeekCloser
}
func (r *readSeekCloserWrapper) unwrapReadSeeker() ReadSeeker {
	return r.ReadSeekCloser
}
func (r *readSeekCloserWrapper) unwrapReadCloser() ReadCloser {
	return r.ReadSeekCloser
}
func (r *readSeekCloserWrapper) unwrapReader() Reader {
	return r.ReadSeekCloser
}
func (r *readSeekCloserWrapper) unwrapSeeker() Seeker {
	return r.ReadSeekCloser
}
func (r *readSeekCloserWrapper) unwrapCloser() Closer {
	return r.ReadSeekCloser
}

func CtxReadSeekCloserWrapper(r io.ReadSeekCloser) ReadCloser {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReadSeekCloser:
		if innerReadSeekCloser := r.unwrapReadSeekCloser(); innerReadSeekCloser != nil {
			return innerReadSeekCloser
		}
	}
	return &ctxReadSeekCloserWrapper{r}
}

type ctxReadSeekCloserWrapper struct {
	io.ReadSeekCloser
}

func (r *ctxReadSeekCloserWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadSeekCloser.Read(b)
}
func (r *ctxReadSeekCloserWrapper) Seek(ctx context.Context, offset int64, whence int) (n int64, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadSeekCloser.Seek(offset, whence)
}
func (r *ctxReadSeekCloserWrapper) Close(ctx context.Context) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadSeekCloser.Close()
}
func (r *ctxReadSeekCloserWrapper) unwrapReadSeekCloser() io.ReadSeekCloser {
	return r.ReadSeekCloser
}
func (r *ctxReadSeekCloserWrapper) unwrapReadSeeker() io.ReadSeeker {
	return r.ReadSeekCloser
}
func (r *ctxReadSeekCloserWrapper) unwrapReadCloser() io.ReadCloser {
	return r.ReadSeekCloser
}
func (r *ctxReadSeekCloserWrapper) unwrapReader() io.Reader {
	return r.ReadSeekCloser
}
func (r *ctxReadSeekCloserWrapper) unwrapSeeker() io.Seeker {
	return r.ReadSeekCloser
}
func (r *ctxReadSeekCloserWrapper) unwrapCloser() io.Closer {
	return r.ReadSeekCloser
}

func NoTraceCtxReadSeekCloserWrapper(r io.ReadSeekCloser) ReadCloser {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReadSeekCloser:
		if innerReadSeekCloser := r.unwrapReadSeekCloser(); innerReadSeekCloser != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerReadSeekCloser.(unwrapReadSeekCloser); ok {
				if wrapped := wrapper.unwrapReadSeekCloser(); wrapped != nil {
					return &noTraceCtxReadSeekCloserWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxReadSeekCloserWrapper{r}
}

type noTraceCtxReadSeekCloserWrapper struct {
	io.ReadSeekCloser
}

func (r *noTraceCtxReadSeekCloserWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	return r.ReadSeekCloser.Read(b)
}
func (r *noTraceCtxReadSeekCloserWrapper) Seek(ctx context.Context, offset int64, whence int) (n int64, err error) {
	return r.ReadSeekCloser.Seek(offset, whence)
}
func (r *noTraceCtxReadSeekCloserWrapper) Close(ctx context.Context) error {
	return r.ReadSeekCloser.Close()
}
func (r *noTraceCtxReadSeekCloserWrapper) unwrapReadSeekCloser() io.ReadSeekCloser {
	return r.ReadSeekCloser
}
func (r *noTraceCtxReadSeekCloserWrapper) unwrapReadSeeker() io.ReadSeeker {
	return r.ReadSeekCloser
}
func (r *noTraceCtxReadSeekCloserWrapper) unwrapReadCloser() io.ReadCloser {
	return r.ReadSeekCloser
}
func (r *noTraceCtxReadSeekCloserWrapper) unwrapReader() io.Reader {
	return r.ReadSeekCloser
}
func (r *noTraceCtxReadSeekCloserWrapper) unwrapSeeker() io.Seeker {
	return r.ReadSeekCloser
}
func (r *noTraceCtxReadSeekCloserWrapper) unwrapCloser() io.Closer {
	return r.ReadSeekCloser
}

// Wrappers to allow interoperability of ReadSeekers between io and ctxio

type ReadSeeker interface {
	Reader
	Seeker
}

func ReadSeekerWrapper(ctx context.Context, r ReadSeeker) io.ReadSeeker {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapReadSeeker:
		if innerReadSeeker := r.unwrapReadSeeker(); innerReadSeeker != nil {
			if wrapper, ok := innerReadSeeker.(unwrapCtxReadSeeker); ok {
				if wrapped := wrapper.unwrapReadSeeker(); wrapped != nil {
					return &readSeekerWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &readSeekerWrapper{r, ctx}
}

type readSeekerWrapper struct {
	ReadSeeker
	ctx context.Context
}

func (r *readSeekerWrapper) Read(b []byte) (n int, err error) {
	return r.ReadSeeker.Read(r.ctx, b)
}
func (r *readSeekerWrapper) Seek(offset int64, whence int) (n int64, err error) {
	return r.ReadSeeker.Seek(r.ctx, offset, whence)
}
func (r *readSeekerWrapper) unwrapReadSeeker() ReadSeeker {
	return r.ReadSeeker
}
func (r *readSeekerWrapper) unwrapReader() Reader {
	return r.ReadSeeker
}
func (r *readSeekerWrapper) unwrapSeeker() Seeker {
	return r.ReadSeeker
}

func CtxReadSeekerWrapper(r io.ReadSeeker) ReadSeeker {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReadSeeker:
		if innerReadSeeker := r.unwrapReadSeeker(); innerReadSeeker != nil {
			return innerReadSeeker
		}
	}
	return &ctxReadSeekerWrapper{r}
}

type ctxReadSeekerWrapper struct {
	io.ReadSeeker
}

func (r *ctxReadSeekerWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadSeeker.Read(b)
}
func (r *ctxReadSeekerWrapper) Seek(ctx context.Context, offset int64, whence int) (n int64, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadSeeker.Seek(offset, whence)
}
func (r *ctxReadSeekerWrapper) unwrapReadSeeker() io.ReadSeeker {
	return r.ReadSeeker
}
func (r *ctxReadSeekerWrapper) unwrapReader() io.Reader {
	return r.ReadSeeker
}
func (r *ctxReadSeekerWrapper) unwrapSeeker() io.Seeker {
	return r.ReadSeeker
}

func NoTraceCtxReadSeekerWrapper(r io.ReadSeeker) ReadSeeker {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReadSeeker:
		if innerReadSeeker := r.unwrapReadSeeker(); innerReadSeeker != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerReadSeeker.(unwrapReadSeeker); ok {
				if wrapped := wrapper.unwrapReadSeeker(); wrapped != nil {
					return &noTraceCtxReadSeekerWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxReadSeekerWrapper{r}
}

type noTraceCtxReadSeekerWrapper struct {
	io.ReadSeeker
}

func (r *noTraceCtxReadSeekerWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	return r.ReadSeeker.Read(b)
}
func (r *noTraceCtxReadSeekerWrapper) Seek(ctx context.Context, offset int64, whence int) (n int64, err error) {
	return r.ReadSeeker.Seek(offset, whence)
}
func (r *noTraceCtxReadSeekerWrapper) unwrapReadSeeker() io.ReadSeeker {
	return r.ReadSeeker
}
func (r *noTraceCtxReadSeekerWrapper) unwrapReader() io.Reader {
	return r.ReadSeeker
}
func (r *noTraceCtxReadSeekerWrapper) unwrapSeeker() io.Seeker {
	return r.ReadSeeker
}

// Wrappers to allow interoperability of ReadWriteClosers between io and ctxio

type ReadWriteCloser interface {
	Reader
	Writer
	Closer
}

func ReadWriteCloserWrapper(ctx context.Context, r ReadWriteCloser) io.ReadWriteCloser {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapReadWriteCloser:
		if innerReadWriteCloser := r.unwrapReadWriteCloser(); innerReadWriteCloser != nil {
			if wrapper, ok := innerReadWriteCloser.(unwrapCtxReadWriteCloser); ok {
				if wrapped := wrapper.unwrapReadWriteCloser(); wrapped != nil {
					return &readWriteCloserWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &readWriteCloserWrapper{r, ctx}
}

type readWriteCloserWrapper struct {
	ReadWriteCloser
	ctx context.Context
}

func (r *readWriteCloserWrapper) Read(b []byte) (n int, err error) {
	return r.ReadWriteCloser.Read(r.ctx, b)
}
func (r *readWriteCloserWrapper) Write(b []byte) (n int, err error) {
	return r.ReadWriteCloser.Write(r.ctx, b)
}
func (r *readWriteCloserWrapper) Close() error {
	return r.ReadWriteCloser.Close(r.ctx)
}
func (r *readWriteCloserWrapper) unwrapReadWriteCloser() ReadWriteCloser {
	return r.ReadWriteCloser
}
func (r *readWriteCloserWrapper) unwrapReadCloser() ReadCloser {
	return r.ReadWriteCloser
}
func (r *readWriteCloserWrapper) unwrapReadWriter() ReadWriter {
	return r.ReadWriteCloser
}
func (r *readWriteCloserWrapper) unwrapWriteCloser() WriteCloser {
	return r.ReadWriteCloser
}
func (r *readWriteCloserWrapper) unwrapReader() Reader {
	return r.ReadWriteCloser
}
func (r *readWriteCloserWrapper) unwrapCloser() Closer {
	return r.ReadWriteCloser
}
func (r *readWriteCloserWrapper) unwrapWriter() Writer {
	return r.ReadWriteCloser
}

func CtxReadWriteCloserWrapper(r io.ReadWriteCloser) ReadWriteCloser {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReadWriteCloser:
		if innerReadWriteCloser := r.unwrapReadWriteCloser(); innerReadWriteCloser != nil {
			return innerReadWriteCloser
		}
	}
	return &ctxReadWriteCloserWrapper{r}
}

type ctxReadWriteCloserWrapper struct {
	io.ReadWriteCloser
}

func (r *ctxReadWriteCloserWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadWriteCloser.Read(b)
}
func (r *ctxReadWriteCloserWrapper) Write(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadWriteCloser.Write(b)
}
func (r *ctxReadWriteCloserWrapper) Close(ctx context.Context) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadWriteCloser.Close()
}
func (r *ctxReadWriteCloserWrapper) unwrapReadWriteCloser() io.ReadWriteCloser {
	return r.ReadWriteCloser
}
func (r *ctxReadWriteCloserWrapper) unwrapReadCloser() io.ReadCloser {
	return r.ReadWriteCloser
}
func (r *ctxReadWriteCloserWrapper) unwrapReadWriter() io.ReadWriter {
	return r.ReadWriteCloser
}
func (r *ctxReadWriteCloserWrapper) unwrapWriteCloser() io.WriteCloser {
	return r.ReadWriteCloser
}
func (r *ctxReadWriteCloserWrapper) unwrapReader() io.Reader {
	return r.ReadWriteCloser
}
func (r *ctxReadWriteCloserWrapper) unwrapCloser() io.Closer {
	return r.ReadWriteCloser
}
func (r *ctxReadWriteCloserWrapper) unwrapWriter() io.Writer {
	return r.ReadWriteCloser
}

func NoTraceCtxReadWriteCloserWrapper(r io.ReadWriteCloser) ReadWriteCloser {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReadWriteCloser:
		if innerReadWriteCloser := r.unwrapReadWriteCloser(); innerReadWriteCloser != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerReadWriteCloser.(unwrapReadWriteCloser); ok {
				if wrapped := wrapper.unwrapReadWriteCloser(); wrapped != nil {
					return &noTraceCtxReadWriteCloserWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxReadWriteCloserWrapper{r}
}

type noTraceCtxReadWriteCloserWrapper struct {
	io.ReadWriteCloser
}

func (r *noTraceCtxReadWriteCloserWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	return r.ReadWriteCloser.Read(b)
}
func (r *noTraceCtxReadWriteCloserWrapper) Write(ctx context.Context, b []byte) (n int, err error) {
	return r.ReadWriteCloser.Write(b)
}
func (r *noTraceCtxReadWriteCloserWrapper) Close(ctx context.Context) error {
	return r.ReadWriteCloser.Close()
}
func (r *noTraceCtxReadWriteCloserWrapper) unwrapReadWriteCloser() io.ReadWriteCloser {
	return r.ReadWriteCloser
}
func (r *noTraceCtxReadWriteCloserWrapper) unwrapReadCloser() io.ReadCloser {
	return r.ReadWriteCloser
}
func (r *noTraceCtxReadWriteCloserWrapper) unwrapReadWriter() io.ReadWriter {
	return r.ReadWriteCloser
}
func (r *noTraceCtxReadWriteCloserWrapper) unwrapWriteCloser() io.WriteCloser {
	return r.ReadWriteCloser
}
func (r *noTraceCtxReadWriteCloserWrapper) unwrapReader() io.Reader {
	return r.ReadWriteCloser
}
func (r *noTraceCtxReadWriteCloserWrapper) unwrapCloser() io.Closer {
	return r.ReadWriteCloser
}
func (r *noTraceCtxReadWriteCloserWrapper) unwrapWriter() io.Writer {
	return r.ReadWriteCloser
}

// Wrappers to allow interoperability of ReadWriteSeekers between io and ctxio

type ReadWriteSeeker interface {
	Reader
	Writer
	Seeker
}

func ReadWriteSeekerWrapper(ctx context.Context, r ReadWriteSeeker) io.ReadWriteSeeker {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapReadWriteSeeker:
		if innerReadWriteSeeker := r.unwrapReadWriteSeeker(); innerReadWriteSeeker != nil {
			if wrapper, ok := innerReadWriteSeeker.(unwrapCtxReadWriteSeeker); ok {
				if wrapped := wrapper.unwrapReadWriteSeeker(); wrapped != nil {
					return &readWriteSeekerWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &readWriteSeekerWrapper{r, ctx}
}

type readWriteSeekerWrapper struct {
	ReadWriteSeeker
	ctx context.Context
}

func (r *readWriteSeekerWrapper) Read(b []byte) (n int, err error) {
	return r.ReadWriteSeeker.Read(r.ctx, b)
}
func (r *readWriteSeekerWrapper) Write(b []byte) (n int, err error) {
	return r.ReadWriteSeeker.Write(r.ctx, b)
}
func (r *readWriteSeekerWrapper) Seek(offset int64, whence int) (n int64, err error) {
	return r.ReadWriteSeeker.Seek(r.ctx, offset, whence)
}
func (r *readWriteSeekerWrapper) unwrapReadWriteSeeker() ReadWriteSeeker {
	return r.ReadWriteSeeker
}
func (r *readWriteSeekerWrapper) unwrapReadSeeker() ReadSeeker {
	return r.ReadWriteSeeker
}
func (r *readWriteSeekerWrapper) unwrapReadWriter() ReadWriter {
	return r.ReadWriteSeeker
}
func (r *readWriteSeekerWrapper) unwrapWriteSeeker() WriteSeeker {
	return r.ReadWriteSeeker
}
func (r *readWriteSeekerWrapper) unwrapReader() Reader {
	return r.ReadWriteSeeker
}
func (r *readWriteSeekerWrapper) unwrapSeeker() Seeker {
	return r.ReadWriteSeeker
}
func (r *readWriteSeekerWrapper) unwrapWriter() Writer {
	return r.ReadWriteSeeker
}

func CtxReadWriteSeekerWrapper(r io.ReadWriteSeeker) ReadWriteSeeker {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReadWriteSeeker:
		if innerReadWriteSeeker := r.unwrapReadWriteSeeker(); innerReadWriteSeeker != nil {
			return innerReadWriteSeeker
		}
	}
	return &ctxReadWriteSeekerWrapper{r}
}

type ctxReadWriteSeekerWrapper struct {
	io.ReadWriteSeeker
}

func (r *ctxReadWriteSeekerWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadWriteSeeker.Read(b)
}
func (w *ctxReadWriteSeekerWrapper) Write(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return w.ReadWriteSeeker.Write(b)
}
func (r *ctxReadWriteSeekerWrapper) Seek(ctx context.Context, offset int64, whence int) (n int64, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadWriteSeeker.Seek(offset, whence)
}
func (r *ctxReadWriteSeekerWrapper) unwrapReadWriteSeeker() io.ReadWriteSeeker {
	return r.ReadWriteSeeker
}
func (r *ctxReadWriteSeekerWrapper) unwrapReadSeeker() io.ReadSeeker {
	return r.ReadWriteSeeker
}
func (r *ctxReadWriteSeekerWrapper) unwrapReadWriter() io.ReadWriter {
	return r.ReadWriteSeeker
}
func (r *ctxReadWriteSeekerWrapper) unwrapWriteSeeker() io.WriteSeeker {
	return r.ReadWriteSeeker
}
func (r *ctxReadWriteSeekerWrapper) unwrapReader() io.Reader {
	return r.ReadWriteSeeker
}
func (r *ctxReadWriteSeekerWrapper) unwrapSeeker() io.Seeker {
	return r.ReadWriteSeeker
}
func (r *ctxReadWriteSeekerWrapper) unwrapWriter() io.Writer {
	return r.ReadWriteSeeker
}

func NoTraceCtxReadWriteSeekerWrapper(r io.ReadWriteSeeker) ReadWriteSeeker {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReadWriteSeeker:
		if innerReadWriteSeeker := r.unwrapReadWriteSeeker(); innerReadWriteSeeker != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerReadWriteSeeker.(unwrapReadWriteSeeker); ok {
				if wrapped := wrapper.unwrapReadWriteSeeker(); wrapped != nil {
					return &noTraceCtxReadWriteSeekerWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxReadWriteSeekerWrapper{r}
}

type noTraceCtxReadWriteSeekerWrapper struct {
	io.ReadWriteSeeker
}

func (r *noTraceCtxReadWriteSeekerWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	return r.ReadWriteSeeker.Read(b)
}
func (w *noTraceCtxReadWriteSeekerWrapper) Write(ctx context.Context, b []byte) (n int, err error) {
	return w.ReadWriteSeeker.Write(b)
}
func (r *noTraceCtxReadWriteSeekerWrapper) Seek(ctx context.Context, offset int64, whence int) (n int64, err error) {
	return r.ReadWriteSeeker.Seek(offset, whence)
}
func (r *noTraceCtxReadWriteSeekerWrapper) unwrapReadWriteSeeker() io.ReadWriteSeeker {
	return r.ReadWriteSeeker
}
func (r *noTraceCtxReadWriteSeekerWrapper) unwrapReadSeeker() io.ReadSeeker {
	return r.ReadWriteSeeker
}
func (r *noTraceCtxReadWriteSeekerWrapper) unwrapReadWriter() io.ReadWriter {
	return r.ReadWriteSeeker
}
func (r *noTraceCtxReadWriteSeekerWrapper) unwrapWriteSeeker() io.WriteSeeker {
	return r.ReadWriteSeeker
}
func (r *noTraceCtxReadWriteSeekerWrapper) unwrapReader() io.Reader {
	return r.ReadWriteSeeker
}
func (r *noTraceCtxReadWriteSeekerWrapper) unwrapSeeker() io.Seeker {
	return r.ReadWriteSeeker
}
func (r *noTraceCtxReadWriteSeekerWrapper) unwrapWriter() io.Writer {
	return r.ReadWriteSeeker
}

// Wrappers to allow interoperability of ReadWriters between io and ctxio

type ReadWriter interface {
	Reader
	Writer
}

func ReadWriterWrapper(ctx context.Context, r ReadWriter) io.ReadWriter {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapReadWriter:
		if innerReadWriter := r.unwrapReadWriter(); innerReadWriter != nil {
			if wrapper, ok := innerReadWriter.(unwrapCtxReadWriter); ok {
				if wrapped := wrapper.unwrapReadWriter(); wrapped != nil {
					return &readWriterWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &readWriterWrapper{r, ctx}
}

type readWriterWrapper struct {
	ReadWriter
	ctx context.Context
}

func (r *readWriterWrapper) Read(b []byte) (n int, err error) {
	return r.ReadWriter.Read(r.ctx, b)
}
func (r *readWriterWrapper) Write(b []byte) (n int, err error) {
	return r.ReadWriter.Write(r.ctx, b)
}
func (r *readWriterWrapper) unwrapReadWriter() ReadWriter {
	return r.ReadWriter
}
func (r *readWriterWrapper) unwrapReader() Reader {
	return r.ReadWriter
}
func (r *readWriterWrapper) unwrapWriter() Writer {
	return r.ReadWriter
}

func CtxReadWriterWrapper(r io.ReadWriter) ReadWriter {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReadWriter:
		if innerReadWriter := r.unwrapReadWriter(); innerReadWriter != nil {
			return innerReadWriter
		}
	}
	return &ctxReadWriterWrapper{r}
}

type ctxReadWriterWrapper struct {
	io.ReadWriter
}

func (r *ctxReadWriterWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadWriter.Read(b)
}
func (r *ctxReadWriterWrapper) Write(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReadWriter.Write(b)
}
func (r *ctxReadWriterWrapper) unwrapReadWriter() io.ReadWriter {
	return r.ReadWriter
}
func (r *ctxReadWriterWrapper) unwrapReader() io.Reader {
	return r.ReadWriter
}
func (r *ctxReadWriterWrapper) unwrapWriter() io.Writer {
	return r.ReadWriter
}

func NoTraceCtxReadWriterWrapper(r io.ReadWriter) ReadWriter {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReadWriter:
		if innerReadWriter := r.unwrapReadWriter(); innerReadWriter != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerReadWriter.(unwrapReadWriter); ok {
				if wrapped := wrapper.unwrapReadWriter(); wrapped != nil {
					return &noTraceCtxReadWriterWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxReadWriterWrapper{r}
}

type noTraceCtxReadWriterWrapper struct {
	io.ReadWriter
}

func (r *noTraceCtxReadWriterWrapper) Read(ctx context.Context, b []byte) (n int, err error) {
	return r.ReadWriter.Read(b)
}
func (r *noTraceCtxReadWriterWrapper) Write(ctx context.Context, b []byte) (n int, err error) {
	return r.ReadWriter.Write(b)
}
func (r *noTraceCtxReadWriterWrapper) unwrapReadWriter() io.ReadWriter {
	return r.ReadWriter
}
func (r *noTraceCtxReadWriterWrapper) unwrapReader() io.Reader {
	return r.ReadWriter
}
func (r *noTraceCtxReadWriterWrapper) unwrapWriter() io.Writer {
	return r.ReadWriter
}

// Wrappers to allow interoperability of WriteClosers between io and ctxio

type WriteCloser interface {
	Writer
	Closer
}

func WriteCloserWrapper(ctx context.Context, w WriteCloser) io.WriteCloser {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapWriteCloser:
		if innerWriteCloser := w.unwrapWriteCloser(); innerWriteCloser != nil {
			if wrapper, ok := innerWriteCloser.(unwrapCtxWriteCloser); ok {
				if wrapped := wrapper.unwrapWriteCloser(); wrapped != nil {
					return &writeCloserWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &writeCloserWrapper{w, ctx}
}

type writeCloserWrapper struct {
	WriteCloser
	ctx context.Context
}

func (w *writeCloserWrapper) Write(b []byte) (n int, err error) {
	return w.WriteCloser.Write(w.ctx, b)
}
func (w *writeCloserWrapper) Close() error {
	return w.WriteCloser.Close(w.ctx)
}
func (w *writeCloserWrapper) unwrapWriteCloser() WriteCloser {
	return w.WriteCloser
}
func (w *writeCloserWrapper) unwrapCloser() Closer {
	return w.WriteCloser
}
func (w *writeCloserWrapper) unwrapWriter() Writer {
	return w.WriteCloser
}

func CtxWriteCloserWrapper(w io.WriteCloser) WriteCloser {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapCtxWriteCloser:
		if innerWriteCloser := w.unwrapWriteCloser(); innerWriteCloser != nil {
			return innerWriteCloser
		}
	}
	return &ctxWriteCloserWrapper{w}
}

type ctxWriteCloserWrapper struct {
	io.WriteCloser
}

func (w *ctxWriteCloserWrapper) Write(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return w.WriteCloser.Write(b)
}
func (w *ctxWriteCloserWrapper) Close(ctx context.Context) error {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return w.WriteCloser.Close()
}
func (w *ctxWriteCloserWrapper) unwrapWriteCloser() io.WriteCloser {
	return w.WriteCloser
}
func (w *ctxWriteCloserWrapper) unwrapCloser() io.Closer {
	return w.WriteCloser
}
func (w *ctxWriteCloserWrapper) unwrapWriter() io.Writer {
	return w.WriteCloser
}

func NoTraceCtxWriteCloserWrapper(w io.WriteCloser) WriteCloser {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapCtxWriteCloser:
		if innerWriteCloser := w.unwrapWriteCloser(); innerWriteCloser != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerWriteCloser.(unwrapWriteCloser); ok {
				if wrapped := wrapper.unwrapWriteCloser(); wrapped != nil {
					return &noTraceCtxWriteCloserWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxWriteCloserWrapper{w}
}

type noTraceCtxWriteCloserWrapper struct {
	io.WriteCloser
}

func (w *noTraceCtxWriteCloserWrapper) Write(ctx context.Context, b []byte) (n int, err error) {
	return w.WriteCloser.Write(b)
}
func (w *noTraceCtxWriteCloserWrapper) Close(ctx context.Context) error {
	return w.WriteCloser.Close()
}
func (w *noTraceCtxWriteCloserWrapper) unwrapWriteCloser() io.WriteCloser {
	return w.WriteCloser
}
func (w *noTraceCtxWriteCloserWrapper) unwrapCloser() io.Closer {
	return w.WriteCloser
}
func (w *noTraceCtxWriteCloserWrapper) unwrapWriter() io.Writer {
	return w.WriteCloser
}

// Wrappers to allow interoperability of WriteSeekers between io and ctxio

type WriteSeeker interface {
	Writer
	Seeker
}

func WriteSeekerWrapper(ctx context.Context, w WriteSeeker) io.WriteSeeker {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapWriteSeeker:
		if innerWriteSeeker := w.unwrapWriteSeeker(); innerWriteSeeker != nil {
			if wrapper, ok := innerWriteSeeker.(unwrapCtxWriteSeeker); ok {
				if wrapped := wrapper.unwrapWriteSeeker(); wrapped != nil {
					return &writeSeekerWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &writeSeekerWrapper{w, ctx}
}

type writeSeekerWrapper struct {
	WriteSeeker
	ctx context.Context
}

func (w *writeSeekerWrapper) Write(b []byte) (n int, err error) {
	return w.WriteSeeker.Write(w.ctx, b)
}
func (w *writeSeekerWrapper) Seek(offset int64, whence int) (n int64, err error) {
	return w.WriteSeeker.Seek(w.ctx, offset, whence)
}
func (w *writeSeekerWrapper) unwrapWriteSeeker() WriteSeeker {
	return w.WriteSeeker
}
func (w *writeSeekerWrapper) unwrapSeeker() Seeker {
	return w.WriteSeeker
}
func (w *writeSeekerWrapper) unwrapWriter() Writer {
	return w.WriteSeeker
}

func CtxWriteSeekerWrapper(w io.WriteSeeker) WriteSeeker {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapCtxWriteSeeker:
		if innerWriteSeeker := w.unwrapWriteSeeker(); innerWriteSeeker != nil {
			return innerWriteSeeker
		}
	}
	return &ctxWriteSeekerWrapper{w}
}

type ctxWriteSeekerWrapper struct {
	io.WriteSeeker
}

func (w *ctxWriteSeekerWrapper) Write(ctx context.Context, b []byte) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return w.WriteSeeker.Write(b)
}
func (r *ctxWriteSeekerWrapper) Seek(ctx context.Context, offset int64, whence int) (n int64, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.WriteSeeker.Seek(offset, whence)
}
func (w *ctxWriteSeekerWrapper) unwrapWriteSeeker() io.WriteSeeker {
	return w.WriteSeeker
}
func (w *ctxWriteSeekerWrapper) unwrapSeeker() io.Seeker {
	return w.WriteSeeker
}
func (w *ctxWriteSeekerWrapper) unwrapWriter() io.Writer {
	return w.WriteSeeker
}

func NoTraceCtxWriteSeekerWrapper(w io.WriteSeeker) WriteSeeker {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapCtxWriteSeeker:
		if innerWriteSeeker := w.unwrapWriteSeeker(); innerWriteSeeker != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerWriteSeeker.(unwrapWriteSeeker); ok {
				if wrapped := wrapper.unwrapWriteSeeker(); wrapped != nil {
					return &noTraceCtxWriteSeekerWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxWriteSeekerWrapper{w}
}

type noTraceCtxWriteSeekerWrapper struct {
	io.WriteSeeker
}

func (w *noTraceCtxWriteSeekerWrapper) Write(ctx context.Context, b []byte) (n int, err error) {
	return w.WriteSeeker.Write(b)
}
func (r *noTraceCtxWriteSeekerWrapper) Seek(ctx context.Context, offset int64, whence int) (n int64, err error) {
	return r.WriteSeeker.Seek(offset, whence)
}
func (w *noTraceCtxWriteSeekerWrapper) unwrapWriteSeeker() io.WriteSeeker {
	return w.WriteSeeker
}
func (w *noTraceCtxWriteSeekerWrapper) unwrapSeeker() io.Seeker {
	return w.WriteSeeker
}
func (w *noTraceCtxWriteSeekerWrapper) unwrapWriter() io.Writer {
	return w.WriteSeeker
}

// Wrappers to allow interoperability of ByteReaders between io and ctxio

type ByteReader interface {
	ReadByte(ctx context.Context) (c byte, err error)
}

func ByteReaderWrapper(ctx context.Context, r ByteReader) io.ByteReader {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapByteReader:
		if innerByteReader := r.unwrapByteReader(); innerByteReader != nil {
			if wrapper, ok := innerByteReader.(unwrapCtxByteReader); ok {
				if wrapped := wrapper.unwrapByteReader(); wrapped != nil {
					return &byteReaderWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &byteReaderWrapper{r, ctx}
}

type byteReaderWrapper struct {
	ByteReader
	ctx context.Context
}

func (r *byteReaderWrapper) ReadByte() (c byte, err error) {
	return r.ByteReader.ReadByte(r.ctx)
}
func (r *byteReaderWrapper) unwrapByteReader() ByteReader {
	return r.ByteReader
}

func CtxByteReaderWrapper(r io.ByteReader) ByteReader {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxByteReader:
		if innerByteReader := r.unwrapByteReader(); innerByteReader != nil {
			return innerByteReader
		}
	}
	return &ctxByteReaderWrapper{r}
}

type ctxByteReaderWrapper struct {
	io.ByteReader
}

func (r *ctxByteReaderWrapper) ReadByte(ctx context.Context) (c byte, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ByteReader.ReadByte()
}
func (r *ctxByteReaderWrapper) unwrapByteReader() io.ByteReader {
	return r.ByteReader
}

func NoTraceCtxByteReaderWrapper(r io.ByteReader) ByteReader {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxByteReader:
		if innerByteReader := r.unwrapByteReader(); innerByteReader != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerByteReader.(unwrapByteReader); ok {
				if wrapped := wrapper.unwrapByteReader(); wrapped != nil {
					return &noTraceCtxByteReaderWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxByteReaderWrapper{r}
}

type noTraceCtxByteReaderWrapper struct {
	io.ByteReader
}

func (r *noTraceCtxByteReaderWrapper) ReadByte(ctx context.Context) (c byte, err error) {
	return r.ByteReader.ReadByte()
}
func (r *noTraceCtxByteReaderWrapper) unwrapByteReader() io.ByteReader {
	return r.ByteReader
}

// Wrappers to allow interoperability of ByteScanners between io and ctxio

type ByteScanner interface {
	ByteReader
	UnreadByte() (err error)
}

func ByteScannerWrapper(ctx context.Context, s ByteScanner) io.ByteScanner {
	switch s := s.(type) {
	case nil:
		return nil
	case unwrapByteScanner:
		if innerByteScanner := s.unwrapByteScanner(); innerByteScanner != nil {
			if wrapper, ok := innerByteScanner.(unwrapCtxByteScanner); ok {
				if wrapped := wrapper.unwrapByteScanner(); wrapped != nil {
					return &byteScannerWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &byteScannerWrapper{s, ctx}
}

type byteScannerWrapper struct {
	ByteScanner
	ctx context.Context
}

func (s *byteScannerWrapper) ReadByte() (c byte, err error) {
	return s.ByteScanner.ReadByte(s.ctx)
}
func (s *byteScannerWrapper) UnreadByte() (err error) {
	return s.ByteScanner.UnreadByte()
}
func (s *byteScannerWrapper) unwrapByteScanner() ByteScanner {
	return s.ByteScanner
}

func CtxByteScannerWrapper(s io.ByteScanner) ByteScanner {
	switch s := s.(type) {
	case nil:
		return nil
	case unwrapCtxByteScanner:
		if innerByteScanner := s.unwrapByteScanner(); innerByteScanner != nil {
			return innerByteScanner
		}
	}
	return &ctxByteScannerWrapper{s}
}

type ctxByteScannerWrapper struct {
	io.ByteScanner
}

func (s *ctxByteScannerWrapper) ReadByte(ctx context.Context) (c byte, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return s.ByteScanner.ReadByte()
}
func (s *ctxByteScannerWrapper) UnreadByte() (err error) {
	return s.ByteScanner.UnreadByte()
}
func (s *ctxByteScannerWrapper) unwrapByteScanner() io.ByteScanner {
	return s.ByteScanner
}

func NoTraceCtxByteScannerWrapper(s io.ByteScanner) ByteScanner {
	switch s := s.(type) {
	case nil:
		return nil
	case unwrapCtxByteScanner:
		if innerByteScanner := s.unwrapByteScanner(); innerByteScanner != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerByteScanner.(unwrapByteScanner); ok {
				if wrapped := wrapper.unwrapByteScanner(); wrapped != nil {
					return &noTraceCtxByteScannerWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxByteScannerWrapper{s}
}

type noTraceCtxByteScannerWrapper struct {
	io.ByteScanner
}

func (s *noTraceCtxByteScannerWrapper) ReadByte(ctx context.Context) (c byte, err error) {
	return s.ByteScanner.ReadByte()
}
func (s *noTraceCtxByteScannerWrapper) UnreadByte() (err error) {
	return s.ByteScanner.UnreadByte()
}
func (s *noTraceCtxByteScannerWrapper) unwrapByteScanner() io.ByteScanner {
	return s.ByteScanner
}

// Wrappers to allow interoperability of ByteWriters between io and ctxio

type ByteWriter interface {
	WriteByte(ctx context.Context, c byte) (err error)
}

func ByteWriterWrapper(ctx context.Context, w ByteWriter) io.ByteWriter {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapByteWriter:
		if innerByteWriter := w.unwrapByteWriter(); innerByteWriter != nil {
			if wrapper, ok := innerByteWriter.(unwrapCtxByteWriter); ok {
				if wrapped := wrapper.unwrapByteWriter(); wrapped != nil {
					return &byteWriterWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &byteWriterWrapper{w, ctx}
}

type byteWriterWrapper struct {
	ByteWriter
	ctx context.Context
}

func (w *byteWriterWrapper) WriteByte(c byte) (err error) {
	return w.ByteWriter.WriteByte(w.ctx, c)
}
func (w *byteWriterWrapper) unwrapByteWriter() ByteWriter {
	return w.ByteWriter
}

func CtxByteWriterWrapper(w io.ByteWriter) ByteWriter {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapCtxByteWriter:
		if innerByteWriter := w.unwrapByteWriter(); innerByteWriter != nil {
			return innerByteWriter
		}
	}
	return &ctxByteWriterWrapper{w}
}

type ctxByteWriterWrapper struct {
	io.ByteWriter
}

func (w *ctxByteWriterWrapper) WriteByte(ctx context.Context, c byte) (err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return w.ByteWriter.WriteByte(c)
}
func (w *ctxByteWriterWrapper) unwrapByteWriter() io.ByteWriter {
	return w.ByteWriter
}

func NoTraceCtxByteWriterWrapper(w io.ByteWriter) ByteWriter {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapCtxByteWriter:
		if innerByteWriter := w.unwrapByteWriter(); innerByteWriter != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerByteWriter.(unwrapByteWriter); ok {
				if wrapped := wrapper.unwrapByteWriter(); wrapped != nil {
					return &noTraceCtxByteWriterWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxByteWriterWrapper{w}
}

type noTraceCtxByteWriterWrapper struct {
	io.ByteWriter
}

func (w *noTraceCtxByteWriterWrapper) WriteByte(ctx context.Context, c byte) (err error) {
	return w.ByteWriter.WriteByte(c)
}
func (b *noTraceCtxByteWriterWrapper) unwrapByteWriter() io.ByteWriter {
	return b.ByteWriter
}

// Wrappers to allow interoperability of RuneReaders between io and ctxio

type RuneReader interface {
	ReadRune(ctx context.Context) (c rune, size int, err error)
}

func RuneReaderWrapper(ctx context.Context, r RuneReader) io.RuneReader {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapRuneReader:
		if innerRuneReader := r.unwrapRuneReader(); innerRuneReader != nil {
			if wrapper, ok := innerRuneReader.(unwrapCtxRuneReader); ok {
				if wrapped := wrapper.unwrapRuneReader(); wrapped != nil {
					return &runeReaderWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &runeReaderWrapper{r, ctx}
}

type runeReaderWrapper struct {
	RuneReader
	ctx context.Context
}

func (r *runeReaderWrapper) ReadRune() (c rune, size int, err error) {
	return r.RuneReader.ReadRune(r.ctx)
}
func (r *runeReaderWrapper) unwrapRuneReader() RuneReader {
	return r.RuneReader
}

func CtxRuneReaderWrapper(r io.RuneReader) RuneReader {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxRuneReader:
		if innerRuneReader := r.unwrapRuneReader(); innerRuneReader != nil {
			return innerRuneReader
		}
	}
	return &ctxRuneReaderWrapper{r}
}

type ctxRuneReaderWrapper struct {
	io.RuneReader
}

func (r *ctxRuneReaderWrapper) ReadRune(ctx context.Context) (c rune, size int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.RuneReader.ReadRune()
}
func (r *ctxRuneReaderWrapper) unwrapRuneReader() io.RuneReader {
	return r.RuneReader
}

func NoTraceCtxRuneReaderWrapper(r io.RuneReader) RuneReader {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxRuneReader:
		if innerRuneReader := r.unwrapRuneReader(); innerRuneReader != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerRuneReader.(unwrapRuneReader); ok {
				if wrapped := wrapper.unwrapRuneReader(); wrapped != nil {
					return &noTraceCtxRuneReaderWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxRuneReaderWrapper{r}
}

type noTraceCtxRuneReaderWrapper struct {
	io.RuneReader
}

func (r *noTraceCtxRuneReaderWrapper) ReadRune(ctx context.Context) (c rune, size int, err error) {
	return r.RuneReader.ReadRune()
}
func (r *noTraceCtxRuneReaderWrapper) unwrapRuneReader() io.RuneReader {
	return r.RuneReader
}

// Wrappers to allow interoperability of RuneScanners between io and ctxio

type RuneScanner interface {
	RuneReader
	UnreadRune() (err error)
}

func RuneScannerWrapper(ctx context.Context, s RuneScanner) io.RuneScanner {
	switch s := s.(type) {
	case nil:
		return nil
	case unwrapRuneScanner:
		if innerRuneScanner := s.unwrapRuneScanner(); innerRuneScanner != nil {
			if wrapper, ok := innerRuneScanner.(unwrapCtxRuneScanner); ok {
				if wrapped := wrapper.unwrapRuneScanner(); wrapped != nil {
					return &runeScannerWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &runeScannerWrapper{s, ctx}
}

type runeScannerWrapper struct {
	RuneScanner
	ctx context.Context
}

func (s *runeScannerWrapper) ReadRune() (c rune, size int, err error) {
	return s.RuneScanner.ReadRune(s.ctx)
}
func (s *runeScannerWrapper) UnreadRune() (err error) {
	return s.RuneScanner.UnreadRune()
}
func (s *runeScannerWrapper) unwrapRuneScanner() RuneScanner {
	return s.RuneScanner
}

func CtxRuneScannerWrapper(s io.RuneScanner) RuneScanner {
	switch s := s.(type) {
	case nil:
		return nil
	case unwrapCtxRuneScanner:
		if innerRuneScanner := s.unwrapRuneScanner(); innerRuneScanner != nil {
			return innerRuneScanner
		}
	}
	return &ctxRuneScannerWrapper{s}
}

type ctxRuneScannerWrapper struct {
	io.RuneScanner
}

func (s *ctxRuneScannerWrapper) ReadRune(ctx context.Context) (c rune, size int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return s.RuneScanner.ReadRune()
}
func (s *ctxRuneScannerWrapper) UnreadRune() (err error) {
	return s.RuneScanner.UnreadRune()
}
func (r *ctxRuneScannerWrapper) unwrapRuneScanner() io.RuneScanner {
	return r.RuneScanner
}

func NoTraceCtxRuneScannerWrapper(s io.RuneScanner) RuneScanner {
	switch s := s.(type) {
	case nil:
		return nil
	case unwrapCtxRuneScanner:
		if innerRuneScanner := s.unwrapRuneScanner(); innerRuneScanner != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerRuneScanner.(unwrapRuneScanner); ok {
				if wrapped := wrapper.unwrapRuneScanner(); wrapped != nil {
					return &noTraceCtxRuneScannerWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxRuneScannerWrapper{s}
}

type noTraceCtxRuneScannerWrapper struct {
	io.RuneScanner
}

func (s *noTraceCtxRuneScannerWrapper) ReadRune(ctx context.Context) (c rune, size int, err error) {
	return s.RuneScanner.ReadRune()
}
func (s *noTraceCtxRuneScannerWrapper) UnreadRune() (err error) {
	return s.RuneScanner.UnreadRune()
}
func (s *noTraceCtxRuneScannerWrapper) unwrapRuneScanner() io.RuneScanner {
	return s.RuneScanner
}

// Wrappers to allow interoperability of StringWriters between io and ctxio

type StringWriter interface {
	WriteString(ctx context.Context, s string) (n int, err error)
}

func StringWriterWrapper(ctx context.Context, w StringWriter) io.StringWriter {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapStringWriter:
		if innerStringWriter := w.unwrapStringWriter(); innerStringWriter != nil {
			if wrapper, ok := innerStringWriter.(unwrapCtxStringWriter); ok {
				if wrapped := wrapper.unwrapStringWriter(); wrapped != nil {
					return &stringWriterWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &stringWriterWrapper{w, ctx}
}

type stringWriterWrapper struct {
	StringWriter
	ctx context.Context
}

func (w *stringWriterWrapper) WriteString(s string) (n int, err error) {
	return w.StringWriter.WriteString(w.ctx, s)
}
func (w *stringWriterWrapper) unwrapStringWriter() StringWriter {
	return w.StringWriter
}

func CtxStringWriterWrapper(w io.StringWriter) StringWriter {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapCtxStringWriter:
		if innerStringWriter := w.unwrapStringWriter(); innerStringWriter != nil {
			return innerStringWriter
		}
	}
	return &ctxStringWriterWrapper{w}
}

type ctxStringWriterWrapper struct {
	io.StringWriter
}

func (w *ctxStringWriterWrapper) WriteString(ctx context.Context, s string) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return w.StringWriter.WriteString(s)
}
func (w *ctxStringWriterWrapper) unwrapStringWriter() io.StringWriter {
	return w.StringWriter
}

func NoTraceCtxStringWriterWrapper(w io.StringWriter) StringWriter {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapCtxStringWriter:
		if innerStringWriter := w.unwrapStringWriter(); innerStringWriter != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerStringWriter.(unwrapStringWriter); ok {
				if wrapped := wrapper.unwrapStringWriter(); wrapped != nil {
					return &noTraceCtxStringWriterWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxStringWriterWrapper{w}
}

type noTraceCtxStringWriterWrapper struct {
	io.StringWriter
}

func (w *noTraceCtxStringWriterWrapper) WriteString(ctx context.Context, s string) (n int, err error) {
	return w.StringWriter.WriteString(s)
}
func (w *noTraceCtxStringWriterWrapper) unwrapStringWriter() io.StringWriter {
	return w.StringWriter
}

// Wrapper to allow use of LimitedReaders for ctxio

type LimitedReader struct {
	R Reader // underlying reader
	N int64  // underlying reader
}

func (l *LimitedReader) Read(ctx context.Context, p []byte) (n int, err error) {
	return (&io.LimitedReader{R: ReaderWrapper(ctx, l.R), N: l.N}).Read(p)
}

// Wrappers to allow use of LimitReader, MultiReader, MultiWriter, TeeReader, and SectionReaders for ctxio

func LimitReader(r Reader, n int64) Reader {
	return &LimitedReader{R: r, N: n}
}

type multiReader struct {
	wrappedReaders []*readerWrapper
	multiReader    io.Reader
}

func (m *multiReader) Read(ctx context.Context, p []byte) (n int, err error) {
	for _, e := range m.wrappedReaders {
		e.ctx = ctx
	}
	return m.multiReader.Read(p)
}

func MultiReader(readers ...Reader) Reader {
	ioReaders := make([]io.Reader, len(readers))
	for i, r := range readers {
		ioReaders[i] = ReaderWrapper(context.Background(), r)
	}
	wrappedReaders := make([]*readerWrapper, len(readers))
	for i, r := range ioReaders {
		switch r := r.(type) {
		case *readerWrapper:
			wrappedReaders[i] = r
		}
	}
	return &multiReader{wrappedReaders: wrappedReaders, multiReader: io.MultiReader(ioReaders...)}
}

type multiWriter struct {
	wrappedWriters []*writerWrapper
	multiWriter    io.Writer
}

func (m *multiWriter) Write(ctx context.Context, p []byte) (n int, err error) {
	for _, e := range m.wrappedWriters {
		e.ctx = ctx
	}
	return m.multiWriter.Write(p)
}

func MultiWriter(writers ...Writer) Writer {
	ioWriters := make([]io.Writer, len(writers))
	for i, w := range writers {
		ioWriters[i] = WriterWrapper(context.Background(), w)
	}
	wrappedWriters := make([]*writerWrapper, len(writers))
	for i, w := range ioWriters {
		if w, ok := w.(*writerWrapper); ok {
			wrappedWriters[i] = w
		}
	}
	return &multiWriter{wrappedWriters: wrappedWriters, multiWriter: io.MultiWriter(ioWriters...)}
}

type teeReader struct {
	r Reader
	w Writer
}

func (t *teeReader) Read(ctx context.Context, p []byte) (n int, err error) {
	return io.TeeReader(ReaderWrapper(ctx, t.r), WriterWrapper(ctx, t.w)).Read(p)
}

func TeeReader(r Reader, w Writer) Reader {
	return &teeReader{r: r, w: w}
}

type SectionReader struct {
	*readerAtWrapper
	io.SectionReader
}

func NewSectionReader(r ReaderAt, off int64, n int64) *SectionReader {
	var wrappedReader *readerAtWrapper
	switch r := ReaderAtWrapper(context.Background(), r).(type) {
	case *readerAtWrapper:
		wrappedReader = r
	}
	return &SectionReader{readerAtWrapper: wrappedReader, SectionReader: *io.NewSectionReader(wrappedReader, off, n)}
}

func (s *SectionReader) Read(ctx context.Context, p []byte) (n int, err error) {
	s.readerAtWrapper.ctx = ctx
	return s.SectionReader.Read(p)
}
func (s *SectionReader) ReadAt(ctx context.Context, p []byte, off int64) (n int, err error) {
	s.readerAtWrapper.ctx = ctx
	return s.SectionReader.ReadAt(p, off)
}
func (s *SectionReader) Seek(ctx context.Context, offset int64, whence int) (int64, error) {
	//don't bother to reset the context, SectionReader.Seek is not doing I/O
	return s.SectionReader.Seek(offset, whence)
}
func (s *SectionReader) Size() int64 {
	return s.SectionReader.Size()
}

// Wrappers to allow interoperability of ReaderAts between io and ctxio

type ReaderAt interface {
	ReadAt(ctx context.Context, b []byte, off int64) (n int, err error)
}

func ReaderAtWrapper(ctx context.Context, r ReaderAt) io.ReaderAt {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapReaderAt:
		if innerReaderAt := r.unwrapReaderAt(); innerReaderAt != nil {
			if wrapper, ok := innerReaderAt.(unwrapCtxReaderAt); ok {
				if wrapped := wrapper.unwrapReaderAt(); wrapped != nil {
					return &readerAtWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &readerAtWrapper{r, ctx}
}

type readerAtWrapper struct {
	ReaderAt
	ctx context.Context
}

func (r *readerAtWrapper) ReadAt(b []byte, off int64) (n int, err error) {
	return r.ReaderAt.ReadAt(r.ctx, b, off)
}
func (r *readerAtWrapper) unwrapReaderAt() ReaderAt {
	return r.ReaderAt
}

func CtxReaderAtWrapper(r io.ReaderAt) ReaderAt {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReaderAt:
		if innerReaderAt := r.unwrapReaderAt(); innerReaderAt != nil {
			return innerReaderAt
		}
	}
	return &ctxReaderAtWrapper{r}
}

type ctxReaderAtWrapper struct {
	io.ReaderAt
}

func (r *ctxReaderAtWrapper) ReadAt(ctx context.Context, b []byte, off int64) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return r.ReaderAt.ReadAt(b, off)
}
func (r *ctxReaderAtWrapper) unwrapReaderAt() io.ReaderAt {
	return r.ReaderAt
}

func NoTraceCtxReaderAtWrapper(r io.ReaderAt) ReaderAt {
	switch r := r.(type) {
	case nil:
		return nil
	case unwrapCtxReaderAt:
		if innerReaderAt := r.unwrapReaderAt(); innerReaderAt != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerReaderAt.(unwrapReaderAt); ok {
				if wrapped := wrapper.unwrapReaderAt(); wrapped != nil {
					return &noTraceCtxReaderAtWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxReaderAtWrapper{r}
}

type noTraceCtxReaderAtWrapper struct {
	io.ReaderAt
}

func (r *noTraceCtxReaderAtWrapper) ReadAt(ctx context.Context, b []byte, off int64) (n int, err error) {
	return r.ReaderAt.ReadAt(b, off)
}
func (r *noTraceCtxReaderAtWrapper) unwrapReaderAt() io.ReaderAt {
	return r.ReaderAt
}

// Wrappers to allow interoperability of WriterAts between io and ctxio

type WriterAt interface {
	WriteAt(ctx context.Context, b []byte, off int64) (n int, err error)
}

func WriterAtWrapper(ctx context.Context, w WriterAt) io.WriterAt {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapWriterAt:
		if innerWriterAt := w.unwrapWriterAt(); innerWriterAt != nil {
			if wrapper, ok := innerWriterAt.(unwrapCtxWriterAt); ok {
				if wrapped := wrapper.unwrapWriterAt(); wrapped != nil {
					return &writerAtWrapper{wrapped, ctx}
				}
			}
		}
	}
	return &writerAtWrapper{w, ctx}
}

type writerAtWrapper struct {
	WriterAt
	ctx context.Context
}

func (w *writerAtWrapper) WriteAt(b []byte, off int64) (n int, err error) {
	return w.WriterAt.WriteAt(w.ctx, b, off)
}
func (w *writerAtWrapper) unwrapWriterAt() WriterAt {
	return w.WriterAt
}

func CtxWriterAtWrapper(w io.WriterAt) WriterAt {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapCtxWriterAt:
		if innerWriterAt := w.unwrapWriterAt(); innerWriterAt != nil {
			return innerWriterAt
		}
	}
	return &ctxWriterAtWrapper{w}
}

type ctxWriterAtWrapper struct {
	io.WriterAt
}

func (w *ctxWriterAtWrapper) WriteAt(ctx context.Context, b []byte, off int64) (n int, err error) {
	_, spn := span.StartSpan(ctx)
	defer spn.End()
	return w.WriterAt.WriteAt(b, off)
}
func (w *ctxWriterAtWrapper) unwrapWriterAt() io.WriterAt {
	return w.WriterAt
}

func NoTraceCtxWriterAtWrapper(w io.WriterAt) WriterAt {
	switch w := w.(type) {
	case nil:
		return nil
	case unwrapCtxWriterAt:
		if innerWriterAt := w.unwrapWriterAt(); innerWriterAt != nil {
			// inner struct may trace, but if it's a wrapper we can re-wrap its contents with no trace
			if wrapper, ok := innerWriterAt.(unwrapWriterAt); ok {
				if wrapped := wrapper.unwrapWriterAt(); wrapped != nil {
					return &noTraceCtxWriterAtWrapper{wrapped}
				}
			}
		}
	}
	return &noTraceCtxWriterAtWrapper{w}
}

type noTraceCtxWriterAtWrapper struct {
	io.WriterAt
}

func (w *noTraceCtxWriterAtWrapper) WriteAt(ctx context.Context, b []byte, off int64) (n int, err error) {
	return w.WriterAt.WriteAt(b, off)
}
func (w *noTraceCtxWriterAtWrapper) unwrapWriterAt() io.WriterAt {
	return w.WriterAt
}

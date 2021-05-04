package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

const (
	mb = 1 << 20
)

type Chunkstore struct {
	internalBlobstore interfaces.Blobstore
}

func New(blobstore interfaces.Blobstore) *Chunkstore {
	return &Chunkstore{internalBlobstore: blobstore}
}

func (c *Chunkstore) BlobExists(ctx context.Context, blobName string) (bool, error) {
	return c.chunkExists(ctx, blobName, 0)
}

func (c *Chunkstore) ReadBlob(ctx context.Context, blobName string) ([]byte, error) {
	if data, err := io.ReadAll(c.Reader(ctx, blobName)); err != nil {
		return []byte{}, err
	} else {
		return data, nil
	}
}

func (c *Chunkstore) WriteBlob(ctx context.Context, blobName string, data []byte) error {
	return c.WriteBlobWithBlockSize(ctx, blobName, data, 1*mb)
}

func (c *Chunkstore) WriteBlobWithBlockSize(ctx context.Context, blobName string, data []byte, blockSize int) error {
	c.DeleteBlob(ctx, blobName)
	w := c.Writer(ctx, blobName, blockSize, 5*time.Second)
	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Close()
}

func (c *Chunkstore) DeleteBlob(ctx context.Context, blobName string) error {
	index := uint16(0)
	for {
		if exists, err := c.chunkExists(ctx, blobName, index); err != nil {
			return err
		} else if !exists {
			return nil
		}
		if err := c.deleteChunk(ctx, blobName, index); err != nil {
			return err
		}
		index++
	}
}

func chunkName(blobName string, index uint16) string {
	return blobName + "_" + fmt.Sprintf("%04x", index)
}

func (c *Chunkstore) chunkExists(ctx context.Context, blobName string, index uint16) (bool, error) {
	return c.internalBlobstore.BlobExists(ctx, chunkName(blobName, index))
}

func (c *Chunkstore) readChunk(ctx context.Context, blobName string, index uint16) ([]byte, error) {
	return c.internalBlobstore.ReadBlob(ctx, chunkName(blobName, index))
}

func (c *Chunkstore) writeChunk(ctx context.Context, blobName string, index uint16, data []byte) (int, error) {
	return c.internalBlobstore.WriteBlob(ctx, chunkName(blobName, index), data)
}

func (c *Chunkstore) deleteChunk(ctx context.Context, blobName string, index uint16) error {
	return c.internalBlobstore.DeleteBlob(ctx, chunkName(blobName, index))
}

type chunkstoreReader struct {
	ctx        context.Context
	chunkstore *Chunkstore
	blobName   string
	chunk      []byte
	off        int64
	chunkOff   int
	chunkIndex uint16
}

func (r *chunkstoreReader) advanceOffset(adv int64) {
	r.chunkOff += int(adv)
	r.off += adv
}

func (r *chunkstoreReader) copyToReadBuffer(p []byte) int {
	if r.chunkOff >= len(r.chunk) {
		return 0
	}
	bytesRead := copy(p, r.chunk[r.chunkOff:])
	r.advanceOffset(int64(bytesRead))
	return bytesRead
}

func (r *chunkstoreReader) nextChunkExists() (bool, error) {
	return r.chunkstore.chunkExists(r.ctx, r.blobName, r.chunkIndex+1)
}

func (r *chunkstoreReader) getNextChunk() error {
	if exists, err := r.nextChunkExists(); err != nil {
		return err
	} else if !exists {
		return fmt.Errorf("Opening %v: %w", chunkName(r.blobName, r.chunkIndex+1), os.ErrNotExist)
	}
	r.chunkIndex++
	r.advanceOffset(int64(len(r.chunk) - r.chunkOff))
	r.chunkOff -= len(r.chunk)
	var err error
	r.chunk, err = r.chunkstore.readChunk(r.ctx, r.blobName, r.chunkIndex)
	return err
}

func (r *chunkstoreReader) eof() (bool, error) {
	if r.chunkOff < len(r.chunk) {
		return false, nil
	}
	exists, err := r.nextChunkExists()
	return !exists, err
}

func (r *chunkstoreReader) Read(p []byte) (int, error) {
	bytesRead := r.copyToReadBuffer(p)
	for bytesRead < len(p) {
		err := r.getNextChunk()
		bytesRead += r.copyToReadBuffer(p[bytesRead:])
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && r.chunkIndex != math.MaxUint16 {
				return bytesRead, io.EOF
			}
			return bytesRead, err
		}
	}
	if eof, err := r.eof(); err != nil {
		return bytesRead, err
	} else if eof {
		return bytesRead, io.EOF
	}
	return bytesRead, nil
}

func (r *chunkstoreReader) Seek(offset int64, whence int) (int64, error) {
	if offset == r.off {
		return r.off, nil
	}
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset = r.off + offset
	case io.SeekEnd:
		for {
			if exists, err := r.nextChunkExists(); err != nil {
				return r.off, err
			} else if !exists {
				break
			}
			r.getNextChunk()
		}
		offset += r.off + int64(len(r.chunk)-r.chunkOff)
	default:
		return r.off, syscall.EINVAL
	}

	if offset < 0 {
		return r.off, syscall.EINVAL
	}

	if offset >= r.off-int64(r.chunkOff) && offset < r.off+int64(len(r.chunk)-r.chunkOff) {
		r.advanceOffset(offset - r.off)
		return r.off, nil
	}

	if offset < r.off {
		r.chunkIndex = math.MaxUint16
		r.chunk = []byte{}
		r.off = 0
		r.chunkOff = 0
	}
	r.advanceOffset(offset - r.off)
	return r.off, nil
}

func (r *chunkstoreReader) Close() error {
	r.blobName = ""
	r.chunk = nil
	r.ctx = nil
	r.chunkstore = nil
	return nil
}

func (c *Chunkstore) Reader(ctx context.Context, blobName string) *chunkstoreReader {
	return &chunkstoreReader{chunkstore: c, chunkIndex: math.MaxUint16, chunk: []byte{}, blobName: blobName, ctx: ctx}
}

type writeResult struct {
	err  error
	size int
}

type chunkstoreWriter struct {
	ctx                context.Context
	chunkstore         *Chunkstore
	writeChannel       chan []byte
	writeResultChannel chan writeResult
	blobName           string
	blockSize          int
	timeoutDuration    time.Duration
}

func (w *chunkstoreWriter) Write(p []byte) (int, error) {
	w.writeChannel <- p
	select {
	case result, open := <-w.writeResultChannel:
		if !open {
			return 0, os.ErrClosed
		}
		return result.size, result.err
	case <-time.After(20 * time.Second):
		return 0, context.DeadlineExceeded
	}
}

func (w *chunkstoreWriter) Flush() (int, error) {
	w.writeChannel <- nil
	select {
	case result, open := <-w.writeResultChannel:
		if !open {
			return 0, os.ErrClosed
		}
		return result.size, result.err
	case <-time.After(20 * time.Second):
		return 0, context.DeadlineExceeded
	}
}

func (w *chunkstoreWriter) Close() error {
	close(w.writeChannel)
	select {
	case result, open := <-w.writeResultChannel:
		if !open {
			return fmt.Errorf("Error closing %v: %w", w.blobName, os.ErrClosed)
		}
		return result.err
	case <-time.After(w.timeoutDuration):
		return os.ErrDeadlineExceeded
	}
}

func (w *chunkstoreWriter) writeLoop() {
	chunkIndex := uint16(0)
	chunk := []byte{}
	never := time.Unix(1<<63-62135596801, 999999999)
	flushTime := never
	var writeError error
	bytesFlushed := 0
	closed := false
	for !closed {
		timeout := false
		select {
		case p, open := <-w.writeChannel:
			if !open {
				closed = true
			}
			if open && p == nil {
				flushTime = time.Unix(0, 0)
			} else {
				chunk = append(chunk, p...)
			}
		case <-time.After(time.Until(flushTime)):
			timeout = true
		case <-w.ctx.Done():
			closed = true
		}
		for len(chunk) >= w.blockSize {
			bytesWritten, err := w.chunkstore.writeChunk(w.ctx, w.blobName, chunkIndex, chunk[:w.blockSize])
			bytesFlushed += bytesWritten
			chunk = chunk[bytesWritten:]
			if bytesWritten > 0 {
				chunkIndex++
			}
			if err != nil {
				writeError = err
				break
			}
		}
		if writeError == nil && ((closed && chunkIndex == 0) || (len(chunk) > 0 && (time.Now().After(flushTime) || closed))) {
			fmt.Printf("Flush time is %v, Flushing %v\n", flushTime, chunk)
			bytesWritten, err := w.chunkstore.writeChunk(w.ctx, w.blobName, chunkIndex, chunk)
			bytesFlushed += bytesWritten
			chunk = chunk[bytesWritten:]
			if bytesWritten > 0 {
				chunkIndex++
			}
			if err != nil {
				writeError = err
			}
		}
		if len(chunk) == 0 {
			flushTime = never
		} else if time.Until(flushTime) > (w.timeoutDuration) {
			flushTime = time.Now().Add(w.timeoutDuration)
		}
		if timeout {
			continue
		}
		w.writeResultChannel <- writeResult{size: bytesFlushed, err: writeError}
		writeError = nil
		bytesFlushed = 0
	}
	close(w.writeResultChannel)
}

func (c *Chunkstore) Writer(ctx context.Context, blobName string, blockSize int, timeoutDuration time.Duration) *chunkstoreWriter {
	writer := &chunkstoreWriter{
		chunkstore:         c,
		ctx:                ctx,
		blobName:           blobName,
		blockSize:          blockSize,
		timeoutDuration:    timeoutDuration,
		writeChannel:       make(chan []byte),
		writeResultChannel: make(chan writeResult),
	}

	go writer.writeLoop()
	return writer
}

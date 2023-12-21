package protofile

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	// Max number of goroutines to use for fetching blobs in a single stream.
	fetcherConcurrency = 10
)

// BufferedProtoWriter chunks together and writes protos to blobstore after
// a chunk exceeds maxBufferSizeBytes size. The caller is responsible for
// calling Flush to ensure all data is written.
type BufferedProtoWriter struct {
	lastWriteTime       time.Time
	bs                  interfaces.Blobstore
	writeBuf            *bytes.Buffer
	streamID            string
	maxBufferSizeBytes  int
	writeSequenceNumber int
	writeMutex          sync.Mutex // protects(writeBuf), protects(writeSequenceNumber), protects(lastWriteTime)
}

// MessageAllocator returns a new, empty proto message of a fixed type.
type MessageAllocator func() proto.Message

// BufferedProtoReader reads the chunks written by BufferedProtoWriter. Callers
// should call ReadProto until it returns io.EOF, at which point the stream
// has been exhausted.
type BufferedProtoReader struct {
	bs       interfaces.Blobstore
	fetcher  *fetcher
	readBuf  *bytes.Buffer
	streamID string
	// Buffered messages that have been fetched and unmarshaled.
	buffer []proto.Message
}

func NewBufferedProtoReader(bs interfaces.Blobstore, streamID string, allocator MessageAllocator) *BufferedProtoReader {
	return &BufferedProtoReader{
		streamID: streamID,
		bs:       bs,
		fetcher:  newFetcher(bs, streamID, allocator),
	}
}

func NewBufferedProtoWriter(bs interfaces.Blobstore, streamID string, bufferSizeBytes int) *BufferedProtoWriter {
	return &BufferedProtoWriter{
		streamID:           streamID,
		bs:                 bs,
		maxBufferSizeBytes: bufferSizeBytes,

		writeBuf:            bytes.NewBuffer(make([]byte, 0, bufferSizeBytes)),
		writeSequenceNumber: 0,
		lastWriteTime:       time.Now(),
	}
}

func ChunkName(streamID string, sequenceNumber int) string {
	chunkFileName := fmt.Sprintf("%s-%d.chunk", streamID, sequenceNumber)
	return filepath.Join(streamID, "/chunks/", chunkFileName)
}

func DeleteExistingChunks(ctx context.Context, bs interfaces.Blobstore, streamID string) error {
	// delete blobs from back to front so that this process is recoverable in case
	// of failure (delete error, server crash, etc.)
	var blobsToDelete []string
	for i := 0; ; i++ {
		blobName := ChunkName(streamID, i)
		if exists, err := bs.BlobExists(ctx, blobName); err != nil {
			return err
		} else if !exists {
			break
		}
		blobsToDelete = append([]string{blobName}, blobsToDelete...)
	}
	for _, blobName := range blobsToDelete {
		if err := bs.DeleteBlob(ctx, blobName); err != nil {
			return err
		}
	}
	return nil
}

func (w *BufferedProtoWriter) internalFlush(ctx context.Context) error {
	if w.writeBuf.Len() == 0 {
		return nil
	}

	tmpFilePath := ChunkName(w.streamID, w.writeSequenceNumber)
	if _, err := w.bs.WriteBlob(ctx, tmpFilePath, w.writeBuf.Bytes()); err != nil {
		return err
	}

	w.writeSequenceNumber += 1
	w.lastWriteTime = time.Now()
	w.writeBuf.Reset()
	return nil
}

func (w *BufferedProtoWriter) Flush(ctx context.Context) error {
	w.writeMutex.Lock()
	defer w.writeMutex.Unlock()
	return w.internalFlush(ctx)
}

func (w *BufferedProtoWriter) TimeSinceLastWrite() time.Duration {
	return time.Since(w.lastWriteTime)
}

func (w *BufferedProtoWriter) WriteProtoToStream(ctx context.Context, msg proto.Message) error {
	w.writeMutex.Lock()
	defer w.writeMutex.Unlock()

	protoBytes, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	msgLength := int64(len(protoBytes))
	varintBuf := make([]byte, binary.MaxVarintLen64)
	varintSize := binary.PutVarint(varintBuf, msgLength)
	// Write a header-chunk to know how big the proto is
	if _, err := w.writeBuf.Write(varintBuf[:varintSize]); err != nil {
		return err
	}
	// Then write the proto itself.
	if _, err := w.writeBuf.Write(protoBytes); err != nil {
		return err
	}

	// Flush, if we need to.
	if w.writeBuf.Len() > w.maxBufferSizeBytes {
		return w.internalFlush(ctx)
	}
	return nil
}

type fetchRequest struct {
	Index        int
	ResponseChan chan *fetchResponse
}

type fetchResponse struct {
	Messages []proto.Message
	Error    error
}

// fetcher fetches and unmarshals a sequence of blobs concurrently.
type fetcher struct {
	blobstore interfaces.Blobstore
	streamID  string
	allocator MessageAllocator

	requests chan *fetchRequest
	stop     func()
	// Whether the fetcher has stopped fetching due to an error being returned
	// from Next().
	stopped bool
}

func newFetcher(blobstore interfaces.Blobstore, streamID string, allocator MessageAllocator) *fetcher {
	fetcher := &fetcher{
		blobstore: blobstore,
		streamID:  streamID,
		allocator: allocator,
	}
	return fetcher
}

// Starts a background goroutine to spawn fetches until either the context is
// done or a request returns an error that is surfaced in Next().
func (f *fetcher) start(ctx context.Context) {
	f.requests = make(chan *fetchRequest, fetcherConcurrency)
	ctx, cancel := context.WithCancel(ctx)
	f.stop = func() {
		cancel()
		f.stopped = true
	}
	go func() {
		for i := 0; true; i++ {
			req := &fetchRequest{
				Index:        i,
				ResponseChan: make(chan *fetchResponse, 1),
			}
			select {
			case <-ctx.Done():
				return
			case f.requests <- req:
			}
			go func() {
				data, err := f.blobstore.ReadBlob(ctx, ChunkName(f.streamID, req.Index))
				if err != nil {
					req.ResponseChan <- &fetchResponse{Error: err}
					return
				}
				messages, err := f.unmarshalBlob(data)
				req.ResponseChan <- &fetchResponse{
					Messages: messages,
					Error:    err,
				}
			}()
		}
	}()
}

func (f *fetcher) unmarshalBlob(b []byte) ([]proto.Message, error) {
	buf := bytes.NewBuffer(b)
	var messages []proto.Message
	for buf.Len() > 0 {
		recordLength, err := binary.ReadVarint(buf)
		if err != nil {
			return nil, err
		}
		recordContent := buf.Next(int(recordLength))
		msg := f.allocator()
		if err := proto.Unmarshal(recordContent, msg); err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

// Next returns the next blob in the sequence.
func (f *fetcher) Next(ctx context.Context) ([]proto.Message, error) {
	// The first time Next() is called, start fetching.
	if f.requests == nil {
		f.start(ctx)
	}
	if f.stopped {
		return nil, status.ResourceExhaustedError("fetcher has already completed fetching")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case req := <-f.requests:
		res := <-req.ResponseChan
		if err := res.Error; err != nil {
			f.stop()
			return nil, err
		}
		return res.Messages, nil
	}
}

func (w *BufferedProtoReader) ReadProto(ctx context.Context) (proto.Message, error) {
	for len(w.buffer) == 0 {
		// Wait for next blob in the sequence to be fetched and unmarshaled.
		messages, err := w.fetcher.Next(ctx)
		if err != nil {
			if status.IsNotFoundError(err) {
				return nil, io.EOF
			}
			return nil, err
		}
		w.buffer = messages
	}
	next := w.buffer[0]
	w.buffer = w.buffer[1:]
	return next, nil
}

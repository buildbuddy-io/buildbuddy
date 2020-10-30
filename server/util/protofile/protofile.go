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
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
)

// BufferedProtoWriter chunks together and writes protos to blobstore after
// a chunk exceeds maxBufferSizeBytes size. The caller is responsible for
// calling Flush to ensure all data is written.
type BufferedProtoWriter struct {
	streamID string
	bs       interfaces.Blobstore

	maxBufferSizeBytes int

	// Write Variables
	writeMutex          sync.Mutex // protects(writeBuf), protects(writeSequenceNumber), protects(lastWriteTime)
	writeBuf            *bytes.Buffer
	writeSequenceNumber int
	lastWriteTime       time.Time
}

// BufferedProtoReader reads the chunks written by BufferedProtoWriter. Callers
// should call ReadProto until it returns io.EOF, at which point the stream
// has been exhausted.
// N.B. This *DOES NOT* guarantee that a caller has read all data for a
// streamID, because it may still be written to from another goroutine.
type BufferedProtoReader struct {
	streamID string
	bs       interfaces.Blobstore
	q        *blobQueue

	// Read Variables
	readBuf *bytes.Buffer
}

func NewBufferedProtoReader(bs interfaces.Blobstore, streamID string) *BufferedProtoReader {
	return &BufferedProtoReader{
		streamID: streamID,
		bs:       bs,
		q:        newBlobQueue(bs, streamID),
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

func chunkName(streamID string, sequenceNumber int) string {
	chunkFileName := fmt.Sprintf("%s-%d.chunk", streamID, sequenceNumber)
	return filepath.Join(streamID, "/chunks/", chunkFileName)
}

func (w *BufferedProtoWriter) internalFlush(ctx context.Context) error {
	if w.writeBuf.Len() == 0 {
		return nil
	}

	tmpFilePath := chunkName(w.streamID, w.writeSequenceNumber)
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
	return time.Now().Sub(w.lastWriteTime)
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

type blobReadResult struct {
	data []byte
	err  error
}

type blobFuture chan blobReadResult

type blobQueue struct {
	blobstore      interfaces.Blobstore
	streamID       string
	maxConnections int
	futures        []blobFuture
	numPopped      int
	done           bool
}

func newBlobQueue(blobstore interfaces.Blobstore, streamID string) *blobQueue {
	return &blobQueue{
		blobstore:      blobstore,
		streamID:       streamID,
		maxConnections: 8,
		futures:        make([]blobFuture, 0),
		numPopped:      0,
		done:           false,
	}
}

func newBlobFuture() blobFuture {
	return make(blobFuture, 1)
}

func (q *blobQueue) pushNewFuture(ctx context.Context) {
	future := newBlobFuture()
	sequenceNumber := len(q.futures)
	q.futures = append(q.futures, future)
	go func() {
		defer close(future)

		tmpFilePath := chunkName(q.streamID, sequenceNumber)
		data, err := q.blobstore.ReadBlob(ctx, tmpFilePath)
		if err != nil {
			future <- blobReadResult{
				data: nil,
				err:  err,
			}
			return
		}

		future <- blobReadResult{
			data: data,
			err:  nil,
		}
	}()
}

func (q *blobQueue) pop(ctx context.Context) ([]byte, error) {
	if q.done {
		return nil, status.FailedPreconditionError("Queue has been exhausted.")
	}
	// Make sure maxConnections files are downloading
	numLoading := len(q.futures) - q.numPopped
	numNewConnections := q.maxConnections - numLoading
	for numNewConnections > 0 {
		q.pushNewFuture(ctx)
		numNewConnections--
	}
	result := <-q.futures[q.numPopped]
	q.numPopped++
	if result.err != nil {
		q.done = true
		return nil, result.err
	}
	return result.data, nil
}

func (w *BufferedProtoReader) ReadProto(ctx context.Context, msg proto.Message) error {
	for {
		if w.readBuf == nil {
			// Load file
			fileData, err := w.q.pop(ctx)
			if err != nil {
				return io.EOF
			}
			w.readBuf = bytes.NewBuffer(fileData)
		}
		// read proto from buf
		count, err := binary.ReadVarint(w.readBuf)
		if err != nil {
			w.readBuf = nil
			continue
		}
		protoBytes := make([]byte, count)
		w.readBuf.Read(protoBytes)
		if err := proto.Unmarshal(protoBytes, msg); err != nil {
			return err
		}
		return nil
	}
}

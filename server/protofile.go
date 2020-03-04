package protofile

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
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
	writeMutex          sync.Mutex // protects(writeBuf), protects(writeSequenceNumber)
	writeBuf            *bytes.Buffer
	writeSequenceNumber int
}

// BufferedProtoReader reads the chunks written by BufferedProtoWriter. Callers
// should call ReadProto until it returns io.EOF, at which point the stream
// has been exhausted.
// N.B. This *DOES NOT* guarantee that a caller has read all data for a
// streamID, because it may still be written to from another goroutine.
type BufferedProtoReader struct {
	streamID string
	bs       interfaces.Blobstore

	// Read Variables
	readMutex          sync.Mutex // protects(readBuf), protects(readSequenceNumber)
	readBuf            *bytes.Buffer
	readSequenceNumber int
}

func NewBufferedProtoReader(bs interfaces.Blobstore, streamID string) *BufferedProtoReader {
	return &BufferedProtoReader{
		streamID: streamID,
		bs:       bs,

		readBuf:            new(bytes.Buffer),
		readSequenceNumber: 0,
	}
}

func NewBufferedProtoWriter(bs interfaces.Blobstore, streamID string, bufferSizeBytes int) *BufferedProtoWriter {
	return &BufferedProtoWriter{
		streamID:           streamID,
		bs:                 bs,
		maxBufferSizeBytes: bufferSizeBytes,

		writeBuf:            bytes.NewBuffer(make([]byte, 0, bufferSizeBytes)),
		writeSequenceNumber: 0,
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
	if err := w.bs.WriteBlob(ctx, tmpFilePath, w.writeBuf.Bytes()); err != nil {
		return err
	}

	w.writeSequenceNumber += 1
	w.writeBuf.Reset()
	return nil
}

func (w *BufferedProtoWriter) Flush(ctx context.Context) error {
	w.writeMutex.Lock()
	defer w.writeMutex.Unlock()
	return w.internalFlush(ctx)
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

func (w *BufferedProtoReader) ReadProto(ctx context.Context, msg proto.Message) error {
	for {
		if w.readBuf == nil {
			// Load file
			tmpFilePath := chunkName(w.streamID, w.readSequenceNumber)
			fileData, err := w.bs.ReadBlob(ctx, tmpFilePath)
			if err != nil {
				return io.EOF
			}
			w.readBuf = bytes.NewBuffer(fileData)
			w.readSequenceNumber += 1
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

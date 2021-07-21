package eventlog

import (
	"bufio"
	"bytes"
	"context"
	"math"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"

	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	ansi "github.com/leaanthony/go-ansi-parser"
)

const (
	// Chunks will be flushed to blobstore when they reach this size.
	defaultLogChunkSize = 2_000_000 // 2MB

	// Chunks will also be flushed to blobstore after this much time
	// passes with no new data being written.
	defaultChunkTimeout = 15 * time.Second
)

func GetEventLogPathFromInvocationId(invocationId string) string {
	return invocationId + "/chunks/log/eventlog"
}

// Gets the chunk of the event log specified by the request from the blobstore and returns a response containing it
func GetEventLogChunk(ctx context.Context, env environment.Env, req *elpb.GetEventLogChunkRequest) (*elpb.GetEventLogChunkResponse, error) {
	inv, err := env.GetInvocationDB().LookupInvocation(ctx, req.GetInvocationId())
	if err != nil {
		return nil, err
	}

	invocationInProgress := inv.InvocationStatus == int64(inpb.Invocation_PARTIAL_INVOCATION_STATUS)
	c := chunkstore.New(env.GetBlobstore(), &chunkstore.ChunkstoreOptions{})
	eventLogPath := GetEventLogPathFromInvocationId(req.InvocationId)
	startingIndex, _ := chunkstore.ChunkIndexAsUint16(inv.LastChunkId)

	lastChunkIndex, err := c.GetLastChunkIndex(ctx, eventLogPath, startingIndex)
	if err != nil {
		if startingIndex == math.MaxUint16 {
			// No chunks have been written for this invocation
			if invocationInProgress {
				// If the invocation is in progress, logs may be written in the future.
				// Return an empty chunk with NextChunkId set to 0.
				return &elpb.GetEventLogChunkResponse{
					Chunk: &elpb.GetEventLogChunkResponse_Chunk{
						Buffer: make([]byte, 0),
					},
					NextChunkId: chunkstore.ChunkIndexAsString(0),
				}, nil
			} else {
				// If the invocation is not in progress, no logs will ever exist for
				// this invocation. Return an empty chunk with NextChunkId left blank.
				return &elpb.GetEventLogChunkResponse{
					Chunk: &elpb.GetEventLogChunkResponse_Chunk{
						Buffer: make([]byte, 0),
					},
				}, nil
			}
		} else {
			// The last chunk id recorded in the invocation table is wrong. Most
			// likely, the logs were deleted.
			return nil, err
		}
	}

	chunkIndex := lastChunkIndex
	if len(req.ChunkId) > 0 {
		var err error
		if chunkIndex, err = chunkstore.ChunkIndexAsUint16(req.ChunkId); err != nil {
			return nil, err
		}
		if chunkIndex > lastChunkIndex {
			if invocationInProgress || chunkIndex == math.MaxUint16 {
				// If out-of-bounds and the invocation is in-progress, or if the invalid
				// chunk is requested, return an empty chunk with NextChunkId set to the
				// id of the next chunk to be written and PreviousChunkId set to the id
				// of the last chunk.
				return &elpb.GetEventLogChunkResponse{
					Chunk: &elpb.GetEventLogChunkResponse_Chunk{
						Buffer: make([]byte, 0),
					},
					PreviousChunkId: chunkstore.ChunkIndexAsString(lastChunkIndex),
					NextChunkId:     chunkstore.ChunkIndexAsString(lastChunkIndex + 1),
				}, nil
			} else {
				// If out-of-bounds and the invocation is not in-progress, return an
				// empty chunk with NextChunkId left blank and PreviousChunkId set to
				// the id of the last chunk.
				return &elpb.GetEventLogChunkResponse{
					Chunk: &elpb.GetEventLogChunkResponse_Chunk{
						Buffer: make([]byte, 0),
					},
					PreviousChunkId: chunkstore.ChunkIndexAsString(lastChunkIndex),
				}, nil
			}
		}
	}

	rsp := &elpb.GetEventLogChunkResponse{
		Chunk: &elpb.GetEventLogChunkResponse_Chunk{
			ChunkId: chunkstore.ChunkIndexAsString(chunkIndex),
			Buffer:  make([]byte, 0),
		},
		NextChunkId:     chunkstore.ChunkIndexAsString(chunkIndex + 1),
		PreviousChunkId: chunkstore.ChunkIndexAsString(chunkIndex - 1),
	}

	readBackward := false
	if req.ReadBackward || req.ChunkId == "" {
		readBackward = true
	}
	boundary := lastChunkIndex + 1
	step := uint16(1)
	if readBackward {
		boundary = math.MaxUint16
		step = math.MaxUint16 // decrements the value when added
	}
	// Fetch one chunk even if the minimum line count is 0
	for lineCount := 0; (rsp.Chunk.ChunkId != "" || lineCount < int(req.MinLines)) && chunkIndex != boundary; chunkIndex += step {
		buffer, err := c.ReadChunk(ctx, eventLogPath, chunkIndex)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(bytes.NewReader(buffer))
		for scanner.Scan() {
			if scanner.Err() != nil {
				return nil, err
			}
			lineCount++
		}
		if readBackward {
			rsp.Chunk.Buffer = append(buffer, rsp.Chunk.Buffer...)
		} else {
			rsp.Chunk.Buffer = append(rsp.Chunk.Buffer, buffer...)
		}
		if rsp.Chunk.ChunkId != chunkstore.ChunkIndexAsString(chunkIndex) {
			// No longer fetching a single chunk, ChunkId cannot be meaningfully set.
			rsp.Chunk.ChunkId = ""
		}
	}
	if readBackward {
		rsp.PreviousChunkId = chunkstore.ChunkIndexAsString(chunkIndex)
	} else {
		rsp.NextChunkId = chunkstore.ChunkIndexAsString(chunkIndex)
	}

	invalidChunkId := chunkstore.ChunkIndexAsString(math.MaxUint16)
	if rsp.NextChunkId == invalidChunkId {
		rsp.NextChunkId = ""
	}
	if rsp.PreviousChunkId == invalidChunkId {
		rsp.PreviousChunkId = ""
	}

	return rsp, nil
}

func NewEventLogWriter(ctx context.Context, b interfaces.Blobstore, invocationId string) *EventLogWriter {
	return &EventLogWriter{
		chunkstore.New(
			b,
			&chunkstore.ChunkstoreOptions{
				WriteBlockSize:       defaultLogChunkSize,
				WriteTimeoutDuration: defaultChunkTimeout,
				NoSplitWrite:         true,
			},
		).Writer(ctx, GetEventLogPathFromInvocationId(invocationId)),
	}
}

type EventLogWriter struct {
	*chunkstore.ChunkstoreWriter
}

func (w *EventLogWriter) WriteLines(p []byte) (int, error) {
	bytesWritten := 0
	scanner := bufio.NewScanner(bytes.NewReader(p))
	if scanner.Err() != nil {
		return bytesWritten, scanner.Err()
	}
	line := scanner.Bytes()
	for scanner.Scan() {
		n, err := w.ChunkstoreWriter.Write(append(line, '\n'))
		bytesWritten += n
		if err != nil {
			return bytesWritten, err
		}
		if scanner.Err() != nil {
			return bytesWritten, scanner.Err()
		}
		line = scanner.Bytes()
	}
	if len(p) != 0 && p[len(p)-1] == '\n' {
		line = append(line, '\n')
	}
	n, err := w.ChunkstoreWriter.Write(append(line, '\n'))
	bytesWritten += n
	return bytesWritten, err
}

// Split ANSI text by line, rewriting ANSI control sequences when necessary such
// that any line printed individually is still formatted as it was in the
// original buffer
func (w *EventLogWriter) WriteLinesANSI(p []byte) (int, error) {
	s := string(p)
	if !ansi.HasEscapeCodes(s) {
		return w.WriteLines(p)
	}
	remainingSegments, err := ansi.Parse(string(s))
	// If parsing fails just fall back to normal write lines.
	if err != nil {
		return w.WriteLines(p)
	}
	bytesWritten := 0
	remainingSegmentsIndex := 0
	for remainingSegmentsIndex < len(remainingSegments) {
		segment := remainingSegments[remainingSegmentsIndex]
		label := segment.Label
		if len(label) == 0 {
			remainingSegmentsIndex += 1
			continue
		}
		scanner := bufio.NewScanner(strings.NewReader(label))
		scanner.Scan()
		if scanner.Err() != nil {
			return bytesWritten, scanner.Err()
		}
		line := scanner.Text()
		for scanner.Scan() {
			segment.Label = line
			n, err := w.ChunkstoreWriter.Write(append([]byte(ansi.String(remainingSegments[:remainingSegmentsIndex+1])), '\n'))
			bytesWritten += n
			if err != nil {
				return bytesWritten, err
			}
			remainingSegments = remainingSegments[remainingSegmentsIndex:]
			remainingSegmentsIndex = 0
			if scanner.Err() != nil {
				return bytesWritten, scanner.Err()
			}
			line = scanner.Text()
		}
		segment.Label = line
		if strings.HasSuffix(label, "\n") {
			n, err := w.ChunkstoreWriter.Write(append([]byte(ansi.String(remainingSegments[:remainingSegmentsIndex+1])), '\n'))
			bytesWritten += n
			if err != nil {
				return bytesWritten, err
			}
			remainingSegments = remainingSegments[remainingSegmentsIndex+1:]
			remainingSegmentsIndex = 0
		} else {
			remainingSegmentsIndex += 1
		}
	}
	if len(remainingSegments) != 0 {
		n, err := w.ChunkstoreWriter.Write([]byte(ansi.String(remainingSegments)))
		bytesWritten += n
		if err != nil {
			return bytesWritten, err
		}
	}
	return bytesWritten, nil
}

func (w *EventLogWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return w.WriteLinesANSI(p)
}

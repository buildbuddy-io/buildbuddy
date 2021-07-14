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
	ansi "github.com/leaanthony/go-ansi-parser"
)

const (
	// Chunks will be flushed to blobstore when they reach this size.
	defaultLogChunkSize = 2_000_000 // 2MB

	// Chunks will also be flushed to blobstore after this much time
	// passes with no new data being written.
	defaultChunkTimeout = 300 * time.Millisecond
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

	c := chunkstore.New(env.GetBlobstore(), &chunkstore.ChunkstoreOptions{})
	eventLogPath := GetEventLogPathFromInvocationId(req.InvocationId)
	var intChunkId uint16
	if len(req.ChunkId) > 0 {
		var err error
		if intChunkId, err = chunkstore.ChunkIndexAsUint16(req.ChunkId); err != nil {
			return nil, err
		}
	} else {
		startingIndex, _ := chunkstore.ChunkIndexAsUint16(inv.LastChunkId)
		var err error
		if intChunkId, err = c.GetLastChunkIndex(ctx, eventLogPath, startingIndex); err != nil {
			return nil, err
		}
	}

	rsp := &elpb.GetEventLogChunkResponse{
		Chunk: &elpb.GetEventLogChunkResponse_Chunk{
			ChunkId: chunkstore.ChunkIndexAsString(intChunkId),
			Lines: make([][]byte, 0),
		},
	}
	if intChunkId < math.MaxInt16-1 {
		rsp.NextChunkId = chunkstore.ChunkIndexAsString(intChunkId + 1)
	}
	if intChunkId > 0 {
		rsp.PreviousChunkId = chunkstore.ChunkIndexAsString(intChunkId - 1)
	}

	for {
		buffer, err := c.ReadChunk(ctx, eventLogPath, intChunkId)
		if err != nil {
			return nil, err
		}
		lines := make([][]byte, 0)
		scanner := bufio.NewScanner(bytes.NewReader(buffer))
		for scanner.Scan() {
			if scanner.Err() != nil {
				return nil, err
			}
			lines = append(lines, scanner.Bytes())
		}
		rsp.Chunk.Lines = append(lines, rsp.Chunk.Lines...)
		if len(rsp.Chunk.Lines) >= int(req.MinLines) {
			break
		} else if req.ReadBackward || req.ChunkId == "" {
			// If the client specified a backwards Read or the client requested the tail of the log
			if intChunkId == 0 {
				rsp.PreviousChunkId = ""
				break
			}
			intChunkId--
			// No longer fetching a single chunk, ChunkId cannot be menaingfully set.
			rsp.Chunk.ChunkId = ""
			rsp.PreviousChunkId = chunkstore.ChunkIndexAsString(intChunkId - 1)
		} else {
			if intChunkId == math.MaxInt16 - 1 {
				rsp.NextChunkId = ""
				break
			}
			exists, err := c.ChunkExists(ctx, eventLogPath, intChunkId+1)
			if err != nil {
				return nil, err
			}
			if !exists {
				break
			}
			intChunkId++
			// No longer fetching a single chunk, ChunkId cannot be menaingfully set.
			rsp.Chunk.ChunkId = ""
			rsp.NextChunkId = chunkstore.ChunkIndexAsString(intChunkId + 1)
		}
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

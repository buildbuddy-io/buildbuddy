package eventlog

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"math"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/terminal"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

const (
	// Chunks will be flushed to blobstore when they reach this size.
	defaultLogChunkSize = 2_000_000 // 2MB

	// Chunks will also be flushed to blobstore after this much time
	// passes with no new data being written.
	defaultChunkTimeout = 15 * time.Second

	// Number of lines to keep in the screen buffer so that they may be modified
	// by ANSI Cursor control codes.
	linesToRetainForANSICursor = 6
)

func getEventLogPathFromInvocationId(invocationId string) string {
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
	eventLogPath := getEventLogPathFromInvocationId(req.InvocationId)

	lastChunkId, err := c.GetLastChunkId(ctx, eventLogPath, inv.LastChunkId)
	if err != nil {
		if inv.LastChunkId != chunkstore.ChunkIndexAsStringId(math.MaxUint16) {
			// The last chunk id recorded in the invocation table is wrong; the only
			// valid reason for GetLastChunkId to fail with the starting index
			// recorded in the invocation table is if no chunks have yet been written,
			// in which case the starting index is equal to invalid chunk id. Most
			// likely, the logs were deleted.
			return nil, err
		}

		// No chunks have been written for this invocation
		if invocationInProgress {
			// If the invocation is in progress, logs may be written in the future.
			// Return an empty chunk with NextChunkId set to 0.
			return &elpb.GetEventLogChunkResponse{
				Buffer:      []byte{},
				NextChunkId: chunkstore.ChunkIndexAsStringId(0),
			}, nil
		}
		// The invocation is not in progress, no logs will ever exist for this
		// invocation. Return an empty chunk with NextChunkId left blank.
		return &elpb.GetEventLogChunkResponse{
			Buffer: []byte{},
		}, nil
	}
	lastChunkIndex, err := chunkstore.ChunkIdAsUint16Index(lastChunkId)
	if err != nil {
		return nil, err
	}

	startIndex := lastChunkIndex
	if req.ChunkId != "" {
		var err error
		if startIndex, err = chunkstore.ChunkIdAsUint16Index(req.ChunkId); err != nil {
			return nil, err
		}
		if startIndex > lastChunkIndex {
			if startIndex == math.MaxUint16 {
				// The client requested the invalid id; this is an error.
				return nil, status.ResourceExhaustedErrorf("Log index limit exceeded.")
			}
			// If out-of-bounds, return an empty chunk with PreviousChunkId set to the
			// id of the last chunk.
			rsp := &elpb.GetEventLogChunkResponse{
				Buffer:          []byte{},
				PreviousChunkId: chunkstore.ChunkIndexAsStringId(lastChunkIndex),
			}
			if invocationInProgress {
				// If out-of-bounds and the invocation is in-progress, set NextChunkId
				// to the id of the next chunk to be written.
				rsp.NextChunkId = chunkstore.ChunkIndexAsStringId(lastChunkIndex + 1)
			}
			return rsp, nil
		}
	}

	rsp := &elpb.GetEventLogChunkResponse{
		Buffer:          []byte{},
		NextChunkId:     chunkstore.ChunkIndexAsStringId(startIndex + 1),
		PreviousChunkId: chunkstore.ChunkIndexAsStringId(startIndex - 1),
	}

	boundary := lastChunkIndex
	step := uint16(1)
	if req.ChunkId == "" {
		boundary = 0
		step = math.MaxUint16 // decrements the value when added
	}
	lineCount := 0
	// Fetch one chunk even if the minimum line count is 0
	for chunkIndex := startIndex; chunkIndex != boundary+step; chunkIndex += step {
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
		if step == 1 {
			rsp.Buffer = append(rsp.Buffer, buffer...)
			rsp.NextChunkId = chunkstore.ChunkIndexAsStringId(chunkIndex + step)
		} else {
			rsp.Buffer = append(buffer, rsp.Buffer...)
			rsp.PreviousChunkId = chunkstore.ChunkIndexAsStringId(chunkIndex + step)
		}
		if lineCount >= int(req.MinLines) {
			break
		}
	}

	if rsp.PreviousChunkId == chunkstore.ChunkIndexAsStringId(math.MaxUint16) {
		rsp.PreviousChunkId = ""
	}

	return rsp, nil
}

func NewEventLogWriter(ctx context.Context, b interfaces.Blobstore, invocationId string) *EventLogWriter {
	cw := chunkstore.New(
		b,
		&chunkstore.ChunkstoreOptions{
			WriteBlockSize:       defaultLogChunkSize,
			WriteTimeoutDuration: defaultChunkTimeout,
			NoSplitWrite:         true,
		},
	).Writer(ctx, getEventLogPathFromInvocationId(invocationId))
	return &EventLogWriter{
		WriteCloser: &ANSICursorBufferWriter{
			WriteCloser:  cw,
			screenWriter: terminal.NewScreenWriter(),
		},
		ChunkstoreWriter: cw,
	}
}

type EventLogWriter struct {
	io.WriteCloser
	ChunkstoreWriter *chunkstore.ChunkstoreWriter
}

func (w *EventLogWriter) Write(p []byte) (int, error) {
	return w.WriteCloser.Write(p)
}

type ANSICursorBufferWriter struct {
	io.WriteCloser
	screenWriter *terminal.ScreenWriter
}

func (w *ANSICursorBufferWriter) Write(p []byte) (int, error) {
	if p == nil || len(p) == 0 {
		return w.WriteCloser.Write(p)
	}
	if _, err := w.screenWriter.Write(p); err != nil {
		return 0, err
	}
	popped := w.screenWriter.PopExtraLinesAsANSI(linesToRetainForANSICursor)
	if len(popped) == 0 {
		return 0, nil
	}
	return w.WriteCloser.Write(append(popped, '\n'))
}

func (w *ANSICursorBufferWriter) Close() error {
	if _, err := w.WriteCloser.Write(w.screenWriter.RenderAsANSI()); err != nil {
		return err
	}
	return w.WriteCloser.Close()
}

type ANSILineWriter struct {
	io.WriteCloser
}

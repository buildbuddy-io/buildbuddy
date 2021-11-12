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
	"github.com/buildbuddy-io/buildbuddy/server/util/keyval"
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
	linesToRetainForANSICursor = 10

	// Max number of workers to run in parallel when fetching chunks.
	numReadWorkers = 16
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

	if inv.LastChunkId == "" {
		return &elpb.GetEventLogChunkResponse{}, nil
	}

	invocationInProgress := inv.InvocationStatus == int64(inpb.Invocation_PARTIAL_INVOCATION_STATUS)
	c := chunkstore.New(env.GetBlobstore(), &chunkstore.ChunkstoreOptions{})
	eventLogPath := GetEventLogPathFromInvocationId(req.InvocationId)

	// Get the id of the last chunk on disk after the last id stored in the db
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
			// If the invocation is in progress and the chunk requested is not on
			// disk, check the cache to see if the live chunk is being requested.
			liveChunk := &elpb.LiveEventLogChunk{}
			if err := keyval.GetProto(ctx, env.GetKeyValStore(), eventLogPath, liveChunk); err == nil {
				if req.ChunkId == liveChunk.ChunkId {
					return &elpb.GetEventLogChunkResponse{
						Buffer:      liveChunk.Buffer,
						NextChunkId: liveChunk.ChunkId,
						Live:        true,
					}, nil
				}
			} else if !status.IsNotFoundError(err) {
				return nil, err
			}
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

		if startIndex == math.MaxUint16 {
			// The client requested the invalid id; this is an error.
			return nil, status.ResourceExhaustedErrorf("Log index limit exceeded.")
		}

		if startIndex > lastChunkIndex {
			if invocationInProgress {
				// If the invocation is in progress and the chunk requested is not on
				// disk, check the cache to see if the live chunk is being requested.
				liveChunk := &elpb.LiveEventLogChunk{}
				if err := keyval.GetProto(ctx, env.GetKeyValStore(), eventLogPath, liveChunk); err == nil {
					if chunkstore.ChunkIndexAsStringId(startIndex) == liveChunk.ChunkId {
						return &elpb.GetEventLogChunkResponse{
							Buffer:      liveChunk.Buffer,
							NextChunkId: liveChunk.ChunkId,
							Live:        true,
						}, nil
					}
				} else if !status.IsNotFoundError(err) {
					return nil, err
				}
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
	q := newChunkQueue(c, eventLogPath, startIndex, step, boundary)
	lineCount := 0
	// Fetch one chunk even if the minimum line count is 0
	for chunkIndex := startIndex; chunkIndex != boundary+step; chunkIndex += step {
		buffer, err := q.pop(ctx)
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

type chunkReadResult struct {
	data []byte
	err  error
}

type chunkFuture chan chunkReadResult

func newChunkFuture() chunkFuture {
	return make(chunkFuture, 1)
}

type chunkQueue struct {
	store          *chunkstore.Chunkstore
	maxConnections int

	eventLogPath string
	start        uint16
	step         uint16
	boundary     uint16

	futures   []chunkFuture
	numPopped int
	done      bool
}

func newChunkQueue(c *chunkstore.Chunkstore, eventLogPath string, start, step, boundary uint16) *chunkQueue {
	return &chunkQueue{
		store:          c,
		maxConnections: numReadWorkers,
		eventLogPath:   eventLogPath,
		start:          start,
		step:           step,
		boundary:       boundary,
	}
}

func (q *chunkQueue) pushNewFuture(ctx context.Context, index uint16) {
	future := newChunkFuture()
	q.futures = append(q.futures, future)

	if index == q.boundary+q.step {
		future <- chunkReadResult{err: io.EOF}
		return
	}

	go func() {
		defer close(future)

		data, err := q.store.ReadChunk(ctx, q.eventLogPath, index)
		future <- chunkReadResult{
			data: data,
			err:  err,
		}
	}()
}

func (q *chunkQueue) pop(ctx context.Context) ([]byte, error) {
	if q.done {
		return nil, status.ResourceExhaustedError("Queue has been exhausted.")
	}
	// Make sure maxConnections files are downloading
	numLoading := len(q.futures) - q.numPopped
	numNewConnections := q.maxConnections - numLoading
	for numNewConnections > 0 {
		index := q.start + uint16(len(q.futures))*q.step
		q.pushNewFuture(ctx, index)
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

func NewEventLogWriter(ctx context.Context, b interfaces.Blobstore, c interfaces.KeyValStore, invocationId string) *EventLogWriter {
	eventLogPath := GetEventLogPathFromInvocationId(invocationId)
	chunkstoreOptions := &chunkstore.ChunkstoreOptions{
		WriteBlockSize: defaultLogChunkSize,
	}
	var writeHook func(ctx context.Context, writeRequest *chunkstore.WriteRequest, writeResult *chunkstore.WriteResult, chunk []byte, volatileTail []byte)
	if c != nil {
		writeHook = func(ctx context.Context, writeRequest *chunkstore.WriteRequest, writeResult *chunkstore.WriteResult, chunk []byte, volatileTail []byte) {
			if writeResult.Close {
				keyval.SetProto(ctx, c, eventLogPath, nil)
				return
			}
			chunkId := chunkstore.ChunkIndexAsStringId(writeResult.LastChunkIndex + 1)
			if chunkId == chunkstore.ChunkIndexAsStringId(math.MaxUint16) {
				keyval.SetProto(ctx, c, eventLogPath, nil)
				return
			}
			keyval.SetProto(
				ctx,
				c,
				eventLogPath,
				&elpb.LiveEventLogChunk{
					ChunkId: chunkId,
					Buffer:  append(chunk, volatileTail...),
				},
			)
			return
		}
	}
	chunkstoreWriterOptions := &chunkstore.ChunkstoreWriterOptions{
		WriteTimeoutDuration: defaultChunkTimeout,
		NoSplitWrite:         true,
		WriteHook:            writeHook,
	}
	cw := chunkstore.New(b, chunkstoreOptions).Writer(ctx, eventLogPath, chunkstoreWriterOptions)
	return &EventLogWriter{
		WriteCloserWithContext: &ANSICursorBufferWriter{
			WriteWithTailCloser: cw,
			screenWriter:        terminal.NewScreenWriter(),
		},
		chunkstoreWriter: cw,
	}
}

type WriteCloserWithContext interface {
	Write(context.Context, []byte) (int, error)
	Close(context.Context) error
}

type EventLogWriter struct {
	WriteCloserWithContext
	chunkstoreWriter *chunkstore.ChunkstoreWriter
}

func (w *EventLogWriter) GetLastChunkId(ctx context.Context) string {
	return chunkstore.ChunkIndexAsStringId(w.chunkstoreWriter.GetLastChunkIndex(ctx))
}

type WriteWithTailCloser interface {
	Close(context.Context) error
	WriteWithTail(context.Context, []byte, []byte) (int, error)
}

// Parses text passed into it as ANSI text and flushes it to the WriteCloser,
// retaining a buffer of the last N lines. On Close, all lines are flushed. This
// is necessary so that ANSI cursor control sequences can freely modify the last
// N lines.
type ANSICursorBufferWriter struct {
	WriteWithTailCloser
	screenWriter *terminal.ScreenWriter
}

func (w *ANSICursorBufferWriter) Write(ctx context.Context, p []byte) (int, error) {
	if p == nil || len(p) == 0 {
		return w.WriteWithTailCloser.WriteWithTail(ctx, p, nil)
	}
	if _, err := w.screenWriter.Write(p); err != nil {
		return 0, err
	}
	popped := w.screenWriter.PopExtraLinesAsANSI(linesToRetainForANSICursor)
	if len(popped) != 0 {
		popped = append(popped, '\n')
	}
	return w.WriteWithTailCloser.WriteWithTail(ctx, popped, w.screenWriter.RenderAsANSI())
}

func (w *ANSICursorBufferWriter) Close(ctx context.Context) error {
	return w.WriteWithTailCloser.Close(ctx)
}

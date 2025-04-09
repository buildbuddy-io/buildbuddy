package eventlog

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/keyval"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/terminal"

	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
)

const (
	// Chunks will be flushed to blobstore when they reach this size.
	defaultLogChunkSize = 2_000_000 // 2MB

	// Chunks will also be flushed to blobstore after this much time
	// passes with no new data being written.
	defaultChunkTimeout = 15 * time.Second

	// Max number of workers to run in parallel when fetching chunks.
	numReadWorkers = 16
)

var (
	//Id of an empty log
	// TODO(zoey): actually use this in the file; it's clearer.
	EmptyId = chunkstore.ChunkIndexAsStringId(chunkstore.EmptyIndex)

	// Max size of the buffer in the EventLogChunkResponse returned by GetEventLogChunk
	MaxBufferSize = defaultLogChunkSize * 16
)

func GetEventLogPathFromInvocationIdAndAttempt(invocationId string, attempt uint64) string {
	if attempt == 0 {
		// This invocation predates the attempt-tracking functionality, so its logs
		// are not in a corresponding subdirectory.
		return invocationId + "/chunks/log/eventlog"
	}
	return invocationId + "/" + strconv.FormatUint(attempt, 10) + "/chunks/log/eventlog"
}

func GetEventLogPubSubChannel(invocationID string) string {
	return fmt.Sprintf("eventlog/%s/updates", invocationID)
}

// Gets the chunk of the event log specified by the request from the blobstore and returns a response containing it
func GetEventLogChunk(ctx context.Context, env environment.Env, req *elpb.GetEventLogChunkRequest) (*elpb.GetEventLogChunkResponse, error) {
	// TODO(zoey): this function is way too long; split it up.
	inv, err := env.GetInvocationDB().LookupInvocation(ctx, req.GetInvocationId())
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("invocation not found")
		}
		return nil, err
	}

	if inv.LastChunkId == "" {
		// This invocation does not have chunked event logs; return an empty
		// response to indicate that to the client.
		return &elpb.GetEventLogChunkResponse{}, nil
	}

	invocationInProgress := inv.InvocationStatus == int64(inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS)
	c := chunkstore.New(env.GetBlobstore(), &chunkstore.ChunkstoreOptions{})
	eventLogPath := GetEventLogPathFromInvocationIdAndAttempt(req.InvocationId, inv.Attempt)

	// Get the id of the last chunk on disk after the last id stored in the db
	lastChunkId, err := c.GetLastChunkId(ctx, eventLogPath, inv.LastChunkId)

	// TODO(zoey): this should check for the status.NotFoundError, as that is the
	// only one we can handle. Any other errors are real errors.
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

	// If the requested chunk ID is empty, we're returning the last N lines of the
	// blob instead of the first N lines beginning at the requested chunk ID, so
	// our starting chunkIndex is the last one we know about.
	startIndex := lastChunkIndex
	if req.ChunkId != "" {
		// The client requested a specific chunkID; we need to convert to a uint16
		// and then validate it as the ID of a fetchable chunk.
		var err error
		startIndex, err = chunkstore.ChunkIdAsUint16Index(req.ChunkId)
		if err != nil {
			return nil, err
		}

		if startIndex == math.MaxUint16 {
			// The client requested the invalid id; this is an error.
			return nil, status.ResourceExhaustedErrorf("Log index limit exceeded.")
		}

		if startIndex > lastChunkIndex {
			// The requested chunk ID is greater than the one that we had recorded on
			// disk when we populated lastChunkIndex above. We should check to see if
			// the client requested the chunk cached in redis, and return it if they
			// did.
			if invocationInProgress {
				// If the invocation is in progress and the chunk requested is not on
				// disk, check the cache to see if the live chunk is being requested.
				liveChunk := &elpb.LiveEventLogChunk{}
				if err := keyval.GetProto(ctx, env.GetKeyValStore(), eventLogPath, liveChunk); err == nil {
					// TODO(zoey): there is a potential race condition here where we
					// retrieve the last chunk ID on disk, then one or more live chunks
					// are written to disk and stored in the database, and then we
					// retrieve the new live chunk here. If the client requested a chunk
					// which was written to disk after we retrieved the last chunk ID,
					// the conditional below will fail and we will fall through to
					// returning an empty response with the last chunk ID we retrieved
					// above. This will result in an unnecessary reconnection by the
					// client to attempt to retrieve that chunk again. Instead, we should
					// do this check FIRST, and then check if the requested chunk ID is
					// less than the live chunk ID. If it is, we should validate it as a
					// fetchable chunk.
					//
					// The same logic applies for when we check the live chunk above.
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
		// The caller is requesting the last n lines instead of the first n lines;
		// We need to read from the end and go backwards until we have enough lines.
		boundary = 0
		step = math.MaxUint16 // decrements the value when added
	}
	q := newChunkQueue(c, eventLogPath, startIndex, step, boundary)
	lineCount := 0
	// Fetch one chunk even if the minimum line count is 0.
	// `step` should only ever be 1 or MaxUint16 (effectively -1), so as long as
	// `boundary` remains invariant, this loop is guaranteed to terminate in
	// 65536 iterations or fewer.
	for chunkIndex := startIndex; chunkIndex != boundary+step; chunkIndex += step {
		buffer, err := q.pop(ctx)
		if err != nil {
			return nil, err
		}
		if len(rsp.Buffer) > 0 && len(buffer)+len(rsp.Buffer) > MaxBufferSize {
			break
		}
		scanner := bufio.NewScanner(bytes.NewReader(buffer))
		for scanner.Scan() {
			if scanner.Err() != nil {
				return nil, err
			}
			lineCount++
		}
		if step == 1 {
			// Reading forwards, append the new lines
			rsp.Buffer = append(rsp.Buffer, buffer...)
			rsp.NextChunkId = chunkstore.ChunkIndexAsStringId(chunkIndex + step)
		} else {
			// Reading backwards, prepend the new lines
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
		// This future was the last valid index; append EOF.
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
		// the index of the next future is the index of the start plus or minus the
		// current length of the queue.
		index := q.start + uint16(len(q.futures))*q.step
		// TODO: we shouldn't push futures if we already pushed EOF; add an
		// indicator to q as to whether or not that has happened so that we can
		// break this this loop when we reach the end of the availiable chunks.
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

func NewEventLogWriter(ctx context.Context, b interfaces.Blobstore, c interfaces.KeyValStore, pubsub interfaces.PubSub, pubsubChannel string, eventLogPath string, numLinesToRetain int) (*EventLogWriter, error) {
	chunkstoreOptions := &chunkstore.ChunkstoreOptions{
		WriteBlockSize: defaultLogChunkSize,
	}
	eventLogWriter := &EventLogWriter{
		keyValueStore: c,
		pubsub:        pubsub,
		pubsubChannel: pubsubChannel,
		eventLogPath:  eventLogPath,
	}
	var writeHook func(ctx context.Context, writeRequest *chunkstore.WriteRequest, writeResult *chunkstore.WriteResult, chunk []byte, volatileTail []byte)
	if c != nil {
		writeHook = eventLogWriter.writeChunkToKeyValStore
	}
	chunkstoreWriterOptions := &chunkstore.ChunkstoreWriterOptions{
		WriteTimeoutDuration: defaultChunkTimeout,
		NoSplitWrite:         true,
		WriteHook:            writeHook,
	}
	cw := chunkstore.New(b, chunkstoreOptions).Writer(ctx, eventLogPath, chunkstoreWriterOptions)
	sw, err := terminal.NewScreenWriter(numLinesToRetain)
	if err != nil {
		return nil, err
	}
	eventLogWriter.WriteCloserWithContext = &ANSICursorBufferWriter{
		WriteWithTailCloser: cw,
		terminalWriter:      sw,
	}
	eventLogWriter.chunkstoreWriter = cw

	return eventLogWriter, nil
}

type WriteCloserWithContext interface {
	Write(context.Context, []byte) (int, error)
	Close(context.Context) error
}

type EventLogWriter struct {
	WriteCloserWithContext
	chunkstoreWriter *chunkstore.ChunkstoreWriter
	lastChunk        *elpb.LiveEventLogChunk
	keyValueStore    interfaces.KeyValStore
	pubsub           interfaces.PubSub
	pubsubChannel    string
	eventLogPath     string
}

func (w *EventLogWriter) writeChunkToKeyValStore(ctx context.Context, writeRequest *chunkstore.WriteRequest, writeResult *chunkstore.WriteResult, chunk []byte, volatileTail []byte) {
	if writeResult.Close {
		keyval.SetProto(ctx, w.keyValueStore, w.eventLogPath, nil)
		return
	}
	chunkId := chunkstore.ChunkIndexAsStringId(writeResult.LastChunkIndex + 1)
	if chunkId == chunkstore.ChunkIndexAsStringId(math.MaxUint16) {
		keyval.SetProto(ctx, w.keyValueStore, w.eventLogPath, nil)
		return
	}
	curChunk := &elpb.LiveEventLogChunk{
		ChunkId: chunkId,
		Buffer:  append(chunk, volatileTail...),
	}
	if proto.Equal(w.lastChunk, curChunk) {
		return
	}
	keyval.SetProto(
		ctx,
		w.keyValueStore,
		w.eventLogPath,
		curChunk,
	)
	w.lastChunk = curChunk
	if w.pubsub != nil {
		w.pubsub.Publish(ctx, w.pubsubChannel, "")
	}
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
	terminalWriter *terminal.ScreenWriter
}

func (w *ANSICursorBufferWriter) Write(ctx context.Context, p []byte) (int, error) {
	if len(p) == 0 {
		return w.WriteWithTailCloser.WriteWithTail(ctx, p, nil)
	}
	if _, err := w.terminalWriter.Write(p); err != nil {
		return 0, err
	}
	if !w.terminalWriter.AccumulatingOutput() {
		// We aren't accumulating scrolled-out output, which means this isn't using
		// curses. Just render the terminal and write the rendered output.
		n, err := w.WriteWithTailCloser.WriteWithTail(ctx, []byte(w.terminalWriter.Render()), []byte{})
		if err != nil {
			return 0, err
		}
		w.terminalWriter.Reset(0)
		return n, err
	}
	if w.terminalWriter.WriteErr != nil {
		return 0, w.terminalWriter.WriteErr
	}
	popped := w.terminalWriter.OutputAccumulator.String()
	w.terminalWriter.OutputAccumulator.Reset()
	return w.WriteWithTailCloser.WriteWithTail(ctx, []byte(popped), []byte(w.terminalWriter.Render()))
}

func (w *ANSICursorBufferWriter) Close(ctx context.Context) error {
	return w.WriteWithTailCloser.Close(ctx)
}

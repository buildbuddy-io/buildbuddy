package eventlog

import (
	"bufio"
	"bytes"
	"context"
	"math"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
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
	startingIndex, err := chunkstore.ChunkIdAsUint16Index(inv.LastChunkId)
	if err != nil {
		return nil, err
	}

	lastChunkId, err := c.GetLastChunkId(ctx, eventLogPath, inv.LastChunkId)
	if err != nil {
		if startingIndex != math.MaxUint16 {
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
				Chunk: &elpb.GetEventLogChunkResponse_Chunk{
					Buffer: make([]byte, 0),
				},
				NextChunkId: chunkstore.ChunkIndexAsStringId(0),
			}, nil
		}
		// The invocation is not in progress, no logs will ever exist for this
		// invocation. Return an empty chunk with NextChunkId left blank.
		return &elpb.GetEventLogChunkResponse{
			Chunk: &elpb.GetEventLogChunkResponse_Chunk{
				Buffer: make([]byte, 0),
			},
		}, nil
	}
	lastChunkIndex, err := chunkstore.ChunkIdAsUint16Index(lastChunkId)
	if err != nil {
		return nil, err
	}

	chunkIndex := lastChunkIndex
	if len(req.ChunkId) > 0 {
		var err error
		if chunkIndex, err = chunkstore.ChunkIdAsUint16Index(req.ChunkId); err != nil {
			return nil, err
		}
		if chunkIndex > lastChunkIndex {
			if chunkIndex == math.MaxUint16 {
				// The client requested the invalid id; this is an error.
				return nil, status.ResourceExhaustedErrorf("Log index limit exceeded.")
			}
			// If out-of-bounds, return an empty chunk with PreviousChunkId set to the
			// id of the last chunk.
			rsp := &elpb.GetEventLogChunkResponse{
				Chunk: &elpb.GetEventLogChunkResponse_Chunk{
					Buffer: make([]byte, 0),
				},
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
		Chunk: &elpb.GetEventLogChunkResponse_Chunk{
			ChunkId: chunkstore.ChunkIndexAsStringId(chunkIndex),
			Buffer:  make([]byte, 0),
		},
		NextChunkId:     chunkstore.ChunkIndexAsStringId(chunkIndex + 1),
		PreviousChunkId: chunkstore.ChunkIndexAsStringId(chunkIndex - 1),
	}

	readBackward := req.ReadBackward || req.ChunkId == ""
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
		if rsp.Chunk.ChunkId != chunkstore.ChunkIndexAsStringId(chunkIndex) {
			// No longer fetching a single chunk, ChunkId cannot be meaningfully set.
			rsp.Chunk.ChunkId = ""
		}
	}
	if readBackward {
		rsp.PreviousChunkId = chunkstore.ChunkIndexAsStringId(chunkIndex)
	} else {
		rsp.NextChunkId = chunkstore.ChunkIndexAsStringId(chunkIndex)
	}

	if rsp.PreviousChunkId == chunkstore.ChunkIndexAsStringId(math.MaxUint16) {
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
		).Writer(ctx, getEventLogPathFromInvocationId(invocationId)),
	}
}

type EventLogWriter struct {
	*chunkstore.ChunkstoreWriter
}

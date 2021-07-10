package eventlog

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/environment"

	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
)

func GetEventLogPathFromInvocationId(invocationId string) string {
	return invocationId + "/chunks/log/eventlog"
}

// Gets the chunk of the event log specified by the request from the blobstore and returns a response containing it
func GetEventLogChunk(ctx context.Context, env environment.Env, req *elpb.GetEventLogChunkRequest) (*elpb.GetEventLogChunkResponse, error) {
	// TODO: Check permissions

	c := chunkstore.New(env.GetBlobstore(), &chunkstore.ChunkstoreOptions{})
	var intChunkId uint16
	if len(req.ChunkId) > 0 {
		n, err := strconv.ParseUint(req.ChunkId, 16, 16)
		if err != nil {
			return nil, err
		}
		intChunkId = uint16(n)
	} else {
		lastChunkID, err := c.GetLastChunkIndex(ctx, GetEventLogPathFromInvocationId(req.GetInvocationId()))
		if err != nil {
			return nil, err
		}
		intChunkId = lastChunkID
	}

	rsp := &elpb.GetEventLogChunkResponse{
		Chunk: &elpb.GetEventLogChunkResponse_Chunk{
			ChunkId: fmt.Sprintf("%04x", intChunkId),
		},
	}
	var err error
	if rsp.Chunk.Buffer, err = c.ReadChunk(ctx, GetEventLogPathFromInvocationId(req.InvocationId), intChunkId); err != nil {
		return nil, err
	}
	if intChunkId > 0 {
		rsp.PreviousChunkId = fmt.Sprintf("%04x", intChunkId-1)
	}
	if intChunkId < math.MaxInt16 {
		rsp.NextChunkId = fmt.Sprintf("%04x", intChunkId+1)
	}

	print(rsp)
	return rsp, nil
}

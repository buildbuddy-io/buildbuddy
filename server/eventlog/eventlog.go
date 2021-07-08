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

func GetEventLogChunk(ctx context.Context, env environment.Env, req *elpb.GetEventLogChunkRequest) (*elpb.GetEventLogChunkResponse, error) {
	if _, err := env.GetInvocationDB().LookupInvocation(ctx, req.GetInvocationId()); err != nil {
		return nil, err
	}

	c := chunkstore.New(env.GetBlobstore(), &chunkstore.ChunkstoreOptions{})
	var intChunkId uint16
	if len(req.ChunkId) > 0 {
		n, err := strconv.ParseUint(req.ChunkId, 16, 16)
		if err != nil {
			return nil, err
		}
		intChunkId = uint16(n)
	} else {
		var err error
		if intChunkId, err = c.GetLastChunkIndex(ctx, req.InvocationId); err != nil {
			return nil, err
		}
	}

	resp := &elpb.GetEventLogChunkResponse{
		Chunk: &elpb.GetEventLogChunkResponse_Chunk{
			ChunkId: fmt.Sprintf("%04x", intChunkId),
		},
	}
	var err error
	if resp.Chunk.Buffer, err = c.ReadChunk(ctx, req.InvocationId+"/chunks/log/eventlog", intChunkId); err != nil {
		return nil, err
	}
	if intChunkId > 0 {
		resp.PreviousChunkId = fmt.Sprintf("%04x", intChunkId-1)
	}
	if intChunkId < math.MaxInt16 {
		resp.NextChunkId = fmt.Sprintf("%04x", intChunkId+1)
	}

	return resp, nil
}

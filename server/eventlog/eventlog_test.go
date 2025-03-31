package eventlog_test

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockinvocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockstore"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
)

// This test ensures that we stop continuing to accrue chunks in the response
// buffer if we go over the maximum buffer size.
func TestGetEventLogChunkMaxBufferSize(t *testing.T) {
	env := testenv.GetTestEnv(t)

	invocationDB := &mockinvocationdb.MockInvocationDB{DB: make(map[string]*tables.Invocation)}
	env.SetInvocationDB(invocationDB)

	// Make a bare bones test invocation in the DB
	testID := "test_id"
	invocationDB.DB[testID] = &tables.Invocation{
		InvocationID: testID,
		Attempt:      1,
		LastChunkId:  fmt.Sprintf("%04x", uint16(3)),
	}

	blobstore := mockstore.New()
	env.SetBlobstore(blobstore)

	blobPath := eventlog.GetEventLogPathFromInvocationIdAndAttempt(testID, 1)

	// Make a chunked log for the invocation. Chunked logs can have more chunks
	// than LastChunkId would suggest due to latency between blobstore writes
	// and db writes recording them.
	blobstore.BlobMap[chunkstore.ChunkName(blobPath, uint16(0))] = make([]byte, 8)
	blobstore.BlobMap[chunkstore.ChunkName(blobPath, uint16(1))] = make([]byte, 8)
	blobstore.BlobMap[chunkstore.ChunkName(blobPath, uint16(2))] = make([]byte, 8)
	blobstore.BlobMap[chunkstore.ChunkName(blobPath, uint16(3))] = make([]byte, 8)
	blobstore.BlobMap[chunkstore.ChunkName(blobPath, uint16(4))] = make([]byte, 8)
	blobstore.BlobMap[chunkstore.ChunkName(blobPath, uint16(5))] = make([]byte, 8)

	// Set a smaller MaxBufferSize to make testing clearer.
	oldMaxBufferSize := eventlog.MaxBufferSize
	eventlog.MaxBufferSize = 20
	t.Cleanup(func() {
		eventlog.MaxBufferSize = oldMaxBufferSize
	})

	// Request a truly absurd number of lines (though our test data is only one
	// line anyway) for the invocation logs, starting at chunk 0.
	rsp, err := eventlog.GetEventLogChunk(
		env.GetServerContext(),
		env,
		&elpb.GetEventLogChunkRequest{
			InvocationId: testID,
			ChunkId:      fmt.Sprintf("%04x", uint16(0)),
			MinLines:     1_000_000_000,
		},
	)
	require.NoError(t, err)

	// Each chunk is size 8, so with 20 MaxBufferSize, we should only retrieve
	// two chunks' worth of data (so 16 bytes).
	assert.Len(t, rsp.Buffer, 16)

	// If we retrieved two chunks starting at chunk 0, the next chunk should
	// be index 2, and the previous chunk should be empty.
	assert.Equal(t, "", rsp.PreviousChunkId)
	assert.Equal(t, fmt.Sprintf("%04x", 2), rsp.NextChunkId)

	// Request a truly absurd number of lines (though our test data is only one
	// line anyway) for the invocation logs, starting at the end.
	rsp, err = eventlog.GetEventLogChunk(
		env.GetServerContext(),
		env,
		&elpb.GetEventLogChunkRequest{
			InvocationId: testID,
			ChunkId:      "",
			MinLines:     1_000_000_000,
		},
	)
	require.NoError(t, err)

	// Each chunk is size 8, so with 20 MaxBufferSize, we should only retrieve
	// two chunks' worth of data (so 16 bytes).
	assert.Len(t, rsp.Buffer, 16)

	// If we retrieved two chunks starting at chunk 5, the previous chunk should
	// be index 3, and the next chunk should be index 6.
	assert.Equal(t, fmt.Sprintf("%04x", 3), rsp.PreviousChunkId)
	assert.Equal(t, fmt.Sprintf("%04x", 6), rsp.NextChunkId)
}

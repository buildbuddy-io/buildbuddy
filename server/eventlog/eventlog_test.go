package eventlog_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockinvocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockstore"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/terminal"
	"github.com/buildbuddy-io/buildbuddy/server/util/terminal/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
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

type testLog struct {
	head bytes.Buffer
	tail []byte
}

func (w *testLog) WriteWithTail(ctx context.Context, p []byte, tail []byte) (int, error) {
	w.head.Write(p)
	w.tail = tail
	return len(p), nil
}

func (w *testLog) Close(ctx context.Context) error {
	w.head.Write(w.tail)
	w.tail = nil
	return nil
}

func (w *testLog) String() string {
	return w.head.String() + string(w.tail)
}

func TestComplexScreenWriting(t *testing.T) {
	// Run each of the basic test cases both with a window height of 0 (no
	// curses) and 1 (with curses).
	var testCases []testdata.TestCase
	for _, windowHeight := range []int{0, 1, 2, 3} {
		for _, tc := range testdata.BasicScreenWritingTestcases {
			testCases = append(testCases, testdata.TestCase{
				Name:       fmt.Sprintf("WindowHeight=%d/%s", windowHeight, tc.Name),
				Write:      tc.Write,
				WantLog:    tc.WantLog,
				ScreenRows: windowHeight,
			})
		}
	}
	// Add some advanced test cases which use cursor positioning or which
	// are specifically testing certain screen dimensions.
	testCases = append(testCases, testdata.AdvancedScreenWritingTestcases...)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			log.SetOutput(&testLogOutput{t: t})
			// Disable timestamps in logs
			log.SetFlags(0)
			log.Printf("Running test case %q", tc.Name)
			ctx := context.Background()
			tl := &testLog{}
			screen, err := terminal.NewScreenWriter(math.MaxInt, tc.ScreenRows)
			require.NoError(t, err)
			if tc.ScreenCols > 0 {
				if tc.ScreenRows > 0 {
					screen.Screen.SetSize(tc.ScreenCols, tc.ScreenRows)
				} else {
					screen.Screen.SetSize(tc.ScreenCols, 32)
				}
			}
			w := eventlog.NewANSICursorBufferWriter(tl, screen)
			for _, s := range tc.Write {
				_, err = w.Write(ctx, []byte(s))
				require.NoError(t, err)
				require.NoError(t, screen.WriteErr)
			}
			require.Equal(t, ansiDebugString(tc.WantLog), ansiDebugString(tl.String()))
		})
	}
}

// ansiDebugString replaces ANSI escape sequences with a string representation
// that can be displayed as a diff without messing up the terminal.
func ansiDebugString(s string) string {
	// Quote the string, so escape sequences like \x1b[32m show up as text
	// instead of actual colors.
	content := fmt.Sprintf("%q", s)
	// Remove the surrounding quotes.
	content = content[1 : len(content)-1]
	// Preserve newlines so that it's easier to diff the expected vs. actual
	// output.
	content = strings.ReplaceAll(content, "\\n", "\n")
	return content
}

type testLogOutput struct{ t *testing.T }

func (w *testLogOutput) Write(p []byte) (int, error) {
	w.t.Log(string(p[:len(p)-1]))
	return len(p), nil
}

func TestGetEventLogChunk_RunLogs(t *testing.T) {
	env := testenv.GetTestEnv(t)

	invocationDB := &mockinvocationdb.MockInvocationDB{DB: make(map[string]*tables.Invocation)}
	env.SetInvocationDB(invocationDB)

	// Make a bare bones test invocation in the DB
	testID := "test_id"
	invocationDB.DB[testID] = &tables.Invocation{
		InvocationID: testID,
		RunStatus:    int64(inspb.OverallStatus_IN_PROGRESS),
	}

	blobstore := mockstore.New()
	env.SetBlobstore(blobstore)

	// Mock run logs.
	blobPath := eventlog.GetRunLogPathFromInvocationId(testID)
	blobstore.BlobMap[chunkstore.ChunkName(blobPath, uint16(0))] = []byte("Chunk 0\n")
	blobstore.BlobMap[chunkstore.ChunkName(blobPath, uint16(1))] = []byte("Chunk 1\n")

	rsp, err := eventlog.GetEventLogChunk(
		env.GetServerContext(),
		env,
		&elpb.GetEventLogChunkRequest{
			InvocationId: testID,
			MinLines:     5,
			Type:         elpb.LogType_RUN_LOG,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "Chunk 0\nChunk 1\n", string(rsp.Buffer))
}

package eventlog_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockinvocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockstore"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/terminal"
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
	type testCase struct {
		name           string
		screenRows     int
		screenCols     int
		initialContent string
		write          []string
		wantLog        string
	}
	basicTestCases := []testCase{
		{
			name:    "single write with space",
			write:   []string{" "},
			wantLog: " ",
		},
		{
			name:    "single blankline write",
			write:   []string{"\n"},
			wantLog: "\n",
		},
		{
			name:    "multiple blankline writes",
			write:   []string{"\n", "\n"},
			wantLog: "\n\n",
		},
		{
			name:    "multiple writes with multiple lines",
			write:   []string{"1\n", "2\n"},
			wantLog: "1\n2\n",
		},
		{
			name:    "single write with multiple lines",
			write:   []string{"1\n2\n"},
			wantLog: "1\n2\n",
		},
		{
			name:    "multiple writes with multiple lines and double-newline",
			write:   []string{"1\n\n", "2\n"},
			wantLog: "1\n\n2\n",
		},
		{
			name:    "multiple writes with mix of trailing and leading whitespace",
			write:   []string{"1", "2\n", "3\n ", "4\n\n", "5 \n", " 6"},
			wantLog: "12\n3\n 4\n\n5 \n 6",
		},
	}
	// Run each of the basic test cases both with a window height of 0 (no
	// curses) and 1 (with curses).
	var testCases []testCase
	for _, windowHeight := range []int{0, 1} {
		for _, tc := range basicTestCases {
			testCases = append(testCases, testCase{
				name:       fmt.Sprintf("WindowHeight=%d/%s", windowHeight, tc.name),
				write:      tc.write,
				wantLog:    tc.wantLog,
				screenRows: windowHeight,
			})
		}
	}
	// Add some advanced test cases which use cursor positioning or which
	// are specifically testing certain screen dimensions.
	const (
		CUU  = "\x1b[A"  // Cursor Up
		CHA  = "\x1b[G"  // Cursor Horizontal Absolute (go to column 1)
		EL_2 = "\x1b[2K" // Erase in line; 2="Entire line"
		ED_2 = "\x1b[2J" // Erase in display; 2="Entire display"
		CUP  = "\x1b[H"  // Cursor Position (go to row 1, column 1)
	)
	testCases = append(testCases, []testCase{
		{
			name:       "scrollout of single line wrapped to multiple rows",
			screenCols: 1,
			screenRows: 2,
			// The first line "ab" should be split and wrapped to the second row
			// since maxCols is 1. We should get "ab" back as a single line;
			// it should not be artificially wrapped.
			write:   []string{"ab\n", "c\n", "d"},
			wantLog: "ab\nc\nd",
		},
		{
			name:       "overwrite line contents",
			write:      []string{"123456789" + EL_2 + CHA + "ABC"},
			wantLog:    "ABC",
			screenRows: 1,
		},
		{
			name:       "overwrite line contents with newline",
			write:      []string{"123456789" + EL_2 + CHA + "ABC\n"},
			wantLog:    "ABC\n",
			screenRows: 1,
		},
		{
			name: "overwrite multiple line contents",
			write: []string{
				// Write two lines
				"123456789\n",
				"ABCDEFG\n",
				// Go back up to the first line
				CUU + CUU,
				// Clear the current line and write "Hello" on its own line
				EL_2 + CHA + "Hello\n",
				// Clear the current line and write "World" on its own line
				EL_2 + CHA + "World\n"},
			wantLog:    "Hello\nWorld\n",
			screenRows: 2,
		},
		{
			name: "overwrite multiple line contents via screen clearing",
			write: []string{
				// Write two lines
				"123456789\n",
				"ABCDEFG\n",
				// Clear screen and position cursor at top-left
				ED_2 + CUP,
				// Write "Hello" on its own line
				"Hello\n",
				// Write "World" on its own line
				"World\n"},
			wantLog:    "Hello\nWorld\n",
			screenRows: 2,
		},
		{
			name:       "reset sequence at end of line",
			write:      []string{"\x1b[32mINFO: ...\x1b[0m\n"},
			wantLog:    "\x1b[32mINFO: ...\x1b[m\n",
			screenRows: 1,
		},
	}...)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			log.SetOutput(&testLogOutput{t: t})
			// Disable timestamps in logs
			log.SetFlags(0)
			log.Printf("Running test case %q", tc.name)
			// Override columns
			// DO NOT MERGE - avoid monkey-patching
			if tc.screenCols != 0 {
				defaultCols := terminal.Columns
				terminal.Columns = tc.screenCols
				t.Cleanup(func() { terminal.Columns = defaultCols })
			}
			ctx := context.Background()
			tl := &testLog{}
			tl.head.WriteString(tc.initialContent)
			screen, err := terminal.NewScreenWriter(tc.screenRows)
			require.NoError(t, err)
			w := &eventlog.ANSICursorBufferWriter{
				WriteWithTailCloser: tl,
				Terminal:            screen,
			}
			for _, s := range tc.write {
				_, err = w.Write(ctx, []byte(s))
				require.NoError(t, err)
			}
			require.Equal(t, ansiDebugString(tc.wantLog), ansiDebugString(tl.String()))
		})
	}
}

// ansiDebugString replaces ANSI escape sequences with a string representation
// that can be displayed as a diff without messing up the terminal.
func ansiDebugString(s string) string {
	// \x1b[0m is interpreted the same as \x1b[m. Normalize these to make
	// assertions easier.
	s = strings.ReplaceAll(s, "\x1b[0m", "\x1b[m")
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

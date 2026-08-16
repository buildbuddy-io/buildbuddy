package eventlog_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_kvstore"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockinvocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockstore"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/terminal"
	"github.com/buildbuddy-io/buildbuddy/server/util/terminal/testdata"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
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

func TestANSICursorBufferWriterCloseTrimsCursorControlBlankTail(t *testing.T) {
	ctx := context.Background()
	tailLog := &testLog{}
	screen, err := terminal.NewScreenWriter(math.MaxInt, 4)
	require.NoError(t, err)
	w := eventlog.NewANSICursorBufferWriter(tailLog, screen)

	_, err = w.Write(ctx, []byte("INFO: start\n"))
	require.NoError(t, err)
	_, err = w.Write(ctx, []byte("\x1b[1A\x1b[K\x1b[32mINFO:\x1b[m\n\n\n"))
	require.NoError(t, err)
	require.NoError(t, w.Close(ctx))

	assert.Contains(t, tailLog.String(), "INFO:")
	assert.False(
		t,
		strings.HasSuffix(tailLog.String(), "\n\n"),
		"tail should not end with blank curses rows: %q",
		tailLog.String(),
	)
}

func TestANSICursorBufferWriterClosePreservesNonCursorBlankTail(t *testing.T) {
	ctx := context.Background()
	tailLog := &testLog{}
	screen, err := terminal.NewScreenWriter(math.MaxInt, 4)
	require.NoError(t, err)
	w := eventlog.NewANSICursorBufferWriter(tailLog, screen)

	_, err = w.Write(ctx, []byte("INFO: start\n\n\n"))
	require.NoError(t, err)
	require.NoError(t, w.Close(ctx))

	assert.True(
		t,
		strings.HasSuffix(tailLog.String(), "\n\n"),
		"non-cursor blank rows should be preserved: %q",
		tailLog.String(),
	)
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

func TestANSICursorBufferWriterCloseAfterWriteLoopShutsDown(t *testing.T) {
	m := mockstore.New()
	ctx, cancel := context.WithCancel(t.Context())

	// Create a chunkstore writer with a write hook that signals when the
	// write loop shuts down.
	shutdown := make(chan struct{})
	cw := chunkstore.New(m, &chunkstore.ChunkstoreOptions{}).Writer(ctx, "eventlog", &chunkstore.ChunkstoreWriterOptions{
		WriteHook: func(_ context.Context, _ *chunkstore.WriteRequest, result *chunkstore.WriteResult, _, _ []byte) {
			if result.Close {
				close(shutdown)
			}
		},
	})
	screen, err := terminal.NewScreenWriter(math.MaxInt, 4)
	require.NoError(t, err)
	w := eventlog.NewANSICursorBufferWriter(cw, screen)

	// Write log output containing a carriage return, like the progress output
	// bazel writes when curses are enabled. This makes Close write the final
	// screen contents before closing the underlying writer.
	_, err = w.Write(ctx, []byte("Analyzing: //foo:foo\r"))
	require.NoError(t, err)

	// Cancel the context, simulating an abrupt client disconnect. The write
	// loop notices the cancellation on its own, flushes the log contents, and
	// shuts down.
	cancel()
	select {
	case <-shutdown:
	case <-time.After(15 * time.Second):
		require.FailNow(t, "timed out waiting for the write loop to shut down")
	}

	// Closing the writer should succeed even though the write loop already
	// flushed the log contents and shut down.
	require.NoError(t, w.Close(t.Context()))
}

// testKeyValStore wraps MemoryKeyValStore to count writes.
type testKeyValStore struct {
	*memory_kvstore.MemoryKeyValStore

	setCount           atomic.Int64
	replaceSuffixCount atomic.Int64
}

func newTestKeyValStore(t *testing.T) *testKeyValStore {
	m, err := memory_kvstore.NewMemoryKeyValStore()
	require.NoError(t, err)
	return &testKeyValStore{MemoryKeyValStore: m}
}

func (s *testKeyValStore) Set(ctx context.Context, key string, val []byte) error {
	if err := s.MemoryKeyValStore.Set(ctx, key, val); err != nil {
		return err
	}
	// A nil value deletes the key; don't count deletes as sets.
	if val != nil {
		s.setCount.Add(1)
	}
	return nil
}

func (s *testKeyValStore) ReplaceSuffix(ctx context.Context, key string, expectedLength, offset int64, data []byte) error {
	if err := s.MemoryKeyValStore.ReplaceSuffix(ctx, key, expectedLength, offset, data); err != nil {
		return err
	}
	s.replaceSuffixCount.Add(1)
	return nil
}

// waitForWrites waits until the store has recorded the given number of Set
// and ReplaceSuffix calls.
func waitForWrites(t *testing.T, kv *testKeyValStore, setCount, replaceSuffixCount int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return kv.setCount.Load() == setCount && kv.replaceSuffixCount.Load() == replaceSuffixCount
	}, 5*time.Second, 10*time.Millisecond)
}

// newInProgressInvocation returns an invocation row for an invocation that is
// in progress and has no log chunks in blobstore yet, so that event log reads
// are served from the live chunk in the key-value store.
func newInProgressInvocation(iid string) *tables.Invocation {
	return &tables.Invocation{
		InvocationID:     iid,
		Attempt:          1,
		LastChunkId:      chunkstore.ChunkIndexAsStringId(math.MaxUint16),
		InvocationStatus: int64(inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS),
	}
}

func TestEventLogWriterSuffixWrites(t *testing.T) {
	ctx := t.Context()
	testID := "d42dec38-e4db-4f8a-a1f6-1e2f1b370b52"
	kv := newTestKeyValStore(t)
	env := testenv.GetTestEnv(t)
	env.SetBlobstore(mockstore.New())
	env.SetKeyValStore(kv)
	env.SetInvocationDB(&mockinvocationdb.MockInvocationDB{DB: map[string]*tables.Invocation{
		testID: newInProgressInvocation(testID),
	}})

	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"build_event_stream.live_chunk_suffix_writes_enabled": {
			State:          memprovider.Enabled,
			DefaultVariant: "true",
			Variants:       map[string]any{"true": true},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))
	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)

	path := eventlog.GetEventLogPathFromInvocationIdAndAttempt(testID, 1)
	v2Key := path + "/v2"
	w, err := eventlog.NewEventLogWriter(ctx, env.GetBlobstore(), kv, nil, fp, "unused-pubsub-channel", path, 80, 10)
	require.NoError(t, err)

	// Write "1\n" - should call Set since there's nothing to append to.
	_, err = w.Write(ctx, []byte("1\n"))
	require.NoError(t, err)
	waitForWrites(t, kv, 1, 0)

	// Should be able to read back "1\n"
	rsp, err := eventlog.GetEventLogChunk(ctx, env, &elpb.GetEventLogChunkRequest{
		InvocationId: testID,
		ChunkId:      chunkstore.ChunkIndexAsStringId(0),
		MinLines:     100,
	})
	require.NoError(t, err)
	require.True(t, rsp.GetLive())
	require.Equal(t, "1\n", string(rsp.GetBuffer()))

	// Write line 2 - this should call ReplaceSuffix to just append "2\n"
	_, err = w.Write(ctx, []byte("2\n"))
	require.NoError(t, err)
	waitForWrites(t, kv, 1, 1)

	// Should be able to read back "1\n2\n"
	rsp, err = eventlog.GetEventLogChunk(ctx, env, &elpb.GetEventLogChunkRequest{
		InvocationId: testID,
		ChunkId:      chunkstore.ChunkIndexAsStringId(0),
		MinLines:     100,
	})
	require.NoError(t, err)
	require.True(t, rsp.GetLive())
	require.Equal(t, "1\n2\n", string(rsp.GetBuffer()))

	// The v1 (proto-based storage) key should not be written when suffix writes
	// are enabled.
	_, err = kv.Get(ctx, path)
	require.True(t, status.IsNotFoundError(err), "expected NotFound for v1 key, got %v", err)

	// Overwrite line 2 using cursor control. We should still call ReplaceSuffix
	// in this case, since "1\n" is still a common prefix.
	_, err = w.Write(ctx, []byte("\x1b[A\r3\n"))
	require.NoError(t, err)
	waitForWrites(t, kv, 1, 2)

	// Should now be able to read back "1\n3\n"
	rsp, err = eventlog.GetEventLogChunk(ctx, env, &elpb.GetEventLogChunkRequest{
		InvocationId: testID,
		ChunkId:      chunkstore.ChunkIndexAsStringId(0),
		MinLines:     100,
	})
	require.NoError(t, err)
	require.True(t, rsp.GetLive())
	require.Equal(t, "1\n3\n", string(rsp.GetBuffer()))

	// Evict the stored value so the next suffix write fails, due to the prefix
	// length mismatching the expected length.
	require.NoError(t, kv.Set(ctx, v2Key, nil))
	_, err = w.Write(ctx, []byte("4\n"))
	require.NoError(t, err)
	waitForWrites(t, kv, 2, 2)

	// Should be able to read back "1\n3\n4\n" - the "1\n3\n" should not get
	// lost even though the key was evicted.
	rsp, err = eventlog.GetEventLogChunk(ctx, env, &elpb.GetEventLogChunkRequest{
		InvocationId: testID,
		ChunkId:      chunkstore.ChunkIndexAsStringId(0),
		MinLines:     100,
	})
	require.NoError(t, err)
	require.True(t, rsp.GetLive())
	require.Equal(t, "1\n3\n4\n", string(rsp.GetBuffer()))
}

func TestEventLogWriterFullWritesWhenExperimentDisabled(t *testing.T) {
	ctx := t.Context()
	testID := "d42dec38-e4db-4f8a-a1f6-1e2f1b370b52"
	kv := newTestKeyValStore(t)
	env := testenv.GetTestEnv(t)
	env.SetBlobstore(mockstore.New())
	env.SetKeyValStore(kv)
	env.SetInvocationDB(&mockinvocationdb.MockInvocationDB{DB: map[string]*tables.Invocation{
		testID: newInProgressInvocation(testID),
	}})

	path := eventlog.GetEventLogPathFromInvocationIdAndAttempt(testID, 1)
	w, err := eventlog.NewEventLogWriter(ctx, env.GetBlobstore(), kv, nil, nil, "unused-pubsub-channel", path, 80, 10)
	require.NoError(t, err)

	_, err = w.Write(ctx, []byte("1\n"))
	require.NoError(t, err)
	_, err = w.Write(ctx, []byte("2\n"))
	require.NoError(t, err)

	// Should call Set() twice - each Set() overwrites the full cumulative live
	// chunk.
	waitForWrites(t, kv, 2, 0)

	rsp, err := eventlog.GetEventLogChunk(ctx, env, &elpb.GetEventLogChunkRequest{
		InvocationId: testID,
		ChunkId:      chunkstore.ChunkIndexAsStringId(0),
		MinLines:     100,
	})
	require.NoError(t, err)
	require.True(t, rsp.GetLive())
	require.Equal(t, "1\n2\n", string(rsp.GetBuffer()))
}

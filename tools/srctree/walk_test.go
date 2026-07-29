package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protodelim"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	spb "github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

// makeLog builds a compact execution log holding the given paths, in the same
// format Bazel writes: zstd-compressed, length-delimited ExecLogEntry protos.
func makeLog(t *testing.T, paths ...string) []byte {
	t.Helper()
	entries := []*spb.ExecLogEntry{{
		Id: 1,
		Type: &spb.ExecLogEntry_Invocation_{Invocation: &spb.ExecLogEntry_Invocation{
			HashFunctionName: "BLAKE3",
			Id:               "test-invocation",
		}},
	}}
	for i, p := range paths {
		entries = append(entries, &spb.ExecLogEntry{
			Id: uint32(i + 2),
			Type: &spb.ExecLogEntry_File_{File: &spb.ExecLogEntry_File{
				Path:   p,
				Digest: &spb.Digest{Hash: fmt.Sprintf("%064x", i+1), SizeBytes: int64(i + 1)},
			}},
		})
	}
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	for _, e := range entries {
		_, err := protodelim.MarshalTo(zw, e)
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

// fakeSource serves canned pages of invocations and their logs.
type fakeSource struct {
	// pages holds the logs to return, one slice per search page. A nil entry
	// stands for an invocation without an execution log.
	pages [][][]byte

	// failDownloads makes nil entries look like failed downloads rather than
	// invocations that simply have no execution log.
	failDownloads bool

	// cached makes the logs look like they came off local disk.
	cached bool

	searchCalls    int
	downloadedLogs int
	// onDownload runs before each batch is returned, for tests that need to
	// interfere partway through.
	onDownload func()
}

func (f *fakeSource) searchPage(ctx context.Context, repo string, since time.Time, pageToken string) ([]*inpb.Invocation, string, error) {
	f.searchCalls++
	page := 0
	if pageToken != "" {
		fmt.Sscanf(pageToken, "page-%d", &page)
	}
	if page >= len(f.pages) {
		return nil, "", nil
	}
	invocations := make([]*inpb.Invocation, len(f.pages[page]))
	for i := range invocations {
		invocations[i] = &inpb.Invocation{InvocationId: fmt.Sprintf("inv-%d-%d", page, i)}
	}
	next := ""
	if page+1 < len(f.pages) {
		next = fmt.Sprintf("page-%d", page+1)
	}
	return invocations, next, nil
}

func (f *fakeSource) downloadBatch(ctx context.Context, invocations []*inpb.Invocation) []downloadResult {
	if f.onDownload != nil {
		f.onDownload()
	}
	results := make([]downloadResult, len(invocations))
	for i, inv := range invocations {
		var page, idx int
		fmt.Sscanf(inv.GetInvocationId(), "inv-%d-%d", &page, &idx)
		if data := f.pages[page][idx]; data != nil {
			results[i] = downloadResult{data: data, wireBytes: int64(len(data)), cached: f.cached}
			f.downloadedLogs++
		} else if f.failDownloads {
			results[i] = downloadResult{err: fmt.Errorf("blob not found")}
		}
	}
	return results
}

// walkTest runs a walk over the given pages, capping it at max invocations
// (0 for no cap).
func walkTest(t *testing.T, ctx context.Context, src *fakeSource, max int) (*tree, error) {
	t.Helper()
	oldMax := *maxInvocations
	*maxInvocations = max
	t.Cleanup(func() { *maxInvocations = oldMax })

	tr := newTree()
	return tr, walk(ctx, src, tr, "https://github.com/org/repo", time.Time{})
}

// A run of logs that add nothing doesn't end the walk: only the search window,
// the cap, and an interrupt do. The last log here would be missed if a plateau
// stopped us early.
func TestWalkKeepsGoingWhenTreeStopsGrowing(t *testing.T) {
	sameFiles := makeLog(t, "a.go", "b.go")
	src := &fakeSource{pages: [][][]byte{{
		sameFiles, sameFiles, sameFiles, sameFiles, sameFiles, sameFiles, sameFiles,
		makeLog(t, "d.go"),
	}}}

	tr, err := walkTest(t, context.Background(), src, 0)
	require.NoError(t, err)

	assert.Equal(t, 8, len(tr.logs))
	assert.Equal(t, 3, tr.shownFiles)
	assert.Contains(t, tr.root.children, "d.go")
}

// The byte tally covers everything pulled over the wire, including the rest of
// the batch we were in when the walk stopped.
func TestWalkTalliesDownloadedBytes(t *testing.T) {
	logs := [][]byte{makeLog(t, "a.go"), makeLog(t, "b.go"), makeLog(t, "c.go")}
	total := 0
	for _, l := range logs {
		total += len(l)
	}
	src := &fakeSource{pages: [][][]byte{logs}}

	var out bytes.Buffer
	oldOut := progressOut
	progressOut = &out
	t.Cleanup(func() { progressOut = oldOut })

	// Stop after the first log; the other two were downloaded in the same batch
	// and their bytes are spent either way.
	_, err := walkTest(t, context.Background(), src, 1)
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 1)
	assert.Contains(t, lines[0], units.HumanSize(float64(total)))
}

func TestWalkFollowsPagesUntilHistoryRunsOut(t *testing.T) {
	src := &fakeSource{pages: [][][]byte{
		{makeLog(t, "a/1.go"), makeLog(t, "a/2.go")},
		{makeLog(t, "b/3.go"), makeLog(t, "b/4.go")},
		{makeLog(t, "c/5.go")},
	}}

	tr, err := walkTest(t, context.Background(), src, 0)
	require.NoError(t, err)

	assert.Equal(t, 5, tr.shownFiles)
	assert.Equal(t, 3, tr.shownDirs)
	// One search per page: the last page reports that it's the last, so there's
	// no wasted request for an empty page.
	assert.Equal(t, 3, src.searchCalls)
}

func TestWalkHonorsMaxInvocations(t *testing.T) {
	src := &fakeSource{pages: [][][]byte{{
		makeLog(t, "a.go"), makeLog(t, "b.go"), makeLog(t, "c.go"), makeLog(t, "d.go"),
	}}}

	tr, err := walkTest(t, context.Background(), src, 2)
	require.NoError(t, err)

	assert.Equal(t, 2, len(tr.logs))
	assert.Equal(t, 2, tr.shownFiles)
}

// Invocations without an execution log shouldn't count towards the streak of
// logs that added nothing: they aren't evidence either way.
func TestWalkSkipsInvocationsWithoutLogs(t *testing.T) {
	same := makeLog(t, "a.go")
	src := &fakeSource{pages: [][][]byte{{
		same, nil, nil, nil, nil, nil, same, makeLog(t, "b.go"),
	}}}

	tr, err := walkTest(t, context.Background(), src, 0)
	require.NoError(t, err)

	assert.Equal(t, 3, len(tr.logs))
	assert.Equal(t, 2, tr.shownFiles)
}

func TestWalkStopsWhenInterrupted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	src := &fakeSource{pages: [][][]byte{
		{makeLog(t, "a.go"), makeLog(t, "b.go")},
		{makeLog(t, "c.go")},
	}}
	// Interrupt after the first batch has been handed over.
	src.onDownload = func() { cancel() }

	tr, err := walkTest(t, ctx, src, 0)
	require.NoError(t, err)

	// The first batch still gets merged, and the walk stops there.
	assert.Equal(t, 2, tr.shownFiles)
	assert.Equal(t, 1, src.searchCalls)
}

func TestWalkWithoutAnyLogsFails(t *testing.T) {
	src := &fakeSource{pages: [][][]byte{{nil, nil}}}

	_, err := walkTest(t, context.Background(), src, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "compact execution log")
}

// Failed downloads are ordinary - old blobs get evicted - so they're skipped
// like missing logs rather than failing the walk.
func TestWalkSkipsFailedDownloads(t *testing.T) {
	src := &fakeSource{
		failDownloads: true,
		pages:         [][][]byte{{makeLog(t, "a.go"), nil, nil, makeLog(t, "b.go")}},
	}

	tr, err := walkTest(t, context.Background(), src, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, len(tr.logs))
	assert.Equal(t, 2, tr.shownFiles)
}

func TestWalkWithOnlyFailedDownloadsReportsThem(t *testing.T) {
	src := &fakeSource{failDownloads: true, pages: [][][]byte{{nil, nil}}}

	_, err := walkTest(t, context.Background(), src, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "2 failed to download")
}

func TestWalkInterruptedBeforeMergingFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	src := &fakeSource{pages: [][][]byte{{makeLog(t, "a.go")}}}

	_, err := walkTest(t, ctx, src, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "interrupted")
}

// The download has to go through the app's /file/download endpoint, passing the
// invocation ID alongside the bytestream URL: that's what lets the app fall
// back to the invocation's stored copy when the CAS entry is gone.
func TestDownloadUsesFileDownloadEndpoint(t *testing.T) {
	const (
		invocationID = "abc-123"
		uri          = "bytestream://remote.buildbuddy.io/blobs/deadbeef/1234"
	)
	var got *http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		if r.URL.Query().Get("invocation_id") != invocationID {
			http.Error(w, "File not found.", http.StatusNotFound)
			return
		}
		w.Write([]byte("log contents"))
	}))
	defer server.Close()

	c := withAppURL(t, server.URL, "test-api-key")
	data, err := c.download(context.Background(), invocationID, uri, executionLogName)
	require.NoError(t, err)
	assert.Equal(t, []byte("log contents"), data)

	require.NotNil(t, got)
	assert.Equal(t, "/file/download", got.URL.Path)
	assert.Equal(t, invocationID, got.URL.Query().Get("invocation_id"))
	assert.Equal(t, uri, got.URL.Query().Get("bytestream_url"))
	assert.Equal(t, executionLogName, got.URL.Query().Get("filename"))
	assert.Equal(t, "test-api-key", got.Header.Get("x-buildbuddy-api-key"))
	// The log is already zstd-compressed; it must arrive byte for byte.
	assert.Equal(t, "identity", got.Header.Get("Accept-Encoding"))
}

func TestDownloadReportsServerErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "File not found.", http.StatusNotFound)
	}))
	defer server.Close()

	c := withAppURL(t, server.URL, "test-api-key")
	_, err := c.download(context.Background(), "abc-123", "bytestream://host/blobs/d/1", executionLogName)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "File not found.")
}

func TestDownloadHonorsTrailingSlashInAppURL(t *testing.T) {
	var path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
	}))
	defer server.Close()

	c := withAppURL(t, server.URL+"/", "")
	_, err := c.download(context.Background(), "abc-123", "bytestream://host/blobs/d/1", executionLogName)
	require.NoError(t, err)
	assert.Equal(t, "/file/download", path)
}

// withAppURL builds a client pointed at a test server.
func withAppURL(t *testing.T, url, key string) *apiClient {
	t.Helper()
	oldURL, oldKey := *appURL, *apiKey
	*appURL, *apiKey = url, key
	t.Cleanup(func() { *appURL, *apiKey = oldURL, oldKey })
	return &apiClient{http: &http.Client{Timeout: downloadTimeout}}
}

// useTempLogCache points the on-disk log cache at a scratch directory.
func useTempLogCache(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	old := *logCacheDir
	*logCacheDir = dir
	t.Cleanup(func() { *logCacheDir = old })
	return dir
}

func TestLogCacheRoundTrip(t *testing.T) {
	dir := useTempLogCache(t)
	data := makeLog(t, "a.go")
	loc := cacheLocation{host: "remote.buildbuddy.io", instanceName: "ci"}

	_, ok := readCachedLog("inv-one")
	assert.False(t, ok, "nothing cached yet")

	cacheLog("inv-one", loc, data)
	entry, ok := readCachedLog("inv-one")
	require.True(t, ok)
	assert.Equal(t, filepath.Join(dir, "inv-one"), entry.path)
	// Where the build's blobs live survives the round trip, so a cached run can
	// still check the CAS and build diff links, and so does what the log cost
	// to download.
	assert.Equal(t, loc, entry.cache)
	assert.Equal(t, int64(len(data)), entry.wireBytes)
	assert.False(t, entry.noLog)

	// The log is stored expanded, so re-runs don't decompress it again.
	stored, err := os.ReadFile(entry.path)
	require.NoError(t, err)
	assert.Greater(t, len(stored), len(data))
	assert.NotEqual(t, data[:4], stored[:4], "the zstd magic should be gone")

	// A merge of the expanded file sees the same tree as the compressed bytes.
	fromFile, fromBytes := newTree(), newTree()
	require.NoError(t, fromFile.parseFile(logInfo{name: "expanded"}, entry.path))
	require.NoError(t, fromBytes.parseBytes(logInfo{name: "compressed"}, data))
	assert.Equal(t, fromBytes.numFiles, fromFile.numFiles)
	assert.Equal(t, fromBytes.root.hash(), fromFile.root.hash())

	// The invocation ID is the log's filename, and nothing else is left behind
	// but its metadata.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := []string{}
	for _, e := range entries {
		names = append(names, e.Name())
	}
	assert.ElementsMatch(t, []string{"inv-one", "inv-one.meta"}, names)
}

// Entries from an older layout - which kept the log compressed - are ignored
// rather than handed to the parser as if they were expanded.
func TestLogCacheIgnoresOldFormats(t *testing.T) {
	dir := useTempLogCache(t)
	data := makeLog(t, "a.go")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "inv-one"), data, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "inv-one.meta"),
		fmt.Appendf(nil, `{"size_bytes":%d}`, len(data)), 0644))

	_, ok := readCachedLog("inv-one")
	assert.False(t, ok)
}

// The metadata is written second, so a log without it is a half-finished entry
// and has to be fetched again.
func TestLogCacheNeedsMetadata(t *testing.T) {
	dir := useTempLogCache(t)
	data := makeLog(t, "a.go")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "inv-one"), data, 0644))

	_, ok := readCachedLog("inv-one")
	assert.False(t, ok)
}

// A log that doesn't match the recorded size was truncated somehow, so it's a
// miss rather than a corrupt log.
func TestLogCacheIgnoresWrongSize(t *testing.T) {
	dir := useTempLogCache(t)
	data := makeLog(t, "a.go")
	cacheLog("inv-one", cacheLocation{}, data)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "inv-one"), data[:len(data)-1], 0644))

	_, ok := readCachedLog("inv-one")
	assert.False(t, ok)
}

func TestLogCacheDisabled(t *testing.T) {
	useTempLogCache(t)
	data := makeLog(t, "a.go")
	cacheLog("inv-one", cacheLocation{}, data)
	_, ok := readCachedLog("inv-one")
	require.True(t, ok)

	*logCacheDir = ""
	_, ok = readCachedLog("inv-one")
	assert.False(t, ok)
	// And writing is a no-op rather than an error.
	cacheLog("inv-one", cacheLocation{}, data)
}

// "This invocation has no execution log" is worth remembering too: it saves
// the invocation fetch it took to find that out.
func TestLogCacheRemembersNoLog(t *testing.T) {
	dir := useTempLogCache(t)
	cacheNoLog("inv-one")

	entry, ok := readCachedLog("inv-one")
	require.True(t, ok, "the answer is cached, even though there's no log")
	assert.True(t, entry.noLog)
	assert.Empty(t, entry.path)

	// A negative is metadata only: there's no log file to write.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "inv-one.meta", entries[0].Name())
}

// fakeBB implements the one RPC executionLog uses. The embedded interface is
// nil, so calling anything else panics rather than passing silently.
type fakeBB struct {
	bbspb.BuildBuddyServiceClient
	inv   *inpb.Invocation
	err   error
	calls int
}

func (f *fakeBB) GetInvocation(ctx context.Context, req *inpb.GetInvocationRequest, opts ...grpc.CallOption) (*inpb.GetInvocationResponse, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &inpb.GetInvocationResponse{Invocation: []*inpb.Invocation{f.inv}}, nil
}

// An invocation that finished without an execution log is remembered, so the
// next run doesn't pay for the lookup again.
func TestExecutionLogCachesNoLog(t *testing.T) {
	useTempLogCache(t)
	bb := &fakeBB{inv: &inpb.Invocation{
		InvocationId:     "inv-done",
		InvocationStatus: inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS,
	}}
	c := &apiClient{bb: bb}

	result := c.executionLog(context.Background(), "inv-done")
	assert.Nil(t, result.data)
	assert.NoError(t, result.err)
	require.Equal(t, 1, bb.calls)

	result = c.executionLog(context.Background(), "inv-done")
	assert.Nil(t, result.data)
	assert.NoError(t, result.err)
	assert.Equal(t, 1, bb.calls, "the invocation shouldn't be fetched a second time")
}

// A lookup that failed says nothing about whether the invocation has a log, so
// the next run tries again.
func TestExecutionLogDoesNotCacheFailedLookups(t *testing.T) {
	useTempLogCache(t)
	bb := &fakeBB{err: fmt.Errorf("unavailable")}
	c := &apiClient{bb: bb}

	require.Error(t, c.executionLog(context.Background(), "inv-one").err)
	require.Error(t, c.executionLog(context.Background(), "inv-one").err)
	assert.Equal(t, 2, bb.calls)
}

// A build still running may not have uploaded its log yet, so "no log" isn't
// the final answer.
func TestExecutionLogDoesNotCacheUnfinishedInvocations(t *testing.T) {
	useTempLogCache(t)
	for _, status := range []inspb.InvocationStatus{
		inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS,
		inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS,
		inspb.InvocationStatus_UNKNOWN_INVOCATION_STATUS,
	} {
		t.Run(status.String(), func(t *testing.T) {
			useTempLogCache(t)
			bb := &fakeBB{inv: &inpb.Invocation{InvocationId: "inv-live", InvocationStatus: status}}
			c := &apiClient{bb: bb}

			require.Nil(t, c.executionLog(context.Background(), "inv-live").data)
			require.Nil(t, c.executionLog(context.Background(), "inv-live").data)
			assert.Equal(t, 2, bb.calls, "an unfinished build should be checked again")
		})
	}
}

// The cache sits at a predictable path, so an ID that could escape the
// directory isn't used as a file name at all.
func TestLogCacheRejectsUnsafeIDs(t *testing.T) {
	dir := useTempLogCache(t)
	for _, id := range []string{"", ".", "..", "../escape", "nested/id", `back\slash`} {
		cacheLog(id, cacheLocation{}, makeLog(t, "a.go"))
		cacheNoLog(id)
		_, ok := readCachedLog(id)
		assert.False(t, ok, "id %q", id)
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestParseBlobURI(t *testing.T) {
	// Digest hashes are the full width for their function, which is what the
	// resource name parser insists on.
	const hash = "09e6fe6e1fd8c8734339a0a84c3c7a0eb121b57a45d21cfeb1f265bffe4c4888"

	ref := parseBlobURI("bytestream://remote.buildbuddy.io/my-instance/blobs/" + hash + "/4096")
	assert.Equal(t, "remote.buildbuddy.io", ref.cache.host)
	assert.Equal(t, "my-instance", ref.cache.instanceName)
	assert.Equal(t, hash, ref.hash)
	assert.Equal(t, int64(4096), ref.size)

	// A nested instance name, and a digest function segment.
	ref = parseBlobURI("bytestream://remote.buildbuddy.io/org/repo/ci/blobs/blake3/" + hash + "/10")
	assert.Equal(t, "org/repo/ci", ref.cache.instanceName)
	assert.Equal(t, hash, ref.hash)
	assert.Equal(t, int64(10), ref.size)

	// No instance name at all.
	ref = parseBlobURI("bytestream://remote.buildbuddy.io/blobs/" + hash + "/10")
	assert.Empty(t, ref.cache.instanceName)
	assert.Equal(t, hash, ref.hash)

	// Anything unparseable leaves the digest empty, which disables caching for
	// that log rather than caching it under a wrong name.
	assert.Empty(t, parseBlobURI("bytestream://host/not-a-blob-path").hash)
	assert.Empty(t, parseBlobURI("://").hash)
}

// Logs read from disk count towards the total, and are called out separately so
// it's clear what the run actually cost.
func TestWalkTalliesCachedBytes(t *testing.T) {
	logs := [][]byte{makeLog(t, "a.go"), makeLog(t, "b.go")}
	src := &fakeSource{pages: [][][]byte{logs}, cached: true}

	var out bytes.Buffer
	oldOut := progressOut
	progressOut = &out
	t.Cleanup(func() { progressOut = oldOut })

	_, err := walkTest(t, context.Background(), src, 0)
	require.NoError(t, err)

	total := int64(len(logs[0]) + len(logs[1]))
	last := strings.Split(strings.TrimSpace(out.String()), "\n")[1]
	assert.Contains(t, last, byteSummary(byteTally{read: total, cached: total}))
	assert.Contains(t, last, "cached)")
}

func TestByteSummary(t *testing.T) {
	assert.Equal(t, "20.4MB", byteSummary(byteTally{read: 20_400_000}))
	assert.Equal(t, "20.4MB (2.6MB cached)",
		byteSummary(byteTally{read: 20_400_000, cached: 2_600_000}))
}

func TestProgressLine(t *testing.T) {
	tr := newTree()
	tr.add("a/b.go", newDigest("aaaa", 1))
	inv := &inpb.Invocation{
		InvocationId:  "inv",
		UpdatedAtUsec: time.Date(2026, 7, 21, 14, 2, 3, 0, time.Local).UnixMicro(),
	}

	// Counts that are zero stay off the line.
	assert.Equal(t,
		"fetched 3 logs · 4.5MB · 1 dirs · 1 files · back to 2026-07-21 14:02:03",
		progressLine(tr, 3, 0, 0, byteTally{read: 4_500_000}, inv))

	assert.Equal(t,
		"fetched 3 logs · 4.5MB · 1 dirs · 1 files · 2 with nothing new · 5 failed to fetch · back to 2026-07-21 14:02:03",
		progressLine(tr, 3, 2, 5, byteTally{read: 4_500_000}, inv))

	// Before anything has merged there's no timestamp to report.
	assert.Equal(t, "fetched 0 logs · 0B · 1 dirs · 1 files · 1 failed to fetch", progressLine(tr, 0, 0, 1, byteTally{}, nil))
}

// A failed fetch shouldn't move the reported timestamp: it refers to the last
// log actually merged.
func TestProgressTimestampTracksLastMergedLog(t *testing.T) {
	tr := newTree()
	p := &progress{tty: true}
	merged := &inpb.Invocation{UpdatedAtUsec: time.Date(2026, 7, 21, 14, 2, 3, 0, time.Local).UnixMicro()}

	p.update(tr, 1, 0, 0, byteTally{read: 100}, merged)
	assert.Equal(t, merged, p.lastMerged)

	p.refresh(tr, 1, 0, 1, byteTally{read: 200})
	assert.Equal(t, merged, p.lastMerged)
}

func TestMakeLogRoundTrips(t *testing.T) {
	tr := newTree()
	require.NoError(t, tr.parseBytes(logInfo{name: "synthetic"}, makeLog(t, "a/b.go", "bazel-out/gen.go")))
	assert.Equal(t, "BLAKE3", tr.hashFunction)
	assert.Equal(t, 2, tr.numFiles)
	assert.Equal(t, 1, tr.shownFiles)
	assert.Empty(t, tr.conflicts)
}

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/docker/go-units"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"
	"golang.org/x/term"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
)

// executionLogName is the name Bazel gives the compact execution log in the
// buildToolLogs build event. Bazel only uploads it when the invocation was run
// with --experimental_execution_log_compact_file.
const executionLogName = "execution_log.binpb.zst"

// ciRole is the invocation role CI builds are tagged with.
const ciRole = "CI"

// maxConcurrentDownloads bounds how many invocations we fetch at once. Logs are
// merged in strict history order, so this is also how far ahead of the merge we
// download.
const maxConcurrentDownloads = 8

// searchPageSize is how many invocations we ask for per search request.
const searchPageSize = 50

// stopReason says why the walk back through build history ended.
type stopReason string

const (
	stoppedInterrupted stopReason = "interrupted"
	stoppedEndOfWindow stopReason = "ran out of invocations in the search window"
	stoppedMaxMerged   stopReason = "reached --max_invocations"
)

// downloadTimeout bounds a single execution log download.
const downloadTimeout = 5 * time.Minute

// apiClient talks to the BuildBuddy server two ways: gRPC to the API target for
// invocation metadata and cache lookups, and HTTP to the app for the log
// contents. See download for why the contents don't come over gRPC.
type apiClient struct {
	bb   bbspb.BuildBuddyServiceClient
	cas  repb.ContentAddressableStorageClient
	http *http.Client
}

// dialAPI connects to the API target. The returned closer shuts the connection
// down, which the caller should hold open for as long as the UI might need it.
func dialAPI() (*apiClient, io.Closer, error) {
	conn, err := grpc_client.DialSimpleWithoutPooling(*target)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", *target, err)
	}
	return &apiClient{
		bb:   bbspb.NewBuildBuddyServiceClient(conn),
		cas:  repb.NewContentAddressableStorageClient(conn),
		http: &http.Client{Timeout: downloadTimeout},
	}, conn, nil
}

// apiContext attaches the API key to outgoing requests.
func apiContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
}

// fetchAndMerge picks a repo and merges its CI invocations' execution logs into
// the tree, walking back through build history until it runs out or the user
// interrupts.
func fetchAndMerge(ctx context.Context, c *apiClient, t *tree) error {
	ctx = apiContext(ctx)
	since := time.Now().AddDate(0, 0, -*days)

	repo := *repoURL
	if repo == "" {
		repos, err := c.repos(ctx, since)
		if err != nil {
			return err
		}
		if len(repos) == 0 {
			return fmt.Errorf("no repos found in the last %d days", *days)
		}
		if repo, err = pickRepo(repos); err != nil {
			return err
		}
	}
	t.repo = repo

	// Ctrl-C stops the walk and shows what we have so far, rather than killing
	// the tool. Restoring the default handler on the way out means a second
	// Ctrl-C still quits.
	walkCtx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	return walk(walkCtx, c, t, repo, since)
}

// invocationSource is the part of the API that walking build history depends
// on. apiClient is the real implementation; tests supply their own.
type invocationSource interface {
	// searchPage returns one page of invocations plus the token for the next.
	searchPage(ctx context.Context, repo string, since time.Time, pageToken string) ([]*inpb.Invocation, string, error)
	// downloadBatch returns the outcome of fetching each invocation's execution
	// log, in the order the invocations were passed in.
	downloadBatch(ctx context.Context, invocations []*inpb.Invocation) []downloadResult
}

// cacheLocation is where an invocation's blobs live, taken from the bytestream
// URI of one of its files.
type cacheLocation struct {
	host         string
	instanceName string
}

// blobRef is everything a bytestream URI tells us about one blob: where it
// lives, and what it is.
type blobRef struct {
	cache cacheLocation
	hash  string
	size  int64
}

// parseBlobURI takes apart a bytestream URI, e.g.
// bytestream://host/instance/blobs/<hash>/<size>.
func parseBlobURI(uri string) blobRef {
	u, err := url.Parse(uri)
	if err != nil {
		return blobRef{}
	}
	ref := blobRef{cache: cacheLocation{host: u.Host}}
	rn, err := digest.ParseDownloadResourceName(u.Path)
	if err != nil {
		return ref
	}
	// The instance name comes back with the URI path's leading slash on it.
	// FindMissingBlobs compares it against what the build wrote, and building a
	// resource name back up adds its own separator, so strip it.
	ref.cache.instanceName = strings.Trim(rn.GetInstanceName(), "/")
	ref.hash = rn.GetDigest().GetHash()
	ref.size = rn.GetDigest().GetSizeBytes()
	return ref
}

// An invocation's execution log never changes, so a log we've already
// downloaded can be read straight off disk on the next run. Keying the cache by
// invocation - rather than by the log's digest - means a hit also saves the
// GetInvocation call it would have taken to learn that digest, which is the
// most expensive part of the walk: it returns the invocation's whole build
// event stream.

// cacheFormat is the layout of a cache entry. Entries written by an older
// version are ignored rather than misread: format 1 kept the log exactly as
// downloaded, where format 2 keeps it expanded.
const cacheFormat = 2

// cachedLogMeta is what we know about a cached invocation besides the log
// bytes. It's written after the log itself, so its presence is what marks an
// entry complete: a log without it is treated as a miss and fetched again.
type cachedLogMeta struct {
	Format int `json:"format"`
	// SizeBytes is the size of the expanded log on disk, which is checked
	// against the file to catch a truncated write.
	SizeBytes int64 `json:"size_bytes"`
	// WireSizeBytes is what the log cost to download. The byte tally reports it
	// whether or not we had to pay it again.
	WireSizeBytes int64  `json:"wire_size_bytes"`
	CacheHost     string `json:"cache_host"`
	InstanceName  string `json:"instance_name"`
	// NoExecutionLog records that the invocation was fetched and simply didn't
	// have a log. That answer never changes for a finished build, and it saves
	// re-fetching the invocation to find out again.
	NoExecutionLog bool `json:"no_execution_log,omitempty"`
}

// cachedEntry is a cached answer about an invocation: where its expanded log
// sits on disk, or the knowledge that it doesn't have one. It holds a path
// rather than bytes so that a big log can be streamed into the merge instead of
// being read into memory whole.
type cachedEntry struct {
	path      string
	wireBytes int64
	cache     cacheLocation
	noLog     bool
}

func cachedLogPath(invocationID string) string {
	return filepath.Join(*logCacheDir, invocationID)
}

func cachedLogMetaPath(invocationID string) string {
	return filepath.Join(*logCacheDir, invocationID+".meta")
}

// cacheableInvocationID reports whether an ID is safe to use as a file name.
// The cache lives at a predictable path, so anything that could escape the
// directory doesn't get cached at all.
func cacheableInvocationID(invocationID string) bool {
	if *logCacheDir == "" || invocationID == "" {
		return false
	}
	if invocationID == "." || invocationID == ".." {
		return false
	}
	// Both separators, not just this platform's: an ID is a UUID, so rejecting
	// either costs nothing.
	return !strings.ContainsAny(invocationID, `/\`)
}

// readCachedLog returns what we already know about an invocation, and whether
// we know anything at all.
func readCachedLog(invocationID string) (cachedEntry, bool) {
	if !cacheableInvocationID(invocationID) {
		return cachedEntry{}, false
	}
	metaBytes, err := os.ReadFile(cachedLogMetaPath(invocationID))
	if err != nil {
		return cachedEntry{}, false
	}
	var meta cachedLogMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		log.Debugf("Ignoring cached log %s: %s", invocationID, err)
		return cachedEntry{}, false
	}
	if meta.Format != cacheFormat {
		log.Debugf("Ignoring cached log %s: format %d, want %d", invocationID, meta.Format, cacheFormat)
		return cachedEntry{}, false
	}
	if meta.NoExecutionLog {
		// Recorded as having no log, so there's no log file to read.
		return cachedEntry{noLog: true}, true
	}
	// Stat rather than read: the merge streams the file, so a huge log never
	// has to be held in memory.
	info, err := os.Stat(cachedLogPath(invocationID))
	if err != nil {
		log.Debugf("Ignoring cached log %s: %s", invocationID, err)
		return cachedEntry{}, false
	}
	if info.Size() != meta.SizeBytes {
		log.Debugf("Ignoring cached log %s: have %d bytes, want %d", invocationID, info.Size(), meta.SizeBytes)
		return cachedEntry{}, false
	}
	return cachedEntry{
		path:      cachedLogPath(invocationID),
		wireBytes: meta.WireSizeBytes,
		cache:     cacheLocation{host: meta.CacheHost, instanceName: meta.InstanceName},
	}, true
}

// cacheLog stores a downloaded log for next time, expanded, so that later runs
// skip the decompression as well as the download. Failures are not worth
// interrupting the walk over.
func cacheLog(invocationID string, loc cacheLocation, compressed []byte) {
	if !cacheableInvocationID(invocationID) {
		return
	}
	if err := os.MkdirAll(*logCacheDir, 0755); err != nil {
		log.Debugf("Not caching logs in %s: %s", *logCacheDir, err)
		return
	}
	size, err := expandToFile(cachedLogPath(invocationID), compressed)
	if err != nil {
		log.Debugf("Not caching log %s: %s", invocationID, err)
		return
	}
	// The log goes down first and the metadata second, so a run interrupted
	// between the two leaves an entry that reads as a miss rather than as a
	// truncated log.
	writeCachedMeta(invocationID, cachedLogMeta{
		Format:        cacheFormat,
		SizeBytes:     size,
		WireSizeBytes: int64(len(compressed)),
		CacheHost:     loc.host,
		InstanceName:  loc.instanceName,
	})
}

// cacheNoLog records that an invocation has no execution log at all, which is
// metadata only: there's nothing to store.
func cacheNoLog(invocationID string) {
	if !cacheableInvocationID(invocationID) {
		return
	}
	if err := os.MkdirAll(*logCacheDir, 0755); err != nil {
		log.Debugf("Not caching logs in %s: %s", *logCacheDir, err)
		return
	}
	writeCachedMeta(invocationID, cachedLogMeta{Format: cacheFormat, NoExecutionLog: true})
}

func writeCachedMeta(invocationID string, meta cachedLogMeta) {
	b, err := json.Marshal(meta)
	if err != nil {
		log.Debugf("Not caching log %s: %s", invocationID, err)
		return
	}
	if err := writeFileAtomically(cachedLogMetaPath(invocationID), b); err != nil {
		log.Debugf("Not caching log %s: %s", invocationID, err)
	}
}

// expandToFile decompresses a log into the cache, returning how big it turned
// out to be. It streams, so the expanded log is never held in memory.
func expandToFile(path string, compressed []byte) (int64, error) {
	zr, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return 0, err
	}
	defer zr.Close()

	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return 0, err
	}
	defer os.Remove(f.Name())
	size, err := io.Copy(f, zr)
	if err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	return size, os.Rename(f.Name(), path)
}

// writeFileAtomically writes through a temporary file and renames, so readers
// never see a half-written file under its final name.
func writeFileAtomically(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// downloadResult is the outcome of fetching one invocation's execution log.
// Missing logs and failed fetches are both ordinary - most invocations aren't
// run with --experimental_execution_log_compact_file, and old blobs get evicted
// from the cache - so they're counted and reported once at the end rather than
// warned about one by one.
type downloadResult struct {
	// data holds a freshly downloaded log, still compressed. path points at an
	// expanded one in the local cache. Both are empty when the invocation has
	// no execution log.
	data []byte
	path string
	// wireBytes is what the log cost to download, reported whether or not this
	// run had to pay it.
	wireBytes int64
	// cache is where the log's blob lived, and so where the rest of the
	// invocation's blobs live too.
	cache cacheLocation
	// cached is true when the log came off local disk instead of the network.
	cached bool
	err    error
}

// empty reports that the invocation had no execution log.
func (r downloadResult) empty() bool {
	return len(r.data) == 0 && r.path == ""
}

// decode reads the log and hands its tree updates to emit, streaming it off
// disk when it's one we already expanded into the cache.
func (r downloadResult) decode(emit func([]decodedEntry) error) error {
	if r.path != "" {
		return decodeFile(r.path, emit)
	}
	return decodeBytes(r.data, emit)
}

// logStream is a log being decoded in the background, ahead of its turn to be
// merged.
type logStream struct {
	batches chan []decodedEntry
	// err is written before batches is closed, so a reader that has drained the
	// channel can read it safely.
	err error
}

// decodeAhead is how many decoded batches a stream may run ahead of the merge.
// It bounds how much decoded-but-unmerged data is held in memory.
const decodeAhead = 2

// startDecoding kicks off a decoder per log. Decoding is most of the work of a
// merge and doesn't touch the tree, so it runs in parallel with the merge of
// the logs before it. Cancelling the context stops the decoders, which is what
// releases them when the walk stops part way through a batch.
func startDecoding(ctx context.Context, results []downloadResult) []*logStream {
	streams := make([]*logStream, len(results))
	for i, result := range results {
		if result.empty() {
			continue
		}
		s := &logStream{batches: make(chan []decodedEntry, decodeAhead)}
		streams[i] = s
		go func() {
			defer close(s.batches)
			s.err = result.decode(func(batch []decodedEntry) error {
				select {
				case s.batches <- batch:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}()
	}
	return streams
}

// merge applies a decoded log to the tree, in the order the walk reached it.
func (s *logStream) merge(t *tree, src logInfo) error {
	logIdx := t.beginLog(src)
	for batch := range s.batches {
		t.applyEntries(logIdx, batch)
	}
	return s.err
}

// walk merges execution logs from newest to oldest until one of the stop
// conditions is met, printing a running tally as it goes.
func walk(ctx context.Context, c invocationSource, t *tree, repo string, since time.Time) error {
	log.Infof("Walking back through %s invocations for %s (last %d days); ctrl-c to stop early", ciRole, repo, *days)
	p := newProgress()
	defer p.finish()

	var (
		reason    stopReason
		pageToken string
		scanned   int
		merged    int
		// unchanged is how many logs in a row have added nothing new. Nothing
		// stops on it; it's shown so you can tell when there's no more to learn
		// from going further back.
		unchanged int
		// missing counts invocations with no execution log; failed counts logs
		// we couldn't fetch or read.
		missing int
		failed  int
		// bytes is every compact execution log byte we've read, including logs
		// that turn out to be unreadable or that we stop before merging.
		bytes byteTally
	)
walk:
	for {
		// Check for an interrupt directly rather than relying on in-flight
		// requests to fail, but only between batches: logs that have already
		// been downloaded are worth merging.
		if ctx.Err() != nil {
			reason = stoppedInterrupted
			break walk
		}
		invocations, nextPageToken, err := c.searchPage(ctx, repo, since, pageToken)
		if err != nil {
			if ctx.Err() != nil {
				reason = stoppedInterrupted
				break walk
			}
			return err
		}
		for batch := range slices.Chunk(invocations, maxConcurrentDownloads) {
			if ctx.Err() != nil {
				reason = stoppedInterrupted
				break walk
			}
			batchStart := time.Now()
			tracef("batch started: %d invocations", len(batch))
			results := c.downloadBatch(ctx, batch)
			tracef("batch finished: %d invocations in %s", len(batch), time.Since(batchStart).Round(time.Millisecond))
			// Count the whole batch up front: those bytes are spent even if we
			// stop before merging all of them.
			for _, result := range results {
				bytes.read += result.wireBytes
				if result.cached {
					bytes.cached += result.wireBytes
				}
			}
			// Decode the whole batch in the background while the logs are
			// merged one at a time: the tree updates have to stay in history
			// order, but the decoding they wait on doesn't. This doesn't hang
			// off the walk's context: an interrupt stops the walk after this
			// batch, and logs we've already downloaded are worth merging.
			batchCtx, cancelDecoding := context.WithCancel(context.Background())
			streams := startDecoding(batchCtx, results)
			hitMax := false
			for i, result := range results {
				inv := batch[i]
				scanned++
				if result.err != nil {
					failed++
					log.Debugf("Skipping invocation %s: %s", inv.GetInvocationId(), result.err)
					p.refresh(t, merged, unchanged, failed, bytes)
					continue
				}
				if result.empty() {
					missing++
					continue
				}
				before := t.shownFiles
				src := logInfo{
					name:          invocationLabel(inv),
					invocationID:  inv.GetInvocationId(),
					branch:        inv.GetBranchName(),
					cacheHost:     result.cache.host,
					instanceName:  result.cache.instanceName,
					updatedAtUsec: inv.GetUpdatedAtUsec(),
				}
				if err := streams[i].merge(t, src); err != nil {
					failed++
					log.Debugf("Skipping invocation %s: %s", inv.GetInvocationId(), err)
					p.refresh(t, merged, unchanged, failed, bytes)
					continue
				}
				merged++
				if t.shownFiles == before {
					unchanged++
				} else {
					unchanged = 0
				}
				p.update(t, merged, unchanged, failed, bytes, inv)

				if *maxInvocations > 0 && merged >= *maxInvocations {
					hitMax = true
					break
				}
			}
			// Release any decoders still running ahead of where we stopped.
			cancelDecoding()
			if hitMax {
				reason = stoppedMaxMerged
				break walk
			}
			if ctx.Err() != nil {
				reason = stoppedInterrupted
				break walk
			}
		}
		if nextPageToken == "" {
			reason = stoppedEndOfWindow
			break walk
		}
		pageToken = nextPageToken
	}
	p.finish()

	if merged == 0 {
		if reason == stoppedInterrupted {
			return fmt.Errorf("interrupted before any execution log was merged")
		}
		if failed > 0 {
			return fmt.Errorf("none of the %d %s invocations for %s yielded an execution log: %d had none, %d failed to download (run with --verbose for the errors)",
				scanned, ciRole, repo, missing, failed)
		}
		return fmt.Errorf("none of the %d %s invocations for %s had a compact execution log: they must be run with --experimental_execution_log_compact_file",
			scanned, ciRole, repo)
	}
	log.Infof("Stopped after %d invocations: %s", scanned, reason)
	log.Infof("Merged %d execution logs, %s; skipped %d invocations without one%s",
		merged, byteSummary(bytes), missing, failedSuffix(failed))
	if reason == stoppedEndOfWindow {
		log.Infof("The tree may still be incomplete; pass --days=%d or more to look further back", *days*2)
	}
	return nil
}

func failedSuffix(failed int) string {
	if failed == 0 {
		return ""
	}
	return fmt.Sprintf(" and %d that failed to download (run with --verbose for the errors)", failed)
}

// repos returns the repos built in the given time window, most built first: the
// tree is assembled from CI history, so the repo with the most builds behind it
// is the one there's most to reconstruct from.
func (c *apiClient) repos(ctx context.Context, since time.Time) ([]*inpb.InvocationStat, error) {
	group := *groupID
	if group == "" {
		var err error
		if group, err = c.groupID(ctx, since); err != nil {
			return nil, err
		}
	}
	rsp, err := c.bb.GetInvocationStat(ctx, &inpb.GetInvocationStatRequest{
		RequestContext:  &ctxpb.RequestContext{GroupId: group},
		AggregationType: inpb.AggType_REPO_URL_AGGREGATION_TYPE,
		Query:           &inpb.InvocationStatQuery{UpdatedAfter: timestamppb.New(since)},
		Limit:           1000,
	})
	if err != nil {
		return nil, fmt.Errorf("list repos: %w", err)
	}
	repos := make([]*inpb.InvocationStat, 0, len(rsp.GetInvocationStat()))
	for _, s := range rsp.GetInvocationStat() {
		// Builds that weren't run against a repo aggregate under an empty name.
		if s.GetName() != "" {
			repos = append(repos, s)
		}
	}
	slices.SortFunc(repos, compareRepos)
	return repos, nil
}

// compareRepos orders repos most built first, and most recently built first
// among repos with the same number of builds.
func compareRepos(a, b *inpb.InvocationStat) int {
	if n := b.GetTotalNumBuilds() - a.GetTotalNumBuilds(); n != 0 {
		return int(min(max(n, -1), 1))
	}
	return int(min(max(b.GetLatestBuildTimeUsec()-a.GetLatestBuildTimeUsec(), -1), 1))
}

// groupID looks up the group the API key belongs to, which the stats API needs
// stated explicitly. Invocation search doesn't need it - it's already scoped to
// the authenticated group - so we can find any recent invocation and ask who
// owns it.
func (c *apiClient) groupID(ctx context.Context, since time.Time) (string, error) {
	rsp, err := c.bb.SearchInvocation(ctx, &inpb.SearchInvocationRequest{
		Query: &inpb.InvocationQuery{UpdatedAfter: timestamppb.New(since)},
		Count: 1,
	})
	if err != nil {
		return "", fmt.Errorf("look up group ID: search invocations: %w", err)
	}
	if len(rsp.GetInvocation()) == 0 {
		return "", fmt.Errorf("no invocations found in the last %d days; pass --group_id to list repos anyway", *days)
	}
	owner, err := c.bb.GetInvocationOwner(ctx, &inpb.GetInvocationOwnerRequest{
		InvocationId: rsp.GetInvocation()[0].GetInvocationId(),
	})
	if err != nil {
		return "", fmt.Errorf("look up group ID: %w (pass --group_id to skip this lookup)", err)
	}
	return owner.GetGroupId(), nil
}

// pickRepo prompts for one of the repos, on stderr so that the tree output can
// still be piped to a file.
func pickRepo(repos []*inpb.InvocationStat) (string, error) {
	if len(repos) == 1 {
		log.Infof("Only one repo built in the last %d days: %s", *days, repos[0].GetName())
		return repos[0].GetName(), nil
	}
	for i, r := range repos {
		fmt.Fprintf(os.Stderr, "%3d. %-60s %6d builds, latest %s\n", i+1, r.GetName(),
			r.GetTotalNumBuilds(), time.UnixMicro(r.GetLatestBuildTimeUsec()).Format(time.DateTime))
	}
	fmt.Fprintf(os.Stderr, "Select a repo [1-%d]: ", len(repos))
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read selection: %w", err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || n < 1 || n > len(repos) {
		return "", fmt.Errorf("invalid selection %q: expected a number between 1 and %d", strings.TrimSpace(line), len(repos))
	}
	return repos[n-1].GetName(), nil
}

// searchPage returns one page of CI invocations for the repo, newest first,
// along with the token for the page after it.
func (c *apiClient) searchPage(ctx context.Context, repo string, since time.Time, pageToken string) ([]*inpb.Invocation, string, error) {
	rsp, err := c.bb.SearchInvocation(ctx, &inpb.SearchInvocationRequest{
		Query: &inpb.InvocationQuery{
			RepoUrl: repo,
			Role:    []string{ciRole},
			GenericFilters: []*stat_filter.GenericFilter{
				{
					Type:    stat_filter.FilterType_BRANCH_FILTER_TYPE,
					Operand: stat_filter.FilterOperand_IN_OPERAND,
					Value: &stat_filter.FilterValue{
						StringValue: []string{"main", "master"},
					},
				},
				{
					Type:    stat_filter.FilterType_COMMAND_FILTER_TYPE,
					Operand: stat_filter.FilterOperand_IN_OPERAND,
					Value: &stat_filter.FilterValue{
						StringValue: []string{"build", "test"},
					},
				},
			},
			UpdatedAfter: timestamppb.New(since),
		},
		Sort: &inpb.InvocationSort{
			SortField: inpb.InvocationSort_UPDATED_AT_USEC_SORT_FIELD,
			Ascending: false,
		},
		Count:     searchPageSize,
		PageToken: pageToken,
	})
	if err != nil {
		return nil, "", fmt.Errorf("search invocations: %w", err)
	}
	return rsp.GetInvocation(), rsp.GetNextPageToken(), nil
}

// downloadBatch downloads a batch of execution logs concurrently, returning the
// results in the order of the invocations passed in so that they can be merged
// newest-first. Failures are returned rather than reported, so that the walk
// can count them and report once.
func (c *apiClient) downloadBatch(ctx context.Context, invocations []*inpb.Invocation) []downloadResult {
	results := make([]downloadResult, len(invocations))
	eg := &errgroup.Group{}
	eg.SetLimit(maxConcurrentDownloads)
	for i, inv := range invocations {
		eg.Go(func() error {
			result := c.executionLog(ctx, inv.GetInvocationId())
			// Failures caused by our own interrupt aren't worth counting.
			if result.err != nil && ctx.Err() != nil {
				return nil
			}
			results[i] = result
			return nil
		})
	}
	// The callbacks never return an error; failures travel in the results.
	eg.Wait()
	return results
}

// tracef logs a step of the fetch when --trace is on. The log prefix carries
// millisecond timestamps, so the gaps between lines are where the time went.
func tracef(format string, args ...any) {
	if *trace {
		log.Infof(format, args...)
	}
}

// blobCheckTimeout bounds a cache presence lookup, which happens while the user
// waits with a file expanded.
const blobCheckTimeout = 15 * time.Second

// missingBlobs reports which of the given digests are no longer in the CAS.
// The instance name and digest function have to match the build that wrote
// them, or everything comes back missing.
func (c *apiClient) missingBlobs(ctx context.Context, instanceName string, digestFunction repb.DigestFunction_Value, digests []*repb.Digest) ([]*repb.Digest, error) {
	ctx, cancel := context.WithTimeout(apiContext(ctx), blobCheckTimeout)
	defer cancel()
	rsp, err := c.cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		InstanceName:   instanceName,
		BlobDigests:    digests,
		DigestFunction: digestFunction,
	})
	if err != nil {
		return nil, err
	}
	return rsp.GetMissingBlobDigests(), nil
}

// byteTally is how much execution log data we've read, and how much of that
// came off local disk instead of the network.
type byteTally struct {
	read   int64
	cached int64
}

// byteSummary renders a tally as "20.4MB (2.6MB cached)", dropping the
// parenthetical when nothing came from the cache.
func byteSummary(b byteTally) string {
	s := units.HumanSize(float64(b.read))
	if b.cached > 0 {
		s += fmt.Sprintf(" (%s cached)", units.HumanSize(float64(b.cached)))
	}
	return s
}

// progressOut is where the status line goes. It's a variable so that tests can
// read what was printed.
var progressOut io.Writer = os.Stderr

// progress reports the growing tree while logs are being merged. It rewrites a
// single line when stderr is a terminal, and prints one line per log otherwise
// so that piped output stays readable.
type progress struct {
	tty     bool
	printed bool
	// lastMerged is the invocation of the most recently merged log, which is
	// what the reported timestamp refers to. Skipped invocations don't move it.
	lastMerged *inpb.Invocation
}

func newProgress() *progress {
	// Rewriting one line in place would eat the trace output, so when tracing
	// the tally prints a line at a time like it does when piped.
	return &progress{tty: !*trace && term.IsTerminal(int(os.Stderr.Fd()))}
}

// update reports the tree as it stands after merging an invocation's log.
func (p *progress) update(t *tree, merged, unchanged, failed int, bytes byteTally, inv *inpb.Invocation) {
	p.lastMerged = inv
	p.render(progressLine(t, merged, unchanged, failed, bytes, inv), true)
}

// refresh redraws the tally after something that isn't a merge, like a log that
// failed to download. It only does anything on a terminal, so that piped output
// stays at one line per merged log.
func (p *progress) refresh(t *tree, merged, unchanged, failed int, bytes byteTally) {
	if !p.tty {
		return
	}
	p.render(progressLine(t, merged, unchanged, failed, bytes, p.lastMerged), false)
}

// progressLine is the status line itself: how much of the tree we have, how
// much data it cost, and how far back through history the last merged log was.
func progressLine(t *tree, merged, unchanged, failed int, bytes byteTally, lastMerged *inpb.Invocation) string {
	line := fmt.Sprintf("fetched %d logs · %s · %d dirs · %d files",
		merged, byteSummary(bytes), t.shownDirs, t.shownFiles)
	if unchanged > 0 {
		line += fmt.Sprintf(" · %d with nothing new", unchanged)
	}
	if failed > 0 {
		line += fmt.Sprintf(" · %d failed to fetch", failed)
	}
	if usec := lastMerged.GetUpdatedAtUsec(); usec > 0 {
		line += fmt.Sprintf(" · back to %s", time.UnixMicro(usec).Format(time.DateTime))
	}
	return line
}

// render writes the status line, either in place or as its own line. Lines that
// aren't forced are dropped when we can't rewrite in place.
func (p *progress) render(line string, force bool) {
	if !p.tty {
		if force {
			p.printed = true
			fmt.Fprintln(progressOut, line)
		}
		return
	}
	p.printed = true
	// \r and erase-to-end-of-line, so the tally updates in place.
	fmt.Fprintf(progressOut, "\r\033[K%s", line)
}

// finish closes out the progress line. It is safe to call more than once.
func (p *progress) finish() {
	if p.tty && p.printed {
		fmt.Fprintln(progressOut)
	}
	p.printed = false
}

// invocationLabel labels an invocation's log in the merged output. The commit
// is more meaningful than the invocation ID when reading the modified file
// list, but keep both so the invocation can still be looked up.
func invocationLabel(inv *inpb.Invocation) string {
	if sha := inv.GetCommitSha(); len(sha) >= 7 {
		return fmt.Sprintf("%s@%s", inv.GetInvocationId(), sha[:7])
	}
	return inv.GetInvocationId()
}

// executionLog downloads one invocation's compact execution log, returning nil
// if the invocation doesn't have one.
func (c *apiClient) executionLog(ctx context.Context, invocationID string) downloadResult {
	start := time.Now()
	tracef("invocation fetch started: %s", invocationID)
	defer func() {
		tracef("invocation fetch finished: %s in %s", invocationID, time.Since(start).Round(time.Millisecond))
	}()

	// Ask the local cache before the server: a hit skips the invocation lookup
	// below, which carries the whole build event stream back with it.
	if entry, ok := readCachedLog(invocationID); ok {
		if entry.noLog {
			tracef("log fetch finished: %s cached, no execution log", invocationID)
			return downloadResult{}
		}
		tracef("log fetch started: %s from the local cache", invocationID)
		tracef("log fetch finished: %s cached, %s in %s",
			invocationID, entry.path, time.Since(start).Round(time.Millisecond))
		return downloadResult{
			path:      entry.path,
			wireBytes: entry.wireBytes,
			cache:     entry.cache,
			cached:    true,
		}
	}
	// Anything between here and the next line is the invocation lookup, which
	// brings the whole build event stream back with it.
	rsp, err := c.bb.GetInvocation(ctx, &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{InvocationId: invocationID},
	})
	if err != nil {
		// Don't record anything: a lookup that failed says nothing about
		// whether the invocation has a log, and we want to try again.
		return downloadResult{err: fmt.Errorf("get invocation: %w", err)}
	}
	if len(rsp.GetInvocation()) == 0 {
		return downloadResult{err: fmt.Errorf("no such invocation")}
	}
	inv := rsp.GetInvocation()[0]
	file := findExecutionLog(inv)
	if file == nil {
		tracef("log fetch skipped: %s has no execution log (%s)", invocationID, inv.GetInvocationStatus())
		// Remember that this invocation has no log, but only once the build has
		// finished: one still running may simply not have uploaded it yet, and
		// that "no" would stick forever.
		if inv.GetInvocationStatus() == inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS {
			cacheNoLog(invocationID)
		}
		return downloadResult{}
	}
	// Small build tool logs are inlined in the build event itself.
	if contents := file.GetContents(); len(contents) > 0 {
		tracef("log fetch finished: %s inline, %d bytes", invocationID, len(contents))
		cacheLog(invocationID, cacheLocation{}, contents)
		return downloadResult{data: contents, wireBytes: int64(len(contents))}
	}
	ref := parseBlobURI(file.GetUri())
	downloadStart := time.Now()
	tracef("log fetch started: %s downloading %d bytes", invocationID, ref.size)
	data, err := c.download(ctx, invocationID, file.GetUri(), file.GetName())
	tracef("log fetch finished: %s downloaded, %d bytes in %s",
		invocationID, len(data), time.Since(downloadStart).Round(time.Millisecond))
	if err == nil {
		cacheLog(invocationID, ref.cache, data)
	}
	return downloadResult{data: data, wireBytes: int64(len(data)), cache: ref.cache, err: err}
}

// findExecutionLog returns the compact execution log file in the invocation's
// buildToolLogs event, if it has one.
func findExecutionLog(inv *inpb.Invocation) *bespb.File {
	for _, event := range inv.GetEvent() {
		for _, file := range event.GetBuildEvent().GetBuildToolLogs().GetLog() {
			if file.GetName() == executionLogName {
				return file
			}
		}
	}
	return nil
}

// download fetches an invocation's file through the app's /file/download
// endpoint, which takes the bytestream URI whole.
//
// The endpoint reads the CAS first and, if the blob isn't there any more, falls
// back to the copy the app persisted in blobstore under the invocation
// (<invocation_id>/artifacts/cache/<uri path>, written by the build event
// handler for every buildToolLogs URI). Walking back through weeks of history
// means asking for a lot of blobs that have long since been evicted from the
// cache, so that fallback is the difference between reconstructing an old tree
// and giving up on it.
//
// Reading the blob over the gRPC ByteStream API instead can't work: a
// ByteStream resource name has nowhere to put an invocation ID, so the server
// has no way to find the persisted copy and can only ever serve the CAS.
func (c *apiClient) download(ctx context.Context, invocationID, bytestreamURI, filename string) ([]byte, error) {
	u, err := url.Parse(strings.TrimSuffix(*appURL, "/") + "/file/download")
	if err != nil {
		return nil, fmt.Errorf("parse --app_url %q: %w", *appURL, err)
	}
	u.RawQuery = url.Values{
		"invocation_id":  {invocationID},
		"bytestream_url": {bytestreamURI},
		"filename":       {filename},
	}.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-buildbuddy-api-key", *apiKey)
	// Keep the bytes exactly as stored: the log is zstd-compressed already, and
	// we want to hand those bytes straight to the parser.
	req.Header.Set("Accept-Encoding", "identity")

	rsp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", filename, err)
	}
	defer rsp.Body.Close()
	if rsp.StatusCode != http.StatusOK {
		message := rsp.Status
		if body, _ := io.ReadAll(io.LimitReader(rsp.Body, 1024)); len(bytes.TrimSpace(body)) > 0 {
			message = string(bytes.TrimSpace(body))
		}
		return nil, fmt.Errorf("download %s: %s", filename, message)
	}
	data, err := io.ReadAll(rsp.Body)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", filename, err)
	}
	return data, nil
}

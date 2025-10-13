package main

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	"github.com/buildbuddy-io/buildbuddy/proto/invocation"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/gcs"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/compression"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/trace_events"
	"golang.org/x/sync/errgroup"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/metadata"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
)

var (
	invocationIDsFile = flag.String("invocation_ids_file", "", "File containing invocation IDs (one per line)")
	apiKey            = flag.String("api_key", "", "BuildBuddy API key for the org that owns the executions")
	apiTarget         = flag.String("api_target", "grpcs://remote.buildbuddy.io", "BuildBuddy gRPC API target")
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

type APIs struct {
	BuildBuddyService buildbuddy_service.BuildBuddyServiceClient
	ByteStream        bytestream.ByteStreamClient
	Blobstore         interfaces.Blobstore
}

func run() error {
	flag.Parse()

	if *invocationIDsFile == "" {
		return fmt.Errorf("executions_file flag is required")
	}
	if *apiKey == "" {
		return fmt.Errorf("api_key flag is required")
	}

	invocationIDs, err := readInvocationIDs(*invocationIDsFile)
	if err != nil {
		return fmt.Errorf("read invocation IDs: %w", err)
	}
	log.Infof("Processing %d invocation IDs", len(invocationIDs))

	ctx := context.Background()

	conn, err := grpc_client.DialSimpleWithoutPooling(*apiTarget)
	if err != nil {
		return fmt.Errorf("create BuildBuddy client: %w", err)
	}
	defer conn.Close()

	bbClient := buildbuddy_service.NewBuildBuddyServiceClient(conn)
	bsClient := bytestream.NewByteStreamClient(conn)

	// Get configured blobstore
	blobstore, err := gcs.NewGCSBlobStoreFromFlags(ctx)
	if err != nil {
		return fmt.Errorf("create GCS blobstore: %w", err)
	}

	apis := &APIs{
		BuildBuddyService: bbClient,
		ByteStream:        bsClient,
		Blobstore:         blobstore,
	}

	// Fetch invocation data and process timing profiles
	results, err := processInvocations(ctx, apis, invocationIDs)
	if err != nil {
		return fmt.Errorf("failed to process invocations: %w", err)
	}

	// Compute and display statistics
	displayStats(results)

	return nil
}

func readInvocationIDs(file string) ([]string, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read invocation IDs file: %w", err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n"), nil
}

func getShortOutputPath(outputPath string) string {
	parts := strings.Split(outputPath, "/")
	if len(parts) < 3 {
		return ""
	}
	if parts[0] != "bazel-out" || parts[2] != "bin" {
		return ""
	}
	return strings.Join(parts[3:], "/")
}

func processInvocations(ctx context.Context, apis *APIs, invocationIDs []string) ([]*ActionStats, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)

	type criticalPathFetchResult struct {
		InvocationID string
		CriticalPath *InvocationCriticalPath
		Err          error
	}
	ch := make(chan criticalPathFetchResult, 100)
	// Concurrently fetch critical paths in the background, and send results on
	// the channel.
	go func() {
		var eg errgroup.Group
		eg.SetLimit(4)
		defer func() {
			_ = eg.Wait()
			close(ch)
		}()
		for _, invocationID := range invocationIDs {
			eg.Go(func() error {
				criticalPath, err := getCriticalPathForInvocation(ctx, apis, invocationID)
				ch <- criticalPathFetchResult{
					InvocationID: invocationID,
					CriticalPath: criticalPath,
					Err:          err,
				}
				return nil
			})
		}
	}()

	// Process fetches as they are sent on the channel, populating stats.
	statsByActionProcessingEventName := make(map[string]*ActionStats)
	for res := range ch {
		invocationID, criticalPath, err := res.InvocationID, res.CriticalPath, res.Err
		if err != nil {
			log.Warningf("Failed to get critical path for invocation %s: %v", invocationID, err)
			continue
		}
		for action, components := range criticalPath.ComponentsByActionProcessingEventName {
			s := statsByActionProcessingEventName[action]
			if s == nil {
				s = &ActionStats{
					ActionProcessingEventName: action,

					ActionMnemonic: components[0].ActionMnemonic,
					TargetLabel:    components[0].TargetLabel,
					OutputPath:     components[0].OutputPath,
				}
				statsByActionProcessingEventName[action] = s
			}
			// Compute total critical path duration for this action
			var invocationCriticalPathUsec int64
			for _, c := range components {
				invocationCriticalPathUsec += c.DurationUsec
			}
			s.invocationCriticalPathDurationsUsec = append(s.invocationCriticalPathDurationsUsec, invocationCriticalPathUsec)
			s.invocationCriticalPathFractions = append(s.invocationCriticalPathFractions, float64(invocationCriticalPathUsec)/float64(criticalPath.TotalDurationUsec))
		}
	}
	// Compute final stats (p50 etc.)
	stats := slices.Collect(maps.Values(statsByActionProcessingEventName))
	for _, s := range stats {
		s.P50InvocationCriticalPathDurationUsec = quantile(s.invocationCriticalPathDurationsUsec, 0.5)
		s.P50InvocationCriticalPathFraction = quantile(s.invocationCriticalPathFractions, 0.5)
	}

	return stats, nil
}

func getCriticalPathForInvocation(ctx context.Context, apis *APIs, invocationID string) (*InvocationCriticalPath, error) {
	// Check cache first to avoid refetching the invocation and timing profile.
	cacheKey := filepath.Join(invocationID + ".criticalpath.json")
	if b, found, err := toolCacheGet(cacheKey); err != nil {
		return nil, fmt.Errorf("get critical path from cache: %w", err)
	} else if found {
		out := &InvocationCriticalPath{}
		if err := json.Unmarshal(b, out); err != nil {
			return nil, fmt.Errorf("unmarshal cached critical path: %w", err)
		}
		return out, nil
	}

	// Fetch invocation events using the internal API. The events should contain
	// the timing profile reference.
	req := &invocation.GetInvocationRequest{
		Lookup: &invocation.InvocationLookup{
			InvocationId: invocationID,
		},
	}
	resp, err := apis.BuildBuddyService.GetInvocation(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("get invocation: %w", err)
	}
	if len(resp.Invocation) == 0 {
		return nil, fmt.Errorf("invocation response contains no invocations")
	}
	profileFile := findTimingProfileFile(resp.Invocation[0])
	if profileFile == nil {
		return nil, fmt.Errorf("timing profile not found in invocation events")
	}

	// Fetch the full timing profile.
	profile, err := fetchTimingProfile(ctx, apis, invocationID, profileFile)
	if err != nil {
		return nil, fmt.Errorf("fetch timing profile: %w", err)
	}

	// Parse the critical path information from the profile.
	criticalPath := parseCriticalPathFromProfile(profile)

	// Cache critical path JSON.
	b, err := json.Marshal(criticalPath)
	if err != nil {
		return nil, fmt.Errorf("marshal critical path: %w", err)
	}
	if err := toolCacheSet(cacheKey, b); err != nil {
		return nil, fmt.Errorf("cache critical path: %w", err)
	}

	return criticalPath, nil
}

func toolCacheRoot() (string, error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("get user cache directory: %w", err)
	}
	return filepath.Join(cacheDir, "buildbuddy", "criticalpath"), nil
}

func toolCacheGet(key string) (data []byte, ok bool, err error) {
	cacheRoot, err := toolCacheRoot()
	if err != nil {
		return nil, false, fmt.Errorf("get cache root: %w", err)
	}
	cacheFile := filepath.Join(cacheRoot, key) + ".zst"
	b, err := os.ReadFile(cacheFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read cache file: %w", err)
	}
	b, err = compression.DecompressZstd(nil, b)
	if err != nil {
		return nil, false, fmt.Errorf("decompress cached contents: %w", err)
	}
	return b, true, nil
}

func toolCacheSet(key string, data []byte) error {
	cacheRoot, err := toolCacheRoot()
	if err != nil {
		return fmt.Errorf("get cache root: %w", err)
	}
	cacheFile := filepath.Join(cacheRoot, key) + ".zst"
	if err := os.MkdirAll(filepath.Dir(cacheFile), 0755); err != nil {
		return fmt.Errorf("create cache directory: %w", err)
	}
	tempFile := cacheFile + "." + strconv.Itoa(rand.IntN(1e12)) + ".tmp"
	data = compression.CompressZstd(nil, data)
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("write %s: %w", tempFile, err)
	}
	if err := os.Rename(tempFile, cacheFile); err != nil {
		return fmt.Errorf("rename %q to %q: %w", tempFile, cacheFile, err)
	}
	return nil
}

func findTimingProfileFile(inv *invocation.Invocation) *bespb.File {
	// TODO: Parse command line for --profile flag.
	const defaultProfileFileName = "command.profile.gz"
	for _, invEvent := range inv.Event {
		for _, file := range invEvent.GetBuildEvent().GetBuildToolLogs().GetLog() {
			if file.Name == defaultProfileFileName {
				return file
			}
		}
	}
	return nil
}

func fetchTimingProfile(ctx context.Context, apis *APIs, invocationID string, profileFile *bespb.File) (*trace_events.Profile, error) {
	// Strip protocol, host, and port to get the download resource name.
	u, err := url.Parse(profileFile.GetUri())
	if err != nil {
		return nil, fmt.Errorf("parse timing profile URI %q: %w", profileFile.GetUri(), err)
	}
	casResourceName, err := digest.ParseDownloadResourceName(u.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse resource name %s: %w", u.Path, err)
	}

	// Download the compressed profile from cache or GCS.
	var compressedBuf bytes.Buffer
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	if err := cachetools.GetBlob(ctx, apis.ByteStream, casResourceName, &compressedBuf); err != nil {
		blobName := path.Join(invocationID, "artifacts", "cache", casResourceName.DownloadString())
		if b, err := apis.Blobstore.ReadBlob(ctx, blobName); err != nil {
			return nil, fmt.Errorf("read timing profile blob %q from blobstore: %w", blobName, err)
		} else {
			compressedBuf.Reset()
			compressedBuf.Write(b)
		}
	}

	// Decompress.
	var data []byte
	if strings.HasSuffix(profileFile.GetName(), ".gz") {
		gzr, err := gzip.NewReader(&compressedBuf)
		if err != nil {
			return nil, fmt.Errorf("create gzip reader: %w", err)
		}
		defer gzr.Close()
		b, err := io.ReadAll(gzr)
		if err != nil {
			return nil, fmt.Errorf("read profile data: %w", err)
		}
		data = b
	} else {
		data = compressedBuf.Bytes()
	}

	// Parse and return the trace profile.
	profile := &trace_events.Profile{}
	if err := json.Unmarshal(data, profile); err != nil {
		return nil, fmt.Errorf("unmarshal profile: %w", err)
	}
	return profile, nil
}

func getHTTPEndpoint() string {
	if strings.HasSuffix(*apiTarget, ".buildbuddy.io") {
		return "https://app.buildbuddy.io"
	} else if strings.HasSuffix(*apiTarget, ".buildbuddy.dev") {
		return "https://app.buildbuddy.dev"
	} else {
		return "http://localhost:8080"
	}
}

type InvocationCriticalPath struct {
	ComponentsByActionProcessingEventName map[string][]*CriticalPathComponent
	// ComponentsByTargetLabel map[string][]*CriticalPathComponent
	// NOTE: this won't be populated unless
	// --experimental_profile_include_primary_output is set.
	// ComponentsByOutputPath map[string][]*CriticalPathComponent
	TotalDurationUsec int64
}

type CriticalPathComponent struct {
	ActionProcessingEventName string

	// NOTE: these aren't always populated.
	TargetLabel    string
	OutputPath     string
	ActionMnemonic string

	DurationUsec int64
}

// Finds critical path components in Bazel's timing profile as well as
// corresponding action processing events.
func parseCriticalPathFromProfile(profile *trace_events.Profile) *InvocationCriticalPath {
	componentsByActionProcessingEventName := map[string][]*CriticalPathComponent{}
	// componentsByOutputPath := map[string][]*CriticalPathComponent{}
	// componentsByTargetLabel := map[string][]*CriticalPathComponent{}

	// First pass: build critical path events by action event name, and track
	// total duration.
	var maxEndTimeUsec int64
	for _, event := range profile.TraceEvents {
		if event.Category == "critical path component" {
			actionProcessingEventName := parseActionProcessingEventNameFromCriticalPathEventName(event)
			c := &CriticalPathComponent{
				ActionProcessingEventName: actionProcessingEventName,
				DurationUsec:              event.Duration,
			}
			componentsByActionProcessingEventName[actionProcessingEventName] = append(componentsByActionProcessingEventName[actionProcessingEventName], c)
		}
		// Track overall duration
		maxEndTimeUsec = max(maxEndTimeUsec, event.Timestamp+event.Duration)
	}

	// Second pass: associate events in "action processing" category with
	// critical path events based on the event name.
	for _, event := range profile.TraceEvents {
		if event.Category != "action processing" {
			continue
		}
		actionEventName := event.Name
		cpcs := componentsByActionProcessingEventName[actionEventName]
		outputPath := event.Output
		if outputPath != "" {
			for _, cpe := range cpcs {
				cpe.OutputPath = outputPath
			}
			// componentsByOutputPath[outputPath] = append(componentsByOutputPath[outputPath], cpcs...)
		}
		if args := event.Args; args != nil && args["target"] != "" {
			targetLabel := args["target"].(string)
			for _, cpc := range cpcs {
				cpc.TargetLabel = targetLabel
			}
			// componentsByTargetLabel[targetLabel] = append(componentsByTargetLabel[targetLabel], cpcs...)
		}
	}

	return &InvocationCriticalPath{
		ComponentsByActionProcessingEventName: componentsByActionProcessingEventName,
		TotalDurationUsec:                     maxEndTimeUsec,
	}
}

func sdump(obj any) string {
	b, _ := json.Marshal(obj)
	return string(b)
}

func parseActionProcessingEventNameFromCriticalPathEventName(event *trace_events.Event) string {
	// Parse the action name from critical path events
	// Extract the action content between the quotes
	eventName := event.Name
	if !strings.HasPrefix(eventName, "action '") {
		return ""
	}
	start := strings.Index(eventName, "'") + 1
	end := strings.LastIndex(eventName, "'")
	if start <= 0 || end <= start {
		return ""
	}
	return eventName[start:end]
}

type ActionStats struct {
	ActionProcessingEventName string

	ActionMnemonic string
	TargetLabel    string
	OutputPath     string

	// TotalInvocationCriticalPathDurationUsec int64
	// TotalInvocationDurationUsec             int64

	invocationCriticalPathFractions     []float64
	invocationCriticalPathDurationsUsec []int64

	P50InvocationCriticalPathFraction     float64
	P50InvocationCriticalPathDurationUsec int64
}

func (s *ActionStats) InvocationCount() int64 {
	return int64(len(s.invocationCriticalPathFractions))
}

func displayStats(allActionStats []*ActionStats) {
	// Sort by p50 critical path duration * invocation count. The median gives a
	// good estimation of how long the action "normally" takes (and is robust to
	// outliers), while the invocation count gives higher weight to actions that
	// are run more frequently.
	allActionStats = slices.Clone(allActionStats)
	slices.SortFunc(allActionStats, func(a, b *ActionStats) int {
		va := a.P50InvocationCriticalPathDurationUsec * a.InvocationCount()
		vb := b.P50InvocationCriticalPathDurationUsec * b.InvocationCount()
		return -cmp.Compare(va, vb)
	})
	// Display summary
	log.Infof("Total unique actions on critical path: %d", len(allActionStats))

	header := []string{"ACTION", "INVOCATIONS", "P50(s)", "P50%"}
	columns := map[string][]string{}
	nRows := 0

	for i, stats := range allActionStats {
		if i >= 32 {
			break
		}
		// totalCriticalPathSec := float64(stats.TotalInvocationCriticalPathDurationUsec) / 1e6
		// avgCriticalPathSec := totalCriticalPathSec / float64(stats.InvocationCount)

		columns["ACTION"] = append(columns["ACTION"], truncateString(stats.ActionProcessingEventName, 120))
		columns["INVOCATIONS"] = append(columns["INVOCATIONS"], fmt.Sprintf("%d", stats.InvocationCount()))
		// columns["TOTAL(s)"] = append(columns["TOTAL(s)"], fmt.Sprintf("%.1f", totalCriticalPathSec))
		// columns["AVG(s)"] = append(columns["AVG(s)"], fmt.Sprintf("%.1f", avgCriticalPathSec))
		// columns["AVG%"] = append(columns["AVG%"], fmt.Sprintf("%.2f%%", 100*stats.AverageCriticalPathFraction()))
		columns["P50(s)"] = append(columns["P50(s)"], fmt.Sprintf("%.2f", float64(stats.P50InvocationCriticalPathDurationUsec)/1e6))
		columns["P50%"] = append(columns["P50%"], fmt.Sprintf("%.1f%%", stats.P50InvocationCriticalPathFraction*100))
		nRows++
	}

	columnWidths := map[string]int{}
	for _, header := range header {
		columnWidths[header] = len(header)
	}
	for h, column := range columns {
		for _, cell := range column {
			columnWidths[h] = max(columnWidths[h], len(cell))
		}
	}
	const columnDivider = "  "
	for c, header := range header {
		format := "%-*s" // Left-align
		if c > 0 {
			format = "%*s" // Right-align
		}
		fmt.Printf(format, columnWidths[header], header)
		if c < len(header)-1 {
			fmt.Print(columnDivider)
		}
	}
	fmt.Println()
	for r := range nRows {
		for c, header := range header {
			format := "%-*s" // Left-align
			if c > 0 {
				format = "%*s" // Right-align
			}
			fmt.Printf(format, columnWidths[header], columns[header][r])
			if c < len(header)-1 {
				fmt.Print(columnDivider)
			}
		}
		fmt.Println()
	}
}

func abbreviateOutputPath(s string) string {
	s = strings.ReplaceAll(s, "bazel-out/", "O/")
	s = strings.ReplaceAll(s, "linux_x86_64", "L64")
	s = strings.ReplaceAll(s, "/platform_", "/[p]_")
	return s
}

func shortOutputPath(outputPath string) string {
	parts := strings.Split(outputPath, "/")
	if len(parts) < 3 {
		return outputPath
	}
	if parts[0] != "bazel-out" || parts[2] != "bin" {
		return outputPath
	}
	return strings.Join(parts[3:], "/")
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func sum[T ~int64 | ~float64](s []T) (out T) {
	for _, v := range s {
		out += v
	}
	return out
}

func avg[T ~int64 | ~float64](s []T) (out float64) {
	return float64(sum(s)) / float64(len(s))
}

func quantile[T ~int64 | ~float64](s []T, q float64) (out T) {
	s = slices.Clone(s)
	slices.Sort(s)
	return s[int(q*float64(len(s)))]
}

// Tool to analyze differences between 'teed' executions and the original
// execution they were teed from.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

var (
	// Required flags
	target        = flag.String("target", "", "BuildBuddy gRPC target.")
	scriptGroupID = flag.String("admin_group_id", "", "Group ID for impersonation requests. Must be the server admin group ID.")
	scriptAPIKey  = flag.String("admin_api_key", "", "API key for impersonation requests. Must be an 'Org admin' key.")

	// Flags for customizing "Teed execution" log fetching.
	// -log_lookback and -log_min_age control the time window.
	// -log_filters can help query more efficiently, e.g.: -log_filters='resource.labels.cluster_name="prod-xxxx"'
	logFilters   = flag.Slice("log_filters", []string{}, "Additional filters to apply to the log query.")
	logBatchSize = flag.Int("log_batch_size", 2500, "Number of log entries to fetch for each batch. Setting this too low may result in rate limiting.")
	logLimit     = flag.Int("log_limit", 100_000, "Max number of log entries to fetch.")
	logLookback  = flag.Duration("log_lookback", 3*time.Hour, "How far to look back in logs.")
	logMinAge    = flag.Duration("log_min_age", 30*time.Minute, "Don't fetch teed executions newer than this duration (to give them some time to complete).")

	// Execution filters
	teeExecutorID       = flag.String("tee_executor_id", "", "Ignore executions that didn't occur on this executor_id")
	ignoreIsolationType = flag.String("ignore_isolation_type", "", "Ignore executions that requested this workload-isolation-type value.")
	ignoreDockerNetwork = flag.String("ignore_docker_network", "", "Ignore executions that requested this dockerNetwork value.")

	// Analysis options
	replayCount = flag.Int("replay_count", 8, "Number of times to replay actions when the teed execution result didn't match the original.")
)

var (
	executionIDsRegexp = regexp.MustCompile(`Teed execution "(.*?)" for original execution "(.*)"`)
)

var (
	impersonationKeyCache = &APIKeyCache{}
)

type APIKeyCache struct {
	mu   sync.Mutex
	keys map[string]*APIKey
}

type APIKey struct {
	Value  string
	Expiry time.Time
}

func getAPIKeyForGroup(ctx context.Context, client bbspb.BuildBuddyServiceClient, cache *APIKeyCache, groupID string) (string, error) {
	cache.mu.Lock()
	if cache.keys == nil {
		cache.keys = map[string]*APIKey{}
	}
	k := cache.keys[groupID]
	cache.mu.Unlock()

	if k != nil && k.Expiry.After(time.Now().Add(15*time.Minute)) {
		return k.Value, nil
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *scriptAPIKey)
	res, err := client.CreateImpersonationApiKey(ctx, &akpb.CreateImpersonationApiKeyRequest{
		RequestContext: &ctxpb.RequestContext{
			GroupId:              groupID,
			ImpersonatingGroupId: groupID,
		},
	})
	if err != nil {
		return "", fmt.Errorf("create new impersonation key: %w", err)
	}

	k = &APIKey{
		Value:  res.GetApiKey().GetValue(),
		Expiry: time.UnixMicro(res.GetApiKey().GetExpiryUsec()),
	}

	cache.mu.Lock()
	cache.keys[groupID] = k
	cache.mu.Unlock()

	return k.Value, nil
}

func main() {
	flag.Parse()
	_ = log.Configure()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

type Stats struct {
	TeeFetchErrors      int
	OriginalFetchErrors int

	TeeCount   int
	Mismatches int
}

type TeedExecution struct {
	GroupID      string
	APIKey       string
	InvocationID string

	OriginalExecutionID     string
	OriginalExecuteResponse *repb.ExecuteResponse

	TeeExecutionID     string
	TeeExecuteResponse *repb.ExecuteResponse
}

func run() error {
	ctx := context.Background()

	if *target == "" {
		return fmt.Errorf("missing -target flag")
	}

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return fmt.Errorf("dial %q: %w", *target, err)
	}
	defer conn.Close()

	env := real_environment.NewBatchEnv()

	// rexec package gets ExecutionClient from env :|
	env.SetRemoteExecutionClient(repb.NewExecutionClient(conn))

	bb := bbspb.NewBuildBuddyServiceClient(conn)
	ac := repb.NewActionCacheClient(conn)
	bs := bspb.NewByteStreamClient(conn)

	query := append([]string{
		`sourceLocation.file="execution_server.go"`,
		`jsonPayload.message:"Teed execution"`,
	}, *logFilters...)

	ch := make(chan *LogEntry, 2000)
	go func() {
		defer close(ch)
		log.Info("Starting log fetcher...")
		if err := ReadAllLogs(ctx, ch, time.Now().Add(-*logLookback), time.Now().Add(-*logMinAge), query...); err != nil && ctx.Err() == nil {
			log.Errorf("Failed to read logs: %s", err)
		}
	}()

	apiKeyCache := &APIKeyCache{}
	var groupIDs []string

	var statsMu sync.Mutex
	groupStats := map[string]*Stats{}

	var groupNameMu sync.Mutex
	groupNameCache := map[string]string{}

	groupName := func(groupID string) string {
		if groupID == "" {
			return "ANON"
		}

		groupNameMu.Lock()
		name, ok := groupNameCache[groupID]
		groupNameMu.Unlock()
		if ok {
			return name
		}

		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *scriptAPIKey)
		grp, err := bb.GetGroup(ctx, &grpb.GetGroupRequest{
			RequestContext: &ctxpb.RequestContext{
				GroupId: *scriptGroupID,
			},
			GroupId: groupID,
		})

		name = groupID
		if err != nil {
			log.Warningf("Failed to get group name: %s", err)
		} else {
			name = grp.GetName()
		}

		groupNameMu.Lock()
		groupNameCache[groupID] = name
		groupNameMu.Unlock()

		return name
	}

	printStats := func() {
		statsMu.Lock()
		defer statsMu.Unlock()

		log.Infof("Stats:")
		rows := [][]any{
			{"GID", "NAME", "N", "MISMATCH", "FETCHERR_O", "FETCHERR_T"},
		}

		groupIDs = slices.Clone(groupIDs)
		slices.SortFunc(groupIDs, func(g1, g2 string) int {
			return -(groupStats[g1].TeeCount - groupStats[g2].TeeCount)
		})

		for _, groupID := range groupIDs {
			stats := groupStats[groupID]
			rows = append(rows, []any{
				groupID, strings.ReplaceAll(groupName(groupID), " ", "_"), stats.TeeCount, stats.Mismatches, stats.OriginalFetchErrors, stats.TeeFetchErrors,
			})
		}
		const colPadding = 2
		colWidth := make([]int, len(rows[0]))
		isTextColumn := map[int]bool{}
		for r, row := range rows {
			for c, cell := range row {
				if r > 0 && !isNumber(fmt.Sprint(cell)) {
					isTextColumn[c] = true
				}
				colWidth[c] = max(colWidth[c], len(fmt.Sprint(cell)))
			}
		}
		for _, row := range rows {
			var line []string
			for c, cell := range row {
				pad := leftPad
				if isTextColumn[c] {
					pad = rightPad
				}
				line = append(line, pad(fmt.Sprint(cell), colWidth[c]))
			}
			fmt.Println(strings.Join(line, strings.Repeat(" ", colPadding)))
		}
	}

	var replayGroup errgroup.Group
	replayCh := make(chan *TeedExecution, 1000)
	for i := 0; i < 4; i++ {
		replayGroup.Go(func() error {
			for task := range replayCh {
				result, err := replay(ctx, env, task)
				if err != nil {
					log.Errorf("Failed to replay: %s", err)
				} else {
					origStr := executionDesc(ctx, bs, task.APIKey, task.OriginalExecutionID, task.OriginalExecuteResponse)
					teeStr := executionDesc(ctx, bs, task.APIKey, task.TeeExecutionID, task.TeeExecuteResponse)
					replayInfo := result.Info(ctx, bs)
					origRN, err := digest.ParseUploadResourceName(task.OriginalExecutionID)
					origDigestHash := ""
					if err != nil {
						log.Errorf("Failed to parse execution ID %q: %s", task.OriginalExecutionID, err)
					} else {
						origDigestHash = origRN.GetDigest().GetHash()
					}

					log.Infof("Execution mismatch (group_id=%q, group_name=%q, invocation_id=%q):", task.GroupID, groupName(task.GroupID), task.InvocationID)
					fmt.Printf("Orig invocation link: https://app.buildbuddy.io/invocation/%s?executionFilter=%s#execution\n", task.InvocationID, origDigestHash)
					fmt.Printf("Orig: %s\n", origStr)
					fmt.Printf("Teed: %s\n", teeStr)
					fmt.Printf("Replay results:\n")
					fmt.Println(replayInfo)
				}
			}
			return nil
		})
	}

	var responseFetchGroup errgroup.Group
	responseFetchGroup.SetLimit(32)

	i := -1
	for entry := range ch {
		i++
		// Parse execution IDs from message
		m := executionIDsRegexp.FindStringSubmatch(entry.Message())
		if len(m) == 0 {
			log.Warningf("Could not extract execution ID from message %q", entry.Message())
			continue
		}
		teeExecutionID := m[1]
		originalExecutionID := m[2]

		groupID := entry.GetPayloadAttr("group_id")
		invocationID := entry.GetPayloadAttr("invocation_id")

		if groupStats[groupID] == nil {
			groupIDs = append(groupIDs, groupID)
			groupStats[groupID] = &Stats{}
		}

		var apiKey string
		if groupID != "" {
			k, err := getAPIKeyForGroup(ctx, bb, apiKeyCache, groupID)
			if err != nil {
				return fmt.Errorf("get API key for group: %w", err)
			}
			apiKey = k
		}

		responseFetchGroup.Go(func() error {
			// If action filters are set, fetch the action first.
			if *ignoreIsolationType != "" || *ignoreDockerNetwork != "" {
				action, err := getAction(ctx, bs, apiKey, originalExecutionID)
				if err != nil {
					statsMu.Lock()
					defer statsMu.Unlock()
					groupStats[groupID].OriginalFetchErrors++
					log.Warningf("Failed to get Action for %q: %s (group_id=%s)", originalExecutionID, err, groupID)
					return nil
				}

				// Note: normally we parse exec properties from the Command
				// proto, but Bazel also includes them in the Action.
				for _, p := range action.GetPlatform().GetProperties() {
					if *ignoreIsolationType != "" && p.Name == "workload-isolation-type" && p.Value == *ignoreIsolationType {
						log.Debugf("Skipping execution %q with isolation type %s (group_id=%s)", originalExecutionID, *ignoreIsolationType, groupID)
						return nil
					}
					if *ignoreDockerNetwork != "" && p.Name == "dockerNetwork" && p.Value == *ignoreDockerNetwork {
						log.Debugf("Skipping execution %q with dockerNetwork %s (group_id=%s)", originalExecutionID, *ignoreDockerNetwork, groupID)
					}
				}

			}

			teeResponse, err := getCachedExecuteResponse(ctx, ac, apiKey, teeExecutionID)
			if err != nil {
				statsMu.Lock()
				defer statsMu.Unlock()
				groupStats[groupID].TeeFetchErrors++

				log.Warningf("Failed to get teed ExecuteResponse for %q: %s (group_id=%q)", teeExecutionID, err, groupID)
				return nil
			}
			if *teeExecutorID != "" && teeResponse.GetResult().GetExecutionMetadata().GetExecutorId() != *teeExecutorID {
				return nil
			}

			originalResponse, err := getCachedExecuteResponse(ctx, ac, apiKey, originalExecutionID)
			if err != nil {
				statsMu.Lock()
				defer statsMu.Unlock()
				groupStats[groupID].OriginalFetchErrors++

				log.Warningf("Failed to get original ExecuteResponse for %q: %s (group_id=%q)", originalExecutionID, err, groupID)
				return nil
			}

			var diffs []string

			if originalResponse.GetResult().GetExitCode() != teeResponse.GetResult().GetExitCode() {
				diffs = append(diffs, fmt.Sprintf("exit_code:orig=%d,tee=%d", originalResponse.GetResult().GetExitCode(), teeResponse.GetResult().GetExitCode()))
			}

			statsMu.Lock()
			defer statsMu.Unlock()

			if len(diffs) > 0 {
				log.Warningf("%s: tee=%q, original=%q, diffs=%s", groupName(groupID), teeExecutionID, originalExecutionID, diffs)
				groupStats[groupID].Mismatches++

				// Diagnose the mismatch.
				replayCh <- &TeedExecution{
					GroupID:                 groupID,
					InvocationID:            invocationID,
					APIKey:                  apiKey,
					OriginalExecutionID:     originalExecutionID,
					OriginalExecuteResponse: originalResponse,
					TeeExecutionID:          teeExecutionID,
					TeeExecuteResponse:      teeResponse,
				}
			}
			groupStats[groupID].TeeCount++

			return nil
		})

		if i > 0 && i%1_000 == 0 {
			printStats()
		}
	}

	if err := responseFetchGroup.Wait(); err != nil {
		return err
	}

	close(replayCh)
	replayGroup.Wait()

	printStats()

	return nil
}

var numberRegexp = regexp.MustCompile(`^[0-9]+$`)

func isNumber(s string) bool {
	return numberRegexp.MatchString(s)
}

func leftPad(s string, w int) string {
	return strings.Repeat(" ", w-len(s)) + s
}

func rightPad(s string, w int) string {
	return s + strings.Repeat(" ", w-len(s))
}

func getCachedExecuteResponse(ctx context.Context, ac repb.ActionCacheClient, apiKey, executionID string) (*repb.ExecuteResponse, error) {
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}
	rn, err := digest.ParseUploadResourceName(executionID)
	if err != nil {
		return nil, fmt.Errorf("parse execution ID: %w", err)
	}
	ad, err := digest.Compute(strings.NewReader(executionID), rn.GetDigestFunction())
	if err != nil {
		return nil, fmt.Errorf("compute execute response digest: %w", err)
	}
	res, err := ac.GetActionResult(ctx, &repb.GetActionResultRequest{
		ActionDigest:   ad,
		InstanceName:   rn.GetInstanceName(),
		DigestFunction: rn.GetDigestFunction(),
	})
	if err != nil {
		return nil, err
	}
	// ExecuteResponse is marshaled in stdout_raw
	executeResponse := &repb.ExecuteResponse{}
	if err := proto.Unmarshal(res.StdoutRaw, executeResponse); err != nil {
		return nil, fmt.Errorf("unmarshal ExecuteResponse: %w", err)
	}
	return executeResponse, nil
}

type ReplayResults struct {
	TeedExecution *TeedExecution

	OriginalReplayResults []*replayResult
	TeeReplayResults      []*replayResult
}

func (rr *ReplayResults) Info(ctx context.Context, bs bspb.ByteStreamClient) string {
	var lines []string
	lines = append(lines, "Original:")
	for i, r := range rr.OriginalReplayResults {
		lines = append(lines, fmt.Sprintf("- %d: %s", i, executionDesc(ctx, bs, rr.TeedExecution.APIKey, r.ExecutionID, r.ExecuteResponse)))
	}
	lines = append(lines, "Teed:")
	for i, r := range rr.TeeReplayResults {
		lines = append(lines, fmt.Sprintf("- %d: %s", i, executionDesc(ctx, bs, rr.TeedExecution.APIKey, r.ExecutionID, r.ExecuteResponse)))
	}

	return strings.Join(lines, "\n")
}

func trunc(s string, length int) string {
	if len(s) > length {
		return s[:length] + "..."
	}
	return s
}

func executionDesc(ctx context.Context, bs bspb.ByteStreamClient, apiKey, executionID string, res *repb.ExecuteResponse) string {
	var stderr []byte
	if res.GetResult().GetExitCode() != 0 {
		var err error
		stderr, err = getExecutionArtifact(ctx, bs, apiKey, executionID, res.GetResult().GetStderrDigest())
		if err != nil {
			stderr = []byte(fmt.Sprintf("<fetch failed: %q>", err.Error()))
		}
	}
	var statusMsg string
	if res.GetStatus().GetCode() != 0 {
		statusMsg = gstatus.ErrorProto(res.GetStatus()).Error()
	}

	return fmt.Sprintf("exit=%d, status=%q, stderr=%q, execution_id=%q", res.Result.GetExitCode(), statusMsg, trunc(string(stderr), 120), executionID)
}

func getAction(ctx context.Context, bs bspb.ByteStreamClient, apiKey, executionID string) (*repb.Action, error) {
	xrn, err := digest.ParseUploadResourceName(executionID)
	if err != nil {
		return nil, fmt.Errorf("parse execution ID: %w", err)
	}
	b, err := getExecutionArtifact(ctx, bs, apiKey, executionID, xrn.GetDigest())
	if err != nil {
		return nil, err
	}
	action := &repb.Action{}
	if err := proto.Unmarshal(b, action); err != nil {
		return nil, err
	}
	return action, nil
}

func getExecutionArtifact(ctx context.Context, bs bspb.ByteStreamClient, apiKey, executionID string, d *repb.Digest) ([]byte, error) {
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}

	if d.GetSizeBytes() == 0 {
		return nil, nil
	}

	xrn, err := digest.ParseUploadResourceName(executionID)
	if err != nil {
		return nil, fmt.Errorf("parse execution ID: %w", err)
	}
	rn := digest.NewResourceName(d, xrn.GetInstanceName(), rspb.CacheType_CAS, xrn.GetDigestFunction())

	buf := &bytes.Buffer{}
	if err := cachetools.GetBlob(ctx, bs, rn, buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Replays the original and teed executions to try and diagnose why their
// results were different.
func replay(ctx context.Context, env environment.Env, tx *TeedExecution) (*ReplayResults, error) {
	var mu sync.Mutex
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(2)

	res := &ReplayResults{TeedExecution: tx}

	// Replay original
	for i := 0; i < *replayCount; i++ {
		eg.Go(func() error {
			pool := ""
			rsp, err := replayExecution(ctx, env, tx.APIKey, pool, tx.OriginalExecutionID)
			if err != nil {
				return fmt.Errorf("replay original execution: %w", err)
			}
			mu.Lock()
			defer mu.Unlock()
			res.OriginalReplayResults = append(res.OriginalReplayResults, rsp)
			return nil
		})
	}

	// Replay teed (against tee pool)
	for i := 0; i < *replayCount; i++ {
		eg.Go(func() error {
			pool := "tee"
			rsp, err := replayExecution(ctx, env, tx.APIKey, pool, tx.TeeExecutionID)
			if err != nil {
				return fmt.Errorf("replay teed execution: %w", err)
			}
			mu.Lock()
			defer mu.Unlock()
			res.TeeReplayResults = append(res.TeeReplayResults, rsp)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return res, nil
}

type replayResult struct {
	*repb.ExecuteResponse
	ExecutionID string
}

func replayExecution(ctx context.Context, env environment.Env, apiKey, pool, executionID string) (*replayResult, error) {
	log.Infof("Replaying %s", executionID)
	rn, err := digest.ParseUploadResourceName(executionID)
	if err != nil {
		return nil, fmt.Errorf("parse execution ID: %w", err)
	}
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	}
	if pool != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-platform.pool", pool)
	}
	stream, err := rexec.Start(ctx, env, rn.ToProto())
	if err != nil {
		return nil, fmt.Errorf("start execution: %w", err)
	}
	rsp, err := rexec.Wait(stream)
	if err != nil {
		return nil, fmt.Errorf("wait execution: %w", err)
	}
	return &replayResult{
		ExecuteResponse: rsp.ExecuteResponse,
		ExecutionID:     stream.Name(),
	}, nil
}

type LogEntry struct {
	InsertID    string         `json:"insertId"`
	Timestamp   string         `json:"timestamp"`
	JSONPayload map[string]any `json:"jsonPayload"`
	TextPayload string         `json:"textPayload"`
}

func (e *LogEntry) Message() string {
	if message := e.GetPayloadAttr("message"); message != "" {
		return message
	}
	return e.TextPayload
}

// GetPayloadAttr returns a string attribute from the JSONPayload.
func (e *LogEntry) GetPayloadAttr(name string) string {
	s, ok := e.JSONPayload[name].(string)
	if ok {
		return s
	}
	return ""
}

func ReadAllLogs(ctx context.Context, ch chan<- *LogEntry, from, until time.Time, query ...string) error {
	// Track insertIDs to avoid streaming the same logs twice.
	seen := map[string]bool{}

	n := 0
	minTimestamp := from.UTC().Format("2006-01-02T15:04:05.000000000Z")
	maxTimestamp := until.UTC().Format("2006-01-02T15:04:05.000000000Z")
	for n < *logLimit && minTimestamp < maxTimestamp {
		q := append(query, `timestamp>="`+minTimestamp+`"`, `timestamp<="`+maxTimestamp+`"`)
		log.Debugf("Querying logs from %s - %s", minTimestamp, maxTimestamp)
		entries, err := ReadLogs(ctx, min(*logBatchSize, *logLimit-n), "asc", q...)
		if err != nil {
			return err
		}

		newEntries := 0
		for i := range entries {
			entry := &entries[i]
			if seen[entry.InsertID] {
				continue
			}
			seen[entry.InsertID] = true
			ch <- entry
			newEntries++
			n++

			// Next fetch should start from the max observed timestamp.
			minTimestamp = max(minTimestamp, entry.Timestamp)
		}
		if newEntries == 0 {
			// Fetched all logs.
			return nil
		}
	}
	return nil
}

func ReadLogs(ctx context.Context, limit int, order string, query ...string) ([]LogEntry, error) {
	q := strings.Join(query, "\n")
	var stdout bytes.Buffer
	cmd := exec.CommandContext(ctx, "gcloud", "logging", "read", "--order="+order, fmt.Sprintf("--limit=%d", limit), "--format=json", q)
	cmd.Stdout = &stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	var entries []LogEntry
	if err := json.Unmarshal(stdout.Bytes(), &entries); err != nil {
		return nil, fmt.Errorf("unmarshal JSON: %w", err)
	}
	return entries, nil
}

// Package execution_graph_worker polls for completed invocations and analyzes
// their execution graph logs.
//
// For every completed invocation found in the ClickHouse Invocations table,
// the worker replays the invocation's build event stream from blobstore to
// find the execution graph log (uploaded by Bazel when
// --experimental_enable_execution_graph_log is set) and the build phase
// timings, computes the critical path and node / edge / factor drags, stores
// the result in the blobstore alongside the invocation's other derived blobs,
// and increments drag metrics.
//
// Invocations without an execution graph log are skipped.
package execution_graph_worker

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/execution_graph"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang-jwt/jwt/v4"
	"google.golang.org/grpc/metadata"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	egapb "github.com/buildbuddy-io/buildbuddy/proto/execution_graph_analysis"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
)

var (
	pollInterval   = flag.Duration("executiongraph.poll_interval", 30*time.Second, "How often to poll for newly completed invocations.")
	lookback       = flag.Duration("executiongraph.lookback", 24*time.Hour, "On startup, how far back to scan for completed invocations.")
	watermarkLag   = flag.Duration("executiongraph.watermark_lag", time.Minute, "Only scan invocations that completed at least this long ago, to allow OLAP replication to settle.")
	batchSize      = flag.Int("executiongraph.batch_size", 500, "Maximum invocations fetched per poll.")
	maxNodes       = flag.Int("executiongraph.max_nodes", 100_000, "Skip execution graph logs with more than this many nodes.")
	dragThreshold  = flag.Float64("executiongraph.drag_metric_threshold", 0.1, "Only record drag metrics for drags larger than this fraction of the invocation duration.")
	processTimeout = flag.Duration("executiongraph.process_timeout", 5*time.Minute, "Per-invocation processing timeout.")
)

const (
	// Name prefix of the build tool log holding the execution graph, e.g.
	// "execution_graph_dump.proto.zst".
	executionGraphLogPrefix = "execution_graph"

	// Blobstore directory (under the invocation ID) where finalization
	// persists build tool logs; mirrors build_event_handler's
	// cacheArtifactsBlobstorePath.
	cacheArtifactsBlobstorePath = "artifacts/cache"

	// Bound on the in-memory set of recently processed invocation IDs.
	maxSeen = 50_000
)

type Worker struct {
	env environment.Env

	watermarkUsec int64
	seen          map[string]struct{}
}

func New(env environment.Env) (*Worker, error) {
	if env.GetOLAPDBHandle() == nil {
		return nil, status.FailedPreconditionError("execution graph worker requires an OLAP DB (ClickHouse)")
	}
	if env.GetBlobstore() == nil {
		return nil, status.FailedPreconditionError("execution graph worker requires a blobstore")
	}
	return &Worker{
		env:           env,
		watermarkUsec: time.Now().Add(-*lookback).UnixMicro(),
		seen:          make(map[string]struct{}),
	}, nil
}

// Start runs the poll loop until ctx is cancelled.
func (w *Worker) Start(ctx context.Context) {
	ticker := time.NewTicker(*pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.pollOnce(ctx); err != nil {
				log.Errorf("Execution graph worker poll failed: %s", err)
			}
		}
	}
}

type invocationRow struct {
	GroupID        string
	InvocationUUID string
	Attempt        uint64
	DurationUsec   int64
	UpdatedAtUsec  int64
}

func (w *Worker) pollOnce(ctx context.Context) error {
	hiUsec := time.Now().Add(-*watermarkLag).UnixMicro()
	if hiUsec <= w.watermarkUsec {
		return nil
	}
	rq := w.env.GetOLAPDBHandle().NewQuery(ctx, "execution_graph_worker_scan").Raw(`
		SELECT group_id, invocation_uuid, attempt, duration_usec, updated_at_usec
		FROM "Invocations"
		WHERE group_id = ? AND updated_at_usec > ? AND updated_at_usec <= ? AND invocation_status = ?
		ORDER BY updated_at_usec ASC
		LIMIT ?`,
		"GR13538686971810875473", w.watermarkUsec, hiUsec, int(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS), *batchSize)
	rows, err := db.ScanAll(rq, &invocationRow{})
	if err != nil {
		return status.WrapError(err, "scan completed invocations")
	}
	log.Infof("Found %v invocations", len(rows))
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, ok := w.seen[row.InvocationUUID]; ok {
			w.watermarkUsec = max(w.watermarkUsec, row.UpdatedAtUsec)
			continue
		}
		if err := w.processInvocation(ctx, row); err != nil {
			log.Warningf("Failed to process invocation %s: %s", row.InvocationUUID, err)
			metrics.ExecutionGraphFailedInvocations.Inc()
		}
		if len(w.seen) >= maxSeen {
			w.seen = make(map[string]struct{})
		}
		w.seen[row.InvocationUUID] = struct{}{}
		w.watermarkUsec = max(w.watermarkUsec, row.UpdatedAtUsec)
	}
	if len(rows) < *batchSize {
		w.watermarkUsec = max(w.watermarkUsec, hiUsec)
	}
	metrics.ExecutionGraphWatermarkTimestampSeconds.Set(float64(w.watermarkUsec) / 1e6)
	return nil
}

func (w *Worker) processInvocation(ctx context.Context, row *invocationRow) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, *processTimeout)
	defer cancel()

	iid, err := dashedUUID(row.InvocationUUID)
	if err != nil {
		return err
	}
	ctx = log.EnrichContext(ctx, log.InvocationIDKey, iid)
	log.Infof("Processing invocation %q", iid)

	if ok, err := execution_graph.HasAnalysis(ctx, w.env.GetBlobstore(), iid, row.Attempt); err == nil && ok {
		log.Infof("FOUND ANALYSIS FOR INVOCATION https://app.buildbuddy.dev/invocation/%s", iid)
		metrics.ExecutionGraphSkippedInvocations.With(map[string]string{
			metrics.ExecutionGraphSkipReasonLabel: "already_analyzed",
		}).Inc()
		return nil
	}

	bep, err := w.replayBuildEvents(ctx, iid, row.Attempt)
	if err != nil {
		return status.WrapError(err, "replay build events")
	}
	if bep.graphLogURI == nil {
		log.Infof("No graph log for invocation %q", iid)
		// This invocation has no execution graph log; ignore it.
		metrics.ExecutionGraphSkippedInvocations.With(map[string]string{
			metrics.ExecutionGraphSkipReasonLabel: "no_graph_log",
		}).Inc()
		return nil
	}
	log.Infof("FOUND GRAPH FOR INVOCATION https://app.buildbuddy.dev/invocation/%s", iid)

	logBytes, err := w.fetchGraphLog(ctx, iid, row.GroupID, bep.graphLogURI)
	if err != nil {
		return status.WrapError(err, "fetch execution graph log")
	}
	nodes, err := execution_graph.ParseCompressedLog(bytes.NewReader(logBytes), *maxNodes)
	if err != nil {
		return err
	}

	durationMillis := row.DurationUsec / 1000
	analysis, err := execution_graph.Analyze(nodes, &execution_graph.Options{
		InvocationID:                iid,
		InvocationDurationMillis:    durationMillis,
		BuildStartTimestampMillis:   bep.buildStartMillis,
		ActionsExecutionStartMillis: bep.actionsExecutionStartMillis,
		AnalysisPhaseMillis:         bep.analysisPhaseMillis,
		WallTimeMillis:              bep.wallTimeMillis,
	})
	if err != nil {
		return status.WrapError(err, "analyze execution graph")
	}
	analysis.AnalyzedAtUsec = time.Now().UnixMicro()

	if err := execution_graph.WriteAnalysis(ctx, w.env.GetBlobstore(), row.Attempt, analysis); err != nil {
		return status.WrapError(err, "store analysis")
	}
	metrics.ExecutionGraphAnalysisDurationUsec.Observe(float64(time.Since(start).Microseconds()))
	metrics.ExecutionGraphLogSizeBytes.Observe(float64(len(logBytes)))
	metrics.ExecutionGraphAnalysisSizeBytes.Observe(float64(proto.Size(analysis)))
	recordMetrics(analysis, durationMillis)
	log.CtxInfof(ctx, "Analyzed execution graph for invocation %s: %d nodes, critical path %d ms", iid, analysis.GetNumNodes(), analysis.GetCriticalPath().GetDurationMillis())
	return nil
}

// bepData is the data extracted from an invocation's build event stream.
type bepData struct {
	graphLogURI                 *url.URL
	buildStartMillis            int64
	actionsExecutionStartMillis int64
	analysisPhaseMillis         int64
	wallTimeMillis              int64
}

func (w *Worker) replayBuildEvents(ctx context.Context, iid string, attempt uint64) (*bepData, error) {
	inv := &inpb.Invocation{
		InvocationId: iid,
		Attempt:      attempt,
		// Skip console log reconstruction; we only need structured events.
		HasChunkedEventLogs: true,
	}
	data := &bepData{}
	err := build_event_handler.FetchAllInvocationEventsWithCallback(ctx, w.env, inv, redact.RedactionFlagStandardRedactions, func(event *inpb.InvocationEvent) error {
		switch p := event.GetBuildEvent().GetPayload().(type) {
		case *bespb.BuildEvent_Started:
			data.buildStartMillis = p.Started.GetStartTime().AsTime().UnixMilli()
		case *bespb.BuildEvent_BuildToolLogs:
			for _, f := range p.BuildToolLogs.GetLog() {
				if !strings.HasPrefix(f.GetName(), executionGraphLogPrefix) || f.GetUri() == "" {
					continue
				}
				if u, err := url.Parse(f.GetUri()); err == nil && u.Scheme == "bytestream" {
					data.graphLogURI = u
				}
			}
		case *bespb.BuildEvent_BuildMetrics:
			tm := p.BuildMetrics.GetTimingMetrics()
			data.actionsExecutionStartMillis = tm.GetActionsExecutionStartInMs()
			data.analysisPhaseMillis = tm.GetAnalysisPhaseTimeInMs()
			data.wallTimeMillis = tm.GetWallTimeInMs()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

// fetchGraphLog reads the (zstd-compressed) execution graph log. The
// finalization pipeline persists build tool logs to blobstore, so try there
// first; fall back to streaming from the cache with group-scoped auth.
func (w *Worker) fetchGraphLog(ctx context.Context, iid, groupID string, uri *url.URL) ([]byte, error) {
	blobPath := path.Join(iid, cacheArtifactsBlobstorePath, uri.Path)
	if data, err := w.env.GetBlobstore().ReadBlob(ctx, blobPath); err == nil {
		return data, nil
	}
	if w.env.GetPooledByteStreamClient() == nil {
		return nil, status.NotFoundErrorf("execution graph log not found in blobstore at %s", blobPath)
	}
	// Cache requests are group-scoped: authenticate as the invocation's
	// group. Note that the minted claims don't carry group settings (e.g.
	// cache encryption), so this fallback may fail for groups whose cache
	// entries are stored under settings-dependent keys.
	cacheCtx, err := groupAuthContext(ctx, groupID)
	if err != nil {
		return nil, status.WrapError(err, "group auth context")
	}
	var buf bytes.Buffer
	if err := w.env.GetPooledByteStreamClient().StreamBytestreamFile(cacheCtx, uri, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// groupAuthContext returns a context carrying a self-signed JWT for the given
// group, authorizing group-scoped cache reads and writes. Requires auth.jwt_key
// to be configured to the same value as the cache being called. Anonymous
// invocations (empty group ID) get no auth header.
func groupAuthContext(ctx context.Context, groupID string) (context.Context, error) {
	if groupID == "" {
		return ctx, nil
	}
	c := &claims.Claims{
		GroupID:       groupID,
		AllowedGroups: []string{groupID},
		GroupMemberships: []*interfaces.GroupMembership{{
			GroupID:      groupID,
			Capabilities: []cappb.Capability{cappb.Capability_CACHE_WRITE, cappb.Capability_CAS_WRITE},
		}},
		Capabilities: []cappb.Capability{cappb.Capability_CACHE_WRITE, cappb.Capability_CAS_WRITE},
	}
	jwtStr, err := claims.AssembleJWT(c, jwt.SigningMethodHS256)
	if err != nil {
		return nil, err
	}
	return metadata.AppendToOutgoingContext(ctx, authutil.ContextTokenStringKey, jwtStr), nil
}

// recordMetrics increments the drag counters for the analysis. Only drags
// larger than the configured fraction of the invocation duration are
// recorded, to bound metric cardinality.
func recordMetrics(analysis *egapb.ExecutionGraphAnalysis, durationMillis int64) {
	metrics.ExecutionGraphAnalyzedInvocations.Inc()
	metrics.ExecutionGraphInvocationDurationMsec.Add(float64(durationMillis))

	threshold := int64(*dragThreshold * float64(durationMillis))
	nodesByIndex := make(map[int32]*egapb.Node, len(analysis.GetNodes()))
	for _, n := range analysis.GetNodes() {
		nodesByIndex[n.GetIndex()] = n
	}
	nodeLabel := func(index int32) string {
		n := nodesByIndex[index]
		if n.GetTargetLabel() != "" {
			return n.GetTargetLabel()
		}
		if n.GetMnemonic() != "" {
			return n.GetMnemonic()
		}
		return n.GetDescription()
	}

	for _, fd := range analysis.GetFactorDrags() {
		if fd.GetDragMillis() <= threshold {
			continue
		}
		metrics.ExecutionGraphFactorDragMsec.With(map[string]string{
			metrics.ExecutionGraphFactorLabel: fd.GetFactor(),
		}).Add(float64(fd.GetDragMillis()))
	}
	for _, ed := range analysis.GetEdgeDrags() {
		if ed.GetDragMillis() <= threshold {
			continue
		}
		metrics.ExecutionGraphEdgeDragMsec.With(map[string]string{
			metrics.ExecutionGraphTargetLabel:     nodeLabel(ed.GetNodeIndex()),
			metrics.ExecutionGraphDependencyLabel: nodeLabel(ed.GetDepIndex()),
		}).Add(float64(ed.GetDragMillis()))
	}
	for _, nd := range analysis.GetNodeDrags() {
		if nd.GetDragMillis() <= threshold {
			continue
		}
		n := nodesByIndex[nd.GetNodeIndex()]
		metrics.ExecutionGraphNodeDragMsec.With(map[string]string{
			metrics.ExecutionGraphTargetLabel:   nodeLabel(nd.GetNodeIndex()),
			metrics.ExecutionGraphMnemonicLabel: n.GetMnemonic(),
		}).Add(float64(nd.GetDragMillis()))
	}
	for _, td := range analysis.GetTargetDepDrags() {
		if td.GetDragMillis() <= threshold {
			continue
		}
		metrics.ExecutionGraphTargetDepDragMsec.With(map[string]string{
			metrics.ExecutionGraphTargetLabel: td.GetTargetLabel(),
		}).Add(float64(td.GetDragMillis()))
	}
}

// dashedUUID converts a 32-character hex UUID (as stored in ClickHouse) to
// the dashed invocation ID form.
func dashedUUID(hexStr string) (string, error) {
	if len(hexStr) != 32 {
		return "", status.InvalidArgumentErrorf("malformed invocation UUID %q", hexStr)
	}
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexStr[0:8], hexStr[8:12], hexStr[12:16], hexStr[16:20], hexStr[20:32]), nil
}

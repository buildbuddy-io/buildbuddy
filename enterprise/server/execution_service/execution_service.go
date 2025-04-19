package execution_service

import (
	"context"
	"flag"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeseries"
	"github.com/buildbuddy-io/buildbuddy/server/util/trace_events"
	"golang.org/x/sync/errgroup"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	olaptables "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

var (
	primaryDBReadsEnabled = flag.Bool("remote_execution.primary_db_reads_enabled", true, "Whether to read executions from the primary database.")
	olapReadsEnabled      = flag.Bool("remote_execution.olap_reads_enabled", false, "Whether to read executions from the OLAP database (and read in-progress execution info from Redis).")
)

type ExecutionService struct {
	env environment.Env
}

func NewExecutionService(env environment.Env) *ExecutionService {
	return &ExecutionService{
		env: env,
	}
}

func checkPreconditions(req *espb.GetExecutionRequest) error {
	if req.GetExecutionLookup().GetInvocationId() != "" {
		return nil
	}
	return status.FailedPreconditionError("An execution lookup with invocation_id must be provided")
}

func (es *ExecutionService) getInvocationExecutionsFromPrimaryDB(ctx context.Context, invocationID, actionDigestHash string) ([]*tables.Execution, error) {
	// Note: the invocation row may not be created yet because workflow
	// invocations are created by the execution itself.
	q := query_builder.NewQuery(`
		SELECT e.* FROM "InvocationExecutions" ie
		JOIN "Executions" e ON e.execution_id = ie.execution_id
		LEFT JOIN "Invocations" i ON i.invocation_id = e.invocation_id
	`)
	q.AddWhereClause(`ie.invocation_id = ?`, invocationID)
	if actionDigestHash != "" {
		q.AddWhereClause(`e.execution_id LIKE ?`, "%/"+actionDigestHash+"/%")
	}
	dbh := es.env.GetDBHandle()

	permClauses, err := perms.GetPermissionsCheckClauses(ctx, es.env, q, "e")
	if err != nil {
		return nil, err
	}
	// If an authenticated invocation has OTHERS_READ perms (i.e. it is owned by a
	// group but made public), then let child executions inherit that OTHERS_READ
	// bit. An alternative here would be to explicitly mark all child executions
	// with OTHERS_READ, but that is somewhat complex. So we use this simple
	// permissions inheriting approach instead.
	permClauses.AddOr("i.perms IS NOT NULL AND i.perms & ? != 0", perms.OTHERS_READ)
	permQuery, permArgs := permClauses.Build()
	q.AddWhereClause("("+permQuery+")", permArgs...)

	queryStr, args := q.Build()
	rq := dbh.NewQuery(ctx, "execution_server_get_executions").Raw(queryStr, args...)
	executions, err := db.ScanAll(rq, &tables.Execution{})
	if err != nil {
		return nil, err
	}
	// It's unlikely, but our digest predicate might return false positives
	// since we aren't doing a strict match on the hash part of the execution
	// ID. Filter out false positives here.
	if actionDigestHash != "" {
		executions = slices.DeleteFunc(executions, func(e *tables.Execution) bool {
			parts := strings.Split(e.ExecutionID, "/")
			return len(parts) < 2 || parts[len(parts)-2] != actionDigestHash
		})
	}
	return executions, nil
}

func (es *ExecutionService) getInvocationExecutionsFromOLAPDB(ctx context.Context, invocationID, actionDigestHash string) ([]*olaptables.Execution, error) {
	var eg errgroup.Group

	// If the invocation is still in progress, completed executions will be
	// buffered, waiting to be flushed to the OLAP database as part of a batch
	// of writes. So we read both buffered and flushed executions here.
	var bufferedExecutions []*olaptables.Execution
	var flushedExecutions []*olaptables.Execution
	eg.Go(func() error {
		ex, err := es.env.GetExecutionCollector().GetExecutions(ctx, invocationID, 0, -1)
		if err != nil {
			return err
		}
		// Respect actionDigestHash filter if provided.
		if actionDigestHash != "" {
			ex = slices.DeleteFunc(ex, func(e *repb.StoredExecution) bool {
				parts := strings.Split(e.ExecutionId, "/")
				return len(parts) < 2 || parts[len(parts)-2] != actionDigestHash
			})
		}
		// Converted buffered representation to OLAP table representation.
		bufferedExecutions = make([]*olaptables.Execution, len(ex))
		for i, e := range ex {
			// Note: invocation metadata is not needed in this case (since this
			// RPC only returns the execution metadata).
			bufferedExecutions[i] = clickhouse.ExecutionFromProto(e, nil /*=invocation*/)
		}
		return nil
	})
	eg.Go(func() error {
		// The Executions table is optimized for trends, so the primary key
		// doesn't allow efficient querying by invocation ID. To mitigate this,
		// we filter the timestamp range based on the start/end timestamps of
		// the invocation, which requires looking up the invocation from the
		// primary DB.
		inv, err := es.env.GetInvocationDB().LookupInvocation(ctx, invocationID)
		if err != nil {
			return status.WrapError(err, "lookup invocation")
		}
		// Execution runtime is capped at 24 hours currently
		// (execution.max_task_timeout), and because of action merging,
		// executions for an invocation could have started hours prior to the
		// invocation being created. So we need to look back prior to when the
		// invocation was created.
		//
		// This might seem like a very large window, but ClickHouse should be
		// able to handle this with no problem in most cases; we're really just
		// trying to get a coarse-grained limit here to avoid scanning the full
		// execution history.
		rangeStartUsec := inv.Model.CreatedAtUsec - 25*time.Hour.Microseconds()
		// If the invocation is completed, we can scan up to the invocation end
		// timestamp, plus a few minutes just to be safe. Otherwise, use the
		// current time as the end of the range, but limit to 24h since the
		// invocation was last updated in the primary DB.
		var rangeEndUsec int64
		if inv.InvocationStatus == int64(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS) {
			rangeEndUsec = inv.Model.UpdatedAtUsec + 10*time.Minute.Microseconds()
		} else {
			rangeEndUsec = time.Now().UnixMicro() + 30*time.Minute.Microseconds()
			// Safeguard against scanning too long of a range.
			if rangeEndUsec-inv.Model.UpdatedAtUsec > 24*time.Hour.Microseconds() {
				rangeEndUsec = inv.Model.UpdatedAtUsec + 24*time.Hour.Microseconds()
			}
		}
		// group_id ACL check should have been done in LookupInvocation, but
		// check it again here just to be explicit.
		if err := authutil.AuthorizeGroupAccess(ctx, es.env, inv.GroupID); err != nil {
			return err
		}
		// Select only the execution fields that will be shown on the Executions
		// page.
		q := query_builder.NewQuery(`
			SELECT ` + strings.Join(execution.ExecutionListingColumns(), ", ") + `
			FROM "Executions"
		`)
		q.AddWhereClause(`group_id = ?`, inv.GroupID)
		q.AddWhereClause(`invocation_uuid = ?`, strings.ReplaceAll(invocationID, "-", ""))
		if actionDigestHash != "" {
			q.AddWhereClause(`execution_id LIKE ?`, "%/"+actionDigestHash+"/%")
		}
		q.AddWhereClause(`updated_at_usec >= ?`, rangeStartUsec)
		q.AddWhereClause(`updated_at_usec <= ?`, rangeEndUsec)
		queryStr, args := q.Build()
		rq := es.env.GetOLAPDBHandle().NewQuery(ctx, "execution_server_get_executions").Raw(queryStr, args...)
		rows, err := db.ScanAll(rq, &olaptables.Execution{})
		if err != nil {
			return err
		}
		flushedExecutions = rows
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// Combine the buffered and flushed executions. Dedupe by execution ID,
	// since the concurrent reads from Redis and OLAP might occur during an
	// in-progress flush, in which case the Redis executions might not've been
	// cleaned up yet.
	executions := append(flushedExecutions, bufferedExecutions...)
	executionIDs := make(map[string]bool)
	dedupedExecutions := make([]*olaptables.Execution, 0, len(executions))
	for _, e := range executions {
		if _, ok := executionIDs[e.ExecutionID]; !ok {
			executionIDs[e.ExecutionID] = true
			dedupedExecutions = append(dedupedExecutions, e)
		}
	}

	return dedupedExecutions, nil
}

func (es *ExecutionService) GetExecution(ctx context.Context, req *espb.GetExecutionRequest) (*espb.GetExecutionResponse, error) {
	if es.env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	if err := checkPreconditions(req); err != nil {
		return nil, err
	}

	var primaryDBExecutions []*tables.Execution
	var olapExecutions []*olaptables.Execution

	var eg errgroup.Group
	if *primaryDBReadsEnabled {
		eg.Go(func() error {
			ex, err := es.getInvocationExecutionsFromPrimaryDB(ctx, req.GetExecutionLookup().GetInvocationId(), req.GetExecutionLookup().GetActionDigestHash())
			if err != nil {
				return err
			}
			primaryDBExecutions = ex
			return nil
		})
	}
	if *olapReadsEnabled {
		eg.Go(func() error {
			ex, err := es.getInvocationExecutionsFromOLAPDB(ctx, req.GetExecutionLookup().GetInvocationId(), req.GetExecutionLookup().GetActionDigestHash())
			if err != nil {
				return err
			}
			olapExecutions = ex
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// Combine the executions from the primary DB and OLAP.
	// Prefer OLAP executions if they exist, otherwise fall back to primary DB.
	executions := make([]*espb.Execution, 0, len(olapExecutions))
	olapExecutionIDs := make(map[string]bool)
	for _, e := range olapExecutions {
		proto, err := execution.OLAPExecToClientProto(e)
		if err != nil {
			return nil, status.WrapError(err, "convert OLAP execution to client proto")
		}
		executions = append(executions, proto)
		olapExecutionIDs[e.ExecutionID] = true
	}
	for _, e := range primaryDBExecutions {
		if olapExecutionIDs[e.ExecutionID] {
			continue
		}
		proto, err := execution.TableExecToClientProto(e)
		if err != nil {
			return nil, status.WrapError(err, "convert execution to client proto")
		}
		executions = append(executions, proto)
	}

	// Sort the executions by queued timestamp.
	sort.Slice(executions, func(i, j int) bool {
		ti := executions[i].GetExecutedActionMetadata().GetQueuedTimestamp().AsTime()
		tj := executions[j].GetExecutedActionMetadata().GetQueuedTimestamp().AsTime()
		return ti.Before(tj)
	})

	rsp := &espb.GetExecutionResponse{
		Execution: executions,
	}

	if req.GetInlineExecuteResponse() {
		// If inlined responses are requested, fetch them now.
		var eg errgroup.Group
		for _, ex := range rsp.Execution {
			ex := ex
			eg.Go(func() error {
				// TODO: if the authenticated user has access to the group
				// that owns the execution, switch to that group's ctx.
				// Also if the execution was done anonymously, switch to
				// anonymous ctx.
				res, err := execution.GetCachedExecuteResponse(ctx, es.env.GetActionCacheClient(), ex.ExecutionId)
				if err != nil {
					return err
				}
				ex.ExecuteResponse = res
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			log.CtxInfof(ctx, "Failed to fetch inline execution response(s): %s", err)
		}
	}
	return rsp, nil
}

func (es *ExecutionService) WaitExecution(req *espb.WaitExecutionRequest, stream bbspb.BuildBuddyService_WaitExecutionServer) error {
	if es.env.GetRemoteExecutionClient() == nil {
		return status.UnimplementedError("not implemented")
	}
	client, err := es.env.GetRemoteExecutionClient().WaitExecution(stream.Context(), &repb.WaitExecutionRequest{
		Name: req.GetExecutionId(),
	})
	if err != nil {
		return status.WrapError(err, "create WaitExecution stream")
	}
	// Directly forward stream messages back to the Web client stream. Note: we
	// don't try to automatically reconnect here, since the client has to handle
	// disconnects anyway (to deal with the current server going away).
	for {
		op, err := client.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		res := &espb.WaitExecutionResponse{Operation: op}
		if err = stream.Send(res); err != nil {
			return status.WrapError(err, "send")
		}
	}
}

// WriteExecutionProfile writes the uncompressed JSON execution profile in
// Google's Trace Event Format.
func (es *ExecutionService) WriteExecutionProfile(ctx context.Context, w io.Writer, executionID string) error {
	res, err := execution.GetCachedExecuteResponse(ctx, es.env.GetActionCacheClient(), executionID)
	if err != nil {
		return status.WrapError(err, "get cached execute response")
	}

	metadata := res.GetResult().GetExecutionMetadata()
	stats := metadata.GetUsageStats()

	timestampsMillis := timeseries.DeltaDecode(stats.GetTimeline().GetTimestamps())
	cumulativeCPUMillis := timeseries.DeltaDecode(stats.GetTimeline().GetCpuSamples())
	memoryUsageKB := timeseries.DeltaDecode(stats.GetTimeline().GetMemoryKbSamples())
	cumulativeDiskRbytes := timeseries.DeltaDecode(stats.GetTimeline().GetRbytesTotalSamples())
	cumulativeDiskWbytes := timeseries.DeltaDecode(stats.GetTimeline().GetWbytesTotalSamples())
	cumulativeDiskRios := timeseries.DeltaDecode(stats.GetTimeline().GetRiosTotalSamples())
	cumulativeDiskWios := timeseries.DeltaDecode(stats.GetTimeline().GetWiosTotalSamples())

	// Before we start writing the HTTP response, perform basic validation, so
	// that we can return an error status in the header if it fails.

	// All lists of samples that are set (nonempty) should be 1-1 with
	// timestamps.
	if len(cumulativeCPUMillis) > 0 && len(timestampsMillis) != len(cumulativeCPUMillis) {
		return status.UnknownErrorf("length mismatch: timestamps[%d], cpu[%d]", len(timestampsMillis), len(cumulativeCPUMillis))
	}
	if len(memoryUsageKB) > 0 && len(timestampsMillis) != len(memoryUsageKB) {
		return status.UnknownErrorf("length mismatch: timestamps[%d], memory[%d]", len(timestampsMillis), len(memoryUsageKB))
	}
	if len(cumulativeDiskRbytes) > 0 && len(timestampsMillis) != len(cumulativeDiskRbytes) {
		return status.UnknownErrorf("length mismatch: timestamps[%d], disk read bytes[%d]", len(timestampsMillis), len(cumulativeDiskRbytes))
	}
	if len(cumulativeDiskWbytes) > 0 && len(timestampsMillis) != len(cumulativeDiskWbytes) {
		return status.UnknownErrorf("length mismatch: timestamps[%d], disk write bytes[%d]", len(timestampsMillis), len(cumulativeDiskWbytes))
	}
	if len(cumulativeDiskRios) > 0 && len(timestampsMillis) != len(cumulativeDiskRios) {
		return status.UnknownErrorf("length mismatch: timestamps[%d], disk read ios[%d]", len(timestampsMillis), len(cumulativeDiskRios))
	}
	if len(cumulativeDiskWios) > 0 && len(timestampsMillis) != len(cumulativeDiskWios) {
		return status.UnknownErrorf("length mismatch: timestamps[%d], disk write ios[%d]", len(timestampsMillis), len(cumulativeDiskWios))
	}

	// The UI expects a full profile object, rather than just a list of events.
	if _, err := io.WriteString(w, `{"traceEvents":[`); err != nil {
		return status.WrapError(err, "write response")
	}
	out := trace_events.NewEventWriter(w)

	// Write thread name metadata. Identify app and executor as separate
	// "threads" which has the effect of showing them in separate sections in
	// the trace viewer. Note: Bazel uses "Complete" events instead of
	// "Metadata" events for some reason; we just do the same here.
	appTID := int64(1)
	executorTID := int64(2)

	appMD := &trace_events.Event{
		ThreadID: appTID,
		Name:     "thread_name",
		Args:     map[string]any{"name": "buildbuddy-execution-server"},
		Phase:    trace_events.PhaseComplete,
	}
	if err := out.WriteEvent(appMD); err != nil {
		return status.WrapError(err, "write response")
	}

	executorMD := &trace_events.Event{
		ThreadID: executorTID,
		Name:     "thread_name",
		Args:     map[string]any{"name": "buildbuddy-execution-worker"},
		Phase:    trace_events.PhaseComplete,
	}
	if err := out.WriteEvent(executorMD); err != nil {
		return status.WrapError(err, "write response")
	}

	// Clock skew can cause the queued timestamp and worker start timestamp to
	// appear out of order; determine the profile start time as the minimum.
	profileStartTimestampUsec := min(
		metadata.GetQueuedTimestamp().AsTime().UnixMicro(),
		metadata.GetWorkerStartTimestamp().AsTime().UnixMicro(),
	)

	// Write spans
	for _, span := range []struct {
		name  string
		tid   int64
		start *tspb.Timestamp
		end   *tspb.Timestamp
	}{
		{
			name:  "queued",
			tid:   appTID,
			start: metadata.GetQueuedTimestamp(),
			end:   metadata.GetWorkerStartTimestamp(),
		},
		{
			name:  "execute action",
			tid:   executorTID,
			start: metadata.GetWorkerStartTimestamp(),
			end:   metadata.GetWorkerCompletedTimestamp(),
		},
		{
			name:  "prepare runner",
			tid:   executorTID,
			start: metadata.GetWorkerStartTimestamp(),
			end:   metadata.GetInputFetchStartTimestamp(),
		},
		{
			name:  "fetch inputs",
			tid:   executorTID,
			start: metadata.GetInputFetchStartTimestamp(),
			end:   metadata.GetInputFetchCompletedTimestamp(),
		},
		{
			name:  "run command",
			tid:   executorTID,
			start: metadata.GetExecutionStartTimestamp(),
			end:   metadata.GetExecutionCompletedTimestamp(),
		},
		{
			name:  "upload outputs",
			tid:   executorTID,
			start: metadata.GetOutputUploadStartTimestamp(),
			end:   metadata.GetOutputUploadCompletedTimestamp(),
		},
	} {
		event := &trace_events.Event{
			ThreadID:  span.tid,
			Name:      span.name,
			Timestamp: span.start.AsTime().UnixMicro() - profileStartTimestampUsec,
			Duration:  max(0, span.end.AsTime().Sub(span.start.AsTime()).Microseconds()),
			Phase:     trace_events.PhaseComplete,
		}
		if err := out.WriteEvent(event); err != nil {
			return status.WrapError(err, "write response")
		}
	}

	timeseriesStartTime := stats.GetTimeline().GetStartTime().AsTime()
	// Derive current CPU core count from timestamps and cumulative CPU usage.
	const cpuScale = 1.0 // 1 cpu_ms/ms = 1 cpu_core/s
	cpuUsage := computeRate(timestampsMillis, cumulativeCPUMillis, cpuScale, timeseriesStartTime)

	// Convert memory usage samples from []int64 to []float64
	var memoryUsage []float64
	for _, m := range memoryUsageKB {
		memoryUsage = append(memoryUsage, float64(m))
	}

	// Derive disk bandwidth in MB/s.
	const diskBandwidthScale = 1e-3 // 1 byte/ms = 1e-3 MB/s
	diskReadBW := computeRate(timestampsMillis, cumulativeDiskRbytes, diskBandwidthScale, timeseriesStartTime)
	diskWriteBW := computeRate(timestampsMillis, cumulativeDiskWbytes, diskBandwidthScale, timeseriesStartTime)

	// Derive disk IOPS (ops/s).
	const diskIOPSScale = 1000.0 // 1 op/ms = 1000 ops/s
	diskReadIOPS := computeRate(timestampsMillis, cumulativeDiskRios, diskIOPSScale, timeseriesStartTime)
	diskWriteIOPS := computeRate(timestampsMillis, cumulativeDiskWios, diskIOPSScale, timeseriesStartTime)

	// Write timeseries data as events
	for _, series := range []struct {
		// Display name and short map key (keep in sync with trace_events.ts)
		name, key string
		data      []float64
	}{
		{
			name: "CPU usage (cores)",
			key:  "cpu",
			data: cpuUsage,
		},
		{
			name: "Memory usage (KB)",
			key:  "memory",
			data: memoryUsage,
		},
		{
			name: "Disk read bandwidth (MB/s)",
			key:  "disk-read-bw",
			data: diskReadBW,
		},
		{
			name: "Disk write bandwidth (MB/s)",
			key:  "disk-write-bw",
			data: diskWriteBW,
		},
		{
			name: "Disk read IOPS",
			key:  "disk-read-iops",
			data: diskReadIOPS,
		},
		{
			name: "Disk write IOPS",
			key:  "disk-write-iops",
			data: diskWriteIOPS,
		},
	} {
		for i, value := range series.data {
			event := &trace_events.Event{
				ThreadID:  executorTID,
				Name:      series.name,
				Timestamp: timestampsMillis[i]*1000 - profileStartTimestampUsec, // ms => us
				Args:      map[string]any{series.key: value},
				Phase:     trace_events.PhaseCounter,
			}
			if err := out.WriteEvent(event); err != nil {
				return status.WrapError(err, "write response")
			}
		}
	}

	// Close the events list and the outer profile object.
	if _, err := io.WriteString(w, "]}"); err != nil {
		return status.WrapError(err, "write response")
	}

	return nil
}

// computeRate computes a sliding-window average rate using the given timestamps
// and samples. A rate is computed for each timestamp by considering the net
// change from samples within a lookback window. It assumes that the sample
// value at startTime is 0, which is used when computing the initial rate.
func computeRate(timestampsMillis, samples []int64, scale float64, startTime time.Time) []float64 {
	const windowSize = 500 * time.Millisecond
	windowStartIdx := -1

	out := make([]float64, 0, len(samples))
	for i := range samples {
		windowStartCutoffMillis := timestampsMillis[i] - windowSize.Milliseconds()

		var windowStartTimestampMillis int64
		var windowStartSample int64

		// Move the sliding window forward and compute the start timestamp and
		// start sample of the window. Note that windowStartIdx is -1 initially.
		for ; windowStartIdx < i; windowStartIdx++ {
			var t, s int64
			if windowStartIdx >= 0 {
				t, s = timestampsMillis[windowStartIdx], samples[windowStartIdx]
			} else {
				t, s = startTime.UnixMilli(), 0
			}
			if t >= windowStartCutoffMillis {
				windowStartTimestampMillis = t
				windowStartSample = s
				break
			}
		}
		f := float64(0)
		if windowStartTimestampMillis > 0 {
			sampleDelta := samples[i] - windowStartSample
			dtMillis := timestampsMillis[i] - windowStartTimestampMillis
			if dtMillis > 0 {
				f = float64(sampleDelta) / float64(dtMillis) * scale
			}
		}
		out = append(out, f)
	}
	return out
}

package execution_service

import (
	"context"
	"io"
	"slices"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
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
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
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

func (es *ExecutionService) getInvocationExecutions(ctx context.Context, invocationID, actionDigestHash string) ([]*tables.Execution, error) {
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

func (es *ExecutionService) GetExecution(ctx context.Context, req *espb.GetExecutionRequest) (*espb.GetExecutionResponse, error) {
	if es.env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	if err := checkPreconditions(req); err != nil {
		return nil, err
	}
	executions, err := es.getInvocationExecutions(ctx, req.GetExecutionLookup().GetInvocationId(), req.GetExecutionLookup().GetActionDigestHash())
	if err != nil {
		return nil, err
	}
	// Sort the executions by start time.
	sort.Slice(executions, func(i, j int) bool {
		return executions[i].Model.CreatedAtUsec < executions[j].Model.CreatedAtUsec
	})
	rsp := &espb.GetExecutionResponse{}
	for _, ex := range executions {
		protoExec, err := execution.TableExecToClientProto(ex)
		if err != nil {
			return nil, err
		}
		rsp.Execution = append(rsp.Execution, protoExec)
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
				res, err := execution.GetCachedExecuteResponse(ctx, es.env, ex.ExecutionId)
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
	res, err := execution.GetCachedExecuteResponse(ctx, es.env, executionID)
	if err != nil {
		return status.WrapError(err, "get cached execute response")
	}

	metadata := res.GetResult().GetExecutionMetadata()
	stats := metadata.GetUsageStats()

	timestampsMillis := timeseries.DeltaDecode(stats.GetTimeline().GetTimestamps())
	cumulativeCPUMillis := timeseries.DeltaDecode(stats.GetTimeline().GetCpuSamples())

	// Before we start writing the HTTP response, perform basic validation, so
	// that we can return an error status in the header if it fails.

	// All lists of samples should have the same length.
	if len(timestampsMillis) != len(cumulativeCPUMillis) {
		return status.UnknownErrorf("length mismatch: timestamps[%d], cpu[%d]", len(timestampsMillis), len(cumulativeCPUMillis))
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
			name:  "pull image",
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

	// Derive current CPU core count from timestamps and cumulative CPU usage
	var cpuUsage []float64
	for i := range timestampsMillis {
		var prevTimestampMillis int64
		var prevCPUMillis int64
		if i == 0 {
			prevTimestampMillis = stats.GetTimeline().GetStartTime().AsTime().UnixMilli()
			prevCPUMillis = 0
		} else {
			prevTimestampMillis = timestampsMillis[i-1]
			prevCPUMillis = cumulativeCPUMillis[i-1]
		}
		dtMillis := timestampsMillis[i] - prevTimestampMillis
		cpuMillisDelta := cumulativeCPUMillis[i] - prevCPUMillis
		cpu := float64(0)
		if float64(dtMillis) > 0 {
			cpu = float64(cpuMillisDelta) / float64(dtMillis)
		}
		cpuUsage = append(cpuUsage, cpu)
	}

	// Write timeseries data as events
	for _, series := range []struct {
		// Display name and short map key (keep in sync with trace_events.ts)
		name, key string
		data      []float64
	}{
		{
			name: "CPU usage",
			key:  "cpu",
			data: cpuUsage,
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

package execution_service

import (
	"context"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/golang/protobuf/ptypes"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	timestamppb "github.com/golang/protobuf/ptypes/timestamp"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
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

func (es *ExecutionService) getInvocationExecutions(ctx context.Context, invocationID string) ([]tables.Execution, error) {
	db := es.env.GetDBHandle()
	q := query_builder.NewQuery(`SELECT * FROM Executions as e`)
	q = q.AddWhereClause(`e.invocation_id = ?`, invocationID)
	if err := perms.AddPermissionsCheckToQueryWithTableAlias(ctx, es.env, q, "e"); err != nil {
		return nil, err
	}
	queryStr, args := q.Build()
	rows, err := db.Raw(queryStr, args...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	executions := make([]tables.Execution, 0)
	for rows.Next() {
		var exec tables.Execution
		if err := db.ScanRows(rows, &exec); err != nil {
			return nil, err
		}
		executions = append(executions, exec)
	}
	return executions, nil
}

func timestampProto(timeInUsec int64) *timestamppb.Timestamp {
	if tp, err := ptypes.TimestampProto(timeutil.FromUsec(timeInUsec)); err == nil {
		return tp
	}
	return nil
}

func tableExecToProto(in tables.Execution) (*espb.Execution, error) {
	_, d, err := digest.ExtractDigestFromDownloadResourceName(in.ExecutionID)
	if err != nil {
		return nil, err
	}

	out := &espb.Execution{
		ActionDigest: d,
		Status: &statuspb.Status{
			Code:    in.StatusCode,
			Message: in.StatusMessage,
		},
		Stage: repb.ExecutionStage_Value(in.Stage),
		IoStats: &espb.IOStats{
			FileDownloadCount:        in.FileDownloadCount,
			FileDownloadSizeBytes:    in.FileDownloadSizeBytes,
			FileDownloadDurationUsec: in.FileDownloadDurationUsec,
			FileUploadCount:          in.FileUploadCount,
			FileUploadSizeBytes:      in.FileUploadSizeBytes,
			FileUploadDurationUsec:   in.FileUploadDurationUsec,
		},
		ExecutedActionMetadata: &repb.ExecutedActionMetadata{
			Worker:                         in.Worker,
			QueuedTimestamp:                timestampProto(in.QueuedTimestampUsec),
			WorkerStartTimestamp:           timestampProto(in.WorkerStartTimestampUsec),
			WorkerCompletedTimestamp:       timestampProto(in.WorkerCompletedTimestampUsec),
			InputFetchStartTimestamp:       timestampProto(in.InputFetchStartTimestampUsec),
			InputFetchCompletedTimestamp:   timestampProto(in.InputFetchCompletedTimestampUsec),
			ExecutionStartTimestamp:        timestampProto(in.ExecutionStartTimestampUsec),
			ExecutionCompletedTimestamp:    timestampProto(in.ExecutionCompletedTimestampUsec),
			OutputUploadStartTimestamp:     timestampProto(in.OutputUploadStartTimestampUsec),
			OutputUploadCompletedTimestamp: timestampProto(in.OutputUploadCompletedTimestampUsec),
		},
		CommandSnippet: in.CommandSnippet,
	}

	return out, nil
}

func (es *ExecutionService) GetExecution(ctx context.Context, req *espb.GetExecutionRequest) (*espb.GetExecutionResponse, error) {
	if es.env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	if err := checkPreconditions(req); err != nil {
		return nil, err
	}
	executions, err := es.getInvocationExecutions(ctx, req.GetExecutionLookup().GetInvocationId())
	if err != nil {
		return nil, err
	}
	// Sort the executions by start time.
	sort.Slice(executions, func(i, j int) bool {
		return executions[i].Model.CreatedAtUsec < executions[j].Model.CreatedAtUsec
	})
	rsp := &espb.GetExecutionResponse{}
	for _, execution := range executions {
		protoExec, err := tableExecToProto(execution)
		if err != nil {
			return nil, err
		}
		rsp.Execution = append(rsp.Execution, protoExec)
	}
	return rsp, nil
}

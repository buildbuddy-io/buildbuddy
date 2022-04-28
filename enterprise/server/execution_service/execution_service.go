package execution_service

import (
	"context"
	"sort"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
	dbh := es.env.GetDBHandle()
	q := query_builder.NewQuery(`SELECT * FROM Executions as e`)
	q = q.AddWhereClause(`e.invocation_id = ?`, invocationID)
	if err := perms.AddPermissionsCheckToQueryWithTableAlias(ctx, es.env, q, "e"); err != nil {
		return nil, err
	}
	queryStr, args := q.Build()
	rows, err := dbh.DB(ctx).Raw(queryStr, args...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	executions := make([]tables.Execution, 0)
	for rows.Next() {
		var exec tables.Execution
		if err := dbh.DB(ctx).ScanRows(rows, &exec); err != nil {
			return nil, err
		}
		executions = append(executions, exec)
	}
	return executions, nil
}

func tableExecToProto(in tables.Execution) (*espb.Execution, error) {
	r, err := digest.ParseDownloadResourceName(in.ExecutionID)
	if err != nil {
		return nil, err
	}

	var actionResultDigest *repb.Digest
	if in.StatusCode == int32(codes.OK) && in.ExitCode == 0 {
		// Action Result with unmodified action digest is only uploaded when there is no error
		// from the CommandResult(i.e. status code is OK) and the exit code is zero.
		actionResultDigest = proto.Clone(r.GetDigest()).(*repb.Digest)
	} else {
		actionResultDigest, err = digest.AddInvocationIDToDigest(r.GetDigest(), in.InvocationID)
		if err != nil {
			return nil, err
		}
	}

	out := &espb.Execution{
		ActionDigest:       r.GetDigest(),
		ActionResultDigest: actionResultDigest,
		Status: &statuspb.Status{
			Code:    in.StatusCode,
			Message: in.StatusMessage,
		},
		ExitCode: in.ExitCode,
		Stage:    repb.ExecutionStage_Value(in.Stage),
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
			QueuedTimestamp:                timestamppb.New(time.UnixMicro(in.QueuedTimestampUsec)),
			WorkerStartTimestamp:           timestamppb.New(time.UnixMicro(in.WorkerStartTimestampUsec)),
			WorkerCompletedTimestamp:       timestamppb.New(time.UnixMicro(in.WorkerCompletedTimestampUsec)),
			InputFetchStartTimestamp:       timestamppb.New(time.UnixMicro(in.InputFetchStartTimestampUsec)),
			InputFetchCompletedTimestamp:   timestamppb.New(time.UnixMicro(in.InputFetchCompletedTimestampUsec)),
			ExecutionStartTimestamp:        timestamppb.New(time.UnixMicro(in.ExecutionStartTimestampUsec)),
			ExecutionCompletedTimestamp:    timestamppb.New(time.UnixMicro(in.ExecutionCompletedTimestampUsec)),
			OutputUploadStartTimestamp:     timestamppb.New(time.UnixMicro(in.OutputUploadStartTimestampUsec)),
			OutputUploadCompletedTimestamp: timestamppb.New(time.UnixMicro(in.OutputUploadCompletedTimestampUsec)),
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

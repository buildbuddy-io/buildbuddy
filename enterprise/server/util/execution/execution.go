package execution

import (
	"context"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
)

func TableExecToProto(in *tables.Execution, invLink *sipb.StoredInvocationLink, metadata *repb.RequestMetadata) *repb.StoredExecution {
	se := &repb.StoredExecution{
		GroupId:                            in.GroupID,
		UpdatedAtUsec:                      in.UpdatedAtUsec,
		ExecutionId:                        in.ExecutionID,
		InvocationUuid:                     strings.ReplaceAll(invLink.GetInvocationId(), "-", ""),
		InvocationLinkType:                 int32(invLink.GetType()),
		CreatedAtUsec:                      in.CreatedAtUsec,
		UserId:                             in.UserID,
		Worker:                             in.Worker,
		Stage:                              in.Stage,
		FileDownloadCount:                  in.FileDownloadCount,
		FileDownloadSizeBytes:              in.FileDownloadSizeBytes,
		FileDownloadDurationUsec:           in.FileDownloadDurationUsec,
		FileUploadCount:                    in.FileUploadCount,
		FileUploadSizeBytes:                in.FileUploadSizeBytes,
		FileUploadDurationUsec:             in.FileUploadDurationUsec,
		PeakMemoryBytes:                    in.PeakMemoryBytes,
		CpuNanos:                           in.CPUNanos,
		EstimatedMemoryBytes:               in.EstimatedMemoryBytes,
		EstimatedMilliCpu:                  in.EstimatedMilliCPU,
		QueuedTimestampUsec:                in.QueuedTimestampUsec,
		WorkerStartTimestampUsec:           in.WorkerStartTimestampUsec,
		WorkerCompletedTimestampUsec:       in.WorkerCompletedTimestampUsec,
		InputFetchStartTimestampUsec:       in.InputFetchStartTimestampUsec,
		InputFetchCompletedTimestampUsec:   in.InputFetchCompletedTimestampUsec,
		ExecutionStartTimestampUsec:        in.ExecutionStartTimestampUsec,
		ExecutionCompletedTimestampUsec:    in.ExecutionCompletedTimestampUsec,
		OutputUploadStartTimestampUsec:     in.OutputUploadStartTimestampUsec,
		OutputUploadCompletedTimestampUsec: in.OutputUploadCompletedTimestampUsec,
		StatusCode:                         in.StatusCode,
		ExitCode:                           in.ExitCode,
	}
	if metadata != nil {
		se.TargetLabel = metadata.TargetId
		se.ActionMnemonic = metadata.ActionMnemonic
		se.ConfigurationId = metadata.ConfigurationId
	}
	return se
}

func TableExecToClientProto(in *tables.Execution) (*espb.Execution, error) {
	r, err := digest.ParseUploadResourceName(in.ExecutionID)
	if err != nil {
		return nil, err
	}

	executeResponseDigest, err := digest.Compute(strings.NewReader(in.ExecutionID), r.GetDigestFunction())
	if err != nil {
		return nil, status.WrapError(err, "compute execute response digest")
	}

	out := &espb.Execution{
		ExecutionId:           in.ExecutionID,
		ActionDigest:          r.GetDigest(),
		ExecuteResponseDigest: executeResponseDigest,
		Status: &statuspb.Status{
			Code:    in.StatusCode,
			Message: in.StatusMessage,
		},
		ExitCode: in.ExitCode,
		Stage:    repb.ExecutionStage_Value(in.Stage),
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
			IoStats: &repb.IOStats{
				FileDownloadCount:        in.FileDownloadCount,
				FileDownloadSizeBytes:    in.FileDownloadSizeBytes,
				FileDownloadDurationUsec: in.FileDownloadDurationUsec,
				FileUploadCount:          in.FileUploadCount,
				FileUploadSizeBytes:      in.FileUploadSizeBytes,
				FileUploadDurationUsec:   in.FileUploadDurationUsec,
			},
			UsageStats: &repb.UsageStats{
				CpuNanos:        in.CPUNanos,
				PeakMemoryBytes: in.PeakMemoryBytes,
			},
		},
		CommandSnippet: in.CommandSnippet,
	}

	return out, nil
}

func GetCachedExecuteResponse(ctx context.Context, env environment.Env, taskID string) (*repb.ExecuteResponse, error) {
	rn, err := digest.ParseUploadResourceName(taskID)
	if err != nil {
		return nil, err
	}
	d, err := digest.Compute(strings.NewReader(taskID), rn.GetDigestFunction())
	if err != nil {
		return nil, err
	}
	req := &repb.GetActionResultRequest{
		ActionDigest:   d,
		InstanceName:   rn.GetInstanceName(),
		DigestFunction: rn.GetDigestFunction(),
	}
	rsp, err := env.GetActionCacheClient().GetActionResult(ctx, req)
	if err != nil {
		return nil, err
	}
	executeResponse := &repb.ExecuteResponse{}
	if err := proto.Unmarshal(rsp.StdoutRaw, executeResponse); err != nil {
		return nil, err
	}
	return executeResponse, nil
}

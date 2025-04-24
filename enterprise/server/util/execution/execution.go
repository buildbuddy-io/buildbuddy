package execution

import (
	"context"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
	olaptables "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
)

func TableExecToProto(in *tables.Execution, invLink *sipb.StoredInvocationLink) *repb.StoredExecution {
	return &repb.StoredExecution{
		GroupId:                            in.GroupID,
		UpdatedAtUsec:                      in.UpdatedAtUsec,
		ExecutionId:                        in.ExecutionID,
		InvocationUuid:                     strings.Replace(invLink.GetInvocationId(), "-", "", -1),
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
		StatusMessage:                      in.StatusMessage,
		ExitCode:                           in.ExitCode,
		CachedResult:                       in.CachedResult,
		DoNotCache:                         in.DoNotCache,
		CommandSnippet:                     in.CommandSnippet,
	}
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

	// NOTE: keep in sync with ExecutionListingColumns
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
			DoNotCache: in.DoNotCache,
		},
		CommandSnippet: in.CommandSnippet,
	}

	return out, nil
}

func OLAPExecToClientProto(in *olaptables.Execution) (*espb.Execution, error) {
	r, err := digest.ParseUploadResourceName(in.ExecutionID)
	if err != nil {
		return nil, err
	}

	executeResponseDigest, err := digest.Compute(strings.NewReader(in.ExecutionID), r.GetDigestFunction())
	if err != nil {
		return nil, status.WrapError(err, "compute execute response digest")
	}

	// NOTE: keep in sync with ExecutionListingColumns
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
			DoNotCache: in.DoNotCache,
		},
		CommandSnippet: in.CommandSnippet,
	}

	return out, nil
}

// ExecutionListingColumns returns the set of OLAP columns that are included in
// the Execution proto returned to the client when listing executions (e.g.
// Executions tab, Drilldown tab.)
func ExecutionListingColumns() []string {
	// NOTE: keep in sync with ClientProtoColumns and OLAPExecToClientProto
	return []string{
		"execution_id",
		"status_code",
		"status_message",
		"exit_code",
		"stage",
		"worker",
		"queued_timestamp_usec",
		"worker_start_timestamp_usec",
		"worker_completed_timestamp_usec",
		"input_fetch_start_timestamp_usec",
		"input_fetch_completed_timestamp_usec",
		"execution_start_timestamp_usec",
		"execution_completed_timestamp_usec",
		"output_upload_start_timestamp_usec",
		"output_upload_completed_timestamp_usec",
		"file_download_count",
		"file_download_size_bytes",
		"file_download_duration_usec",
		"file_upload_count",
		"file_upload_size_bytes",
		"file_upload_duration_usec",
		"cpu_nanos",
		"peak_memory_bytes",
		"do_not_cache",
		"command_snippet",
	}
}

func GetCachedExecuteResponse(ctx context.Context, ac repb.ActionCacheClient, taskID string) (*repb.ExecuteResponse, error) {
	rn, err := digest.ParseUploadResourceName(taskID)
	if err != nil {
		return nil, err
	}
	d, err := digest.Compute(strings.NewReader(taskID), rn.GetDigestFunction())
	if err != nil {
		return nil, err
	}
	req := &repb.GetActionResultRequest{
		ActionDigest:        d,
		InstanceName:        rn.GetInstanceName(),
		DigestFunction:      rn.GetDigestFunction(),
		IncludeTimelineData: true,
	}
	rsp, err := ac.GetActionResult(ctx, req)
	if err != nil {
		return nil, err
	}
	executeResponse := &repb.ExecuteResponse{}
	if err := proto.Unmarshal(rsp.StdoutRaw, executeResponse); err != nil {
		return nil, err
	}
	return executeResponse, nil
}

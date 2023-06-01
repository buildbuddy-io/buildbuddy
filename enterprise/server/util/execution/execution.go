package execution

import (
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
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
	}
}

func TableExecToClientProto(in *tables.Execution) (*espb.Execution, error) {
	r, err := digest.ParseDownloadResourceName(in.ExecutionID)
	if err != nil {
		return nil, err
	}

	var actionResultDigest *repb.Digest
	if in.StatusCode == int32(codes.OK) && in.ExitCode == 0 && !in.DoNotCache {
		// Action Result with unmodified action digest is only uploaded when
		// there is no error from the CommandResult(i.e. status code is OK) and
		// the exit code is zero and the action was not marked with DoNotCache.
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

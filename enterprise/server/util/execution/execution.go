package execution

import (
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/tables"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
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

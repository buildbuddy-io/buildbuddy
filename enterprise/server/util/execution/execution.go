package execution

import (
	"strings"

	iepb "github.com/buildbuddy-io/buildbuddy/proto/internal_execution"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
)

func TableExecToProto(in *tables.Execution) *iepb.Execution {
	return &iepb.Execution{
		GroupId:                            in.GroupID,
		UpdatedAtUsec:                      in.UpdatedAtUsec,
		ExecutionId:                        in.ExecutionID,
		InvocationUuid:                     strings.Replace(in.InvocationID, "-", "", -1),
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

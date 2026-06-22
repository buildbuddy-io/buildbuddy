package oom

import (
	"fmt"
	"strconv"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/anypb"

	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

const (
	// ErrorInfoReason is the google.rpc.ErrorInfo reason for executor OOM
	// killer errors.
	ErrorInfoReason = "EXECUTOR_OOM_KILL"
	// ErrorInfoDomain is the google.rpc.ErrorInfo domain for executor OOM
	// killer errors.
	ErrorInfoDomain = "buildbuddy.io"

	// EstimatedMemoryBytesKey is the ErrorInfo metadata key containing the
	// victim's memory estimate in bytes.
	EstimatedMemoryBytesKey = "estimated_memory_bytes"
	// ObservedMemoryBytesKey is the ErrorInfo metadata key containing the
	// victim's observed memory usage in bytes.
	ObservedMemoryBytesKey = "observed_memory_bytes"
)

// Details contains the memory facts attached to an executor OOM killer error.
type Details struct {
	// EstimatedMemoryBytes is the victim's scheduled or expected memory usage.
	EstimatedMemoryBytes int64
	// ObservedMemoryBytes is the victim's memory usage observed by the OOM
	// killer.
	ObservedMemoryBytes int64
}

// Error returns the retryable error reported when the executor OOM killer kills
// a task.
func Error(taskID string, details Details) error {
	metadata := map[string]string{
		EstimatedMemoryBytesKey: strconv.FormatInt(details.EstimatedMemoryBytes, 10),
		ObservedMemoryBytesKey:  strconv.FormatInt(details.ObservedMemoryBytes, 10),
	}
	st := gstatus.New(codes.Unavailable, fmt.Sprintf("executor OOM killer terminated task %q", taskID))
	st, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason:   ErrorInfoReason,
		Domain:   ErrorInfoDomain,
		Metadata: metadata,
	})
	if err != nil {
		return gstatus.Error(codes.Internal, fmt.Sprintf("add OOM killer error details: %s", err))
	}
	return st.Err()
}

// IsError returns true when err contains executor OOM killer details.
func IsError(err error) bool {
	_, ok := DetailsFromError(err)
	return ok
}

// DetailsFromError extracts executor OOM killer details from err.
func DetailsFromError(err error) (*Details, bool) {
	if err == nil {
		return nil, false
	}
	st := gstatus.Convert(err)
	if st == nil || st.Code() != codes.Unavailable {
		return nil, false
	}
	return detailsFromStatusDetails(st.Proto().GetDetails())
}

// DetailsFromStatusProto extracts executor OOM killer details from a
// google.rpc.Status proto with an Unavailable code.
func DetailsFromStatusProto(proto *statuspb.Status) (*Details, bool) {
	if proto == nil {
		return nil, false
	}
	if codes.Code(proto.GetCode()) != codes.Unavailable {
		return nil, false
	}
	return detailsFromStatusDetails(proto.GetDetails())
}

func detailsFromStatusDetails(details []*anypb.Any) (*Details, bool) {
	for _, detail := range details {
		info := &errdetails.ErrorInfo{}
		if err := detail.UnmarshalTo(info); err != nil {
			continue
		}
		d, ok := detailsFromErrorInfo(info)
		if ok {
			return d, true
		}
	}
	return nil, false
}

func detailsFromErrorInfo(info *errdetails.ErrorInfo) (*Details, bool) {
	if info.GetReason() != ErrorInfoReason || info.GetDomain() != ErrorInfoDomain {
		return nil, false
	}
	return &Details{
		EstimatedMemoryBytes: metadataInt64(info.GetMetadata(), EstimatedMemoryBytesKey),
		ObservedMemoryBytes:  metadataInt64(info.GetMetadata(), ObservedMemoryBytesKey),
	}, true
}

func metadataInt64(metadata map[string]string, key string) int64 {
	value, err := strconv.ParseInt(metadata[key], 10, 64)
	if err != nil {
		return 0
	}
	return value
}

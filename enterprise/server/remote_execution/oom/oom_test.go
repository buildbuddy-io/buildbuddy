package oom_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/oom"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/anypb"

	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

func TestErrorRoundTrip(t *testing.T) {
	err := oom.Error("task-123", oom.Details{
		EstimatedMemoryBytes: 100,
		ObservedMemoryBytes:  200,
	})

	require.Equal(t, codes.Unavailable, gstatus.Code(err))
	require.Contains(t, gstatus.Convert(err).Message(), "executor OOM killer terminated task")
	details, ok := oom.DetailsFromError(err)
	require.True(t, ok)
	require.Equal(t, int64(100), details.EstimatedMemoryBytes)
	require.Equal(t, int64(200), details.ObservedMemoryBytes)
}

func TestErrorMetadataContainsMemoryFacts(t *testing.T) {
	err := oom.Error("task-123", oom.Details{
		EstimatedMemoryBytes: 100,
		ObservedMemoryBytes:  200,
	})

	statusDetails := gstatus.Convert(err).Details()
	require.Len(t, statusDetails, 1)
	info, ok := statusDetails[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	require.Equal(t, map[string]string{
		oom.EstimatedMemoryBytesKey: "100",
		oom.ObservedMemoryBytesKey:  "200",
	}, info.GetMetadata())
}

func TestDetailsFromStatusProto(t *testing.T) {
	info := &errdetails.ErrorInfo{
		Reason: oom.ErrorInfoReason,
		Domain: oom.ErrorInfoDomain,
		Metadata: map[string]string{
			oom.EstimatedMemoryBytesKey: "100",
			oom.ObservedMemoryBytesKey:  "200",
		},
	}
	detail, err := anypb.New(info)
	require.NoError(t, err)

	details, ok := oom.DetailsFromStatusProto(&statuspb.Status{
		Code:    int32(codes.Unavailable),
		Details: []*anypb.Any{detail},
	})
	require.True(t, ok)
	require.Equal(t, int64(100), details.EstimatedMemoryBytes)
	require.Equal(t, int64(200), details.ObservedMemoryBytes)
}

func TestDetailsFromErrorRequiresRetryableCode(t *testing.T) {
	info := &errdetails.ErrorInfo{
		Reason: oom.ErrorInfoReason,
		Domain: oom.ErrorInfoDomain,
		Metadata: map[string]string{
			oom.EstimatedMemoryBytesKey: "100",
			oom.ObservedMemoryBytesKey:  "200",
		},
	}
	st, err := gstatus.New(codes.Internal, "not retryable").WithDetails(info)
	require.NoError(t, err)

	details, ok := oom.DetailsFromError(st.Err())
	require.False(t, ok)
	require.Nil(t, details)
}

func TestDetailsFromStatusProtoDefaultsMissingMetadata(t *testing.T) {
	info := &errdetails.ErrorInfo{
		Reason: oom.ErrorInfoReason,
		Domain: oom.ErrorInfoDomain,
	}
	detail, err := anypb.New(info)
	require.NoError(t, err)

	details, ok := oom.DetailsFromStatusProto(&statuspb.Status{
		Code:    int32(codes.Unavailable),
		Details: []*anypb.Any{detail},
	})
	require.True(t, ok)
	require.Equal(t, int64(0), details.EstimatedMemoryBytes)
	require.Equal(t, int64(0), details.ObservedMemoryBytes)
}

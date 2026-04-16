package view

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
)

type fakeViewClient struct {
	logChunks          map[elpb.LogType]*elpb.GetEventLogChunkResponse
	invocation         *inpb.Invocation
	getInvocationCalls int
	requestedLogTypes  []elpb.LogType
}

func (f *fakeViewClient) GetEventLogChunk(ctx context.Context, req *elpb.GetEventLogChunkRequest, opts ...grpc.CallOption) (*elpb.GetEventLogChunkResponse, error) {
	f.requestedLogTypes = append(f.requestedLogTypes, req.GetType())
	if resp, ok := f.logChunks[req.GetType()]; ok {
		return resp, nil
	}
	return &elpb.GetEventLogChunkResponse{}, nil
}

func (f *fakeViewClient) GetInvocation(ctx context.Context, req *inpb.GetInvocationRequest, opts ...grpc.CallOption) (*inpb.GetInvocationResponse, error) {
	f.getInvocationCalls++
	if f.invocation == nil {
		return &inpb.GetInvocationResponse{}, nil
	}
	return &inpb.GetInvocationResponse{Invocation: []*inpb.Invocation{f.invocation}}, nil
}

func TestGetLogs_FetchesBuildAndRunLogsWhenRunStatusSet(t *testing.T) {
	client := &fakeViewClient{
		logChunks: map[elpb.LogType]*elpb.GetEventLogChunkResponse{
			elpb.LogType_BUILD_LOG: {Buffer: []byte("\x1b[32mbuild")},
			elpb.LogType_RUN_LOG:   {Buffer: []byte("run")},
		},
		invocation: &inpb.Invocation{RunStatus: inspb.OverallStatus_FAILURE},
	}

	logs, err := getLogs(context.Background(), client, "123", 42, false)
	require.NoError(t, err)
	require.Equal(t, "===== BUILD LOG =====\n\x1b[32mbuild\n\n\x1b[0m===== RUN LOG =====\nrun\n", string(logs))
	require.Equal(t, []elpb.LogType{elpb.LogType_BUILD_LOG, elpb.LogType_RUN_LOG}, client.requestedLogTypes)
	require.Equal(t, 1, client.getInvocationCalls)
}

func TestGetLogs_SkipsRunLogsWhenRunStatusUnset(t *testing.T) {
	client := &fakeViewClient{
		logChunks: map[elpb.LogType]*elpb.GetEventLogChunkResponse{
			elpb.LogType_BUILD_LOG: {Buffer: []byte("build\n")},
		},
		invocation: &inpb.Invocation{RunStatus: inspb.OverallStatus_UNKNOWN_OVERALL_STATUS},
	}

	logs, err := getLogs(context.Background(), client, "123", 42, false)
	require.NoError(t, err)
	require.Equal(t, "===== BUILD LOG =====\nbuild\n", string(logs))
	require.Equal(t, []elpb.LogType{elpb.LogType_BUILD_LOG}, client.requestedLogTypes)
	require.Equal(t, 1, client.getInvocationCalls)
}

func TestGetLogs_RunOnlySkipsInvocationLookup(t *testing.T) {
	client := &fakeViewClient{
		logChunks: map[elpb.LogType]*elpb.GetEventLogChunkResponse{
			elpb.LogType_RUN_LOG: {Buffer: []byte("run")},
		},
	}

	logs, err := getLogs(context.Background(), client, "123", 42, true)
	require.NoError(t, err)
	require.Equal(t, "===== RUN LOG =====\nrun\n", string(logs))
	require.Equal(t, []elpb.LogType{elpb.LogType_RUN_LOG}, client.requestedLogTypes)
	require.Equal(t, 0, client.getInvocationCalls)
}

func TestNewGetEventLogChunkRequest(t *testing.T) {
	req := newGetEventLogChunkRequest("123", 42, elpb.LogType_RUN_LOG)

	require.Equal(t, "123", req.GetInvocationId())
	require.Equal(t, int32(42), req.GetMinLines())
	require.Equal(t, elpb.LogType_RUN_LOG, req.GetType())
}

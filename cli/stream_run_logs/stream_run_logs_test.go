package stream_run_logs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
)

type fakeRunLogServiceClient struct {
	stream   *fakeRunLogWriteStream
	statuses []inspb.OverallStatus
}

func (f *fakeRunLogServiceClient) WriteEventLog(context.Context, ...grpc.CallOption) (bbspb.BuildBuddyService_WriteEventLogClient, error) {
	return f.stream, nil
}

func (f *fakeRunLogServiceClient) UpdateRunStatus(_ context.Context, req *elpb.UpdateRunStatusRequest, _ ...grpc.CallOption) (*elpb.UpdateRunStatusResponse, error) {
	f.statuses = append(f.statuses, req.GetStatus())
	return &elpb.UpdateRunStatusResponse{}, nil
}

type fakeRunLogWriteStream struct {
	failAfter int
	sendCount int
	sendErr   error
}

func (f *fakeRunLogWriteStream) Send(*elpb.WriteEventLogRequest) error {
	f.sendCount++
	if f.failAfter > 0 && f.sendCount >= f.failAfter {
		return f.sendErr
	}
	return nil
}

func (*fakeRunLogWriteStream) CloseAndRecv() (*elpb.WriteEventLogResponse, error) {
	return &elpb.WriteEventLogResponse{}, nil
}

func (*fakeRunLogWriteStream) Header() (metadata.MD, error) {
	return metadata.MD{}, nil
}

func (*fakeRunLogWriteStream) Trailer() metadata.MD {
	return metadata.MD{}
}

func (*fakeRunLogWriteStream) CloseSend() error {
	return nil
}

func (*fakeRunLogWriteStream) Context() context.Context {
	return context.Background()
}

func (*fakeRunLogWriteStream) SendMsg(any) error {
	return nil
}

func (*fakeRunLogWriteStream) RecvMsg(any) error {
	return nil
}

func TestRunScriptWithStreaming_FailureModeFail_KillsProcessOnUploadFailure(t *testing.T) {
	oldBufferSize := UploadBufferSize
	UploadBufferSize = 1
	t.Cleanup(func() {
		UploadBufferSize = oldBufferSize
	})

	script := writeExecutableScript(t, `#!/bin/bash
echo "first"
sleep 1
echo "second"
read -t 5 -r _ || true
`)

	client := &fakeRunLogServiceClient{
		stream: &fakeRunLogWriteStream{
			failAfter: 1,
			sendErr:   errors.New("stream send failed"),
		},
	}

	start := time.Now()
	exitCode, interrupted, err := runScriptWithStreaming(context.Background(), client, Opts{
		InvocationID: "invocation-id",
		OnFailure:    FailureModeFail,
	}, script, nil)

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to stream output")
	require.Equal(t, 1, exitCode)
	require.True(t, interrupted)
	require.Less(t, time.Since(start), 3*time.Second)
	require.Equal(t, []inspb.OverallStatus{inspb.OverallStatus_DISCONNECTED}, client.statuses)
	require.Equal(t, 1, client.stream.sendCount)
}

func TestRunScriptWithStreaming_FailureModeWarn_ContinuesAfterUploadFailure(t *testing.T) {
	oldBufferSize := UploadBufferSize
	UploadBufferSize = 1
	t.Cleanup(func() {
		UploadBufferSize = oldBufferSize
	})

	script := writeExecutableScript(t, `#!/bin/bash
echo "first"
sleep 1
echo "second"
exit 7
`)

	client := &fakeRunLogServiceClient{
		stream: &fakeRunLogWriteStream{
			failAfter: 1,
			sendErr:   errors.New("stream send failed"),
		},
	}

	exitCode, interrupted, err := runScriptWithStreaming(context.Background(), client, Opts{
		InvocationID: "invocation-id",
		OnFailure:    FailureModeWarn,
	}, script, nil)

	require.NoError(t, err)
	require.Equal(t, 7, exitCode)
	require.False(t, interrupted)
	require.Equal(t, []inspb.OverallStatus{inspb.OverallStatus_DISCONNECTED}, client.statuses)
	require.Equal(t, 1, client.stream.sendCount)
}

func writeExecutableScript(t *testing.T, contents string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "script.sh")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o755))
	return path
}

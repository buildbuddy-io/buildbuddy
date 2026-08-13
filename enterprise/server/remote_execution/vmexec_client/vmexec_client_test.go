package vmexec_client_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vmexec_client"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
)

const bufConnSize = 1024 * 1024

type blockingExecServer struct {
	vmxpb.UnimplementedExecServer

	streamCanceled chan struct{}
	syncCalled     chan struct{}
}

func (s *blockingExecServer) ExecStreamed(stream grpc.BidiStreamingServer[vmxpb.ExecStreamedRequest, vmxpb.ExecStreamedResponse]) error {
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	if msg.GetStart() == nil {
		return status.InvalidArgumentError("missing exec start request")
	}
	// Send one oversized stdout chunk, then keep the stream open until the
	// client cancels it.
	if err := stream.Send(&vmxpb.ExecStreamedResponse{Stdout: []byte("1234567890")}); err != nil {
		return err
	}
	<-stream.Context().Done()
	close(s.streamCanceled)
	return stream.Context().Err()
}

func (s *blockingExecServer) Sync(ctx context.Context, req *vmxpb.SyncRequest) (*vmxpb.SyncResponse, error) {
	close(s.syncCalled)
	return &vmxpb.SyncResponse{}, nil
}

func TestExecute_CancelsStreamOnOutputLimit(t *testing.T) {
	flags.Set(t, "executor.stdouterr_max_size_bytes", 1)

	lis := bufconn.Listen(bufConnSize)
	server := grpc.NewServer()
	execServer := &blockingExecServer{
		streamCanceled: make(chan struct{}),
		syncCalled:     make(chan struct{}),
	}
	vmxpb.RegisterExecServer(server, execServer)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.DialContext(
		t.Context(),
		"bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	result := vmexec_client.Execute(
		t.Context(),
		vmxpb.NewExecClient(conn),
		&repb.Command{Arguments: []string{"sh", "-c", "echo ignored"}},
		"/workspace",
		"",
		nil, /*=statsListener*/
		&interfaces.Stdio{},
	)

	require.Error(t, result.Error)
	assert.Equal(t, commandutil.NoExitCode, result.ExitCode)
	assert.True(t, status.IsResourceExhaustedError(result.Error), "expected ResourceExhaustedError, got %v", result.Error)

	// Execute should cancel the stream before returning the limit error, then
	// sync the guest filesystem as part of interruption cleanup.
	select {
	case <-execServer.streamCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("expected Execute to cancel the vmexec stream before returning output-limit error")
	}
	select {
	case <-execServer.syncCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("expected Execute to sync the guest filesystem after interrupting execution")
	}
}

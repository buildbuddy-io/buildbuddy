package build_event_server_test

import (
	"context"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/require"

	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

func TestPublishBuildToolEventStream_NoEvents(t *testing.T) {
	env := testenv.GetTestEnv(t)
	server, err := build_event_server.NewBuildEventProtocolServer(env, false /*=synchronous*/)
	require.NoError(t, err)
	grpcServer, runServer := testenv.RegisterLocalGRPCServer(env)
	pepb.RegisterPublishBuildEventServer(grpcServer, server)
	go runServer()
	t.Cleanup(func() {
		grpcServer.Stop()
	})

	// Make a PublishBuildToolEventStream RPC but close it without sending
	// anything.
	ctx := context.Background()
	conn, err := testenv.LocalGRPCConn(ctx, env)
	require.NoError(t, err)
	client := pepb.NewPublishBuildEventClient(conn)
	stream, err := client.PublishBuildToolEventStream(ctx)
	require.NoError(t, err)
	err = stream.CloseSend()
	require.NoError(t, err)

	_, err = stream.Recv()
	require.Equal(t, io.EOF, err)
}

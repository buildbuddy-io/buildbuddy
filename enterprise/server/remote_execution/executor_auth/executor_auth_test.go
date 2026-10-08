package executor_auth_test

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor_auth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type credentialEchoServer struct {
	healthpb.UnimplementedHealthServer
}

func credentials(ctx context.Context) metadata.MD {
	md := metadata.MD{}
	for _, key := range []string{authutil.ExecutorAPIKeyHeader, authutil.APIKeyHeader, authutil.ContextTokenStringKey, "test-header"} {
		if values := metadata.ValueFromIncomingContext(ctx, key); len(values) > 0 {
			md[key] = values
		}
	}
	return md
}

func (*credentialEchoServer) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if err := grpc.SendHeader(ctx, credentials(ctx)); err != nil {
		return nil, err
	}
	return &healthpb.HealthCheckResponse{}, nil
}

func (*credentialEchoServer) Watch(req *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	if err := stream.SendHeader(credentials(stream.Context())); err != nil {
		return err
	}
	return stream.Send(&healthpb.HealthCheckResponse{})
}

func TestExecutorCredentialsOnAllRPCs(t *testing.T) {
	env := testenv.GetTestEnv(t)
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, &credentialEchoServer{})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	opts := executor_auth.GRPCDialOptions()
	opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}))
	conn, err := grpc_client.DialInternalWithPoolSize(env, "grpc://bufnet", 1, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := healthpb.NewHealthClient(conn)

	for _, tc := range []struct {
		name          string
		executorKey   string
		taskJWT       string
		primaryAPIKey string
		existingProof string
	}{
		{name: "task JWT", executorKey: "executor-key", taskJWT: "task-jwt"},
		{name: "registration API key", executorKey: "executor-key", primaryAPIKey: "executor-key"},
		{name: "no primary credentials", executorKey: "executor-key"},
		{name: "no executor key", taskJWT: "task-jwt"},
		{name: "replace existing proof", executorKey: "executor-key", taskJWT: "task-jwt", existingProof: "old-executor-key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "executor.api_key", tc.executorKey)
			ctx := metadata.AppendToOutgoingContext(t.Context(), "test-header", "preserved")
			if tc.taskJWT != "" {
				ctx = context.WithValue(ctx, authutil.ContextTokenStringKey, tc.taskJWT)
			}
			if tc.primaryAPIKey != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, tc.primaryAPIKey)
			}
			if tc.existingProof != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, authutil.ExecutorAPIKeyHeader, tc.existingProof)
			}
			originalMD, _ := metadata.FromOutgoingContext(ctx)
			want := metadata.Pairs("test-header", "preserved")
			for k, v := range map[string]string{
				authutil.ExecutorAPIKeyHeader:  tc.executorKey,
				authutil.APIKeyHeader:          tc.primaryAPIKey,
				authutil.ContextTokenStringKey: tc.taskJWT,
			} {
				if v != "" {
					want.Set(k, v)
				}
			}

			var unaryHeaders metadata.MD
			_, err := client.Check(ctx, &healthpb.HealthCheckRequest{}, grpc.Header(&unaryHeaders))
			require.NoError(t, err)
			stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
			require.NoError(t, err)
			streamHeaders, err := stream.Header()
			require.NoError(t, err)
			_, err = stream.Recv()
			require.NoError(t, err)
			_, err = stream.Recv()
			require.ErrorIs(t, err, io.EOF)
			for _, got := range []metadata.MD{unaryHeaders, streamHeaders} {
				got.Delete("content-type")
				require.Equal(t, want, got)
			}
			// Adding proof must not modify the shared task context.
			md, _ := metadata.FromOutgoingContext(ctx)
			require.Equal(t, originalMD, md)
		})
	}
}

package executor_auth

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	apiKey = flag.String("executor.api_key", "", "API Key used to authorize the executor with the BuildBuddy app server.", flag.Secret)
)

// APIKey returns the configured API key used to authenticate the executor with
// the BuildBuddy app server.
func APIKey() string {
	return *apiKey
}

// GRPCDialOptions attaches executor credentials to requests to the app and cache.
// The separate header preserves the task's primary identity and capabilities.
func GRPCDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(withExecutorCredentials(ctx), method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return streamer(withExecutorCredentials(ctx), desc, cc, method, opts...)
		}),
	}
}

func withExecutorCredentials(ctx context.Context) context.Context {
	key := APIKey()
	if key == "" {
		return ctx
	}
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	}
	md.Set(authutil.ExecutorAPIKeyHeader, key)
	return metadata.NewOutgoingContext(ctx, md)
}

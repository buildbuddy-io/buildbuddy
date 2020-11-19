package grpc_client

import (
	"context"
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/rpc/filters"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/google"
)

// DialTarget handles some of the logic around detecting the correct GRPC
// connection type and applying relevant options when connecting.
func DialTarget(target string) (*grpc.ClientConn, error) {
	return DialTargetWithOptions(target, true)
}

func DialTargetWithOptions(target string, grpcsBytestream bool, extraOptions ...grpc.DialOption) (*grpc.ClientConn, error) {
	dialOptions := []grpc.DialOption{
		filters.GetUnaryClientInterceptor(),
		filters.GetStreamClientInterceptor(),
	}
	dialOptions = append(dialOptions, extraOptions...)
	u, err := url.Parse(target)
	if err == nil {
		if u.User != nil {
			dialOptions = append(dialOptions, grpc.WithPerRPCCredentials(newRPCCredentials(u.User.String())))
		}
		if u.Scheme == "grpcs" || (u.Scheme == "bytestream" && grpcsBytestream) {
			dialOptions = append(dialOptions, grpc.WithTransportCredentials(google.NewDefaultCredentials().TransportCredentials()))
		} else {
			dialOptions = append(dialOptions, grpc.WithInsecure())
		}

		if u.Scheme == "grpcs" && u.Port() == "" {
			u.Host += ":443"
		}
		target = u.Host
	}

	// Connect to host/port and create a new client
	return grpc.Dial(target, dialOptions...)
}

type rpcCredentials struct {
	authorization string
}

func newRPCCredentials(authorization string) *rpcCredentials {
	return &rpcCredentials{
		authorization: authorization,
	}
}

func (c *rpcCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": c.authorization,
	}, nil
}

func (c *rpcCredentials) RequireTransportSecurity() bool {
	return false
}

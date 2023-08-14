package grpc_client

import (
	"context"
	"math"
	"net/url"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/google"
	"google.golang.org/grpc/keepalive"
)

// DialTarget handles some of the logic around detecting the correct GRPC
// connection type and applying relevant options when connecting.
func DialTarget(target string, extraOptions ...grpc.DialOption) (*grpc.ClientConn, error) {
	dialOptions := CommonGRPCClientOptions()
	dialOptions = append(dialOptions, extraOptions...)
	u, err := url.Parse(target)
	if err == nil {
		if u.User != nil {
			dialOptions = append(dialOptions, grpc.WithPerRPCCredentials(newRPCCredentials(u.User.String())))
		}
		if u.Scheme == "grpcs" {
			dialOptions = append(dialOptions, grpc.WithTransportCredentials(google.NewDefaultCredentials().TransportCredentials()))
		} else {
			dialOptions = append(dialOptions, grpc.WithInsecure())
		}

		if u.Scheme == "grpcs" && u.Port() == "" {
			u.Host += ":443"
		}

		if u.Scheme != "unix" {
			target = u.Host
		}
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

func CommonGRPCClientOptions() []grpc.DialOption {
	return []grpc.DialOption{
		interceptors.GetUnaryClientInterceptor(),
		interceptors.GetStreamClientInterceptor(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// After a duration of this time if the client doesn't see any activity it
			// pings the server to see if the transport is still alive.
			Time: 30 * time.Second,

			// After having pinged for keepalive check, the client waits for a duration
			// of Timeout and if no activity is seen even after that the connection is
			// closed.
			Timeout: 20 * time.Second,

			// If true, client sends keepalive pings even with no active RPCs.
			PermitWithoutStream: true,

			PingCallback: func(connectionID uint64) {
				log.Debugf("Sending keepalive ping for connection %d", connectionID)
			},
		}),
	}
}

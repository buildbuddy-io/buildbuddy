package grpc_forward

import (
	"context"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/mwitkow/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	kytheBackendToProxy = flag.String("app.kythe_backend_to_proxy", "", "If set, proxy kythe traffic to this backend")

	mu                     sync.RWMutex
	backendConnectionPools map[string]*grpc_client.ClientConnPool
)

func init() {
	mu.Lock()
	backendConnectionPools = make(map[string]*grpc_client.ClientConnPool)
	mu.Unlock()
}

type directorFunc func(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error)

func getProxyDirector() directorFunc {
	if *kytheBackendToProxy != "" {
		return func(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
			target := ""
			if strings.HasPrefix(fullMethodName, "/kythe.service") {
				target = *kytheBackendToProxy
			} else {
				return nil, nil, status.UnimplementedErrorf("unknown service %s", fullMethodName)
			}

			mu.RLock()
			pool, ok := backendConnectionPools[target]
			mu.RUnlock()

			if !ok {
				newPool, err := grpc_client.DialSimple(target)
				if err != nil {
					return nil, nil, err
				}
				mu.Lock()
				backendConnectionPools[target] = newPool
				mu.Unlock()
				pool = newPool
			}

			cc, err := pool.GetReadyConnection()
			if err != nil {
				return nil, nil, err
			}
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				ctx = metadata.NewOutgoingContext(ctx, md.Copy())
			}
			return ctx, cc, nil
		}
	}
	return nil
}

func GetForwardingServerOption() grpc.ServerOption {
	if director := getProxyDirector(); director != nil {
		return grpc.UnknownServiceHandler(proxy.TransparentHandler(proxy.StreamDirector(director)))
	}
	return nil
}

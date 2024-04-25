package content_addressable_storage_server_proxy

import (
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type CASServerProxy struct {
	env         environment.Env
	localCache  interfaces.Cache
	remoteCache repb.ContentAddressableStorageClient
}

func Register(env *real_environment.RealEnv) error {
	casServer, err := NewCASServerProxy(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ContentAddressableStorageServerProxy: %s", err)
	}
	env.SetCASServer(casServer)
	return nil
}

func NewCASServerProxy(env environment.Env) (*CASServerProxy, error) {
	localCache := env.GetCache()
	if localCache == nil {
		return nil, fmt.Errorf("A cache is required to enable the ContentAddressableStorageServerProxy")
	}
	remoteCache := env.GetContentAddressableStorageClient()
	if remoteCache == nil {
		return nil, fmt.Errorf("A ContentAddressableStorageClient is required to enable the ContentAddressableStorageServerProxy")
	}
	return &CASServerProxy{
		env:         env,
		localCache:  localCache,
		remoteCache: remoteCache,
	}, nil
}

func (s *CASServerProxy) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	return s.remoteCache.FindMissingBlobs(ctx, req)
}

func (s *CASServerProxy) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	return s.remoteCache.BatchUpdateBlobs(ctx, req)
}

func (s *CASServerProxy) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	return s.remoteCache.BatchReadBlobs(ctx, req)
}

func (s *CASServerProxy) GetTree(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	remoteStream, err := s.remoteCache.GetTree(context.Background(), req)
	if err != nil {
		return err
	}
	for {
		rsp, err := remoteStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err = stream.Send(rsp); err != nil {
			return err
		}
	}
	return nil
}

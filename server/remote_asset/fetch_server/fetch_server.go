package fetch_server

import (
	"context"
	"log"

	"github.com/buildbuddy-io/buildbuddy/server/environment"

	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
)

/*
rpc FetchBlob(FetchBlobRequest) returns (FetchBlobResponse) {
	option (google.api.http) = { post: "/v1/{instance_name=**}/assets:fetchBlob" body: "*" };
}
rpc FetchDirectory(FetchDirectoryRequest) returns (FetchDirectoryResponse) {
	option (google.api.http) = { post: "/v1/{instance_name=**}/assets:fetchDirectory" body: "*" };
}
*/

type FetchServer struct {
	env environment.Env
}

func NewFetchServer(env environment.Env) *FetchServer {
	return &FetchServer{
		env: env,
	}
}

func (p *FetchServer) FetchBlob(ctx context.Context, req *rapb.FetchBlobRequest) (*rapb.FetchBlobResponse, error) {
	log.Printf("FetchBlob req: %v", req)
	return &rapb.FetchBlobResponse{}, nil
}

func (p *FetchServer) FetchDirectory(ctx context.Context, req *rapb.FetchDirectoryRequest) (*rapb.FetchDirectoryResponse, error) {
	log.Printf("FetchDirectory req: %v", req)
	return &rapb.FetchDirectoryResponse{}, nil
}

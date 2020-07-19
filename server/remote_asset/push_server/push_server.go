package push_server

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

type PushServer struct {
	env environment.Env
}

func NewPushServer(env environment.Env) *PushServer {
	return &PushServer{
		env: env,
	}
}

func (p *PushServer) PushBlob(ctx context.Context, req *rapb.PushBlobRequest) (*rapb.PushBlobResponse, error) {
	log.Printf("PushBlob req: %v", req)
	return &rapb.PushBlobResponse{}, nil
}

func (p *PushServer) PushDirectory(ctx context.Context, req *rapb.PushDirectoryRequest) (*rapb.PushDirectoryResponse, error) {
	log.Printf("PushDirectory req: %v", req)
	return &rapb.PushDirectoryResponse{}, nil
}

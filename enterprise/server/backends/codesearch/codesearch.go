package codesearch

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	csspb "github.com/buildbuddy-io/buildbuddy/proto/codesearch_service"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/index"
	srpb "github.com/buildbuddy-io/buildbuddy/proto/search"
)

var (
	codesearchBackend = flag.String("app.codesearch_backend", "", "Address and port to connect to")
)

type CodesearchService struct {
	client csspb.CodesearchServiceClient
}

func Register(realEnv *real_environment.RealEnv) error {
	if *codesearchBackend == "" {
		return nil
	}
	css, err := New(realEnv)
	if err != nil {
		return err
	}
	realEnv.SetCodesearchService(css)
	return nil
}

func New(env environment.Env) (*CodesearchService, error) {
	conn, err := grpc_client.DialInternal(env, *codesearchBackend)
	if err != nil {
		return nil, status.UnavailableErrorf("could not dial codesearch backend %q: %s", *codesearchBackend, err)
	}
	return &CodesearchService{
		client: csspb.NewCodesearchServiceClient(conn),
	}, nil
}

// Search runs a search RPC against the configured codesearch backend.
func (css *CodesearchService) Search(ctx context.Context, req *srpb.SearchRequest) (*srpb.SearchResponse, error) {
	return css.client.Search(ctx, req)
}

func (css *CodesearchService) Index(ctx context.Context, req *inpb.IndexRequest) (*inpb.IndexResponse, error) {
	return css.client.Index(ctx, req)
}

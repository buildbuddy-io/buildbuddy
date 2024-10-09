package kythe

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ksspb "github.com/buildbuddy-io/buildbuddy/proto/kythe_service"
	kgrpb "kythe.io/kythe/proto/graph_go_proto"
	kxrpb "kythe.io/kythe/proto/xref_go_proto"
)

var (
	kytheBackend = flag.String("app.kythe_backend", "", "Address and port to connect to")
)

type KytheService struct {
	graphClient ksspb.GraphServiceClient
	xrefClient ksspb.XRefServiceClient
}

func Register(realEnv *real_environment.RealEnv) error {
	if *kytheBackend == "" {
		return nil
	}
	css, err := New(realEnv)
	if err != nil {
		return err
	}
	realEnv.SetKytheService(css)
	return nil
}

func New(env environment.Env) (*KytheService, error) {
	conn, err := grpc_client.DialInternal(env, *kytheBackend)
	if err != nil {
		return nil, status.UnavailableErrorf("could not dial kythe backend %q: %s", *kytheBackend, err)
	}
	return &KytheService{
		graphClient: ksspb.NewGraphServiceClient(conn),
		xrefClient: ksspb.NewXRefServiceClient(conn),
	}, nil
}

func (kss *KytheService) Nodes(ctx context.Context, req *kgrpb.NodesRequest) (*kgrpb.NodesReply, error) {
	return kss.graphClient.Nodes(ctx, req)
}
func (kss *KytheService) Decorations(ctx context.Context, req *kxrpb.DecorationsRequest) (*kxrpb.DecorationsReply, error) {
	return kss.xrefClient.Decorations(ctx, req)
}
func (kss *KytheService) CrossReferences(ctx context.Context, req *kxrpb.CrossReferencesRequest) (*kxrpb.CrossReferencesReply, error) {
	return kss.xrefClient.CrossReferences(ctx, req)
}

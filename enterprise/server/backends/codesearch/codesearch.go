package codesearch

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
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
	env    environment.Env
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
	conn, err := grpc_client.DialInternalWithPoolSize(env, *codesearchBackend, 2)
	if err != nil {
		return nil, status.UnavailableErrorf("could not dial codesearch backend %q: %s", *codesearchBackend, err)
	}
	return &CodesearchService{
		client: csspb.NewCodesearchServiceClient(conn),
		env:    env,
	}, nil
}

// Search runs a search RPC against the configured codesearch backend.
func (css *CodesearchService) Search(ctx context.Context, req *srpb.SearchRequest) (*srpb.SearchResponse, error) {
	return css.client.Search(ctx, req)
}

func (css *CodesearchService) getGitRepoAccessToken(ctx context.Context, groupId, repoURL string) (string, error) {
	ghas := css.env.GetGitHubAppService()
	if ghas == nil {
		return "", status.InternalError("GitHub App service is not configured")
	}

	parsedRepoURL, err := git.ParseGitHubRepoURL(repoURL)
	if err != nil {
		return "", status.InvalidArgumentErrorf("invalid repo URL %q: %s", repoURL, err)
	}

	gha, err := ghas.GetGitHubAppForOwner(ctx, parsedRepoURL.Owner)
	if err != nil {
		return "", status.UnavailableErrorf("could not get GitHub app for repo %q: %s", repoURL, err)
	}

	gitRepository := &tables.GitRepository{}
	err = css.env.GetDBHandle().NewQuery(ctx, "hosted_runner_get_for_repo").Raw(`
		SELECT *
		FROM "GitRepositories"
		WHERE group_id = ?
		AND repo_url = ?
	`, groupId, repoURL).Take(gitRepository)
	if err != nil {
		if db.IsRecordNotFound(err) {
			return "", status.NotFoundErrorf("repo %q not found", repoURL)
		}
		return "", status.InternalErrorf("failed to look up repo %s: %s", repoURL, err)
	}

	token, err := gha.GetRepositoryInstallationToken(ctx, gitRepository)
	if err != nil {
		return "", status.UnavailableErrorf("could not get access token for repo %q: %s", repoURL, err)
	}

	return token, nil
}

func (css *CodesearchService) Index(ctx context.Context, req *inpb.IndexRequest) (*inpb.IndexResponse, error) {
	claims, err := claims.ClaimsFromContext(ctx, css.env.GetJWTParser())
	if err != nil {
		return nil, status.UnauthenticatedErrorf("failed to get claims from context: %s", err)
	}

	g, err := css.env.GetUserDB().GetGroupByID(ctx, claims.GetGroupID())
	if err != nil {
		return nil, err
	}
	if !g.CodeSearchEnabled {
		return nil, status.FailedPreconditionError("codesearch is not enabled")
	}

	if req.GetReplacementStrategy() == inpb.ReplacementStrategy_REPLACE_REPO {
		token, err := css.getGitRepoAccessToken(ctx, claims.GetGroupID(), req.GetGitRepo().GetRepoUrl())
		if err != nil {
			return nil, err
		}
		req.GetGitRepo().AccessToken = token
	}

	return css.client.Index(ctx, req)
}

func (css *CodesearchService) RepoStatus(ctx context.Context, req *inpb.RepoStatusRequest) (*inpb.RepoStatusResponse, error) {
	return css.client.RepoStatus(ctx, req)
}

func (css *CodesearchService) IngestAnnotations(ctx context.Context, req *inpb.IngestAnnotationsRequest) (*inpb.IngestAnnotationsResponse, error) {
	return css.client.IngestAnnotations(ctx, req)
}

func (css *CodesearchService) KytheProxy(ctx context.Context, req *srpb.KytheRequest) (*srpb.KytheResponse, error) {
	return css.client.KytheProxy(ctx, req)
}

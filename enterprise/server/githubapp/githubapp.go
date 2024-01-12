package githubapp

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/scratchspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/golang-jwt/jwt"
	"github.com/google/go-github/v43/github"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"

	gh_webhooks "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github"
	ghpb "github.com/buildbuddy-io/buildbuddy/proto/github"
	rppb "github.com/buildbuddy-io/buildbuddy/proto/repo"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	gh_oauth "github.com/buildbuddy-io/buildbuddy/server/backends/github"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	gitobject "github.com/go-git/go-git/v5/plumbing/object"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
)

var (
	enabled       = flag.Bool("github.app.enabled", false, "Whether to enable the BuildBuddy GitHub app server.")
	clientID      = flag.String("github.app.client_id", "", "GitHub app OAuth client ID.")
	clientSecret  = flag.String("github.app.client_secret", "", "GitHub app OAuth client secret.", flag.Secret)
	appID         = flag.String("github.app.id", "", "GitHub app ID.")
	publicLink    = flag.String("github.app.public_link", "", "GitHub app installation URL.")
	privateKey    = flag.String("github.app.private_key", "", "GitHub app private key.", flag.Secret)
	webhookSecret = flag.String("github.app.webhook_secret", "", "GitHub app webhook secret used to verify that webhook payload contents were sent by GitHub.", flag.Secret)

	validPathRegex = regexp.MustCompile(`^[a-zA-Z0-9/_-]*$`)
)

const (
	oauthAppPath = "/auth/github/app/link/"

	// Max page size that GitHub allows for list requests.
	githubMaxPageSize = 100
)

func Register(env *real_environment.RealEnv) error {
	if !*enabled {
		return nil
	}
	app, err := New(env)
	if err != nil {
		return err
	}
	env.SetGitHubApp(app)
	return nil
}

func IsEnabled() bool {
	return *enabled
}

// GitHubApp implements the BuildBuddy GitHub app. Users install the app to
// their personal account or organization, granting access to some or all
// repositories.
//
// Note that in GitHub's terminology, this is a proper "GitHub App" as opposed
// to an OAuth App. This means that it authenticates as its own entity, rather
// than on behalf of a particular user. See
// https://docs.github.com/en/developers/apps/getting-started-with-apps/about-apps
type GitHubApp struct {
	env environment.Env

	oauth *gh_oauth.OAuthHandler

	// privateKey is the GitHub-issued private key for the app. It is used to
	// create JWTs for authenticating with GitHub as the app itself.
	privateKey *rsa.PrivateKey
}

// New returns a new GitHubApp handle.
func New(env environment.Env) (*GitHubApp, error) {
	if *clientID == "" {
		return nil, status.FailedPreconditionError("missing client ID.")
	}
	if *clientSecret == "" {
		return nil, status.FailedPreconditionError("missing client secret.")
	}
	if *appID == "" {
		return nil, status.FailedPreconditionError("missing app ID")
	}
	if *publicLink == "" {
		return nil, status.FailedPreconditionError("missing app public link")
	}
	if *webhookSecret == "" {
		return nil, status.FailedPreconditionError("missing app webhook secret")
	}
	if *privateKey == "" {
		return nil, status.FailedPreconditionError("missing app private key")
	}
	privateKey, err := decodePrivateKey(*privateKey)
	if err != nil {
		return nil, err
	}

	app := &GitHubApp{
		env:        env,
		privateKey: privateKey,
	}
	oauth := gh_oauth.NewOAuthHandler(env, *clientID, *clientSecret, oauthAppPath)
	oauth.HandleInstall = app.handleInstall
	oauth.InstallURL = fmt.Sprintf("%s/installations/new", *publicLink)
	app.oauth = oauth
	return app, nil
}

func (a *GitHubApp) WebhookHandler() http.Handler {
	return http.HandlerFunc(a.handleWebhookRequest)
}

func (a *GitHubApp) handleWebhookRequest(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	b, err := github.ValidatePayload(req, []byte(*webhookSecret))
	if err != nil {
		log.CtxDebugf(ctx, "Failed to validate webhook payload: %s", err)
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	t := github.WebHookType(req)
	event, err := github.ParseWebHook(t, b)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to parse GitHub webhook payload for %q event: %s", t, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Add delivery ID to context so we can correlate logs with GitHub's webhook
	// deliveries UI.
	ctx = log.EnrichContext(ctx, "github_delivery", req.Header.Get("X-GitHub-Delivery"))
	if err := a.handleWebhookEvent(ctx, t, event); err != nil {
		log.CtxErrorf(ctx, "Failed to handle webhook event %q: %s", t, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "OK")
}

func (a *GitHubApp) handleWebhookEvent(ctx context.Context, eventType string, event any) error {
	switch event := event.(type) {
	case *github.InstallationEvent:
		return a.handleInstallationEvent(ctx, eventType, event)
	default:
		return a.handleWorkflowEvent(ctx, eventType, event)
	}
}

func (a *GitHubApp) handleInstallationEvent(ctx context.Context, eventType string, event *github.InstallationEvent) error {
	log.CtxInfof(ctx, "Handling installation event type=%q, owner=%q, id=%d", eventType, event.GetInstallation().GetAccount().GetLogin(), event.GetInstallation().GetID())

	// Only handling uninstall events for now. We proactively handle this event
	// by removing references to the installation ID in the DB. Note, however,
	// that we still need to gracefully handle the case where the installation
	// ID is suddenly no longer valid, since webhook deliveries aren't 100%
	// reliable.
	if event.GetAction() != "deleted" {
		return nil
	}
	result := a.env.GetDBHandle().NewQuery(ctx, "githubapp_uninstall_app").Raw(`
		DELETE FROM "GitHubAppInstallations"
		WHERE installation_id = ?
	`, event.GetInstallation().GetID()).Exec()
	if result.Error != nil {
		return status.InternalErrorf("failed to delete installation: %s", result.Error)
	}

	log.CtxInfof(ctx,
		"Handling GitHub app uninstall event: removed %d installation row(s) for installation %d",
		result.RowsAffected, event.GetInstallation().GetID())
	return nil
}

func (a *GitHubApp) handleWorkflowEvent(ctx context.Context, eventType string, event any) error {
	wd, err := gh_webhooks.ParseWebhookData(event)
	if err != nil {
		return err
	}
	if wd == nil {
		// Could not parse any webhook data relevant to workflows;
		// nothing to do.
		log.CtxInfof(ctx, "No webhook data parsed for %q event", eventType)
		return nil
	}
	log.CtxInfof(ctx, "Parsed webhook data: %s", webhook_data.DebugString(wd))
	repoURL, err := gitutil.ParseGitHubRepoURL(wd.TargetRepoURL)
	if err != nil {
		return err
	}
	row := &struct {
		InstallationID int64
		*tables.GitRepository
	}{}
	err = a.env.GetDBHandle().NewQuery(ctx, "githubapp_get_installation_for_event").Raw(`
		SELECT i.installation_id, r.*
		FROM "GitHubAppInstallations" i, "GitRepositories" r
		WHERE r.repo_url = ?
		AND i.owner = ?
		AND i.group_id = r.group_id
	`, repoURL.String(), repoURL.Owner).Take(row)
	if err != nil {
		return status.NotFoundError("the repository as well as a BuildBuddy GitHub app installation must be linked to a BuildBuddy org in order to use workflows")
	}
	tok, err := a.createInstallationToken(ctx, row.InstallationID)
	if err != nil {
		return err
	}
	return a.env.GetWorkflowService().HandleRepositoryEvent(
		ctx, row.GitRepository, wd, tok.GetToken())
}

func (a *GitHubApp) GetInstallationTokenForStatusReportingOnly(ctx context.Context, owner string) (string, error) {
	var installation tables.GitHubAppInstallation
	err := a.env.GetDBHandle().NewQuery(ctx, "githubapp_get_installation_token_for_status").Raw(`
		SELECT *
		FROM "GitHubAppInstallations"
		WHERE owner = ?
	`, owner).Take(&installation)
	if err != nil {
		if db.IsRecordNotFound(err) {
			return "", status.NotFoundErrorf("failed to look up GitHub app installation: %s", err)
		}
		return "", err
	}
	tok, err := a.createInstallationToken(ctx, installation.InstallationID)
	if err != nil {
		return "", err
	}
	return tok.GetToken(), nil
}

func (a *GitHubApp) GetRepositoryInstallationToken(ctx context.Context, repo *tables.GitRepository) (string, error) {
	if err := perms.AuthorizeGroupAccess(ctx, a.env, repo.GroupID); err != nil {
		return "", err
	}
	repoURL, err := gitutil.ParseGitHubRepoURL(repo.RepoURL)
	if err != nil {
		return "", err
	}
	var installation tables.GitHubAppInstallation
	err = a.env.GetDBHandle().NewQuery(ctx, "githubapp_get_installation_token").Raw(`
		SELECT *
		FROM "GitHubAppInstallations"
		WHERE group_id = ?
		AND owner = ?
	`, repo.GroupID, repoURL.Owner).Take(&installation)
	if err != nil {
		if db.IsRecordNotFound(err) {
			return "", status.NotFoundErrorf("failed to look up GitHub app installation: %s", err)
		}
		return "", err
	}
	tok, err := a.createInstallationToken(ctx, installation.InstallationID)
	if err != nil {
		return "", err
	}
	return tok.GetToken(), nil
}

func (a *GitHubApp) GetGitHubAppInstallations(ctx context.Context, req *ghpb.GetAppInstallationsRequest) (*ghpb.GetAppInstallationsResponse, error) {
	u, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return nil, err
	}
	// List installations linked to the org.
	rq := a.env.GetDBHandle().NewQuery(ctx, "githubapp_get_installations").Raw(`
		SELECT *
		FROM "GitHubAppInstallations"
		WHERE group_id = ?
		ORDER BY owner ASC
	`, u.GetGroupID())
	res := &ghpb.GetAppInstallationsResponse{}
	err = db.ScanEach(rq, func(ctx context.Context, row *tables.GitHubAppInstallation) error {
		res.Installations = append(res.Installations, &ghpb.AppInstallation{
			GroupId:        row.GroupID,
			InstallationId: row.InstallationID,
			Owner:          row.Owner,
		})
		return nil
	})
	if err != nil {
		return nil, status.InternalErrorf("failed to get installations: %s", err)
	}
	return res, nil
}

func (a *GitHubApp) LinkGitHubAppInstallation(ctx context.Context, req *ghpb.LinkAppInstallationRequest) (*ghpb.LinkAppInstallationResponse, error) {
	u, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return nil, err
	}
	in, err := a.getInstallation(ctx, req.GetInstallationId())
	if err != nil {
		return nil, err
	}
	if err := a.linkInstallation(ctx, in, u.GetGroupID()); err != nil {
		return nil, err
	}
	return &ghpb.LinkAppInstallationResponse{}, nil
}

func (a *GitHubApp) linkInstallation(ctx context.Context, installation *github.Installation, groupID string) error {
	u, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return err
	}
	if err := authutil.AuthorizeGroupRole(u, groupID, role.Admin); err != nil {
		return err
	}
	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return err
	}
	if tu.GithubToken == "" {
		return status.UnauthenticatedError("failed to link GitHub app installation: GitHub account link is required")
	}
	if err := a.authorizeUserInstallationAccess(ctx, tu.GithubToken, installation.GetID()); err != nil {
		return err
	}
	err = a.createInstallation(ctx, &tables.GitHubAppInstallation{
		GroupID:        groupID,
		InstallationID: installation.GetID(),
		Owner:          installation.GetAccount().GetLogin(),
	})
	if err != nil {
		return status.InternalErrorf("failed to link GitHub app installation: %s", err)
	}
	return nil
}

func (a *GitHubApp) createInstallation(ctx context.Context, in *tables.GitHubAppInstallation) error {
	if in.Owner == "" {
		return status.FailedPreconditionError("owner field is required")
	}
	log.CtxInfof(ctx,
		"Linking GitHub app installation %d (%s) to group %s",
		in.InstallationID, in.Owner, in.GroupID)
	return a.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		// If an installation already exists with the given owner, unlink it
		// first. That installation must be stale since GitHub only allows
		// one installation per owner.
		err := tx.NewQuery(ctx, "githubapp_delete_existing_installation").Raw(`
			DELETE FROM "GitHubAppInstallations"
			WHERE owner = ?`,
			in.Owner,
		).Exec().Error
		if err != nil {
			return err
		}
		// Note: (GroupID, InstallationID) is the primary key, so this will fail
		// if the installation is already linked to another group.
		return tx.NewQuery(ctx, "githubapp_create_installation").Create(in)
	})
}

func (a *GitHubApp) UnlinkGitHubAppInstallation(ctx context.Context, req *ghpb.UnlinkAppInstallationRequest) (*ghpb.UnlinkAppInstallationResponse, error) {
	u, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return nil, err
	}
	if req.GetInstallationId() == 0 {
		return nil, status.FailedPreconditionError("missing installation_id")
	}
	dbh := a.env.GetDBHandle()
	err = dbh.Transaction(ctx, func(tx interfaces.DB) error {
		var ti tables.GitHubAppInstallation
		err := tx.NewQuery(ctx, "githubapp_get_installation_for_unlink").Raw(`
			SELECT *
			FROM "GitHubAppInstallations"
			WHERE installation_id = ?
			`+dbh.SelectForUpdateModifier()+`
		`, req.GetInstallationId()).Take(&ti)
		if err != nil {
			return err
		}
		if err := authutil.AuthorizeGroupRole(u, ti.GroupID, role.Admin); err != nil {
			return err
		}
		return tx.NewQuery(ctx, "githubapp_unlink_installation").Raw(`
			DELETE FROM "GitHubAppInstallations"
			WHERE installation_id = ?
		`, req.GetInstallationId()).Exec().Error
	})
	if err != nil {
		return nil, err
	}
	return &ghpb.UnlinkAppInstallationResponse{}, nil
}

func (a *GitHubApp) GetInstallationByOwner(ctx context.Context, owner string) (*tables.GitHubAppInstallation, error) {
	u, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return nil, err
	}
	installation := &tables.GitHubAppInstallation{}
	err = a.env.GetDBHandle().NewQuery(ctx, "githubapp_get_installations_by_owner").Raw(`
		SELECT * FROM "GitHubAppInstallations"
		WHERE group_id = ?
		AND owner = ?
	`, u.GetGroupID(), owner).Take(installation)
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundErrorf("no GitHub app installation for %q was found for the authenticated group", owner)
		}
		return nil, status.InternalErrorf("failed to look up GitHub app installation: %s", err)
	}
	return installation, nil
}

func (a *GitHubApp) GetLinkedGitHubRepos(ctx context.Context) (*ghpb.GetLinkedReposResponse, error) {
	u, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return nil, err
	}
	rq := a.env.GetDBHandle().NewQuery(ctx, "githubapp_get_linked_repos").Raw(`
		SELECT *
		FROM "GitRepositories"
		WHERE group_id = ?
		ORDER BY repo_url ASC
	`, u.GetGroupID())
	if err != nil {
		return nil, status.InternalErrorf("failed to query repo rows: %s", err)
	}
	res := &ghpb.GetLinkedReposResponse{}
	err = db.ScanEach(rq, func(ctx context.Context, row *tables.GitRepository) error {
		res.RepoUrls = append(res.RepoUrls, row.RepoURL)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}
func (a *GitHubApp) LinkGitHubRepo(ctx context.Context, req *ghpb.LinkRepoRequest) (*ghpb.LinkRepoResponse, error) {
	repoURL, err := gitutil.ParseGitHubRepoURL(req.GetRepoUrl())
	if err != nil {
		return nil, err
	}

	// Make sure an installation exists and that the user has access to the
	// repo.
	installation, err := a.GetInstallationByOwner(ctx, repoURL.Owner)
	if err != nil {
		return nil, err
	}
	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	// findUserRepo checks user-repo-installation authentication.
	if _, err := a.findUserRepo(ctx, tu.GithubToken, installation.InstallationID, repoURL.Repo); err != nil {
		return nil, err
	}

	if _, err := perms.AuthenticatedUser(ctx, a.env); err != nil {
		return nil, err
	}
	p, err := perms.ForAuthenticatedGroup(ctx, a.env)
	if err != nil {
		return nil, err
	}
	repo := &tables.GitRepository{
		UserID:  p.UserID,
		GroupID: p.GroupID,
		Perms:   p.Perms,
		RepoURL: repoURL.String(),
	}
	if err := a.env.GetDBHandle().NewQuery(ctx, "githubapp_create_repo").Create(repo); err != nil {
		return nil, status.InternalErrorf("failed to link repo: %s", err)
	}

	// Also clean up any associated workflows, since repo linking is meant to
	// replace workflows.
	deleteReq := &wfpb.DeleteWorkflowRequest{
		RequestContext: req.GetRequestContext(),
		RepoUrl:        req.GetRepoUrl(),
	}
	if _, err := a.env.GetWorkflowService().DeleteWorkflow(ctx, deleteReq); err != nil {
		log.CtxInfof(ctx, "Failed to delete legacy workflow for linked repo: %s", err)
	} else {
		log.CtxInfof(ctx, "Deleted legacy workflow for linked repo")
	}

	return &ghpb.LinkRepoResponse{}, nil
}
func (a *GitHubApp) UnlinkGitHubRepo(ctx context.Context, req *ghpb.UnlinkRepoRequest) (*ghpb.UnlinkRepoResponse, error) {
	norm, err := gitutil.NormalizeRepoURL(req.GetRepoUrl())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("failed to parse repo URL: %s", err)
	}
	req.RepoUrl = norm.String()
	u, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return nil, err
	}
	result := a.env.GetDBHandle().NewQuery(ctx, "githubapp_unlink_repo").Raw(`
		DELETE FROM "GitRepositories"
		WHERE group_id = ?
		AND repo_url = ?
	`, u.GetGroupID(), req.GetRepoUrl()).Exec()
	if result.Error != nil {
		return nil, status.InternalErrorf("failed to unlink repo: %s", err)
	}
	if result.RowsAffected == 0 {
		return nil, status.NotFoundError("repo not found")
	}
	return &ghpb.UnlinkRepoResponse{}, nil
}

func (a *GitHubApp) GetAccessibleGitHubRepos(ctx context.Context, req *ghpb.GetAccessibleReposRequest) (*ghpb.GetAccessibleReposResponse, error) {
	req.Query = strings.TrimSpace(req.Query)

	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	userClient, err := a.newAuthenticatedClient(ctx, tu.GithubToken)
	if err != nil {
		return nil, err
	}
	// Note: the search API (filtering "user:{installationOwner}") does not show
	// private repos and also doesn't filter only to the installations
	// accessible to the installation. So instead we fetch the first page of
	// repos accessible to the installation and search through them here.
	opts := &github.ListOptions{PerPage: githubMaxPageSize}
	result, response, err := userClient.Apps.ListUserRepos(ctx, req.GetInstallationId(), opts)
	if err := checkResponse(response, err); err != nil {
		return nil, err
	}
	urls := make([]string, 0, len(result.Repositories))
	foundExactMatch := false
	for _, r := range result.Repositories {
		repo, err := gitutil.ParseGitHubRepoURL(r.GetCloneURL())
		if err != nil {
			return nil, err
		}
		if !strings.Contains(strings.ToLower(repo.Repo), strings.ToLower(req.Query)) {
			continue
		}
		if strings.EqualFold(req.Query, repo.Repo) {
			foundExactMatch = true
		}
		urls = append(urls, repo.String())
	}
	// We only fetch the first page of results (ordered alphabetically - GitHub
	// doesn't let us order any other way). As a result, we're not searching
	// across all repo URLs. So if we didn't find an exact match, make an extra
	// request to retry the search query as an exact match.
	if req.Query != "" && !foundExactMatch {
		ir, err := a.findUserRepo(ctx, tu.GithubToken, req.GetInstallationId(), req.Query)
		if err != nil {
			log.CtxDebugf(ctx, "Could not find exact repo match: %s", err)
		} else {
			norm, err := gitutil.NormalizeRepoURL(ir.repository.GetCloneURL())
			if err != nil {
				return nil, err
			}
			urls = append([]string{norm.String()}, urls...)
		}
	}
	return &ghpb.GetAccessibleReposResponse{RepoUrls: urls}, nil
}

func (a *GitHubApp) CreateRepo(ctx context.Context, req *rppb.CreateRepoRequest) (*rppb.CreateRepoResponse, error) {
	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	if tu.GithubToken == "" {
		return nil, status.UnauthenticatedError("github account link is required")
	}

	// Pick the right client based on the request (organization or user).
	var githubClient *github.Client
	var token = tu.GithubToken
	if req.Organization == "" {
		githubClient, err = a.newAuthenticatedClient(ctx, token)
	} else {
		githubClient, token, err = a.newInstallationClient(ctx, req.InstallationId)
	}
	if err != nil {
		return nil, err
	}

	repoURL := fmt.Sprintf("https://github.com/%s/%s", req.Organization, req.Name)

	// Create a new repository on github, if requested
	if !req.SkipRepo {
		_, _, err := githubClient.Repositories.Create(ctx, req.Organization, &github.Repository{
			Name:        github.String(req.Name),
			Description: github.String(req.Description),
			Private:     github.Bool(req.Private),
		})
		if err != nil {
			return nil, err
		}
	}

	// If we have a template, copy the template contents to the new repo
	if req.Template != "" {
		tmpDirName := fmt.Sprintf("template-repo-%d-%s-*", req.InstallationId, req.Name)
		err = cloneTemplate(tu.Email, tmpDirName, token, req.Template, repoURL+".git", req.TemplateDirectory, req.DestinationDirectory, req.TemplateVariables, !req.SkipRepo)
		if err != nil {
			return nil, err
		}
	}

	// Link new repository to enable workflows, if requested
	if !req.SkipLink {
		wfs := a.env.GetWorkflowService()
		if wfs == nil {
			return nil, status.UnimplementedErrorf("no workflow service configured")
		}
		_, err = a.LinkGitHubRepo(ctx, &ghpb.LinkRepoRequest{
			RequestContext: req.RequestContext,
			RepoUrl:        repoURL,
		})
		if err != nil {
			return nil, err
		}
	}

	return &rppb.CreateRepoResponse{
		RepoUrl: repoURL,
	}, nil
}

// TODO(siggisim): consider moving template cloning to a remote action if it causes us troubles doing this on apps.
func cloneTemplate(email, tmpDirName, token, srcURL, destURL, srcDir, destDir string, templateVariables map[string]string, needsInit bool) error {
	// Make a temporary directory for the template
	templateTmpDir, err := scratchspace.MkdirTemp(tmpDirName + "-template")
	if err != nil {
		return err
	}
	defer os.RemoveAll(templateTmpDir)

	// Clone the template into the directory
	auth := &githttp.BasicAuth{
		Username: "github",
		Password: token,
	}
	_, err = git.PlainClone(templateTmpDir, false, &git.CloneOptions{
		URL:  srcURL,
		Auth: auth,
	})
	if err != nil {
		return err
	}
	// Remove existing git information so we can create a single initial commit
	err = os.RemoveAll(filepath.Join(templateTmpDir, ".git"))
	if err != nil {
		return err
	}
	// Remove any github workflows directories, because we won't have permission to create them
	err = os.RemoveAll(filepath.Join(templateTmpDir, ".github/workflows"))
	if err != nil {
		return err
	}
	// Don't allow template paths with dots or other non alphanumeric path components
	if !validPathRegex.MatchString(srcDir) {
		return status.FailedPreconditionErrorf("invalid template path: %q", srcDir)
	}
	path := filepath.Join(templateTmpDir, srcDir)
	// Intentionally using Lstat to avoid following symlinks
	if fileInfo, err := os.Lstat(path); err != nil || !fileInfo.IsDir() {
		return status.FailedPreconditionErrorf("not a valid directory: %q", srcDir)
	}
	// Replace any template variables
	if len(templateVariables) > 0 {
		replace(path, templateVariables)
	}
	// Make a temporary directory for the new repo
	repoTmpDir, err := scratchspace.MkdirTemp(tmpDirName + "-repo")
	if err != nil {
		return err
	}
	defer os.RemoveAll(repoTmpDir)

	newPath := repoTmpDir

	// If we have a destination directory that's not the repo root, update the path
	if destDir != "" {
		// Don't allow destinationPath paths with dots or other non alphanumeric path components
		if !validPathRegex.MatchString(destDir) {
			return status.FailedPreconditionErrorf("invalid destination path: %q", destDir)
		}
		if err := os.RemoveAll(newPath); err != nil {
			return err
		}
		newPath = filepath.Join(repoTmpDir, destDir)
		if err := os.MkdirAll(newPath, os.ModePerm); err != nil {
			return err
		}
	}
	// Initialize or clone the destination repo
	gitRepo, err := initOrClone(needsInit, newPath, destURL, auth)
	if err != nil {
		return err
	}
	fileInfo, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	// Move template files into the destination repo
	for _, file := range fileInfo {
		old := filepath.Join(path, file.Name())
		new := filepath.Join(newPath, file.Name())
		if err := os.RemoveAll(new); err != nil {
			return err
		}
		if err := os.Rename(old, new); err != nil {
			return err
		}
	}
	gitWorkTree, err := gitRepo.Worktree()
	if err != nil {
		return err
	}
	_, err = gitWorkTree.Add(".")
	if err != nil {
		return err
	}

	commitMessage := "Initial commit"
	if !needsInit {
		commitMessage = "Update"
	}

	_, err = gitWorkTree.Commit(commitMessage, &git.CommitOptions{All: true, Author: &gitobject.Signature{
		Email: email,
		When:  time.Now(),
	}})
	if err != nil {
		return err
	}
	return gitRepo.Push(&git.PushOptions{
		RemoteName: git.DefaultRemoteName,
		Auth:       auth,
		Force:      true,
	})
}

func initOrClone(init bool, dir, url string, auth transport.AuthMethod) (*git.Repository, error) {
	if !init {
		return git.PlainClone(dir, false, &git.CloneOptions{
			URL:  url,
			Auth: auth,
		})
	}
	gitRepo, err := git.PlainInit(dir, false)
	if err != nil {
		return nil, err
	}
	if err := gitRepo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.Main)); err != nil {
		return nil, err
	}
	_, err = gitRepo.CreateRemote(&config.RemoteConfig{
		Name: git.DefaultRemoteName,
		URLs: []string{url},
	})
	return gitRepo, err
}

// Walks the given directory and performs a find and replace in all file contents.
// All instance of the keys in the replacements map are replaced by the values in the map.
func replace(dir string, replacements map[string]string) error {
	return filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		contents, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		for k, v := range replacements {
			contents = bytes.Replace(contents, []byte(k), []byte(v), -1)
		}
		return ioutil.WriteFile(path, contents, os.ModePerm)
	})
}

type installationRepository struct {
	installation *github.Installation
	repository   *github.Repository
}

// findUserRepo finds a repo within an installation, checking the user's access
// to the repo. It attempts to work around the fact that "apps.ListUserRepos"
// doesn't have any filtering options.
func (a *GitHubApp) findUserRepo(ctx context.Context, userToken string, installationID int64, repo string) (*installationRepository, error) {
	if err := a.authorizeUserInstallationAccess(ctx, userToken, installationID); err != nil {
		return nil, err
	}
	installation, err := a.getInstallation(ctx, installationID)
	if err != nil {
		return nil, err
	}
	owner := installation.GetAccount().GetLogin()
	// Fetch repository so that we know the canonical repo name (the input
	// `repo` parameter might be equal ignoring case, but not exactly equal).
	installationClient, _, err := a.newInstallationClient(ctx, installationID)
	if err != nil {
		return nil, err
	}
	repository, response, err := installationClient.Repositories.Get(ctx, owner, repo)
	if err := checkResponse(response, err); err != nil {
		return nil, err
	}
	// Fetch the associated installation to confirm whether the repository
	// is actually installed.
	appClient, err := a.newAppClient(ctx)
	if err != nil {
		return nil, err
	}
	_, _, err = appClient.Apps.FindRepositoryInstallation(ctx, owner, repo)
	if err != nil {
		return nil, err
	}
	return &installationRepository{
		installation: installation,
		repository:   repository,
	}, nil

}

func (a *GitHubApp) getInstallation(ctx context.Context, id int64) (*github.Installation, error) {
	client, err := a.newAppClient(ctx)
	if err != nil {
		return nil, status.WrapError(err, "failed to get installation")
	}
	inst, res, err := client.Apps.GetInstallation(ctx, id)
	if err := checkResponse(res, err); err != nil {
		return nil, status.WrapError(err, "failed to get installation")
	}
	return inst, nil
}

func (a *GitHubApp) createInstallationToken(ctx context.Context, installationID int64) (*github.InstallationToken, error) {
	client, err := a.newAppClient(ctx)
	if err != nil {
		return nil, err
	}
	t, res, err := client.Apps.CreateInstallationToken(ctx, installationID, nil)
	if err := checkResponse(res, err); err != nil {
		return nil, status.UnauthenticatedErrorf("failed to create installation token: %s", status.Message(err))
	}
	return t, nil
}

func (a *GitHubApp) authorizeUserInstallationAccess(ctx context.Context, userToken string, installationID int64) error {
	const installTimeout = 10 * time.Second
	ctx, cancel := context.WithTimeout(ctx, installTimeout)
	defer cancel()
	client, err := a.newAuthenticatedClient(ctx, userToken)
	if err != nil {
		return err
	}
	_, res, err := client.Apps.ListUserRepos(ctx, installationID, &github.ListOptions{PerPage: 1})
	if err := checkResponse(res, err); err != nil {
		return status.WrapError(err, "failed to authorize user installation access")
	}
	return nil
}

func (a *GitHubApp) OAuthHandler() http.Handler {
	return a.oauth
}

func (a *GitHubApp) handleInstall(ctx context.Context, groupID, setupAction string, installationID int64) (string, error) {
	// GitHub might take a second or two to actually create the installation.
	// We need to wait for the installation to be created since we store the
	// owner field in the DB.
	installation, err := a.waitForInstallation(ctx, installationID)
	if err != nil {
		return "", err
	}

	// If group ID is empty, the user initiated via the install via GitHub, not
	// the UI. Redirect them to a page that shows the group picker. The UI can
	// then make an RPC to complete the installation by linking it to the
	// desired org.
	if groupID == "" {
		redirect := fmt.Sprintf(
			"/settings/org/github/complete-installation?installation_id=%d&installation_owner=%s",
			installationID, installation.GetAccount().GetLogin())
		return redirect, nil
	}
	if err := a.linkInstallation(ctx, installation, groupID); err != nil {
		return "", err
	}
	return "", nil
}

// Waits for GitHub to create the installation. This is needed since GitHub
// doesn't atomically create the installation, but it should be created very
// shortly after the install (seconds).
func (a *GitHubApp) waitForInstallation(ctx context.Context, installationID int64) (*github.Installation, error) {
	const timeout = 15 * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	r := retry.DefaultWithContext(ctx)
	var lastErr error
	for r.Next() {
		in, err := a.getInstallation(ctx, installationID)
		if err != nil {
			lastErr = err
			continue
		}
		return in, nil
	}
	return nil, status.DeadlineExceededErrorf("timed out waiting for installation %d to exist: %s", installationID, lastErr)
}

// newAppClient returns a GitHub client authenticated as the app.
func (a *GitHubApp) newAppClient(ctx context.Context) (*github.Client, error) {
	// Create and sign JWT
	t := jwt.New(jwt.GetSigningMethod("RS256"))
	t.Claims = &jwt.StandardClaims{
		Issuer:    *appID,
		IssuedAt:  time.Now().Add(-1 * time.Minute).Unix(),
		ExpiresAt: time.Now().Add(5 * time.Minute).Unix(),
	}
	jwtStr, err := t.SignedString(a.privateKey)
	if err != nil {
		log.Errorf("Failed to sign JWT: %s", err)
		return nil, status.InternalErrorf("failed to sign JWT")
	}
	return a.newAuthenticatedClient(ctx, jwtStr)
}

func (a *GitHubApp) newInstallationClient(ctx context.Context, installationID int64) (*github.Client, string, error) {
	token, err := a.createInstallationToken(ctx, installationID)
	if err != nil {
		return nil, "", err
	}
	client, err := a.newAuthenticatedClient(ctx, token.GetToken())
	return client, token.GetToken(), err
}

// newAuthenticatedClient returns a GitHub client authenticated with the given
// access token.
func (a *GitHubApp) newAuthenticatedClient(ctx context.Context, accessToken string) (*github.Client, error) {
	if accessToken == "" {
		return nil, status.UnauthenticatedError("missing user access token")
	}
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})
	tc := oauth2.NewClient(ctx, ts)

	if gh_oauth.IsEnterpriseConfigured() {
		host := fmt.Sprintf("https://%s/", gh_oauth.GithubHost())
		return github.NewEnterpriseClient(host, host, tc)
	}

	return github.NewClient(tc), nil
}

// decodePrivateKey decodes a PEM-format RSA private key.
func decodePrivateKey(contents string) (*rsa.PrivateKey, error) {
	contents = strings.TrimSpace(contents)
	block, rest := pem.Decode([]byte(contents))
	if block == nil {
		return nil, status.FailedPreconditionError("failed to decode PEM block from private key")
	}
	if len(rest) > 0 {
		return nil, status.FailedPreconditionErrorf("PEM block is followed by extraneous data (length %d)", len(rest))
	}
	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

func setCookie(w http.ResponseWriter, name, value string) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Expires:  time.Now().Add(time.Hour),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Path:     "/",
	})
}

func getCookie(r *http.Request, name string) string {
	if c, err := r.Cookie(name); err == nil {
		return c.Value
	}
	return ""
}

// checkResponse is a convenience function for checking both the HTTP client
// error returned from the go-github library as well as the HTTP response code
// returned by GitHub.
func checkResponse(res *github.Response, err error) error {
	if err != nil {
		return status.UnknownErrorf("GitHub API request failed: %s", err)
	}
	if res.StatusCode >= 300 {
		return status.UnknownErrorf("GitHub API request failed: unexpected HTTP status %s", res.Status)
	}
	return nil
}

func (a *GitHubApp) getGithubClient(ctx context.Context) (*github.Client, error) {
	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	if tu.GithubToken == "" {
		return nil, status.UnauthenticatedError("github account link is required")
	}
	return a.newAuthenticatedClient(ctx, tu.GithubToken)
}

func (a *GitHubApp) GetGithubUserInstallations(ctx context.Context, req *ghpb.GetGithubUserInstallationsRequest) (*ghpb.GetGithubUserInstallationsResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	installations, _, err := client.Apps.ListUserInstallations(ctx, nil)
	if err != nil {
		return nil, err
	}

	res := &ghpb.GetGithubUserInstallationsResponse{
		Installations: []*ghpb.UserInstallation{},
	}

	for _, i := range installations {
		installation := &ghpb.UserInstallation{
			Id:         i.GetID(),
			Login:      i.Account.GetLogin(),
			Url:        i.GetHTMLURL(),
			TargetType: i.GetTargetType(),
			Permissions: &ghpb.UserInstallationPermissions{
				Administration:  i.GetPermissions().GetAdministration(),
				RepositoryHooks: i.GetPermissions().GetRepositoryHooks(),
				PullRequests:    i.GetPermissions().GetPullRequests(),
			},
		}
		res.Installations = append(res.Installations, installation)
	}

	return res, nil
}

func (a *GitHubApp) GetGithubUser(ctx context.Context, req *ghpb.GetGithubUserRequest) (*ghpb.GetGithubUserResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	user, _, err := client.Users.Get(ctx, "")
	if err != nil {
		return nil, err
	}

	return &ghpb.GetGithubUserResponse{
		Name:      user.GetName(),
		Login:     user.GetLogin(),
		AvatarUrl: user.GetAvatarURL(),
	}, nil
}

func (a *GitHubApp) GetGithubRepo(ctx context.Context, req *ghpb.GetGithubRepoRequest) (*ghpb.GetGithubRepoResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	repo, _, err := client.Repositories.Get(ctx, req.Owner, req.Repo)
	if err != nil {
		return nil, err
	}

	return &ghpb.GetGithubRepoResponse{
		DefaultBranch: repo.GetDefaultBranch(),
		Permissions: &ghpb.RepoPermissions{
			Push: repo.GetPermissions()["push"],
		},
	}, nil
}

func (a *GitHubApp) GetGithubContent(ctx context.Context, req *ghpb.GetGithubContentRequest) (*ghpb.GetGithubContentResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	contents, _, err := client.Repositories.DownloadContents(ctx, req.Owner, req.Repo, req.Path, &github.RepositoryContentGetOptions{Ref: req.Ref})
	if err != nil {
		return nil, err
	}
	defer contents.Close()

	if req.ExistenceOnly {
		return &ghpb.GetGithubContentResponse{}, nil
	}

	contentBytes, err := io.ReadAll(contents)
	if err != nil {
		return nil, err
	}

	return &ghpb.GetGithubContentResponse{
		Content: contentBytes,
	}, nil
}

func (a *GitHubApp) GetGithubTree(ctx context.Context, req *ghpb.GetGithubTreeRequest) (*ghpb.GetGithubTreeResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	tree, _, err := client.Git.GetTree(ctx, req.Owner, req.Repo, req.Ref, false)
	if err != nil {
		return nil, err
	}

	res := &ghpb.GetGithubTreeResponse{
		Sha: tree.GetSHA(),
	}
	for _, entry := range tree.Entries {
		res.Nodes = append(res.Nodes, githubToProtoTree(entry))
	}
	return res, nil
}

func githubToProtoTree(entry *github.TreeEntry) *ghpb.TreeNode {
	node := &ghpb.TreeNode{
		Path:    entry.GetPath(),
		Sha:     entry.GetSHA(),
		Type:    entry.GetType(),
		Mode:    entry.GetMode(),
		Size:    int64(entry.GetSize()),
		Content: []byte(entry.GetContent()),
	}
	return node
}

func protoToGithubTree(node *ghpb.TreeNode) *github.TreeEntry {
	if node.Content != nil {
		content := string(node.Content)
		return &github.TreeEntry{
			Path:    &node.Path,
			Mode:    &node.Mode,
			Content: &content,
		}
	}
	return &github.TreeEntry{
		Path: &node.Path,
		SHA:  &node.Sha,
		Mode: &node.Mode,
	}
}

func (a *GitHubApp) CreateGithubTree(ctx context.Context, req *ghpb.CreateGithubTreeRequest) (*ghpb.CreateGithubTreeResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}
	entries := []*github.TreeEntry{}
	for _, node := range req.Nodes {
		entries = append(entries, protoToGithubTree(node))
	}
	tree, _, err := client.Git.CreateTree(ctx, req.Owner, req.Repo, req.BaseTree, entries)
	if err != nil {
		return nil, err
	}
	return &ghpb.CreateGithubTreeResponse{
		Sha: tree.GetSHA(),
	}, nil
}

func (a *GitHubApp) GetGithubBlob(ctx context.Context, req *ghpb.GetGithubBlobRequest) (*ghpb.GetGithubBlobResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	blob, _, err := client.Git.GetBlobRaw(ctx, req.Owner, req.Repo, req.Sha)
	if err != nil {
		return nil, err
	}

	return &ghpb.GetGithubBlobResponse{
		Content: blob,
	}, nil
}

func (a *GitHubApp) CreateGithubBlob(ctx context.Context, req *ghpb.CreateGithubBlobRequest) (*ghpb.CreateGithubBlobResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	content := string(req.Content)
	blob, _, err := client.Git.CreateBlob(ctx, req.Owner, req.Repo, &github.Blob{
		Content: &content,
	})
	if err != nil {
		return nil, err
	}

	return &ghpb.CreateGithubBlobResponse{
		Sha: blob.GetSHA(),
	}, nil
}

func (a *GitHubApp) CreateGithubPull(ctx context.Context, req *ghpb.CreateGithubPullRequest) (*ghpb.CreateGithubPullResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	pr, _, err := client.PullRequests.Create(ctx, req.Owner, req.Repo, &github.NewPullRequest{
		Head:  &req.Head,
		Base:  &req.Base,
		Title: &req.Title,
		Body:  &req.Body,
		Draft: &req.Draft,
	})
	if err != nil {
		return nil, err
	}

	return &ghpb.CreateGithubPullResponse{
		Url:        pr.GetHTMLURL(),
		PullNumber: int64(pr.GetNumber()),
		Ref:        pr.GetHead().GetRef(),
	}, nil
}

func (a *GitHubApp) MergeGithubPull(ctx context.Context, req *ghpb.MergeGithubPullRequest) (*ghpb.MergeGithubPullResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	_, _, err = client.PullRequests.Merge(ctx, req.Owner, req.Repo, int(req.PullNumber), "", &github.PullRequestOptions{MergeMethod: "squash"})
	if err != nil {
		return nil, err
	}

	return &ghpb.MergeGithubPullResponse{}, nil
}

func (a *GitHubApp) GetGithubCompare(ctx context.Context, req *ghpb.GetGithubCompareRequest) (*ghpb.GetGithubCompareResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	comparison, _, err := client.Repositories.CompareCommits(ctx, req.Owner, req.Repo, req.Base, req.Head, nil)
	if err != nil {
		return nil, err
	}

	res := &ghpb.GetGithubCompareResponse{
		AheadBy: int64(comparison.GetAheadBy()),
		Files:   []*ghpb.File{},
		Commits: []*ghpb.Commit{},
	}

	for _, c := range comparison.Commits {
		res.Commits = append(res.Commits, &ghpb.Commit{
			Sha: c.GetSHA(),
		})
	}

	for _, f := range comparison.Files {
		res.Files = append(res.Files, &ghpb.File{
			Name: f.GetFilename(),
			Sha:  f.GetSHA(),
		})
	}

	return res, nil
}

func (a *GitHubApp) GetGithubForks(ctx context.Context, req *ghpb.GetGithubForksRequest) (*ghpb.GetGithubForksResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	forks, _, err := client.Repositories.ListForks(ctx, req.Owner, req.Repo, nil)
	if err != nil {
		return nil, err
	}

	res := &ghpb.GetGithubForksResponse{
		Forks: []*ghpb.Fork{},
	}

	for _, f := range forks {
		res.Forks = append(res.Forks, &ghpb.Fork{
			Owner: f.GetOwner().GetLogin(),
		})
	}

	return res, nil
}

func (a *GitHubApp) CreateGithubFork(ctx context.Context, req *ghpb.CreateGithubForkRequest) (*ghpb.CreateGithubForkResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	_, _, err = client.Repositories.CreateFork(ctx, req.Owner, req.Repo, nil)
	if _, ok := err.(*github.AcceptedError); !ok && err != nil {
		return nil, err
	}

	return &ghpb.CreateGithubForkResponse{}, nil
}

func (a *GitHubApp) GetGithubCommits(ctx context.Context, req *ghpb.GetGithubCommitsRequest) (*ghpb.GetGithubCommitsResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	commits, _, err := client.Repositories.ListCommits(ctx, req.Owner, req.Repo, &github.CommitsListOptions{
		SHA: req.Sha,
		ListOptions: github.ListOptions{
			PerPage: int(req.PerPage),
		},
	})
	if err != nil {
		return nil, err
	}

	res := &ghpb.GetGithubCommitsResponse{
		Commits: []*ghpb.Commit{},
	}

	for _, c := range commits {
		commit := &ghpb.Commit{
			Sha:     c.GetSHA(),
			TreeSha: c.GetCommit().GetTree().GetSHA(),
		}
		res.Commits = append(res.Commits, commit)
	}

	return res, nil
}

func (a *GitHubApp) CreateGithubCommit(ctx context.Context, req *ghpb.CreateGithubCommitRequest) (*ghpb.CreateGithubCommitResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}
	commit := &github.Commit{
		Message: &req.Message,
		Tree:    &github.Tree{SHA: &req.Tree},
		Parents: []*github.Commit{},
	}
	for _, p := range req.Parents {
		commit.Parents = append(commit.Parents, &github.Commit{SHA: &p})
	}
	c, _, err := client.Git.CreateCommit(ctx, req.Owner, req.Repo, commit)
	if err != nil {
		return nil, err
	}
	return &ghpb.CreateGithubCommitResponse{
		Sha: c.GetSHA(),
	}, nil
}

func (a *GitHubApp) UpdateGithubRef(ctx context.Context, req *ghpb.UpdateGithubRefRequest) (*ghpb.UpdateGithubRefResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	_, _, err = client.Git.UpdateRef(ctx, req.Owner, req.Repo, &github.Reference{Ref: &req.Head, Object: &github.GitObject{SHA: &req.Sha}}, req.Force)
	if err != nil {
		return nil, err
	}

	return &ghpb.UpdateGithubRefResponse{}, nil
}

func (a *GitHubApp) CreateGithubRef(ctx context.Context, req *ghpb.CreateGithubRefRequest) (*ghpb.CreateGithubRefResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	_, _, err = client.Git.CreateRef(ctx, req.Owner, req.Repo, &github.Reference{Ref: &req.Ref, Object: &github.GitObject{SHA: &req.Sha}})
	if err != nil {
		return nil, err
	}

	return &ghpb.CreateGithubRefResponse{}, nil
}

func (a *GitHubApp) GetGithubPullRequest(ctx context.Context, req *ghpb.GetGithubPullRequestRequest) (*ghpb.GetGithubPullRequestResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}
	usernameForFetch := req.GetUser()
	if usernameForFetch == "" {
		usernameForFetch = "@me"
	}
	incoming, outgoing, user, err := a.getIncomingAndOutgoingPRs(ctx, usernameForFetch, client)
	if err != nil {
		return nil, err
	}
	allIssues := append(outgoing.Issues, incoming.Issues...)

	usernameForAttentionSet := req.GetUser()
	if usernameForAttentionSet == "" {
		usernameForAttentionSet = user.GetLogin()
	}
	prs, err := a.populatePRMetadata(ctx, client, allIssues, usernameForAttentionSet)
	if err != nil {
		return nil, err
	}
	resp := &ghpb.GetGithubPullRequestResponse{
		Outgoing: []*ghpb.PullRequest{},
		Incoming: []*ghpb.PullRequest{},
	}
	for _, i := range outgoing.Issues {
		pr := prs[i.GetNodeID()]
		if len(pr.Reviews) == 0 {
			continue
		}
		resp.Outgoing = append(resp.Outgoing, pr)
	}
	for _, i := range incoming.Issues {
		resp.Incoming = append(resp.Incoming, prs[i.GetNodeID()])
	}
	return resp, nil
}

func (a *GitHubApp) GetGithubPullRequestDetails(ctx context.Context, req *ghpb.GetGithubPullRequestDetailsRequest) (*ghpb.GetGithubPullRequestDetailsResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}
	eg, gCtx := errgroup.WithContext(ctx)

	var issue *github.Issue

	eg.Go(func() error {
		r, err := a.cachedIssue(gCtx, client, req.Owner, req.Repo, int(req.Pull))
		if err != nil {
			return err
		}
		issue = r
		return nil
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	eg, gCtx = errgroup.WithContext(ctx)

	var pr *github.PullRequest
	var events []*github.Timeline
	var files []*github.CommitFile

	eg.Go(func() error {
		p, err := a.cachedPullRequests(gCtx, client, req.Owner, req.Repo, int(req.Pull), issue.GetUpdatedAt())
		if err != nil {
			return err
		}
		pr = p
		return nil
	})

	eg.Go(func() error {
		ev, err := a.cachedTimeline(gCtx, client, req.Owner, req.Repo, int(req.Pull))
		if err != nil {
			return err
		}
		events = *ev
		return nil
	})

	eg.Go(func() error {
		f, err := a.cachedFiles(gCtx, client, req.Owner, req.Repo, int(req.Pull))
		if err != nil {
			return err
		}
		files = *f
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	eg, gCtx = errgroup.WithContext(ctx)

	var statuses *github.CombinedStatus
	eg.Go(func() error {
		s, err := a.cachedStatuses(gCtx, client, req.Owner, req.Repo, pr.Head.GetSHA())
		if err != nil {
			return err
		}
		statuses = s
		return nil
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	notExplicitlyRemoved := map[string]struct{}{}
	approved := map[string]struct{}{}

	for _, e := range events {
		switch eventType := e.GetEvent(); eventType {
		case "review_requested":
			notExplicitlyRemoved[e.GetReviewer().GetLogin()] = struct{}{}
		case "review_request_removed":
			delete(notExplicitlyRemoved, e.GetReviewer().GetLogin())
		case "reviewed":
			if e.GetState() == "approved" {
				approved[e.GetUser().GetLogin()] = struct{}{}
			} else if e.GetState() == "changes_requested" {
				delete(approved, e.GetUser().GetLogin())
			}
		}
	}

	activeReviewers := map[string]struct{}{}

	reviewers := make([]*ghpb.Reviewer, 0)
	for _, r := range pr.RequestedReviewers {
		activeReviewers[r.GetLogin()] = struct{}{}
	}

	for r := range notExplicitlyRemoved {
		reviewer := &ghpb.Reviewer{}
		reviewer.Login = r
		if _, ok := activeReviewers[r]; ok {
			reviewer.Attention = true
		}
		if _, ok := approved[r]; ok {
			reviewer.Approved = true
		}
		reviewers = append(reviewers, reviewer)
	}

	fileSummaries := make([]*ghpb.FileSummary, 0)
	for _, f := range files {
		summary := &ghpb.FileSummary{}
		summary.Name = f.GetFilename()
		summary.Additions = int64(f.GetAdditions())
		summary.Deletions = int64(f.GetDeletions())
		summary.Patch = f.GetPatch()
		// TODO(jdhollen): compute comment count.
		fileSummaries = append(fileSummaries, summary)
	}

	actionStatuses := make([]*ghpb.ActionStatus, 0)
	for _, s := range statuses.Statuses {
		status := &ghpb.ActionStatus{}
		status.Name = s.GetContext()
		status.Status = s.GetState()
		status.Url = s.GetTargetURL()
		actionStatuses = append(actionStatuses, status)
	}

	resp := &ghpb.GetGithubPullRequestDetailsResponse{
		Owner:          req.Owner,
		Repo:           req.Repo,
		Pull:           req.Pull,
		Title:          pr.GetTitle(),
		Body:           pr.GetBody(),
		Author:         pr.GetUser().GetLogin(),
		CreatedAtUsec:  pr.GetCreatedAt().UnixMicro(),
		UpdatedAtUsec:  pr.GetUpdatedAt().UnixMicro(),
		Branch:         pr.GetHead().GetLabel(),
		Reviewers:      reviewers,
		Files:          fileSummaries,
		ActionStatuses: actionStatuses,
		Mergeable:      pr.GetMergeableState() == "clean",
		Submitted:      pr.GetMerged(),
		GithubUrl:      pr.GetHTMLURL(),
	}

	return resp, nil
}

func (a *GitHubApp) getIncomingAndOutgoingPRs(ctx context.Context, username string, client *github.Client) (*github.IssuesSearchResult, *github.IssuesSearchResult, *github.User, error) {
	var incoming *github.IssuesSearchResult
	var outgoing *github.IssuesSearchResult
	var user *github.User
	eg, gCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		r, err := a.cachedSearch(gCtx, client, fmt.Sprintf("is:open is:pr user-review-requested:%s archived:false draft:false", username))
		if err != nil {
			return err
		}
		incoming = r
		return nil
	})
	eg.Go(func() error {
		r, err := a.cachedSearch(gCtx, client, fmt.Sprintf("is:open is:pr author:%s archived:false draft:false", username))
		if err != nil {
			return err
		}
		outgoing = r
		return nil
	})
	eg.Go(func() error {
		u, err := a.cachedUser(gCtx, client)
		if err != nil {
			return err
		}
		user = u
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, nil, nil, err
	}
	return incoming, outgoing, user, nil
}

func (a *GitHubApp) populatePRMetadata(ctx context.Context, client *github.Client, prIssues []*github.Issue, requestedUser string) (map[string]*ghpb.PullRequest, error) {
	prsMu := &sync.Mutex{}
	prs := make(map[string]*ghpb.PullRequest, len(prIssues))
	eg, gCtx := errgroup.WithContext(ctx)
	for _, i := range prIssues {
		i := i
		prsMu.Lock()
		prs[i.GetNodeID()] = issueToPullRequestProto(i)
		prsMu.Unlock()
		urlParts := strings.Split(*i.URL, "/")
		if len(urlParts) < 6 {
			return nil, status.FailedPreconditionErrorf("invalid github url: %q", *i.URL)
		}
		eg.Go(func() error {
			pr, err := a.cachedPullRequests(gCtx, client, urlParts[4], urlParts[5], i.GetNumber(), i.GetUpdatedAt())
			if err != nil {
				return err
			}
			prsMu.Lock()
			p := prs[i.GetNodeID()]
			p.Additions = int64(pr.GetAdditions())
			p.Deletions = int64(pr.GetDeletions())
			p.ChangedFiles = int64(pr.GetChangedFiles())
			for _, r := range pr.RequestedReviewers {
				review, ok := prs[i.GetNodeID()].Reviews[r.GetLogin()]
				if !ok {
					review = &ghpb.Review{}
					prs[i.GetNodeID()].Reviews[r.GetLogin()] = review
				}
				review.Requested = true
				if r.GetLogin() == requestedUser {
					review.IsCurrentUser = true
				}
			}
			prsMu.Unlock()
			return nil
		})
		eg.Go(func() error {
			reviews, err := a.cachedReviews(gCtx, client, urlParts[4], urlParts[5], i.GetNumber(), i.GetUpdatedAt())
			if err != nil {
				return err
			}
			for _, r := range *reviews {
				prsMu.Lock()
				review, ok := prs[i.GetNodeID()].Reviews[r.GetUser().GetLogin()]
				if !ok {
					review = &ghpb.Review{}
					prs[i.GetNodeID()].Reviews[r.GetUser().GetLogin()] = review
				}
				review.Status = strings.ToLower(r.GetState())
				review.SubmittedAtUsec = r.GetSubmittedAt().UnixMicro()
				if r.GetUser().GetLogin() == requestedUser {
					review.IsCurrentUser = true
				}
				prsMu.Unlock()
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return prs, nil
}

func (a *GitHubApp) cachedUser(ctx context.Context, client *github.Client) (*github.User, error) {
	key := "user"
	pr, err := a.cached(ctx, key, &github.User{}, time.Hour*24, func() (any, any, error) {
		return client.Users.Get(ctx, "")
	})
	if err != nil {
		return nil, err
	}
	return pr.(*github.User), nil
}

func (a *GitHubApp) cachedSearch(ctx context.Context, client *github.Client, query string) (*github.IssuesSearchResult, error) {
	key := fmt.Sprintf("search/%s", query)
	pr, err := a.cached(ctx, key, &github.IssuesSearchResult{}, time.Second*5, func() (any, any, error) {
		return client.Search.Issues(ctx, query, &github.SearchOptions{ListOptions: github.ListOptions{PerPage: 100}})
	})
	if err != nil {
		return nil, err
	}
	return pr.(*github.IssuesSearchResult), nil
}

func (a *GitHubApp) cachedIssue(ctx context.Context, client *github.Client, owner string, repo string, issue int) (*github.Issue, error) {
	key := fmt.Sprintf("issue/%s/%s/%d", owner, repo, issue)
	value, err := a.cached(ctx, key, &github.Issue{}, time.Second*30, func() (any, any, error) {
		return client.Issues.Get(ctx, owner, repo, issue)
	})
	if err != nil {
		return nil, err
	}
	return value.(*github.Issue), nil
}

func (a *GitHubApp) cachedStatuses(ctx context.Context, client *github.Client, owner string, repo string, ref string) (*github.CombinedStatus, error) {
	key := fmt.Sprintf("statuses/%s/%s/%s", owner, repo, ref)
	value, err := a.cached(ctx, key, &github.CombinedStatus{}, time.Second*30, func() (any, any, error) {
		return client.Repositories.GetCombinedStatus(ctx, owner, repo, ref, &github.ListOptions{PerPage: 100})
	})
	if err != nil {
		return nil, err
	}
	return value.(*github.CombinedStatus), nil
}

func (a *GitHubApp) cachedTimeline(ctx context.Context, client *github.Client, owner, repo string, number int) (*[]*github.Timeline, error) {
	key := fmt.Sprintf("timeline/%s/%s/%d", owner, repo, number)
	pr, err := a.cached(ctx, key, &[]*github.Timeline{}, time.Second*30, func() (any, any, error) {
		rev, res, err := client.Issues.ListIssueTimeline(ctx, owner, repo, number, &github.ListOptions{PerPage: 100})
		return &rev, res, err
	})
	if err != nil {
		return nil, err
	}
	return pr.(*[]*github.Timeline), nil
}

func (a *GitHubApp) cachedFiles(ctx context.Context, client *github.Client, owner, repo string, number int) (*[]*github.CommitFile, error) {
	key := fmt.Sprintf("files/%s/%s/%d", owner, repo, number)
	pr, err := a.cached(ctx, key, &[]*github.CommitFile{}, time.Second*30, func() (any, any, error) {
		rev, res, err := client.PullRequests.ListFiles(ctx, owner, repo, number, &github.ListOptions{PerPage: 100})
		return &rev, res, err
	})
	if err != nil {
		return nil, err
	}
	return pr.(*[]*github.CommitFile), nil
}

func (a *GitHubApp) cachedPullRequests(ctx context.Context, client *github.Client, owner, repo string, number int, updatedAt time.Time) (*github.PullRequest, error) {
	key := fmt.Sprintf("pr/%s/%s/%d/%d", owner, repo, number, updatedAt.Unix())

	pr, err := a.cached(ctx, key, &github.PullRequest{}, time.Hour*24, func() (any, any, error) {
		return client.PullRequests.Get(ctx, owner, repo, number)
	})
	if err != nil {
		return nil, err
	}
	return pr.(*github.PullRequest), nil
}

func (a *GitHubApp) cachedReviews(ctx context.Context, client *github.Client, owner, repo string, number int, updatedAt time.Time) (*[]*github.PullRequestReview, error) {
	key := fmt.Sprintf("pr-reviews/%s/%s/%d/%d", owner, repo, number, updatedAt.Unix())
	pr, err := a.cached(ctx, key, &[]*github.PullRequestReview{}, time.Hour*24, func() (any, any, error) {
		rev, res, err := client.PullRequests.ListReviews(ctx, owner, repo, number, &github.ListOptions{PerPage: 100})
		return &rev, res, err
	})
	if err != nil {
		return nil, err
	}
	return pr.(*[]*github.PullRequestReview), nil
}

type fetchFunction func() (any, any, error)

func (a *GitHubApp) cached(ctx context.Context, key string, v any, exp time.Duration, fetch fetchFunction) (any, error) {
	if a.env.GetDefaultRedisClient() == nil {
		pr, _, err := fetch()
		return pr, err
	}
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	key = fmt.Sprintf("githubapp/0/%s/%s", u.GetUserID(), key)
	if cachedVal, err := a.env.GetDefaultRedisClient().Get(ctx, key).Result(); err == nil {
		if err := json.Unmarshal([]byte(cachedVal), &v); err == nil {
			log.Debugf("got cached github result for key: %s", key)
			return v, nil
		} else {
			log.Errorf("error unmarshalling cached github redis result for key %s: %s", key, err)
		}
	}
	log.Debugf("making github request for key: %s", key)
	pr, _, err := fetch()
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(pr)
	if err != nil {
		return nil, err
	}
	a.env.GetDefaultRedisClient().Set(ctx, key, string(b), exp)
	return pr, nil
}

func issueToPullRequestProto(i *github.Issue) *ghpb.PullRequest {
	return &ghpb.PullRequest{
		Number:        uint64(i.GetNumber()),
		Title:         i.GetTitle(),
		Body:          i.GetBody(),
		Author:        i.GetUser().GetLogin(),
		Url:           i.GetHTMLURL(),
		UpdatedAtUsec: i.GetUpdatedAt().UnixMicro(),
		Reviews:       map[string]*ghpb.Review{},
	}
}

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
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/scratchspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/golang-jwt/jwt"
	"github.com/google/go-github/v59/github"
	"github.com/shurcooL/githubv4"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"

	gh_webhooks "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github"
	gitpb "github.com/buildbuddy-io/buildbuddy/proto/git"
	ghpb "github.com/buildbuddy-io/buildbuddy/proto/github"
	csinpb "github.com/buildbuddy-io/buildbuddy/proto/index"
	rppb "github.com/buildbuddy-io/buildbuddy/proto/repo"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	gh_oauth "github.com/buildbuddy-io/buildbuddy/server/backends/github"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	gitobject "github.com/go-git/go-git/v5/plumbing/object"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
)

var (
	// TODO(Maggie): Once https://github.com/buildbuddy-io/buildbuddy-internal/issues/4672 is fixed,
	// use `flag.Struct` to avoid having to duplicate all the config flags and share validation logic.
	readWriteAppEnabled       = flag.Bool("github.app.enabled", false, "Whether to enable the read-write BuildBuddy GitHub app server.")
	readWriteAppClientID      = flag.String("github.app.client_id", "", "GitHub app OAuth client ID.")
	readWriteAppClientSecret  = flag.String("github.app.client_secret", "", "GitHub app OAuth client secret.", flag.Secret)
	readWriteAppID            = flag.String("github.app.id", "", "GitHub app ID.")
	readWriteAppPublicLink    = flag.String("github.app.public_link", "", "GitHub app installation URL.")
	readWriteAppPrivateKey    = flag.String("github.app.private_key", "", "GitHub app private key.", flag.Secret)
	readWriteAppWebhookSecret = flag.String("github.app.webhook_secret", "", "GitHub app webhook secret used to verify that webhook payload contents were sent by GitHub.", flag.Secret)
	enableReviewMutates       = flag.Bool("github.app.review_mutates_enabled", false, "Perform mutations of PRs via the GitHub API.")

	readOnlyAppEnabled       = flag.Bool("github.read_only_app.enabled", false, "Whether to enable the read-only BuildBuddy GitHub app server.")
	readOnlyAppClientID      = flag.String("github.read_only_app.client_id", "", "Read-only GitHub app OAuth client ID.")
	readOnlyAppClientSecret  = flag.String("github.read_only_app.client_secret", "", "Read-only GitHub app OAuth client secret.", flag.Secret)
	readOnlyAppID            = flag.String("github.read_only_app.id", "", "Read-only GitHub app ID.")
	readOnlyAppPublicLink    = flag.String("github.read_only_app.public_link", "", "Read-only GitHub app installation URL.")
	readOnlyAppPrivateKey    = flag.String("github.read_only_app.private_key", "", "Read-only GitHub app private key.", flag.Secret)
	readOnlyAppWebhookSecret = flag.String("github.read_only_app.webhook_secret", "", "Read-only GitHub app webhook secret used to verify that webhook payload contents were sent by GitHub.", flag.Secret)

	validPathRegex = regexp.MustCompile(`^[a-zA-Z0-9/_-]*$`)
)

const (
	readWriteOauthPath = "/auth/github/app/link/"
	readOnlyOauthPath  = "/auth/github/read_only_app/link/"

	// Max page size that GitHub allows for list requests.
	githubMaxPageSize = 100
)

func Register(env *real_environment.RealEnv) error {
	if !*readWriteAppEnabled && !*readOnlyAppEnabled {
		return nil
	}
	readWriteApp, err := NewReadWriteApp(env)
	if err != nil {
		return err
	}
	readOnlyApp, err := NewReadOnlyApp(env)
	if err != nil {
		return err
	}
	a, err := NewAppService(env, readWriteApp, readOnlyApp)
	if err != nil {
		return err
	}
	env.SetGitHubAppService(a)
	return nil
}

// GitHubAppService is a wrapper for GitHubApp. Because there are 2 BuildBuddy
// GitHub apps (read-only vs read-write), it helps determine the specific app
// the user has installed.
type GitHubAppService struct {
	env environment.Env

	readWriteApp interfaces.GitHubApp
	readOnlyApp  interfaces.GitHubApp
}

func NewAppService(env environment.Env, readWriteApp interfaces.GitHubApp, readOnlyApp interfaces.GitHubApp) (*GitHubAppService, error) {
	return &GitHubAppService{
		env:          env,
		readWriteApp: readWriteApp,
		readOnlyApp:  readOnlyApp,
	}, nil
}

func (s *GitHubAppService) IsReadWriteAppEnabled() bool {
	return *readWriteAppEnabled
}

func (s *GitHubAppService) IsReadOnlyAppEnabled() bool {
	return *readOnlyAppEnabled
}

// GetGitHubApp returns the BB GitHub app that the current user has authorized.
func (s *GitHubAppService) GetGitHubApp(ctx context.Context) (interfaces.GitHubApp, error) {
	u, err := s.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	if u.GithubToken == "" {
		return nil, status.NotFoundErrorf("no linked GitHub account was found")
	}
	// If the user has already linked an app installation, check its app ID.
	// Check our database first over using the GitHub API, because the API is
	// rate limited.
	installations, err := s.GetGitHubAppInstallations(ctx)
	if err != nil {
		log.CtxErrorf(ctx, "failed to get github app installations: %s", err)
	} else if len(installations) > 0 {
		// For now, a user can only have one app linked to BuildBuddy at a time (either read-write
		// or read-only). Just use the first app ID.
		installation := installations[0]
		a, err := s.GetGitHubAppWithID(installation.AppID)
		if err != nil {
			return nil, err
		}
		return a, nil
	}

	// If there are no installations, use the github token stored for the user
	// to determine which app was authorized.
	if s.IsReadWriteAppEnabled() && s.GetReadWriteGitHubApp().IsTokenValid(ctx, u.GithubToken) {
		return s.GetReadWriteGitHubApp(), nil
	} else if s.IsReadOnlyAppEnabled() && s.GetReadOnlyGitHubApp().IsTokenValid(ctx, u.GithubToken) {
		return s.GetReadOnlyGitHubApp(), nil
	}
	return nil, status.InternalErrorf("github token for user %v is not valid for any github apps", u.UserID)
}

func (s *GitHubAppService) GetReadWriteGitHubApp() interfaces.GitHubApp {
	return s.readWriteApp
}

func (s *GitHubAppService) GetReadOnlyGitHubApp() interfaces.GitHubApp {
	return s.readOnlyApp
}

func (s *GitHubAppService) GetGitHubAppWithID(appID int64) (interfaces.GitHubApp, error) {
	if s.IsReadWriteAppEnabled() && appID == s.readWriteApp.AppID() {
		return s.readWriteApp, nil
	} else if s.IsReadWriteAppEnabled() && appID == 0 {
		// TODO(MAGGIE): Delete this after we've backfilled the database
		return s.readWriteApp, nil
	} else if s.IsReadOnlyAppEnabled() && appID == s.readOnlyApp.AppID() {
		return s.readOnlyApp, nil
	}
	return nil, status.InvalidArgumentErrorf("no github app with app ID %v", appID)
}

// InstallPath returns the path the user should hit to enter the GitHub app install flow.
//
// As a prerequisite, users must link their GitHub accounts to BuildBuddy via a
// specific GitHub app (via `github::HandleLinkRepo`). That saves a GitHub token
// for the user. This function returns the installation path for that app.
func (s *GitHubAppService) InstallPath(ctx context.Context) (string, error) {
	app, err := s.GetGitHubApp(ctx)
	if err != nil {
		return "", err
	}

	if s.IsReadWriteAppEnabled() && app.AppID() == s.readWriteApp.AppID() {
		return readWriteOauthPath, nil
	} else if s.IsReadOnlyAppEnabled() && app.AppID() == s.readOnlyApp.AppID() {
		return readOnlyOauthPath, nil
	}
	return "", status.InternalErrorf("app id %d does not correspond to any github apps", app.AppID())
}

// GetGitHubAppInstallations returns all GitHub apps the owner has installed
// and linked to the currently authenticated BuildBuddy org.
//
// Note that GitHub is always the source of truth for whether an app installation
// is still valid. It's possible that access is revoked from GitHub and not reflected
// in our database.
func (s *GitHubAppService) GetGitHubAppInstallations(ctx context.Context) ([]*tables.GitHubAppInstallation, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	rq := s.env.GetDBHandle().NewQuery(ctx, "github_get_installations").Raw(`
		SELECT *
		FROM "GitHubAppInstallations"
		WHERE group_id = ?
		ORDER BY owner ASC
	`, u.GetGroupID())
	installations, err := db.ScanAll(rq, &tables.GitHubAppInstallation{})
	if err != nil {
		return nil, err
	}
	return installations, nil
}

func (s *GitHubAppService) GetLinkedGitHubRepos(ctx context.Context) (*ghpb.GetLinkedReposResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	rq := s.env.GetDBHandle().NewQuery(ctx, "github_get_linked_repos").Raw(`
		SELECT *
		FROM "GitRepositories"
		WHERE group_id = ?
		ORDER BY repo_url ASC
	`, u.GetGroupID())
	res := &ghpb.GetLinkedReposResponse{}
	err = db.ScanEach(rq, func(ctx context.Context, row *tables.GitRepository) error {
		res.RepoUrls = append(res.RepoUrls, row.RepoURL)
		return nil
	})
	if err != nil {
		return nil, status.InternalErrorf("failed to query repo rows: %s", err)
	}
	return res, nil
}

// GitHubApp Users can install either a read-write or read-only BuildBuddy
// GitHub app to authorize BuildBuddy to access certain GitHub resources.
//
// Note that in GitHub's terminology, this is a proper "GitHub App" as opposed
// to an OAuth App. This means that it authenticates as its own entity, rather
// than on behalf of a particular user. See
// https://docs.github.com/en/developers/apps/getting-started-with-apps/about-apps
type GitHubApp struct {
	env environment.Env

	// appID is the ID of the GitHub app.
	// There are only 2 possible app IDs - corresponding to either the read-write
	// or read-only BB GitHub app.
	appID int64

	oauth *gh_oauth.OAuthHandler

	webhookSecret string

	// privateKey is the GitHub-issued private key for the app. It is used to
	// create JWTs for authenticating with GitHub as the app itself.
	privateKey *rsa.PrivateKey
}

// NewReadWriteApp returns a new GitHubApp handle for the read-write BuildBuddy Github app.
func NewReadWriteApp(env environment.Env) (*GitHubApp, error) {
	if *readWriteAppClientID == "" {
		return nil, status.FailedPreconditionError("missing read write client ID.")
	}
	if *readWriteAppClientSecret == "" {
		return nil, status.FailedPreconditionError("missing read write client secret.")
	}
	if *readWriteAppID == "" {
		return nil, status.FailedPreconditionError("missing read write app ID")
	}
	appIDParsed, err := strconv.Atoi(*readWriteAppID)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid read write app ID %v: %s", *readWriteAppID, err)
	}
	if *readWriteAppPublicLink == "" {
		return nil, status.FailedPreconditionError("missing read write app public link")
	}
	if *readWriteAppWebhookSecret == "" {
		return nil, status.FailedPreconditionError("missing read write app webhook secret")
	}
	if *readWriteAppPrivateKey == "" {
		return nil, status.FailedPreconditionError("missing read write app private key")
	}
	privateKey, err := decodePrivateKey(*readWriteAppPrivateKey)
	if err != nil {
		return nil, err
	}

	app := &GitHubApp{
		env:           env,
		privateKey:    privateKey,
		webhookSecret: *readWriteAppWebhookSecret,
		appID:         int64(appIDParsed),
	}
	oauth := gh_oauth.NewOAuthHandler(env, *readWriteAppClientID, *readWriteAppClientSecret, readWriteOauthPath)
	oauth.HandleInstall = app.handleInstall
	oauth.InstallURL = fmt.Sprintf("%s/installations/new", *readWriteAppPublicLink)
	app.oauth = oauth
	return app, nil
}

// NewReadOnlyApp returns a new GitHubApp handle for the read-only BuildBuddy Github app.
func NewReadOnlyApp(env environment.Env) (*GitHubApp, error) {
	if !*readOnlyAppEnabled {
		return nil, nil
	}
	if *readOnlyAppClientID == "" {
		return nil, status.FailedPreconditionError("missing read only client ID.")
	}
	if *readOnlyAppClientSecret == "" {
		return nil, status.FailedPreconditionError("missing read only client secret.")
	}
	if *readOnlyAppID == "" {
		return nil, status.FailedPreconditionError("missing read only app ID")
	}
	appIDParsed, err := strconv.Atoi(*readOnlyAppID)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid read only app ID %v: %s", *readOnlyAppID, err)
	}
	if *readOnlyAppPublicLink == "" {
		return nil, status.FailedPreconditionError("missing read only app public link")
	}
	if *readOnlyAppWebhookSecret == "" {
		return nil, status.FailedPreconditionError("missing read only app webhook secret")
	}
	if *readOnlyAppPrivateKey == "" {
		return nil, status.FailedPreconditionError("missing read only app private key")
	}
	privateKey, err := decodePrivateKey(*readOnlyAppPrivateKey)
	if err != nil {
		return nil, err
	}

	app := &GitHubApp{
		env:           env,
		privateKey:    privateKey,
		webhookSecret: *readOnlyAppWebhookSecret,
		appID:         int64(appIDParsed),
	}
	oauth := gh_oauth.NewOAuthHandler(env, *readOnlyAppClientID, *readOnlyAppClientSecret, readOnlyOauthPath)
	oauth.HandleInstall = app.handleInstall
	oauth.InstallURL = fmt.Sprintf("%s/installations/new", *readOnlyAppPublicLink)
	app.oauth = oauth
	return app, nil
}

func (a *GitHubApp) AppID() int64 {
	return a.appID
}

func (a *GitHubApp) WebhookHandler() http.Handler {
	return http.HandlerFunc(a.handleWebhookRequest)
}

func (a *GitHubApp) handleWebhookRequest(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	b, err := github.ValidatePayload(req, []byte(a.webhookSecret))
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
	// Delegate to the appropriate handler func based on event type.
	switch event := event.(type) {
	case *github.InstallationEvent:
		return a.handleInstallationEvent(ctx, eventType, event)
	case *github.PushEvent:
		return a.handlePushEvent(ctx, eventType, event)
	case *github.PullRequestEvent:
		return a.handlePullRequestEvent(ctx, eventType, event)
	case *github.PullRequestReviewEvent:
		return a.handlePullRequestReviewEvent(ctx, eventType, event)
	default:
		// Event type not yet handled
		return nil
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

func (a *GitHubApp) handlePushEvent(ctx context.Context, eventType string, event *github.PushEvent) error {
	return a.maybeTriggerBuildBuddyWorkflow(ctx, eventType, event)
}

func (a *GitHubApp) handlePullRequestEvent(ctx context.Context, eventType string, event *github.PullRequestEvent) error {
	return a.maybeTriggerBuildBuddyWorkflow(ctx, eventType, event)
}

func (a *GitHubApp) handlePullRequestReviewEvent(ctx context.Context, eventType string, event *github.PullRequestReviewEvent) error {
	return a.maybeTriggerBuildBuddyWorkflow(ctx, eventType, event)
}

func (a *GitHubApp) maybeTriggerBuildBuddyWorkflow(ctx context.Context, eventType string, event any) error {
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
	if err := a.MaybeReindexRepo(ctx, row.GitRepository, wd, repoURL, tok.GetToken()); err != nil {
		log.Debugf("Not indexing repo: %s", err)
	}
	return a.env.GetWorkflowService().HandleRepositoryEvent(
		ctx, row.GitRepository, wd, tok.GetToken())
}

func (a *GitHubApp) MaybeReindexRepo(ctx context.Context, repo *tables.GitRepository, wd *interfaces.WebhookData, repoURL *gitutil.RepoURL, accessToken string) error {
	codesearchService := a.env.GetCodesearchService()
	if codesearchService == nil {
		return nil
	}
	if wd.EventName != webhook_data.EventName.Push || wd.PushedBranch != wd.TargetRepoDefaultBranch {
		return nil
	}
	g, err := a.env.GetUserDB().GetGroupByID(ctx, repo.GroupID)
	if err != nil {
		return err
	}
	if !g.CodeSearchEnabled {
		return nil
	}

	key, err := a.env.GetAuthDB().GetAPIKeyForInternalUseOnly(ctx, repo.GroupID)
	if err != nil {
		return err
	}
	ctx = a.env.GetAuthenticator().AuthContextFromAPIKey(ctx, key.Value)
	_, err = codesearchService.Index(ctx, &csinpb.IndexRequest{
		GitRepo: &gitpb.GitRepo{
			RepoUrl:     repoURL.String(),
			AccessToken: accessToken,
			Username:    repoURL.Owner,
		},
		RepoState: &gitpb.RepoState{
			CommitSha: wd.SHA,
			Branch:    wd.PushedBranch,
		},
		Async: true, // don't wait for an answer.
	})
	return err
}

func (a *GitHubApp) GetInstallationTokenForStatusReportingOnly(ctx context.Context, owner string) (*github.InstallationToken, error) {
	var installation tables.GitHubAppInstallation
	err := a.env.GetDBHandle().NewQuery(ctx, "githubapp_get_installation_token_for_status").Raw(`
		SELECT *
		FROM "GitHubAppInstallations"
		WHERE owner = ?
	`, owner).Take(&installation)
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundErrorf("failed to look up GitHub app installation: %s", err)
		}
		return nil, err
	}
	tok, err := a.createInstallationToken(ctx, installation.InstallationID)
	if err != nil {
		return nil, err
	}
	return tok, nil
}

func (a *GitHubApp) GetRepositoryInstallationToken(ctx context.Context, repo *tables.GitRepository) (string, error) {
	if err := authutil.AuthorizeGroupAccess(ctx, a.env, repo.GroupID); err != nil {
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

// LinkGitHubAppInstallation imports an installed GitHub app to BuildBuddy.
//
// After a user has installed the BuildBuddy Github app from the GitHub side,
// this imports the installation metadata to BuildBuddy and saves it to the database.
// This will fail if the user didn't install the app in GitHub.
//
// Note that GitHub is always the source of truth for whether the app is installed.
// A linked installation existing in the BB doesn't guarantee that the installation
// is still valid from the GitHub side.
func (a *GitHubApp) LinkGitHubAppInstallation(ctx context.Context, req *ghpb.LinkAppInstallationRequest) (*ghpb.LinkAppInstallationResponse, error) {
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
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
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	if err := authutil.AuthorizeOrgAdmin(u, groupID); err != nil {
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
		AppID:          installation.GetAppID(),
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

	// Make sure each groupID only installs 1 BuildBuddy app (either read-only
	// or read-write).
	appIDs := make(map[int64]struct{})
	allInstallations, err := a.env.GetGitHubAppService().GetGitHubAppInstallations(ctx)
	if err != nil {
		return err
	}
	for _, i := range allInstallations {
		appIDs[i.AppID] = struct{}{}
	}
	if len(appIDs) > 1 {
		msg := fmt.Sprintf("unexpected multiple github app IDs installed for group %s", in.GroupID)
		alert.UnexpectedEvent(msg)
		return status.InternalErrorf(msg)
	}
	for alreadyInstalledAppID := range appIDs {
		if alreadyInstalledAppID != in.AppID {
			return status.InvalidArgumentErrorf("cannot install multiple github apps for the same group (%s) - %d already installed, attempting to install %d", in.GroupID, alreadyInstalledAppID, in.AppID)
		}
	}

	log.CtxInfof(ctx,
		"Linking GitHub app installation %d for app %d (%s) to group %s",
		in.InstallationID, in.AppID, in.Owner, in.GroupID)
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
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetInstallationId() == 0 {
		return nil, status.FailedPreconditionError("missing installation_id")
	}
	// TODO(zoey): Could make this one query
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
		if err := authutil.AuthorizeOrgAdmin(u, ti.GroupID); err != nil {
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

// GetInstallationByOwner returns the BuildBuddy GitHub app installation for an
// owner, if it exists.
// Each owner can have at most 1 app installation. GitHub does not allow multiple
// installations for the same owner and app ID. And we do not allow each groupID
// to install multiple app IDs (this is enforced in `createInstallation`).
func (a *GitHubApp) GetInstallationByOwner(ctx context.Context, owner string) (*tables.GitHubAppInstallation, error) {
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
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

// LinkGitHubRepo imports an authorized repo to BuildBuddy.
//
//	 In order to authorize the BB GitHub app to access a specific repo, you have to
//	 (1) Install the GitHub app (on the GitHub side)
//	 (2) Authorize the GitHub app to access specific repos (GitHub side)
//	 (3) From the repos you've authorized in GitHub from #2, explicitly link certain
//		repos in the BuildBuddy UI (BB side).
//
// LinkGitHubRepo does #3. It assumes steps #1 and #2 have already occurred. It only
// lets you link repos that you've already authorized on GitHub in step #2.
func (a *GitHubApp) LinkGitHubRepo(ctx context.Context, repoURL string) (*ghpb.LinkRepoResponse, error) {
	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}

	parsedRepoURL, err := gitutil.ParseGitHubRepoURL(repoURL)
	if err != nil {
		return nil, err
	}

	installation, err := a.GetInstallationByOwner(ctx, parsedRepoURL.Owner)
	if err != nil {
		return nil, err
	}

	// Make sure that this GitHub app is authorized to access this repo (#2 from
	// the function description).
	if _, err := a.findUserRepo(ctx, tu.GithubToken, installation.InstallationID, parsedRepoURL.Repo); err != nil {
		return nil, err
	}

	if _, err := a.env.GetAuthenticator().AuthenticatedUser(ctx); err != nil {
		return nil, err
	}
	p, err := perms.ForAuthenticatedGroup(ctx, a.env)
	if err != nil {
		return nil, err
	}
	repo := &tables.GitRepository{
		UserID:               p.UserID,
		GroupID:              p.GroupID,
		Perms:                p.Perms,
		RepoURL:              parsedRepoURL.String(),
		DefaultNonRootRunner: true,
		AppID:                installation.AppID,
	}
	if err := a.env.GetDBHandle().NewQuery(ctx, "githubapp_create_repo").Create(repo); err != nil {
		return nil, status.InternalErrorf("failed to link repo: %s", err)
	}

	// Clean up deprecated legacy workflows, because repo linking is meant to
	// replace them.
	deleteReq := &wfpb.DeleteWorkflowRequest{
		RepoUrl: repoURL,
	}
	if _, err := a.env.GetWorkflowService().DeleteWorkflow(ctx, deleteReq); err != nil {
		if !db.IsRecordNotFound(err) {
			log.CtxInfof(ctx, "Failed to delete legacy workflow for linked repo: %s", err)
		}
	} else {
		log.CtxInfof(ctx, "Deleted legacy workflow for linked repo")
	}

	return &ghpb.LinkRepoResponse{}, nil
}

// UnlinkGitHubRepo deletes an authorized repo from Buildbuddy.
// It does not remove authorization from the GitHub side. See `LinkGitHubRepo`
// for more details
func (a *GitHubApp) UnlinkGitHubRepo(ctx context.Context, req *ghpb.UnlinkRepoRequest) (*ghpb.UnlinkRepoResponse, error) {
	norm, err := gitutil.NormalizeRepoURL(req.GetRepoUrl())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("failed to parse repo URL: %s", err)
	}
	normalizedURL := norm.String()
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	result := a.env.GetDBHandle().NewQuery(ctx, "githubapp_unlink_repo").Raw(`
		DELETE FROM "GitRepositories"
		WHERE group_id = ?
		AND repo_url = ?
	`, u.GetGroupID(), normalizedURL).Exec()
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
	if req.InstallationTargetType != "Organization" {
		githubClient, err = a.newAuthenticatedClient(ctx, token)
	} else {
		githubClient, token, err = a.newInstallationClient(ctx, token, req.InstallationId)
	}
	if err != nil {
		return nil, err
	}

	repoURL := fmt.Sprintf("https://github.com/%s/%s", req.Owner, req.Name)

	// Create a new repository on github, if requested
	if !req.SkipRepo {
		organization := ""
		if req.InstallationTargetType == "Organization" {
			organization = req.Owner
		}
		_, _, err := githubClient.Repositories.Create(ctx, organization, &github.Repository{
			Name:        github.String(req.Name),
			Description: github.String(req.Description),
			Private:     github.Bool(req.Private),
			AutoInit:    github.Bool(req.Template == ""),
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
		_, err = a.LinkGitHubRepo(ctx, repoURL)
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
	installationClient, _, err := a.newInstallationClient(ctx, userToken, installationID)
	if err != nil {
		return nil, err
	}
	installation, err := a.getInstallation(ctx, installationID)
	if err != nil {
		return nil, err
	}
	owner := installation.GetAccount().GetLogin()
	// Fetch repository so that we know the canonical repo name (the input
	// `repo` parameter might be equal ignoring case, but not exactly equal).
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
			"/settings/org/github/complete-installation?installation_id=%d&installation_owner=%s&app_id=%d",
			installationID, installation.GetAccount().GetLogin(), installation.GetAppID())
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
		Issuer:    fmt.Sprintf("%d", a.appID),
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

func (a *GitHubApp) newInstallationClient(ctx context.Context, userToken string, installationID int64) (*github.Client, string, error) {
	if err := a.authorizeUserInstallationAccess(ctx, userToken, installationID); err != nil {
		return nil, "", err
	}
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

func (a *GitHubApp) newAuthenticatedGraphQLClient(ctx context.Context, accessToken string) (*githubv4.Client, error) {
	if accessToken == "" {
		return nil, status.UnauthenticatedError("missing user access token")
	}
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})
	tc := oauth2.NewClient(ctx, ts)

	if gh_oauth.IsEnterpriseConfigured() {
		host := fmt.Sprintf("https://%s/", gh_oauth.GithubHost())
		return githubv4.NewEnterpriseClient(host, tc), nil
	}

	return githubv4.NewClient(tc), nil
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

func (a *GitHubApp) getGithubGraphQLClient(ctx context.Context) (*githubv4.Client, error) {
	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	if tu.GithubToken == "" {
		return nil, status.UnauthenticatedError("github account link is required")
	}
	return a.newAuthenticatedGraphQLClient(ctx, tu.GithubToken)
}

// IsTokenValid returns whether the oauth token is valid for the current app.
func (a *GitHubApp) IsTokenValid(ctx context.Context, oauthToken string) bool {
	// The Authorizations.Check API requires basic auth.
	tp := github.BasicAuthTransport{
		Username: a.oauth.ClientID,
		Password: a.oauth.ClientSecret,
	}
	client := github.NewClient(tp.Client())
	_, _, err := client.Authorizations.Check(ctx, a.oauth.ClientID, oauthToken)
	return err == nil
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
			AppId:      i.GetAppID(),
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

	entry := &github.TreeEntry{
		Path: &node.Path,
		Mode: &node.Mode,
	}

	if node.Sha != "" {
		entry.SHA = &node.Sha
	}

	return entry
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
		Files:   []*ghpb.FileSummary{},
		Commits: []*ghpb.Commit{},
	}

	for _, c := range comparison.Commits {
		res.Commits = append(res.Commits, &ghpb.Commit{
			Sha: c.GetSHA(),
		})
	}

	for _, f := range comparison.Files {
		summary := &ghpb.FileSummary{
			Name:       f.GetFilename(),
			Sha:        f.GetSHA(),
			Additions:  int64(f.GetAdditions()),
			Deletions:  int64(f.GetDeletions()),
			Patch:      f.GetPatch(),
			ChangeType: FileStatusToChangeType(f.GetStatus()),
		}
		summary.OriginalName = f.GetPreviousFilename()
		if summary.OriginalName == "" {
			summary.OriginalName = summary.Name
		}
		summary.OriginalCommitSha = req.Base
		summary.ModifiedCommitSha = req.Head

		res.Files = append(res.Files, summary)
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
			Message: c.GetCommit().GetMessage(),
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
	c, _, err := client.Git.CreateCommit(ctx, req.Owner, req.Repo, commit, nil)
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
	client, err := a.getGithubGraphQLClient(ctx)
	if err != nil {
		return nil, err
	}
	usernameForFetch := req.GetUser()
	if usernameForFetch == "" {
		usernameForFetch = "@me"
	}
	incoming, outgoing, err := a.getIncomingAndOutgoingPRs(ctx, usernameForFetch, client)
	if err != nil {
		return nil, err
	}

	pendingOutgoing := make([]*ghpb.PullRequest, 0)
	nonPendingOutgoing := make([]*ghpb.PullRequest, 0)
	for _, pr := range outgoing {
		if len(pr.Reviews) > 0 {
			nonPendingOutgoing = append(nonPendingOutgoing, pr)
		} else {
			pendingOutgoing = append(pendingOutgoing, pr)
		}
	}

	resp := &ghpb.GetGithubPullRequestResponse{
		Incoming: incoming,
		Outgoing: nonPendingOutgoing,
		Pending:  pendingOutgoing,
	}
	return resp, nil
}

func (a *GitHubApp) CreateGithubPullRequestComment(ctx context.Context, req *ghpb.CreateGithubPullRequestCommentRequest) (*ghpb.CreateGithubPullRequestCommentResponse, error) {
	if !*enableReviewMutates {
		return nil, status.UnimplementedError("Not implemented")
	}
	graphqlClient, err := a.getGithubGraphQLClient(ctx)
	if err != nil {
		return nil, err
	}

	reviewId := req.GetReviewId()
	var comment reviewComment
	var threadId string

	var side githubv4.DiffSide
	if req.GetSide() == ghpb.CommentSide_LEFT_SIDE {
		side = githubv4.DiffSideLeft
	} else {
		side = githubv4.DiffSideRight
	}

	if reviewId == "" {
		// The Github API is really wonky around new reviews--let's just make
		// a review with a promotional comment for now and figure it out later.
		var m struct {
			AddPullRequestReview struct {
				PullRequestReview struct {
					Id   string
					Body string
				}
			} `graphql:"addPullRequestReview(input: $input)"`
		}
		input := githubv4.AddPullRequestReviewInput{
			PullRequestID: req.GetPullId(),
			Body:          githubv4.NewString("I wrote this review in the BuildBuddy code review UI alpha!"),
		}
		err := graphqlClient.Mutate(ctx, &m, input, nil)
		if err != nil {
			return nil, err
		}
		reviewId = m.AddPullRequestReview.PullRequestReview.Id
	}

	if req.GetThreadId() != "" {
		// This is a comment in reply to an existing thread, just add it.
		var m struct {
			PullRequestReviewThreadReply struct {
				ClientMutationId string
				Comment          reviewComment
			} `graphql:"addPullRequestReviewThreadReply(input: $input)"`
		}

		input := githubv4.AddPullRequestReviewThreadReplyInput{
			PullRequestReviewID:       githubv4.NewID(reviewId),
			PullRequestReviewThreadID: githubv4.String(req.GetThreadId()),
			Body:                      githubv4.String(req.GetBody()),
		}

		err := graphqlClient.Mutate(ctx, &m, input, nil)
		if err != nil {
			return nil, err
		}
		comment = m.PullRequestReviewThreadReply.Comment
		threadId = req.GetThreadId()
	} else {
		// This is a comment in a new thread, create it.
		var m struct {
			AddPullRequestReviewThread struct {
				ClientMutationId string
				Thread           struct {
					Id       string
					Comments struct {
						Nodes []reviewComment
					} `graphql:"comments(last: 1)"`
				}
			} `graphql:"addPullRequestReviewThread(input: $input)"`
		}

		input := githubv4.AddPullRequestReviewThreadInput{
			PullRequestID:       githubv4.NewID(req.GetPullId()),
			PullRequestReviewID: githubv4.NewID(reviewId),
			Path:                githubv4.String(req.GetPath()),
			Line:                githubv4.NewInt(githubv4.Int(int(req.GetLine()))),
			Side:                &side,
			Body:                githubv4.String(req.GetBody()),
		}

		err := graphqlClient.Mutate(ctx, &m, input, nil)
		if err != nil {
			return nil, err
		}
		comment = m.AddPullRequestReviewThread.Thread.Comments.Nodes[0]
		threadId = m.AddPullRequestReviewThread.Thread.Id
	}

	return &ghpb.CreateGithubPullRequestCommentResponse{
		ReviewId:  reviewId,
		CommentId: comment.Id,
		// TODO(jdhollen): Set resolved and side properly.
		Comment: graphQLCommentToProto(&comment, int64(comment.OriginalStartLine), int64(comment.OriginalLine), side, req.GetPath(), threadId, false),
	}, nil
}

func (a *GitHubApp) UpdateGithubPullRequestComment(ctx context.Context, req *ghpb.UpdateGithubPullRequestCommentRequest) (*ghpb.UpdateGithubPullRequestCommentResponse, error) {
	if !*enableReviewMutates {
		return nil, status.UnimplementedError("Not implemented")
	}
	graphqlClient, err := a.getGithubGraphQLClient(ctx)
	if err != nil {
		return nil, err
	}
	var m struct {
		UpdatePullRequestReviewComment struct {
			ClientMutationId string
		} `graphql:"updatePullRequestReviewComment(input: $input)"`
	}
	input := githubv4.UpdatePullRequestReviewCommentInput{
		PullRequestReviewCommentID: req.GetCommentId(),
		Body:                       githubv4.String(req.GetNewBody()),
	}
	err = graphqlClient.Mutate(ctx, &m, input, nil)
	if err != nil {
		return nil, err
	}
	return &ghpb.UpdateGithubPullRequestCommentResponse{}, nil
}

func (a *GitHubApp) DeleteGithubPullRequestComment(ctx context.Context, req *ghpb.DeleteGithubPullRequestCommentRequest) (*ghpb.DeleteGithubPullRequestCommentResponse, error) {
	if !*enableReviewMutates {
		return nil, status.UnimplementedError("Not implemented")
	}
	graphqlClient, err := a.getGithubGraphQLClient(ctx)
	if err != nil {
		return nil, err
	}
	var m struct {
		DeletePullRequestReviewComment struct {
			ClientMutationId string
		} `graphql:"deletePullRequestReviewComment(input: $input)"`
	}
	input := githubv4.DeletePullRequestReviewCommentInput{
		ID: githubv4.ID(req.GetCommentId()),
	}
	err = graphqlClient.Mutate(ctx, &m, input, nil)
	if err != nil {
		return nil, err
	}
	return &ghpb.DeleteGithubPullRequestCommentResponse{}, nil
}

type prUser struct {
	Login string
}

type bot struct {
	Login string
}

type actor struct {
	Typename string `graphql:"__typename"`
	User     prUser `graphql:"... on User"`
	Bot      bot    `graphql:"... on Bot"`
}

type timelineItem struct {
	Typename             string `graphql:"__typename"`
	ReviewRequestedEvent struct {
		RequestedReviewer actor
	} `graphql:"... on ReviewRequestedEvent"`
	ReviewRequestRemovedEvent struct {
		RequestedReviewer actor
	} `graphql:"... on ReviewRequestRemovedEvent"`
	ReviewDismissedEvent struct {
		Actor actor
	} `graphql:"... on ReviewDismissedEvent"`
	PullRequestReview struct {
		Author actor
		State  string
	} `graphql:"... on PullRequestReview"`
}

type reviewComment struct {
	comment
	OriginalCommit struct {
		Oid string
	}
	PullRequestReview struct {
		Id string
	}
	ReplyTo struct {
		Id string
	}
}

type comment struct {
	Author    actor
	BodyHTML  string `graphql:"bodyHTML"`
	BodyText  string
	CreatedAt time.Time
	Id        string

	// We don't strictly need these fields for normal requests, but they make it
	// easier to build replies to mutates, and we're talking about a few ints,
	// so whatever.
	OriginalLine      int
	OriginalStartLine int
}

type commentLink struct {
	Id string
}

type reviewThread struct {
	Id                string
	Path              string
	IsResolved        bool
	DiffSide          githubv4.DiffSide
	OriginalLine      int
	Line              int
	StartDiffSide     githubv4.DiffSide
	OriginalStartLine int
	StartLine         int
	Comments          struct {
		Nodes []reviewComment
	} `graphql:"comments(first: 100)"`
}

type reviewRequest struct {
	RequestedReviewer actor
}

type review struct {
	Id        string
	BodyText  string
	CreatedAt time.Time
	Author    actor
	State     string
}

type file struct {
	Path              string
	Patch             string
	Additions         int
	Deletions         int
	ChangeType        githubv4.PatchStatus
	ViewerViewedState githubv4.FileViewedState
}

type combinedContext struct {
	Typename      string `graphql:"__typename"`
	StatusContext struct {
		Context     string
		CreatedAt   time.Time
		Description string
		TargetUrl   string
		State       githubv4.StatusState
	} `graphql:"... on StatusContext"`
}

type checkSuite struct {
	App struct {
		Id   string
		Name string
	}
	CheckRuns struct {
		TotalCount int
	}
	CreatedAt  time.Time
	Status     githubv4.CheckStatusState
	Conclusion githubv4.CheckConclusionState
	Url        string
}

type prCommit struct {
	Commit struct {
		Oid  string
		Tree struct {
			Oid string
		}
		Message     string
		CheckSuites struct {
			Nodes []checkSuite
		} `graphql:"checkSuites(last: 100)"`
		Status struct {
			CombinedContexts struct {
				Nodes []combinedContext
			} `graphql:"combinedContexts(last: 100)"`
		}
	}
}

type prDetailsQuery struct {
	Viewer struct {
		Login string
	}
	Repository struct {
		PullRequest struct {
			Title            string
			TitleHTML        string `graphql:"titleHTML"`
			Body             string
			BodyHTML         string `graphql:"bodyHTML"`
			Author           actor
			CreatedAt        time.Time
			Id               string
			UpdatedAt        time.Time
			MergeStateStatus string
			Merged           bool
			URL              string `graphql:"url"`
			ChecksURL        string `graphql:"checksUrl"`
			BaseRefOid       string
			HeadRefOid       string
			HeadRefName      string
			TimelineItems    struct {
				Nodes []timelineItem
			} `graphql:"timelineItems(first: 100, itemTypes: [REVIEW_REQUESTED_EVENT, REVIEW_REQUEST_REMOVED_EVENT, REVIEW_DISMISSED_EVENT, PULL_REQUEST_REVIEW])"`
			ReviewRequests struct {
				Nodes []reviewRequest
			} `graphql:"reviewRequests(first: 100)"`
			ReviewThreads struct {
				Nodes []reviewThread
			} `graphql:"reviewThreads(first: 100)"`
			Reviews struct {
				Nodes []review
			} `graphql:"reviews(first: 100)"`
			Commits struct {
				Nodes []prCommit
			} `graphql:"commits(last: 100)"`
			// TODO(jdhollen): Fetch files here
		} `graphql:"pullRequest(number: $pullNumber)"`
	} `graphql:"repository(owner: $repoOwner, name: $repoName)"`
}

func getLogin(a *actor) string {
	switch a.Typename {
	case "Bot":
		return a.Bot.Login
	case "User":
		return a.User.Login
	}
	return ""
}

func (a *GitHubApp) SendGithubPullRequestReview(ctx context.Context, req *ghpb.SendGithubPullRequestReviewRequest) (*ghpb.SendGithubPullRequestReviewResponse, error) {
	if !*enableReviewMutates {
		return nil, status.UnimplementedError("Not implemented")
	}
	graphqlClient, err := a.getGithubGraphQLClient(ctx)
	if err != nil {
		return nil, err
	}

	reviewID := req.GetReviewId()
	prID := req.GetPullRequestId()
	if reviewID == "" && prID == "" {
		return nil, status.InvalidArgumentError("You must specify a pull request or Review ID.")
	}

	replyBody := req.GetBody()
	if replyBody == "" {
		if req.GetApprove() {
			replyBody = "LGTM, Approval"
		} else {
			replyBody = "See comments"
		}
	}

	event := githubv4.PullRequestReviewEventComment
	if req.GetApprove() {
		event = githubv4.PullRequestReviewEventApprove
	}

	if reviewID == "" && prID != "" {
		var m struct {
			AddPullRequestReview struct {
				PullRequestReview struct {
					Id   string
					Body string
				}
			} `graphql:"addPullRequestReview(input: $input)"`
		}
		input := githubv4.AddPullRequestReviewInput{
			PullRequestID: req.GetPullRequestId(),
			Body:          githubv4.NewString(githubv4.String(replyBody)),
			Event:         &event,
		}
		err := graphqlClient.Mutate(ctx, &m, input, nil)
		if err != nil {
			return nil, err
		}
		return &ghpb.SendGithubPullRequestReviewResponse{}, nil
	}

	var m struct {
		SubmitPullRequestReview struct {
			ClientMutationId string
		} `graphql:"submitPullRequestReview(input: $input)"`
	}

	input := githubv4.SubmitPullRequestReviewInput{
		PullRequestReviewID: githubv4.NewID(reviewID),
		Body:                githubv4.NewString(githubv4.String(replyBody)),
		Event:               event,
	}
	err = graphqlClient.Mutate(ctx, &m, input, nil)
	if err != nil {
		return nil, err
	}
	return &ghpb.SendGithubPullRequestReviewResponse{}, nil
}

func isBot(a *actor) bool {
	return a.Typename == "Bot"
}

func checkStateToStatus(s githubv4.CheckStatusState, c githubv4.CheckConclusionState) ghpb.ActionStatusState {
	if s != githubv4.CheckStatusStateCompleted {
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_PENDING
	}
	switch c {
	case githubv4.CheckConclusionStateFailure:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.CheckConclusionStateActionRequired:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.CheckConclusionStateCancelled:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.CheckConclusionStateTimedOut:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.CheckConclusionStateStartupFailure:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.CheckConclusionStateStale:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL
	case githubv4.CheckConclusionStateNeutral:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL
	case githubv4.CheckConclusionStateSkipped:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL
	case githubv4.CheckConclusionStateSuccess:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_SUCCESS
	}
	return ghpb.ActionStatusState_ACTION_STATUS_STATE_UNKNOWN
}

func statusStateToStatus(s githubv4.StatusState) ghpb.ActionStatusState {
	switch s {
	case githubv4.StatusStateError:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.StatusStateFailure:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.StatusStateExpected:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL
	case githubv4.StatusStateSuccess:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_SUCCESS
	case githubv4.StatusStatePending:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_PENDING
	}
	return ghpb.ActionStatusState_ACTION_STATUS_STATE_UNKNOWN
}

func combineStatuses(a ghpb.ActionStatusState, b ghpb.ActionStatusState) ghpb.ActionStatusState {
	if a == ghpb.ActionStatusState_ACTION_STATUS_STATE_PENDING || b == ghpb.ActionStatusState_ACTION_STATUS_STATE_PENDING {
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_PENDING
	} else if a == ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE || b == ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE {
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	} else if a == ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL || b == ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL {
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL
	} else if a == ghpb.ActionStatusState_ACTION_STATUS_STATE_SUCCESS || b == ghpb.ActionStatusState_ACTION_STATUS_STATE_SUCCESS {
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_SUCCESS
	}
	return ghpb.ActionStatusState_ACTION_STATUS_STATE_UNKNOWN
}

type combinedChecksForApp struct {
	Name   string
	Count  int
	Status ghpb.ActionStatusState
	URL    string
}

func graphQLCommentToProto(c *reviewComment, startLine int64, endLine int64, diffSide githubv4.DiffSide, path string, threadId string, resolved bool) *ghpb.Comment {
	comment := &ghpb.Comment{}
	comment.Id = c.Id
	comment.Body = c.BodyText
	comment.Path = path
	// TODO(jdhollen): This commit sha is the commit of the right hand side
	// of the diff.  GitHub only gives us the diff hunk as a way to match
	// the comment with the left hand side commit. Even if the GitHub UI's
	// behavior is internally-consistent (big if, doesn't seem like it), the
	// behavior is definitely opaque to users.  We should strive to match
	// comments to specific commits and *always* show the comment on that
	// revision of the file (effectively ignoring the "side" attribute).
	comment.CommitSha = c.OriginalCommit.Oid
	comment.ReviewId = c.PullRequestReview.Id
	comment.CreatedAtUsec = c.CreatedAt.UnixMicro()
	comment.ParentCommentId = c.ReplyTo.Id
	comment.ThreadId = threadId
	comment.IsResolved = resolved

	position := &ghpb.CommentPosition{}
	position.StartLine = int64(startLine)
	position.EndLine = int64(endLine)

	if diffSide == "LEFT" {
		position.Side = ghpb.CommentSide_LEFT_SIDE
	} else {
		position.Side = ghpb.CommentSide_RIGHT_SIDE
	}
	comment.Position = position

	commenter := &ghpb.ReviewUser{}
	commenter.Login = getLogin(&c.Author)
	commenter.Bot = isBot(&c.Author)
	comment.Commenter = commenter

	return comment
}

func FileStatusToChangeType(status string) ghpb.FileChangeType {
	if status == "added" {
		return ghpb.FileChangeType_FILE_CHANGE_TYPE_ADDED
	} else if status == "removed" {
		return ghpb.FileChangeType_FILE_CHANGE_TYPE_REMOVED
	} else if status == "changed" || status == "modified" {
		return ghpb.FileChangeType_FILE_CHANGE_TYPE_MODIFIED
	} else if status == "renamed" {
		return ghpb.FileChangeType_FILE_CHANGE_TYPE_RENAMED
	} else if status == "copied" {
		return ghpb.FileChangeType_FILE_CHANGE_TYPE_COPIED
	} else if status == "unchanged" {
		return ghpb.FileChangeType_FILE_CHANGE_TYPE_UNCHANGED
	}
	return ghpb.FileChangeType_FILE_CHANGE_TYPE_UNKNOWN
}

func (a *GitHubApp) GetGithubPullRequestDetails(ctx context.Context, req *ghpb.GetGithubPullRequestDetailsRequest) (*ghpb.GetGithubPullRequestDetailsResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	gqClient, err := a.getGithubGraphQLClient(ctx)
	if err != nil {
		return nil, err
	}
	eg, gCtx := errgroup.WithContext(ctx)

	graph := &prDetailsQuery{}
	vars := map[string]interface{}{
		"repoOwner":  githubv4.String(req.GetOwner()),
		"repoName":   githubv4.String(req.GetRepo()),
		"pullNumber": githubv4.Int(req.GetPull()),
	}
	eg.Go(func() error {
		if err := gqClient.Query(gCtx, &graph, vars); err != nil {
			return err
		}
		return nil
	})

	var files []*github.CommitFile
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

	pr := graph.Repository.PullRequest

	outputComments := make([]*ghpb.Comment, 0)
	draftReviewId := ""
	for _, r := range pr.Reviews.Nodes {
		if r.State == "PENDING" {
			draftReviewId = r.Id
		}
	}

	fileCommentCount := make(map[string]int64)
	for _, thread := range pr.ReviewThreads.Nodes {
		for _, c := range thread.Comments.Nodes {
			comment := graphQLCommentToProto(
				&c, int64(thread.OriginalStartLine), int64(thread.OriginalLine), thread.DiffSide, thread.Path, thread.Id, thread.IsResolved)
			if thread.Path != "" {
				fileCommentCount[thread.Path]++
			}
			outputComments = append(outputComments, comment)
		}
	}

	notExplicitlyRemoved := map[string]struct{}{}
	approved := map[string]struct{}{}

	for _, e := range pr.TimelineItems.Nodes {
		switch eventType := e.Typename; eventType {
		case "ReviewRequestedEvent":
			notExplicitlyRemoved[getLogin(&e.ReviewRequestedEvent.RequestedReviewer)] = struct{}{}
		case "ReviewRequestRemovedEvent":
			delete(notExplicitlyRemoved, getLogin(&e.ReviewRequestRemovedEvent.RequestedReviewer))
		case "PullRequestReview":
			if e.PullRequestReview.State == "APPROVED" {
				approved[getLogin(&e.PullRequestReview.Author)] = struct{}{}
			} else if e.PullRequestReview.State == "CHANGES_REQUESTED" {
				delete(approved, getLogin(&e.PullRequestReview.Author))
			}
		}
	}

	activeReviewers := map[string]struct{}{}

	reviewers := make([]*ghpb.Reviewer, 0)
	for _, r := range pr.ReviewRequests.Nodes {
		activeReviewers[getLogin(&r.RequestedReviewer)] = struct{}{}
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
		summary.ChangeType = FileStatusToChangeType(f.GetStatus())

		url, err := url.Parse(f.GetContentsURL())
		if err != nil {
			return nil, err
		}
		ref := url.Query().Get("ref")
		if ref == "" {
			return nil, status.InternalErrorf("Couldn't find SHA for file.")
		}
		summary.OriginalName = f.GetPreviousFilename()
		if summary.OriginalName == "" {
			summary.OriginalName = summary.Name
		}
		summary.OriginalCommitSha = pr.BaseRefOid
		summary.ModifiedCommitSha = pr.HeadRefOid
		fileSummaries = append(fileSummaries, summary)
	}

	statusTrackingMap := make(map[string]*combinedContext, 0)
	checkTrackingMap := make(map[string]*combinedChecksForApp, 0)

	commits := make([]*ghpb.Commit, 0)
	// TODO(jdhollen): show previous checks + status as "outdated" when a new
	// run hasn't fired instead of hiding them.
	if len(pr.Commits.Nodes) > 0 {
		for _, c := range pr.Commits.Nodes {
			commits = append(commits, &ghpb.Commit{Sha: c.Commit.Oid, TreeSha: c.Commit.Tree.Oid, Message: c.Commit.Message})
		}

		lastCommit := pr.Commits.Nodes[len(pr.Commits.Nodes)-1]
		for _, s := range lastCommit.Commit.Status.CombinedContexts.Nodes {
			if s.Typename == "StatusContext" {
				if prev, ok := statusTrackingMap[s.StatusContext.Context]; ok && s.StatusContext.CreatedAt.UnixMicro() < prev.StatusContext.CreatedAt.UnixMicro() {
					continue
				}
				s2 := s
				statusTrackingMap[s.StatusContext.Context] = &s2
			}
		}
		// TODO(jdhollen): Request access to Workflows in Github App, and show
		// GH workflow names instead of just the app name.
		for _, s := range lastCommit.Commit.CheckSuites.Nodes {
			// GitHub creates CheckSuites for all apps that listen for
			// CheckSuite hooks, and can't know if they'll ever create a
			// CheckRun--let's just show the ones that did.
			if s.CheckRuns.TotalCount == 0 {
				continue
			}
			name := s.App.Id + s.App.Name
			v, ok := checkTrackingMap[name]
			if !ok {
				v = &combinedChecksForApp{
					Count:  1,
					Name:   s.App.Name,
					Status: checkStateToStatus(s.Status, s.Conclusion),
					URL:    pr.ChecksURL,
				}
			} else {
				v.Count = v.Count + 1
				v.Status = combineStatuses(v.Status, checkStateToStatus(s.Status, s.Conclusion))
			}
			checkTrackingMap[name] = v
		}
	}
	actionStatuses := make([]*ghpb.ActionStatus, 0)
	for _, s := range statusTrackingMap {
		status := &ghpb.ActionStatus{}
		status.Name = s.StatusContext.Context
		status.Status = statusStateToStatus(s.StatusContext.State)
		status.Url = s.StatusContext.TargetUrl
		actionStatuses = append(actionStatuses, status)
	}
	for _, s := range checkTrackingMap {
		status := &ghpb.ActionStatus{}
		status.Name = fmt.Sprintf("%s (%d actions)", s.Name, s.Count)
		status.Status = s.Status
		status.Url = s.URL
		actionStatuses = append(actionStatuses, status)
	}
	slices.SortFunc(actionStatuses, func(a, b *ghpb.ActionStatus) int {
		return strings.Compare(a.Name, b.Name)
	})

	resp := &ghpb.GetGithubPullRequestDetailsResponse{
		Owner:          req.Owner,
		Repo:           req.Repo,
		Pull:           req.Pull,
		PullId:         pr.Id,
		Title:          pr.Title,
		Body:           pr.Body,
		Author:         getLogin(&pr.Author),
		CreatedAtUsec:  pr.CreatedAt.UnixMicro(),
		UpdatedAtUsec:  pr.UpdatedAt.UnixMicro(),
		Branch:         pr.HeadRefName,
		BaseCommitSha:  pr.BaseRefOid,
		HeadCommitSha:  pr.HeadRefOid,
		Commits:        commits,
		Reviewers:      reviewers,
		Files:          fileSummaries,
		ActionStatuses: actionStatuses,
		Comments:       outputComments,
		// TODO(jdhollen): Switch to MergeStateStatus when it's stable. https://docs.github.com/en/graphql/reference/enums#mergestatestatus
		Mergeable:     pr.MergeStateStatus == "CLEAN",
		Submitted:     pr.Merged,
		GithubUrl:     pr.URL,
		DraftReviewId: draftReviewId,
		ViewerLogin:   graph.Viewer.Login,
	}

	return resp, nil
}

type searchPR struct {
	Title            string
	TitleHTML        string `graphql:"titleHTML"`
	Body             string
	BodyHTML         string `graphql:"bodyHTML"`
	Author           actor
	CreatedAt        time.Time
	Id               string
	Number           int
	UpdatedAt        time.Time
	MergeStateStatus string
	URL              string `graphql:"url"`
	HeadRefName      string
	Additions        int
	Deletions        int
	Repository       struct {
		Name  string
		Owner struct {
			Login string
		}
	}
	ReviewRequests struct {
		Nodes []reviewRequest
	} `graphql:"reviewRequests(first: 100)"`
	Reviews struct {
		Nodes []review
	} `graphql:"reviews(first: 100)"`
}

type searchPRNode struct {
	Typename string   `graphql:"__typename"`
	PR       searchPR `graphql:"... on PullRequest"`
}

type prSearchQuery struct {
	Viewer struct {
		Login string
	}
	Search struct {
		Nodes []searchPRNode
	} `graphql:"search(type: ISSUE, query: $searchQuery, last:100)"`
}

func (a *GitHubApp) getIncomingAndOutgoingPRs(ctx context.Context, username string, client *githubv4.Client) ([]*ghpb.PullRequest, []*ghpb.PullRequest, error) {
	eg, gCtx := errgroup.WithContext(ctx)
	incomingGraph := &prSearchQuery{}
	outgoingGraph := &prSearchQuery{}
	incomingVars := map[string]interface{}{
		"searchQuery": githubv4.String(fmt.Sprintf("is:open is:pr user-review-requested:%s archived:false draft:false", username)),
	}
	outgoingVars := map[string]interface{}{
		"searchQuery": githubv4.String(fmt.Sprintf("is:open is:pr author:%s archived:false draft:false", username)),
	}

	eg.Go(func() error {
		if err := client.Query(gCtx, &incomingGraph, incomingVars); err != nil {
			return err
		}
		return nil
	})
	eg.Go(func() error {
		if err := client.Query(gCtx, &outgoingGraph, outgoingVars); err != nil {
			return err
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	usernameForAttentionSet := username
	if usernameForAttentionSet == "@me" {
		usernameForAttentionSet = incomingGraph.Viewer.Login
	}

	incoming := make([]*ghpb.PullRequest, 0, len(incomingGraph.Search.Nodes))
	for _, pr := range incomingGraph.Search.Nodes {
		incoming = append(incoming, issueToPullRequestProto(&pr.PR, usernameForAttentionSet))
	}
	outgoing := make([]*ghpb.PullRequest, 0, len(outgoingGraph.Search.Nodes))
	for _, pr := range outgoingGraph.Search.Nodes {
		outgoing = append(outgoing, issueToPullRequestProto(&pr.PR, usernameForAttentionSet))
	}

	return incoming, outgoing, nil
}

func (a *GitHubApp) populatePRMetadata(ctx context.Context, client *github.Client, prIssues []*searchPRNode, requestedUser string) (map[string]*ghpb.PullRequest, error) {
	prs := make(map[string]*ghpb.PullRequest, len(prIssues))
	for _, i := range prIssues {
		i := &i.PR
		prs[i.Id] = issueToPullRequestProto(i, requestedUser)
	}
	return prs, nil
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

func issueToPullRequestProto(i *searchPR, requestedUser string) *ghpb.PullRequest {
	p := &ghpb.PullRequest{
		Number:        uint64(i.Number),
		Title:         i.Title,
		Body:          i.Body,
		Author:        i.Author.User.Login,
		Owner:         i.Repository.Owner.Login,
		Repo:          i.Repository.Name,
		UpdatedAtUsec: i.UpdatedAt.UnixMicro(),
		Reviews:       map[string]*ghpb.Review{},
		Additions:     int64(i.Additions),
		Deletions:     int64(i.Deletions),
		Mergeable:     i.MergeStateStatus == "CLEAN",
	}
	for _, r := range i.ReviewRequests.Nodes {
		review, ok := p.Reviews[r.RequestedReviewer.User.Login]
		if !ok {
			review = &ghpb.Review{}
			p.Reviews[r.RequestedReviewer.User.Login] = review
		}
		review.Requested = true
		if r.RequestedReviewer.User.Login == requestedUser {
			review.IsCurrentUser = true
		}
	}
	for _, r := range i.Reviews.Nodes {
		review, ok := p.Reviews[r.Author.User.Login]
		if !ok {
			review = &ghpb.Review{}
			p.Reviews[r.Author.User.Login] = review
		}
		review.Status = strings.ToLower(r.State)
		review.SubmittedAtUsec = r.CreatedAt.UnixMicro()
		if r.Author.User.Login == requestedUser {
			review.IsCurrentUser = true
		}
	}
	return p
}

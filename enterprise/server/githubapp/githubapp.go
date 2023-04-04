package githubapp

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang-jwt/jwt"
	"github.com/google/go-github/v43/github"
	"golang.org/x/oauth2"

	ghpb "github.com/buildbuddy-io/buildbuddy/proto/github"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	gh_oauth "github.com/buildbuddy-io/buildbuddy/server/backends/github"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
)

var (
	enabled       = flag.Bool("github.app.enabled", false, "Whether to enable the BuildBuddy GitHub app server.")
	clientID      = flag.String("github.app.client_id", "", "GitHub app OAuth client ID.")
	clientSecret  = flag.String("github.app.client_secret", "", "GitHub app OAuth client secret.")
	appID         = flag.String("github.app.id", "", "GitHub app ID.")
	publicLink    = flag.String("github.app.public_link", "", "GitHub app installation URL.")
	privateKey    = flag.String("github.app.private_key", "", "GitHub app private key.")
	webhookSecret = flag.String("github.app.webhook_secret", "", "GitHub app webhook secret used to verify that webhook payload contents were sent by GitHub.")
)

const (
	oauthAppPath = "/auth/github/app/link/"

	// Max page size that GitHub allows for list requests.
	githubMaxPageSize = 100
)

func Register(env environment.Env) error {
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

func (a *GitHubApp) GetGitHubAppInstallations(ctx context.Context, req *ghpb.GetAppInstallationsRequest) (*ghpb.GetAppInstallationsResponse, error) {
	u, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return nil, err
	}
	// List installations linked to the org.
	db := a.env.GetDBHandle().DB(ctx)
	rows, err := db.Raw(`
		SELECT *
		FROM GitHubAppInstallations
		WHERE group_id = ?
		ORDER BY owner ASC
	`, u.GetGroupID()).Rows()
	if err != nil {
		return nil, status.InternalErrorf("failed to get installations: %s", err)
	}
	res := &ghpb.GetAppInstallationsResponse{}
	for rows.Next() {
		var row tables.GitHubAppInstallation
		if err := db.ScanRows(rows, &row); err != nil {
			return nil, status.InternalErrorf("failed to scan installation row: %s", err)
		}
		res.Installations = append(res.Installations, &ghpb.AppInstallation{
			GroupId:        row.GroupID,
			InstallationId: row.InstallationID,
			Owner:          row.Owner,
		})
	}
	if rows.Err() != nil {
		return nil, status.InternalErrorf("failed to scan all installation rows: %s", err)
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
	log.Infof(
		"Linking GitHub app installation %d (%s) to group %s",
		in.InstallationID, in.Owner, in.GroupID)
	return a.env.GetDBHandle().DB(ctx).Transaction(func(tx *db.DB) error {
		// If an installation already exists with the given owner, unlink it
		// first. That installation must be stale since GitHub only allows
		// one installation per owner.
		err := tx.Exec(`
			DELETE FROM GitHubAppInstallations
			WHERE owner = ?`,
			in.Owner,
		).Error
		if err != nil {
			return err
		}
		// Note: (GroupID, InstallationID) is the primary key, so this will fail
		// if the installation is already linked to another group.
		return tx.Create(in).Error
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
	err = dbh.DB(ctx).Transaction(func(tx *db.DB) error {
		var ti tables.GitHubAppInstallation
		err := tx.Raw(`
			SELECT `+dbh.SelectForUpdateModifier()+` *
			FROM GitHubAppInstallations
			WHERE installation_id = ?
		`, req.GetInstallationId()).Take(&ti).Error
		if err != nil {
			return err
		}
		if err := authutil.AuthorizeGroupRole(u, ti.GroupID, role.Admin); err != nil {
			return err
		}
		return tx.Exec(`
			DELETE FROM GitHubAppInstallations
			WHERE installation_id = ?
		`, req.GetInstallationId()).Error
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
	err = a.env.GetDBHandle().DB(ctx).Raw(`
		SELECT * FROM GitHubAppInstallations
		WHERE group_id = ?
		AND owner = ?
	`, u.GetGroupID(), owner).Take(installation).Error
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundErrorf("no GitHub app installation for %q was found for the authenticated group", owner)
		}
		return nil, status.InternalErrorf("failed to look up GitHub app installation: %s", err)
	}
	return installation, nil
}

func (a *GitHubApp) GetLinkedGitHubRepos(ctx context.Context, req *ghpb.GetLinkedReposRequest) (*ghpb.GetLinkedReposResponse, error) {
	u, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return nil, err
	}
	d := a.env.GetDBHandle().DB(ctx)
	rows, err := d.Raw(`
		SELECT *
		FROM GitRepositories
		WHERE group_id = ?
		ORDER BY repo_url ASC
	`, u.GetGroupID()).Rows()
	if err != nil {
		return nil, status.InternalErrorf("failed to query repo rows: %s", err)
	}
	res := &ghpb.GetLinkedReposResponse{}
	for rows.Next() {
		var row tables.GitRepository
		if err := d.ScanRows(rows, &row); err != nil {
			return nil, status.InternalErrorf("failed to scan repo row: %s", err)
		}
		res.RepoUrls = append(res.RepoUrls, row.RepoURL)
	}
	if err := rows.Err(); err != nil {
		return nil, status.InternalErrorf("failed to query all repo rows: %s", err)
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
	if err := a.env.GetDBHandle().DB(ctx).Create(repo).Error; err != nil {
		return nil, status.InternalErrorf("failed to link repo: %s", err)
	}

	// Also clean up any associated workflows, since repo linking is meant to
	// replace workflows.
	deleteReq := &wfpb.DeleteWorkflowRequest{
		RequestContext: req.GetRequestContext(),
		RepoUrl:        req.GetRepoUrl(),
	}
	if _, err := a.env.GetWorkflowService().DeleteWorkflow(ctx, deleteReq); err != nil {
		log.Infof("Failed to delete legacy workflow for linked repo: %s", err)
	} else {
		log.Infof("Deleted legacy workflow for linked repo")
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
	result := a.env.GetDBHandle().DB(ctx).Exec(`
		DELETE FROM GitRepositories
		WHERE group_id = ?
		AND repo_url = ?
	`, u.GetGroupID(), req.GetRepoUrl())
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
			log.Debugf("Could not find exact repo match: %s", err)
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
	installationClient, err := a.newInstallationClient(ctx, installationID)
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

func (a *GitHubApp) newInstallationClient(ctx context.Context, installationID int64) (*github.Client, error) {
	token, err := a.createInstallationToken(ctx, installationID)
	if err != nil {
		return nil, err
	}
	return a.newAuthenticatedClient(ctx, token.GetToken())
}

// newAuthenticatedClient returns a GitHub client authenticated with the given
// access token.
func (a *GitHubApp) newAuthenticatedClient(ctx context.Context, accessToken string) (*github.Client, error) {
	if accessToken == "" {
		return nil, status.UnauthenticatedError("missing user access token")
	}
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})
	tc := oauth2.NewClient(ctx, ts)
	return github.NewClient(tc), nil
}

func setURLParam(u *url.URL, key, value string) {
	q := u.Query()
	q.Set(key, value)
	u.RawQuery = q.Encode()
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

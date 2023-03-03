package githubapp

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang-jwt/jwt"
	"github.com/google/go-github/v43/github"
	"golang.org/x/oauth2"

	gh_webhooks "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github"
	ghpb "github.com/buildbuddy-io/buildbuddy/proto/github"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	gh_oauth "github.com/buildbuddy-io/buildbuddy/server/backends/github"
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
	oauthAppPath              = "/auth/github/app/link/"
	postInstallRedirectCookie = "GitHub-App-PostInstall-Redirect"
	stateCookie               = "GitHub-App-Setup-State"
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

	oauthApp *gh_oauth.OAuthApp

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

	oauthApp := gh_oauth.NewOAuthApp(env, *clientID, *clientSecret, oauthAppPath)

	return &GitHubApp{
		env:        env,
		oauthApp:   oauthApp,
		privateKey: privateKey,
	}, nil
}

func (a *GitHubApp) Install(ctx context.Context, req *ghpb.InstallGitHubAppRequest) (*ghpb.InstallGitHubAppResponse, error) {
	if req.GetRepoUrl() != "" {
		norm, err := git.NormalizeRepoURL(req.GetRepoUrl())
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invalid repo URL: %s", err)
		}
		req.RepoUrl = norm.String()
	}

	user, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return nil, err
	}

	u, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	if u.GithubToken == "" {
		return nil, status.UnauthenticatedError("the authenticated user does not have a GitHub account linked")
	}
	dbh := a.env.GetDBHandle().DB(ctx)
	var authorizedInstallationID int64
	if req.GetInstallationId() != 0 {
		// Handle first-time setup flow: register the app installation to
		// the org.
		if err := a.authorizeUserInstallationAccess(ctx, u.GithubToken, req.GetInstallationId()); err != nil {
			return nil, err
		}
		ghInstallation, err := a.getInstallation(ctx, req.GetInstallationId())
		if err != nil {
			return nil, err
		}
		// DO NOT MERGE: The current logic assumes a user will only install the
		// BuildBuddy GH app once, and only use it to access repos associated
		// with one org. Need to decide whether this is OK, and if not, figure
		// out how to manage user installation access. Maybe something like: if
		// it's an org-level installation, link to the BB org; if it's a
		// user-level installation, link to the user BB account (?)

		installation := &tables.GitHubAppInstallation{
			UserID:         user.GetUserID(),
			GroupID:        user.GetGroupID(),
			InstallationID: req.GetInstallationId(),
			Owner:          ghInstallation.GetAccount().GetLogin(),
		}
		if err := a.upsertInstallation(ctx, installation); err != nil {
			return nil, status.InternalErrorf("failed to register installation: %s", err)
		}
		if req.GetRepoUrl() == "" {
			// We're just installing the app, not linking any repos.
			return &ghpb.InstallGitHubAppResponse{}, nil
		}
		authorizedInstallationID = req.GetInstallationId()
	} else {
		owner, _, err := gh_webhooks.ParseOwnerRepo(req.GetRepoUrl())
		if err != nil {
			return nil, status.InvalidArgumentErrorf("failed to parse owner/repo: %s", err)
		}
		installation := &tables.GitHubAppInstallation{
			GroupID: req.GetRequestContext().GetGroupId(),
			Owner:   owner,
		}
		if err := dbh.Where(installation).Take(installation).Error; err != nil {
			if db.IsRecordNotFound(err) {
				// Return NotFound so that the UI can redirect to the install
				// flow via the GitHub UI.
				return nil, status.NotFoundErrorf("app installation was not found")
			}
			return nil, status.InternalErrorf("failed to look up app installation: %s", err)
		}
		// Verify that the user has access to the org's app installation.
		if err := a.authorizeUserInstallationAccess(ctx, u.GithubToken, installation.InstallationID); err != nil {
			return nil, err
		}
		authorizedInstallationID = installation.InstallationID
	}
	if authorizedInstallationID == 0 {
		return nil, status.InternalError("failed to determine app installation ID")
	}

	if req.GetRepoUrl() == "" {
		return nil, status.InvalidArgumentError("a repo URL is required")
	}

	// Make sure that the repo can be accessed by the installation.
	client, err := a.newInstallationClient(ctx, authorizedInstallationID)
	if err != nil {
		return nil, err
	}
	owner, repo, err := gh_webhooks.ParseOwnerRepo(req.GetRepoUrl())
	if err != nil {
		return nil, err
	}
	_, repoRes, err := client.Repositories.Get(ctx, owner, repo)
	if err := checkResponse(repoRes, err); err != nil {
		return nil, status.WrapError(err, "failed to get repository")
	}

	// Link the GitRepository to the org, recording the installation ID.
	permissions := perms.GroupAuthPermissions(req.GetRequestContext().GetGroupId())
	err = dbh.Create(&tables.GitRepository{
		UserID:  permissions.UserID,
		GroupID: permissions.GroupID,
		Perms:   permissions.Perms,
		RepoURL: req.GetRepoUrl(),
	}).Error
	if err != nil {
		return nil, status.InternalErrorf("failed to link GitHub repository: %s", err)
	}

	// Delete the legacy Workflow associated with the repo, if one exists.
	err = dbh.Exec(`
		DELETE FROM Workflows
		WHERE group_id = ? AND repo_url = ?
	`, user.GetGroupID(), req.GetRepoUrl()).Error
	if err != nil {
		return nil, status.InternalErrorf("failed to delete workflow: %s", err)
	}

	return &ghpb.InstallGitHubAppResponse{}, nil
}

func (a *GitHubApp) upsertInstallation(ctx context.Context, installation *tables.GitHubAppInstallation) error {
	if installation.GroupID == "" {
		return status.FailedPreconditionError("installation is missing group_id")
	}
	if installation.Owner == "" {
		return status.FailedPreconditionError("installation is missing owner")
	}
	if installation.InstallationID == 0 {
		return status.FailedPreconditionError("installation is missing installation_id")
	}
	return a.env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
		existing := &tables.GitHubAppInstallation{}
		err := tx.Raw(`
			SELECT *
			FROM GitHubAppInstallations
			WHERE group_id = ? AND owner = ?
			`, installation.GroupID, installation.Owner).Take(existing).Error
		if err != nil {
			if db.IsRecordNotFound(err) {
				existing = nil
			} else {
				return err
			}
		}
		if existing != nil {
			return tx.Exec(`
				UPDATE GitHubAppInstallations
				SET installation_id = ?
				WHERE owner = ?
				`, installation.InstallationID, installation.Owner).Error
		}
		// Installation doesn't exist yet; create it.
		return tx.Create(installation).Error
	})
}

func (a *GitHubApp) GetInstallations(ctx context.Context, req *ghpb.GetGitHubAppInstallationsRequest) (*ghpb.GetGitHubAppInstallationsResponse, error) {
	u, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return nil, err
	}
	db := a.env.GetDBHandle().DB(ctx)
	rows, err := db.Raw(`
		SELECT *
		FROM GitHubAppInstallations
		WHERE group_id = ?
	`, u.GetGroupID()).Rows()
	if err != nil {
		return nil, status.InternalErrorf("failed to query installations: %s", err)
	}
	res := &ghpb.GetGitHubAppInstallationsResponse{}
	ids := map[int64]bool{}
	for rows.Next() {
		row := &tables.GitHubAppInstallation{}
		if err := db.ScanRows(rows, row); err != nil {
			return nil, status.InternalErrorf("failed to scan rows: %s", err)
		}
		res.Installations = append(res.Installations, &ghpb.Installation{
			Id:       row.InstallationID,
			Owner:    row.Owner,
			IsLinked: true,
		})
		ids[row.InstallationID] = true
	}
	if err := rows.Err(); err != nil {
		return nil, status.InternalErrorf("failed to scan rows: %s", err)
	}

	// Also query installations accessible to the user.
	// TODO: parallelize
	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	if tu.GithubToken == "" {
		// No user-installations to look up.
		return res, nil
	}
	client, err := a.newUserClient(ctx, tu.GithubToken)
	if err != nil {
		return nil, err
	}
	installations, rsp, err := client.Apps.ListUserInstallations(ctx, nil /*=opts*/)
	if err := checkResponse(rsp, err); err != nil {
		return nil, err
	}
	for _, inst := range installations {
		if ids[inst.GetID()] {
			continue
		}
		res.Installations = append(res.Installations, &ghpb.Installation{
			Id:       inst.GetID(),
			Owner:    inst.GetAccount().GetLogin(),
			IsLinked: false,
		})
	}

	return res, nil
}

func (a *GitHubApp) getInstallation(ctx context.Context, id int64) (*github.Installation, error) {
	client, err := a.newAppClient(ctx)
	if err != nil {
		return nil, err
	}
	inst, res, err := client.Apps.GetInstallation(ctx, id)
	if err := checkResponse(res, err); err != nil {
		return nil, status.WrapError(err, "failed to get installation")
	}
	return inst, nil
}

func (a *GitHubApp) GetRepoInstallationToken(ctx context.Context, ownerRepo string) (string, error) {
	parts := strings.Split(ownerRepo, "/")
	if len(parts) != 2 {
		return "", status.InvalidArgumentErrorf("invalid owner/repo %q", ownerRepo)
	}
	owner := parts[0]
	installation, err := a.authorizedGroupInstallation(ctx, owner)
	if err != nil {
		return "", err
	}
	return a.createInstallationToken(ctx, installation.InstallationID)
}

func (a *GitHubApp) GetWebhookInstallationToken(ctx context.Context, wd *interfaces.WebhookData) (string, error) {
	norm, err := git.NormalizeRepoURL(wd.TargetRepoURL)
	if err != nil {
		return "", err
	}
	repoURL := norm.String()
	owner, _, err := gh_webhooks.ParseOwnerRepo(repoURL)
	if err != nil {
		return "", err
	}
	row := &struct{ InstallationID int64 }{}
	err = a.env.GetDBHandle().DB(ctx).Raw(`
		SELECT i.installation_id
		FROM GitHubAppInstallations i, GitRepositories r
		WHERE r.repo_url = ?
		AND i.owner = ?
		AND i.group_id = r.group_id
		AND i.installation_id != 0
	`, repoURL, owner).Take(row).Error
	if err != nil {
		return "", err
	}
	return a.createInstallationToken(ctx, row.InstallationID)
}

func (a *GitHubApp) createInstallationToken(ctx context.Context, installationID int64) (string, error) {
	client, err := a.newAppClient(ctx)
	if err != nil {
		return "", err
	}
	t, res, err := client.Apps.CreateInstallationToken(ctx, installationID, nil)
	if err := checkResponse(res, err); err != nil {
		return "", status.UnauthenticatedErrorf("failed to create installation token: %s", status.Message(err))
	}
	return t.GetToken(), nil
}

func (a *GitHubApp) SearchRepos(ctx context.Context, req *ghpb.SearchReposRequest) (*ghpb.SearchReposResponse, error) {
	if req.GetQuery() == "" {
		return nil, status.InvalidArgumentErrorf("query field is required")
	}
	if req.GetOwner() == "" {
		return nil, status.InvalidArgumentError("owner field is required")
	}
	client, err := a.authorizedGroupInstallationClient(ctx, req.GetOwner())
	if err != nil {
		return nil, err
	}
	q := fmt.Sprintf("%q user:%s", req.GetQuery(), req.GetOwner())
	log.Debugf("Search query: %q", q)
	opts := &github.SearchOptions{ListOptions: github.ListOptions{PerPage: 10}}
	searchResult, res, err := client.Search.Repositories(ctx, q, opts)
	if err := checkResponse(res, err); err != nil {
		return nil, err
	}
	response := &ghpb.SearchReposResponse{}
	for _, result := range searchResult.Repositories {
		u, err := git.NormalizeRepoURL(result.GetCloneURL())
		if err != nil {
			return nil, err
		}
		response.Repos = append(response.Repos, &wfpb.Repo{Url: u.String()})
	}
	return response, nil
}

func (a *GitHubApp) GetRepos(ctx context.Context) ([]*tables.GitRepository, error) {
	u, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return nil, err
	}
	q := query_builder.NewQuery(`SELECT * FROM GitRepositories`)
	q.AddWhereClause(`group_id = ?`, u.GetGroupID())
	q.SetOrderBy("repo_url", true /*=ascending*/)
	if err := perms.AddPermissionsCheckToQuery(ctx, a.env, q); err != nil {
		return nil, err
	}
	qStr, qArgs := q.Build()
	dbh := a.env.GetDBHandle().DB(ctx)
	rows, err := dbh.Raw(qStr, qArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*tables.GitRepository
	for rows.Next() {
		tg := &tables.GitRepository{}
		if err := dbh.ScanRows(rows, tg); err != nil {
			return nil, err
		}
		out = append(out, tg)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return out, nil
}

func (a *GitHubApp) GetAccessibleRepos(ctx context.Context, req *wfpb.GetReposRequest) (*wfpb.GetReposResponse, error) {
	return a.getAccessibleRepos(ctx, req.GetOwner())
}

func (a *GitHubApp) getAccessibleRepos(ctx context.Context, owner string) (*wfpb.GetReposResponse, error) {
	// Get installations currently accessible to the user. We don't consult
	// GitHubAppInstallations in the DB here, since it might be out of date due
	// to installations being deleted or suspended.
	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: Maybe return Unauthenticated here so the UI can tell that
	// authentication is needed.
	if tu.GithubToken == "" {
		return &wfpb.GetReposResponse{}, nil
	}
	client, err := a.newUserClient(ctx, tu.GithubToken)
	if err != nil {
		return nil, err
	}
	installations, res, err := client.Apps.ListUserInstallations(ctx, nil)
	if err := checkResponse(res, err); err != nil {
		if res.StatusCode == 401 || res.StatusCode == 403 {
			// Credentials in DB are no longer valid; treat the same as missing
			// creds.
			return &wfpb.GetReposResponse{}, nil
		}
		return nil, err
	}
	if len(installations) == 0 {
		return &wfpb.GetReposResponse{}, nil
	}
	var installation *github.Installation
	if owner != "" {
		for _, in := range installations {
			if in.GetAccount().GetLogin() == owner {
				installation = in
				break
			}
		}
	} else {
		installation = installations[0]
	}
	if installation == nil {
		return nil, status.InvalidArgumentErrorf("could not find BuildBuddy app installation for %q", owner)
	}
	client, err = a.newInstallationClient(ctx, installation.GetID())

	// u, err := perms.AuthenticatedUser(ctx, a.env)
	// if err != nil {
	// 	return nil, err
	// }

	// var client *github.Client
	// var owners []string
	// rows, err := a.env.GetDBHandle().DB(ctx).Raw(`
	// 	SELECT owner, installation_id FROM GitHubAppInstallations
	// 	WHERE group_id = ?
	// 	ORDER BY owner ASC
	// `, u.GetGroupID()).Rows()
	// if err != nil {
	// 	return nil, status.InternalErrorf("failed to query GitHub app installations: %s", err)
	// }
	// for rows.Next() {
	// 	installation := &tables.GitHubAppInstallation{}
	// 	if err := rows.Scan(&installation.Owner, &installation.InstallationID); err != nil {
	// 		return nil, status.InternalErrorf("failed to scan rows: %s", err)
	// 	}
	// 	if client == nil && (owner == "" || installation.Owner == owner) {
	// 		client, err = a.newInstallationClient(ctx, installation.InstallationID)
	// 		if err != nil {
	// 			// The installation might've been suspended or uninstalled.
	// 			// Gracefully handle this case by just skipping the
	// 			// installation.
	// 			log.Warningf("Failed to create GitHub App installation client for %q: %s", installation.Owner, err)
	// 			continue
	// 		}
	// 		owner = installation.Owner
	// 	}
	// 	owners = append(owners, installation.Owner)
	// }
	// if rows.Err() != nil {
	// 	return nil, status.InternalErrorf("failed to query GitHub app installations: %s", err)
	// }

	// TODO: paging
	list, res, err := client.Apps.ListRepos(ctx, &github.ListOptions{PerPage: 50})
	if err := checkResponse(res, err); err != nil {
		return nil, status.WrapError(err, "failed to list repos")
	}
	rr := &wfpb.GetReposResponse{}
	for _, r := range list.Repositories {
		norm, err := git.NormalizeRepoURL(r.GetCloneURL())
		if err != nil {
			return nil, status.InternalErrorf("failed to normalize repo URL: %s", err)
		}
		rr.Repo = append(rr.Repo, &wfpb.Repo{Url: norm.String()})
	}
	for _, in := range installations {
		rr.Owners = append(rr.Owners, in.GetAccount().GetLogin())
	}
	return rr, nil
}

func (a *GitHubApp) handleInstallationEvent(ctx context.Context, event *github.InstallationEvent) error {
	// Only handling uninstall events for now. We proactively handle this event
	// by removing references to the installation ID in the DB. Note, however,
	// that we still need to gracefully handle the case where the installation
	// ID is suddenly no longer valid, since these webhooks aren't 100%
	// reliable.
	if event.GetAction() != "deleted" {
		return nil
	}

	log.Infof("Handling GitHub App uninstall of installation_id=%d", event.GetInstallation().GetID())

	err := a.env.GetDBHandle().DB(ctx).Exec(`
		DELETE FROM GitHubAppInstallations
		WHERE installation_id = ?
	`, event.GetInstallation().GetID()).Error
	if err != nil {
		return err
	}
	err = a.env.GetDBHandle().DB(ctx).Exec(`
		UPDATE GitHubRepositories
		SET installation_id = 0
		WHERE installation_id = ?
	`, event.GetInstallation().GetID()).Error
	if err != nil {
		return err
	}
	return nil
}

func (a *GitHubApp) authorizedGroupInstallationClient(ctx context.Context, owner string) (*github.Client, error) {
	in, err := a.authorizedGroupInstallation(ctx, owner)
	if err != nil {
		return nil, err
	}
	return a.newInstallationClient(ctx, in.InstallationID)
}

func (a *GitHubApp) authorizedGroupInstallation(ctx context.Context, owner string) (*tables.GitHubAppInstallation, error) {
	if owner == "" {
		return nil, status.InvalidArgumentError("missing owner")
	}
	u, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return nil, err
	}
	qb := query_builder.NewQuery(`SELECT * FROM GitHubAppInstallations`)
	qb.AddWhereClause(`group_id = ?`, u.GetGroupID())
	qb.AddWhereClause(`owner = ?`, owner)
	q, args := qb.Build()
	in := &tables.GitHubAppInstallation{}
	if err := a.env.GetDBHandle().DB(ctx).Raw(q, args...).Take(in).Error; err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("app installation not found")
		}
		return nil, status.InternalErrorf("failed to look up app installation: %s", err)
	}
	return in, nil
}

func (a *GitHubApp) GetRepo(ctx context.Context, repoURL string) (*tables.GitRepository, error) {
	// DO NOT MERGE: clarify that this does not do auth checks

	norm, err := git.NormalizeRepoURL(repoURL)
	if err != nil {
		return nil, err
	}
	repoURL = norm.String()
	repo := &tables.GitRepository{RepoURL: repoURL}
	err = a.env.GetDBHandle().DB(ctx).Where(repo).Take(repo).Error
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundErrorf("repo %q not found", repoURL)
		}
		return nil, err
	}
	return repo, nil
}

func (a *GitHubApp) DeleteRepo(ctx context.Context, repoURL string) error {
	u, err := perms.AuthenticatedUser(ctx, a.env)
	if err != nil {
		return err
	}
	norm, err := git.NormalizeRepoURL(repoURL)
	if err != nil {
		return status.InvalidArgumentErrorf("invalid repo URL: %s", err)
	}
	repoURL = norm.String()
	return a.env.GetDBHandle().DB(ctx).Exec(`
		DELETE FROM GitRepositories
		WHERE group_id = ? AND repo_url = ?
	`, u.GetGroupID(), repoURL).Error
}

func (a *GitHubApp) WebhookSecret() string {
	return *webhookSecret
}

func (a *GitHubApp) authorizeUserInstallationAccess(ctx context.Context, userToken string, installationID int64) error {
	const installTimeout = 10 * time.Second
	ctx, cancel := context.WithTimeout(ctx, installTimeout)
	defer cancel()
	client, err := a.newUserClient(ctx, userToken)
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
	return http.HandlerFunc(a.handleOAuth)
}

func (a *GitHubApp) handleOAuth(w http.ResponseWriter, req *http.Request) {
	installationID := req.FormValue("installation_id")
	if installationID != "" {
		state := getCookie(req, stateCookie)
		redirect := getCookie(req, postInstallRedirectCookie)
		if redirect == "" || state == "" || state != req.FormValue("state") {
			// Only respect the redirect URL if the state matches.
			// Otherwise just redirect to the home page.
			redirect = "/"
		}
		setCookie(w, stateCookie, "")
		setCookie(w, postInstallRedirectCookie, "")

		u, err := url.Parse(redirect)
		if err != nil {
			u, _ = url.Parse("/")
		}
		// Append the installation ID param to the redirect URI
		q := u.Query()
		q.Add("installation_id", installationID)
		u.RawQuery = q.Encode()

		w.Header().Add("Location", u.String())
		w.WriteHeader(302)
		return
	}
	a.oauthApp.ServeHTTP(w, req)
}

func (a *GitHubApp) WebhookHandler() http.Handler {
	return http.HandlerFunc(a.handleWebhook)
}

func (a *GitHubApp) InstallHandler() http.Handler {
	return http.HandlerFunc(a.handleInstall)
}

func (a *GitHubApp) handleInstall(w http.ResponseWriter, req *http.Request) {
	// GitHub apps only allow one setup link, but we may have come from some
	// place in the UI like /workflows/new that expects to handle the
	// installation afterwards, e.g. by creating a workflow for the
	// installation. So we store the current redirect URI in a cookie so that
	// the appropriate page can handle the post-install steps.
	setCookie(w, postInstallRedirectCookie, req.FormValue("redirect_url"))
	state := strconv.Itoa(rand.Int())
	setCookie(w, stateCookie, state)
	w.Header().Add("Location", fmt.Sprintf("%s/installations/new?state=%s", *publicLink, state))
	w.WriteHeader(302)
}

// DO NOT MERGE: This is not a thing since the post-install URL is force-set
// to the Oauth2 URL when the GitHub app is also an OAuth app.
func (a *GitHubApp) PostInstallHandler() http.Handler {
	return nil
}

// ServeHTTP serves webhook requests sent by GitHub for the app.
func (a *GitHubApp) handleWebhook(w http.ResponseWriter, req *http.Request) {
	t := github.WebHookType(req)
	log.Infof("GitHub App handling webhook event %q", t)
	if t == "installation" {
		runEventHandler(a, w, req, a.handleInstallationEvent)
		return
	}

	// For all other events, delegate to the workflow service.
	wfs := a.env.GetWorkflowService()
	// Workflows not configured; do nothing
	if wfs == nil {
		w.Write([]byte("OK"))
		return
	}
	wfs.ServeHTTP(w, req)
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
	// Create a GitHub client using the JWT
	return gh_webhooks.NewGitHubClient(ctx, jwtStr), nil
}

func (a *GitHubApp) newInstallationClient(ctx context.Context, installationID int64) (*github.Client, error) {
	token, err := a.createInstallationToken(ctx, installationID)
	if err != nil {
		return nil, err
	}
	return gh_webhooks.NewGitHubClient(ctx, token), nil
}

// newUserClient returns a GitHub client authenticated as the user.
func (a *GitHubApp) newUserClient(ctx context.Context, accessToken string) (*github.Client, error) {
	if accessToken == "" {
		return nil, status.UnauthenticatedError("missing user access token")
	}
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})
	httpClient := oauth2.NewClient(ctx, ts)
	return github.NewClient(httpClient), nil
}

func runEventHandler[T any](a *GitHubApp, w http.ResponseWriter, req *http.Request, handler func(ctx context.Context, event *T) error) {
	b, err := github.ValidatePayload(req, []byte(a.WebhookSecret()))
	if err != nil {
		log.Debugf("Failed to validate webhook payload: %s", err)
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	t := github.WebHookType(req)
	e, err := github.ParseWebHook(t, b)
	if err != nil {
		log.Debugf("Failed to parse webhook payload for %q event: %s", t, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	te, ok := e.(*T)
	if !ok {
		log.Errorf("Unexpected webhook event type %T for event %q", e, t)
		http.Error(w, fmt.Sprintf("unexpected event type %T", e), http.StatusInternalServerError)
		return
	}
	if err := handler(req.Context(), te); err != nil {
		log.Errorf("Failed to handle webhook event %q: %s", t, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
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

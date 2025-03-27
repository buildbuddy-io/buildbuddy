package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/cookie"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-github/v59/github"
)

var (
	statusNameSuffix = flag.String("github.status_name_suffix", "", "Suffix to be appended to all reported GitHub status names. Useful for differentiating BuildBuddy deployments. For example: '(dev)' ** Enterprise only **")
	JwtKey           = flag.String("github.jwt_key", "", "The key to use when signing JWT tokens for github auth.", flag.Secret)
	enterpriseHost   = flag.String("github.enterprise_host", "", "The Github enterprise hostname to use if using GitHub enterprise server, not including https:// and no trailing slash.", flag.Secret)

	// TODO: Mark these deprecated once the new GitHub app is implemented.

	clientID     = flag.String("github.client_id", "", "The client ID of your GitHub Oauth App. ** Enterprise only **")
	clientSecret = flag.String("github.client_secret", "", "The client secret of your GitHub Oauth App. ** Enterprise only **", flag.Secret)
	accessToken  = flag.String("github.access_token", "", "The GitHub access token used to post GitHub commit statuses. ** Enterprise only **", flag.Secret)
)

const (
	// HTTP handler path for the legacy OAuth app flow.
	legacyOAuthAppPath = "/auth/github/link/"

	// GitHub status constants

	ErrorState   State = "error"
	PendingState State = "pending"
	FailureState State = "failure"
	SuccessState State = "success"

	stateCookieName          = "Github-State-Token"
	groupIDCookieName        = "Github-Linked-Group-ID"
	userIDCookieName         = "Github-Linked-User-ID"
	installationIDCookieName = "Github-Linked-Installation-ID"

	tempCookieDuration = 1 * time.Hour

	// Refresh the GitHub token if it's this close to expiring.
	tokenRefreshWindow = 5 * time.Minute
)

func AuthEnabled(env environment.Env) bool {
	return *JwtKey != ""
}

// State represents a status value that GitHub's statuses API understands.
type State string

type GithubStatusPayload = github.RepoStatus

func NewGithubStatusPayload(context, URL, description string, state State) *GithubStatusPayload {
	s := string(state)
	return &GithubStatusPayload{
		Context:     &context,
		TargetURL:   &URL,
		Description: &description,
		State:       &s,
	}
}

type GithubAccessTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	Scope        string `json:"scope"`
	TokenType    string `json:"token_type"`

	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
	ErrorURI         string `json:"error_uri"`
}

type GithubUserResponse struct {
	Login                   string `json:"login"`
	ID                      int    `json:"id"`
	NodeID                  string `json:"node_id"`
	AvatarURL               string `json:"avatar_url"`
	URL                     string `json:"url"`
	HTMLURL                 string `json:"html_url"`
	Type                    string `json:"type"`
	Name                    string `json:"name"`
	Company                 string `json:"company"`
	Location                string `json:"location"`
	Email                   string `json:"email"`
	Bio                     string `json:"bio"`
	CreatedAt               string `json:"created_at"`
	UpdatedAt               string `json:"updated_at"`
	TwoFactorAuthentication bool   `json:"two_factor_authentication"`
}

func (r *GithubAccessTokenResponse) Err() error {
	if r.Error == "" {
		return nil
	}
	return fmt.Errorf("%s", r.ErrorDescription)
}

type GithubClient struct {
	env    environment.Env
	client *http.Client
	oauth  *OAuthHandler

	tokenValue      string
	tokenExpiration *time.Time
	tokenErr        error
}

type githubStatusService struct {
	env environment.Env
}

func NewGitHubStatusService(env environment.Env) *githubStatusService {
	return &githubStatusService{env}
}

func (s *githubStatusService) GetStatusClient(accessToken string) interfaces.GitHubStatusClient {
	return NewGithubClient(s.env, accessToken)
}

func Register(env *real_environment.RealEnv) error {
	githubClient := NewGithubClient(env, "")
	env.GetMux().Handle(
		legacyOAuthAppPath,
		interceptors.WrapAuthenticatedExternalHandler(env, http.HandlerFunc(githubClient.Link)),
	)
	env.SetGitHubStatusService(NewGitHubStatusService(env))
	return nil
}

func NewGithubClient(env environment.Env, token string) *GithubClient {
	return &GithubClient{
		env:        env,
		client:     &http.Client{},
		tokenValue: token,
		oauth:      getLegacyOAuthHandler(env),
	}
}

func getLegacyOAuthHandler(env environment.Env) *OAuthHandler {
	if !IsLegacyOAuthAppEnabled() {
		return nil
	}
	a := NewOAuthHandler(env, *clientID, legacyClientSecret(), legacyOAuthAppPath)
	a.GroupLinkEnabled = true
	// Only enable user-level linking if the new GitHub App is not yet enabled.
	a.UserLinkEnabled = env.GetGitHubApp() == nil
	return a
}

func legacyClientSecret() string {
	if cs := os.Getenv("BB_GITHUB_CLIENT_SECRET"); cs != "" {
		return cs
	}
	return *clientSecret
}

func IsLegacyOAuthAppEnabled() bool {
	if *clientID == "" || legacyClientSecret() == "" {
		return false
	}
	return true
}

func (c *GithubClient) Link(w http.ResponseWriter, r *http.Request) {
	if c.oauth == nil {
		redirectWithError(w, r, status.PermissionDeniedError("Missing GitHub config"))
		return
	}
	c.oauth.ServeHTTP(w, r)
}

// OAuthHandler implements the OAuth HTTP authentication flow for GitHub OAuth
// apps.
type OAuthHandler struct {
	env environment.Env

	// ClientID is the OAuth client ID.
	ClientID string

	// ClientSecret is the OAuth client secret.
	ClientSecret string

	// Path is the HTTP URL path that handles the OAuth flow.
	Path string

	// UserLinkEnabled specifies whether the OAuth app should associate
	// access tokens with the authenticated user.
	UserLinkEnabled bool

	// GroupLinkEnabled specifies whether the OAuth app should associate
	// access tokens with the authenticated group.
	GroupLinkEnabled bool

	// InstallURL is the GitHub app install URL. Only set for GitHub Apps.
	InstallURL string

	// HandleInstall handles a request to install the GitHub App.
	// setupAction is either "install" or "update".
	HandleInstall func(ctx context.Context, groupID, setupAction string, installationID int64) (redirect string, err error)
}

func NewOAuthHandler(env environment.Env, clientID, clientSecret, path string) *OAuthHandler {
	return &OAuthHandler{
		env:             env,
		ClientID:        clientID,
		ClientSecret:    clientSecret,
		Path:            path,
		UserLinkEnabled: true,
	}
}

func (c *OAuthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, err := c.env.GetAuthenticator().AuthenticatedUser(r.Context())
	if err != nil {
		// If not logged in to the app (e.g. when installing directly from
		// GitHub), redirect to the account creation flow.
		loginURL := fmt.Sprintf("/auth/github/?%s", r.URL.RawQuery)
		if *JwtKey == "" {
			loginURL = fmt.Sprintf("/?redirect_url=%s", url.QueryEscape(r.URL.String()))
		}
		http.Redirect(w, r, loginURL, http.StatusTemporaryRedirect)
		return
	}

	// GitHub redirected back to us with an error; handle that here.
	if errDesc := r.FormValue("error_description"); errDesc != "" {
		redirectWithError(w, r, status.PermissionDeniedErrorf("GitHub redirected back with error: %s", errDesc))
		return
	}

	// If code or installation_id is set, this URL was hit as a callback from GitHub.
	githubCallback := r.FormValue("code") != "" || r.FormValue("installation_id") != ""
	if !githubCallback {
		if r.FormValue("install") == "true" && c.InstallURL != "" {
			c.handleInstallApp(w, r)
		} else {
			c.HandleLinkRepo(w, r, c.Path)
		}
		return
	}

	// Verify "state" cookie matches if present
	// Note: It won't be set for GitHub-initiated app installations
	if _, err := validateState(r); err != nil {
		redirectWithError(w, r, err)
		return
	}

	if code := r.FormValue("code"); code != "" {
		if err := c.handleLinkRepoCallback(r); err != nil {
			redirectWithError(w, r, status.WrapError(err, "failed to exchange OAuth code for access token"))
			return
		}
	}

	// Handle new app installation.
	// Note, during the "install & authorize" flow, both the OAuth "code" param
	// and "installation_id" param will be set.
	if installationID := r.FormValue("installation_id"); installationID != "" {
		redirected, err := c.handleInstallAppCallback(w, r, installationID)
		if err != nil {
			redirectWithError(w, r, status.WrapError(err, "could not complete installation"))
			return
		}
		if redirected {
			return
		}
	}

	appRedirectURL := getState(r, cookie.RedirCookie)
	if appRedirectURL == "" {
		appRedirectURL = "/"
	}

	http.Redirect(w, r, appRedirectURL, http.StatusTemporaryRedirect)
}

// HandleLinkRepo handles the case where the user has never connected their
// GitHub repo to BuildBuddy. This will take them to an oauth flow in GitHub.
//
// This flow will create a connection between a GitHub user and BuildBuddy user.
// It will generate a GitHub token that a BuildBuddy user can use for GitHub operations.
// It will not grant specific repo permissions. That must be separately granted
// via the `handleInstallApp` flow, though this oauth step is a prerequisite.
//
// GitHub will redirect back to the callback URL set in the app settings (something like
// auth/github/app/link or auth/github/read_only_app/link) with the "code" query param set.
// This subsequent request should be handled with handleLinkRepoCallback
func (c *OAuthHandler) HandleLinkRepo(w http.ResponseWriter, r *http.Request, redirectPath string) {
	state := fmt.Sprintf("%d", random.RandUint64())
	if !setCookies(w, r, state) {
		return
	}

	authURL := fmt.Sprintf(
		"https://%s/login/oauth/authorize?client_id=%s&state=%s&redirect_uri=%s&scope=%s",
		GithubHost(),
		c.ClientID,
		state,
		url.QueryEscape(build_buddy_url.WithPath(redirectPath).String()),
		"repo")
	http.Redirect(w, r, authURL, http.StatusTemporaryRedirect)
}

func setCookies(w http.ResponseWriter, r *http.Request, state string) bool {
	userID := r.FormValue("user_id")
	groupID := r.FormValue("group_id")
	redirectURL := r.FormValue("redirect_url")
	if err := build_buddy_url.ValidateRedirect(redirectURL); err != nil {
		redirectWithError(w, r, err)
		return false
	}
	expiry := time.Now().Add(tempCookieDuration)
	cookie.SetCookie(w, stateCookieName, state, expiry, true)
	cookie.SetCookie(w, userIDCookieName, userID, expiry, true)
	cookie.SetCookie(w, groupIDCookieName, groupID, expiry, true)
	cookie.SetCookie(w, cookie.RedirCookie, redirectURL, expiry, true)
	return true
}

// handleLinkRepoCallback handles an oauth callback from GitHub.
//
// After a user has authenticated through GitHub's oauth flow (triggered by `HandleLinkRepo`),
// GitHub will redirect back to the callback URL set in the app settings with the "code" query param set.
// This method exchanges that OAuth code for an access token and links the
// access token to either the authenticated group ID or authenticated user ID,
// depending on the state of the OAuth flow.
//
// Note: if the state param is set, it is assumed to be pre-validated against
// the state cookie.
func (c *OAuthHandler) handleLinkRepoCallback(r *http.Request) error {
	ctx := r.Context()
	state, err := validateState(r)
	if err != nil {
		return err
	}

	userID := getState(r, userIDCookieName)
	groupID := getState(r, groupIDCookieName)

	u, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	// If no user ID state (app install flow) then use the authenticated user
	// ID.
	if userID == "" && state == "" {
		userID = u.GetUserID()
	}
	if (groupID == "" || !c.GroupLinkEnabled) && (userID == "" || !c.UserLinkEnabled) {
		return status.FailedPreconditionError("missing group_id or user_id URL params")
	}

	accessToken, err := c.Exchange(r)
	if err != nil {
		return err
	}

	// Associate the token with the org (legacy OAuth app only).
	if groupID != "" && c.GroupLinkEnabled {
		if err := authutil.AuthorizeOrgAdmin(u, groupID); err != nil {
			return status.WrapError(err, "failed to link GitHub account: role not authorized")
		}
		log.Infof("Linking GitHub account for group %s", groupID)
		err = c.env.GetDBHandle().NewQuery(ctx, "github_set_group_token").Raw(
			`UPDATE "Groups" SET github_token = ? WHERE group_id = ?`,
			accessToken, groupID).Exec().Error
		if err != nil {
			return status.PermissionDeniedErrorf("error linking github account to group: %v", err)
		}
	}

	// Restore user ID from state cookie.
	if userID != "" && c.UserLinkEnabled {
		if userID != u.GetUserID() {
			return status.PermissionDeniedErrorf("user ID unexpectedly changed to %s while authenticating with GitHub", userID)
		}
		log.Infof("Linking GitHub account for user %s", userID)
		err = c.env.GetDBHandle().NewQuery(ctx, "github_set_user_token").Raw(
			`UPDATE "Users" SET github_token = ? WHERE user_id = ?`,
			accessToken, userID).Exec().Error
		if err != nil {
			return status.PermissionDeniedErrorf("Error linking github account to user: %v", err)
		}
	}

	return nil
}

func (c *OAuthHandler) Exchange(r *http.Request) (string, error) {
	state, err := validateState(r)
	if err != nil {
		return "", err
	}

	code := r.FormValue("code")
	if code == "" {
		return "", status.PermissionDeniedErrorf("no auth code set for github oauth exchange")
	}

	client := &http.Client{}
	url := fmt.Sprintf(
		"https://%s/login/oauth/access_token?client_id=%s&client_secret=%s&code=%s&state=%s",
		GithubHost(),
		c.ClientID,
		c.ClientSecret,
		code,
		state)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return "", status.PermissionDeniedErrorf("failed to create POST request: %s", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", status.UnavailableErrorf("access token request failed: %s", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", status.UnavailableErrorf("failed to read response: %s", err)
	}
	if resp.StatusCode != 200 {
		return "", status.UnknownErrorf("access token request failed: HTTP %d: %s", resp.StatusCode, string(body))
	}

	var accessTokenResponse GithubAccessTokenResponse
	if err := json.Unmarshal(body, &accessTokenResponse); err != nil {
		return "", status.WrapError(err, "failed to unmarshal GitHub access token response")
	}
	if err := accessTokenResponse.Err(); err != nil {
		return "", status.PermissionDeniedErrorf("GitHub returned error: %s", err)
	}
	if accessTokenResponse.RefreshToken != "" {
		// TODO: Support refresh token.
		log.Warningf("GitHub refresh tokens are currently unsupported. To fix this error, opt out of user-to-server token expiration in GitHub app settings.")
		return "", status.PermissionDeniedErrorf("OAuth token exchange failed: response included unsupported refresh token")
	}

	return accessTokenResponse.AccessToken, nil
}

// handleInstallApp takes the user to the GitHub flow to install an app.
//
// Installing an app will grant access to specific repos. Note that granting oauth
// access (via `HandleLinkRepo`) is a pre-requisite.
//
// GitHub will redirect back to the callback URL set in the app settings (something like
// auth/github/app/link or auth/github/read_only_app/link) with the "installation_id" query param set.
// This subsequent request should be handled with handleInstallAppCallback.
func (c *OAuthHandler) handleInstallApp(w http.ResponseWriter, r *http.Request) {
	if c.InstallURL == "" {
		redirectWithError(w, r, status.InternalErrorf("no install URL for github oauth handler %s", c.ClientID))
		return
	} else if r.FormValue("install") != "true" {
		redirectWithError(w, r, status.InternalErrorf("unexpected install parameter not set"))
		return
	}

	state := fmt.Sprintf("%d", random.RandUint64())
	if !setCookies(w, r, state) {
		return
	}

	authURL := fmt.Sprintf("%s?state=%s", c.InstallURL, state)
	http.Redirect(w, r, authURL, http.StatusTemporaryRedirect)
}

// handleInstallAppCallback handles an app installation callback from GitHub.
//
// After a user has installed an app in GitHub (via `handleInstallApp`),
// GitHub will redirect back to the callback URL set in the app settings with the
// "installation_id" query param set.
//
// This method creates a record of the app installation in the BB database.
//
// Note: if the state param is set, it is assumed to be pre-validated against
// the state cookie.
func (c *OAuthHandler) handleInstallAppCallback(w http.ResponseWriter, r *http.Request, rawID string) (redirected bool, err error) {
	installationID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		redirectWithError(w, r, status.InvalidArgumentErrorf("invalid installation_id %q", r.FormValue("installation_id")))
		return
	}
	if c.HandleInstall == nil {
		return false, status.InternalError("app installation is not supported")
	}
	redirect, err := c.HandleInstall(r.Context(), getState(r, groupIDCookieName), r.FormValue("setup_action"), installationID)
	if err != nil {
		return false, err
	}
	// Set a client readable cookie with installation id, so it knows which installation was most recently installed.
	cookie.SetCookie(w, installationIDCookieName, rawID, time.Now().Add(tempCookieDuration), false)
	if redirect != "" {
		http.Redirect(w, r, redirect, http.StatusTemporaryRedirect)
		return true, nil
	}
	return false, nil
}

func (c *GithubClient) CreateStatus(ctx context.Context, ownerRepo string, commitSHA string, payload *GithubStatusPayload) error {
	if ownerRepo == "" {
		return status.InvalidArgumentErrorf("failed to create GitHub status: ownerRepo argument is empty")
	}
	if commitSHA == "" {
		return status.InvalidArgumentError("failed to create GitHub status: commitSHA argument is empty")
	}

	token, err := c.getToken(ctx, ownerRepo)
	if err != nil {
		return status.WrapErrorf(err, "failed to populate GitHub token")
	}

	url := fmt.Sprintf("https://%s/repos/%s/statuses/%s", apiEndpoint(), ownerRepo, commitSHA)
	body := new(bytes.Buffer)
	if err := json.NewEncoder(body).Encode(appendStatusNameSuffix(payload)); err != nil {
		return status.UnknownErrorf("failed to encode payload: %s", err)
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return status.InternalErrorf("failed to create request: %s", err)
	}

	req.Header.Set("Authorization", "token "+token)
	res, err := c.client.Do(req)
	if err != nil {
		return status.UnavailableErrorf("failed to send request: %s", err)
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		b, err := io.ReadAll(res.Body)
		if err != nil {
			return status.UnknownErrorf("HTTP %s: <failed to read response body>", res.Status)
		}
		return status.UnknownErrorf("HTTP %s: %q", res.Status, string(b))
	}
	log.CtxInfof(ctx, "Successfully posted GitHub status for %q @ commit %q: %q (%s): %q", ownerRepo, commitSHA, payload.GetContext(), payload.GetState(), payload.GetDescription())
	return nil
}

func (c *GithubClient) getAppInstallationToken(ctx context.Context, ownerRepo string) (*github.InstallationToken, error) {
	app := c.env.GetGitHubApp()
	if app == nil {
		return nil, nil
	}
	parts := strings.Split(ownerRepo, "/")
	if len(parts) != 2 {
		return nil, status.InvalidArgumentErrorf("invalid owner/repo %q", ownerRepo)
	}
	return app.GetInstallationTokenForStatusReportingOnly(ctx, parts[0])
}

func (c *GithubClient) getToken(ctx context.Context, ownerRepo string) (string, error) {
	// If we've already tried fetching the token and it failed, don't try
	// again.
	if c.tokenErr != nil {
		return "", c.tokenErr
	}
	// If we already fetched a token and it is not yet expired, return it.
	if c.tokenValue != "" && c.tokenExpiration != nil && time.Now().Before((*c.tokenExpiration).Add(-tokenRefreshWindow)) {
		return c.tokenValue, nil
	}
	// Token has either not yet been fetched or it's expired; fetch it.
	if err := c.fetchToken(ctx, ownerRepo); err != nil {
		c.tokenErr = err
		return "", err
	}
	// Sanity check that it's not empty.
	if c.tokenValue == "" {
		c.tokenErr = status.InternalError("failed to fetch GitHub token")
		return "", c.tokenErr
	}
	return c.tokenValue, nil
}

func (c *GithubClient) fetchToken(ctx context.Context, ownerRepo string) error {
	// Reset state
	c.tokenValue = ""
	c.tokenExpiration = nil
	c.tokenErr = nil

	// Prefer fetching app installation token
	installationToken, err := c.getAppInstallationToken(ctx, ownerRepo)
	if err != nil {
		log.CtxInfof(ctx, "Failed to look up app installation token; falling back to legacy OAuth lookup: %s", err)
	}
	if installationToken != nil {
		c.tokenValue = installationToken.GetToken()
		if installationToken.ExpiresAt != nil {
			t := installationToken.ExpiresAt.Time
			c.tokenExpiration = &t
		}
		return nil
	}

	// Fall back to token specified via flag if configured
	if *accessToken != "" {
		c.tokenValue = *accessToken
		return nil
	}

	// Fall back to legacy org-linked OAuth token if enabled
	if !IsLegacyOAuthAppEnabled() {
		return nil
	}

	dbHandle := c.env.GetDBHandle()
	if dbHandle == nil {
		return nil
	}

	userInfo, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if userInfo == nil || err != nil {
		return nil
	}

	var group tables.Group
	err = dbHandle.NewQuery(ctx, "github_get_token").Raw(`SELECT github_token FROM "Groups" WHERE group_id = ?`,
		userInfo.GetGroupID()).Take(&group)
	if err != nil {
		return err
	}

	if group.GithubToken != nil {
		c.tokenValue = *group.GithubToken
	}
	return nil
}

func appendStatusNameSuffix(p *GithubStatusPayload) *GithubStatusPayload {
	if *statusNameSuffix == "" {
		return p
	}
	name := fmt.Sprintf("%s %s", p.GetContext(), *statusNameSuffix)
	return NewGithubStatusPayload(name, p.GetTargetURL(), p.GetDescription(), State(p.GetState()))
}

func getState(r *http.Request, key string) string {
	if r.FormValue("state") == "" || r.FormValue("state") != cookie.GetCookie(r, stateCookieName) {
		return ""
	}
	c, err := r.Cookie(key)
	if err != nil {
		return ""
	}
	return c.Value
}

func validateState(r *http.Request) (string, error) {
	state := r.FormValue("state")
	// "state" param won't be set for GitHub-initiated installations; this is
	// valid.
	if state == "" {
		return "", nil
	}
	if state != cookie.GetCookie(r, stateCookieName) {
		return "", status.InvalidArgumentErrorf("OAuth state mismatch: URL param %q does not match cookie value %q", state, cookie.GetCookie(r, stateCookieName))
	}
	return state, nil
}

func redirectWithError(w http.ResponseWriter, r *http.Request, err error) {
	log.Warning(err.Error())
	errorParam := err.Error()
	redirectURL := &url.URL{Path: "/"}
	// Respect the original redirect_url parameter that was set when initiating
	// the flow.
	if s := getState(r, cookie.RedirCookie); s != "" {
		if u, err := url.Parse(s); err == nil {
			redirectURL = u
		}
	}
	q := redirectURL.Query()
	q.Set("error", errorParam)
	redirectURL.RawQuery = q.Encode()
	http.Redirect(w, r, redirectURL.String(), http.StatusTemporaryRedirect)
}

func GetUserInfo(token string) (*GithubUserResponse, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://%s/user", apiEndpoint()), nil)
	if err != nil {
		return nil, status.InternalErrorf("failed to create request: %s", err)
	}

	req.Header.Set("Authorization", "token "+token)
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to send request: %s", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to read response: %s", err)
	}
	if res.StatusCode != 200 {
		return nil, status.UnknownErrorf("access token request failed: HTTP %d: %s", res.StatusCode, string(body))
	}
	var userResponse GithubUserResponse
	if err := json.Unmarshal(body, &userResponse); err != nil {
		return nil, status.WrapErrorf(err, "failed to unmarshal GitHub user response: %+v", string(body))
	}
	return &userResponse, nil
}

func IsEnterpriseConfigured() bool {
	return *enterpriseHost != ""
}

func GithubHost() string {
	if IsEnterpriseConfigured() {
		return *enterpriseHost
	}
	return "github.com"
}

func apiEndpoint() string {
	if IsEnterpriseConfigured() {
		return *enterpriseHost + "/api/v3"
	}
	return "api.github.com"
}

package github

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/cookie"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	burl "github.com/buildbuddy-io/buildbuddy/server/util/url"
)

var (
	statusNameSuffix = flag.String("github.status_name_suffix", "", "Suffix to be appended to all reported GitHub status names. Useful for differentiating BuildBuddy deployments. For example: '(dev)' ** Enterprise only **")
	JwtKey           = flagutil.New("github.jwt_key", "", "The key to use when signing JWT tokens for github auth.", flagutil.SecretTag)

	// TODO: Mark these deprecated once the new GitHub app is implemented.

	clientID     = flag.String("github.client_id", "", "The client ID of your GitHub Oauth App. ** Enterprise only **")
	clientSecret = flagutil.New("github.client_secret", "", "The client secret of your GitHub Oauth App. ** Enterprise only **", flagutil.SecretTag)
	accessToken  = flagutil.New("github.access_token", "", "The GitHub access token used to post GitHub commit statuses. ** Enterprise only **", flagutil.SecretTag)
)

const (
	// HTTP handler path for the legacy OAuth app flow.
	legacyOAuthAppPath = "/auth/github/link/"

	// GitHub status constants

	ErrorState   State = "error"
	PendingState State = "pending"
	FailureState State = "failure"
	SuccessState State = "success"

	stateCookieName   = "Github-State-Token"
	groupIDCookieName = "Github-Linked-Group-ID"
	userIDCookieName  = "Github-Linked-User-ID"
)

// State represents a status value that GitHub's statuses API understands.
type State string

type GithubStatusPayload struct {
	State       State  `json:"state"`
	TargetURL   string `json:"target_url"`
	Description string `json:"description"`
	Context     string `json:"context"`
}

func NewGithubStatusPayload(context, URL, description string, state State) *GithubStatusPayload {
	return &GithubStatusPayload{
		Context:     context,
		TargetURL:   URL,
		Description: description,
		State:       state,
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
	env         environment.Env
	client      *http.Client
	oauth       *OAuthHandler
	githubToken string
	tokenLookup sync.Once
}

func Register(env environment.Env) error {
	githubClient := NewGithubClient(env, "")
	env.GetMux().Handle(
		legacyOAuthAppPath,
		interceptors.WrapAuthenticatedExternalHandler(env, http.HandlerFunc(githubClient.Link)),
	)
	return nil
}

func NewGithubClient(env environment.Env, token string) *GithubClient {
	return &GithubClient{
		env:         env,
		client:      &http.Client{},
		githubToken: token,
		oauth:       getLegacyOAuthHandler(env),
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

func (c *OAuthHandler) StartAuthFlow(w http.ResponseWriter, r *http.Request, redirectPath string) {
	state := fmt.Sprintf("%d", random.RandUint64())
	userID := r.FormValue("user_id")
	groupID := r.FormValue("group_id")
	redirectURL := r.FormValue("redirect_url")
	if err := burl.ValidateRedirect(c.env, redirectURL); err != nil {
		redirectWithError(w, r, err)
		return
	}
	setCookie(w, stateCookieName, state)
	setCookie(w, userIDCookieName, userID)
	setCookie(w, groupIDCookieName, groupID)
	setCookie(w, cookie.RedirCookie, redirectURL)

	var authURL string
	if r.FormValue("install") == "true" && c.InstallURL != "" {
		authURL = fmt.Sprintf("%s?state=%s", c.InstallURL, state)
	} else {
		authURL = fmt.Sprintf(
			"https://github.com/login/oauth/authorize?client_id=%s&state=%s&redirect_uri=%s&scope=%s",
			c.ClientID,
			state,
			url.QueryEscape(build_buddy_url.WithPath(redirectPath).String()),
			"repo")
	}

	http.Redirect(w, r, authURL, http.StatusTemporaryRedirect)
}

func (c *OAuthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, err := perms.AuthenticatedUser(r.Context(), c.env)
	if err != nil {
		// If not logged in to the app (e.g. when installing directly from
		// GitHub), redirect to the account creation flow.
		loginURL := fmt.Sprintf("/auth/github/?" + r.URL.RawQuery)
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

	// If we are missing either the OAuth code or app installation ID, start the
	// OAuth flow.
	if r.FormValue("code") == "" && r.FormValue("installation_id") == "" {
		c.StartAuthFlow(w, r, c.Path)
		return
	}

	// Verify "state" cookie matches if present
	// Note: It won't be set for GitHub-initiated app installations
	if _, err := validateState(r); err != nil {
		redirectWithError(w, r, err)
		return
	}

	if code := r.FormValue("code"); code != "" {
		if err := c.requestAccessToken(r, code); err != nil {
			redirectWithError(w, r, status.WrapError(err, "failed to exchange OAuth code for access token"))
			return
		}
	}

	// Handle new app installation.
	// Note, during the "install & authorize" flow, both the OAuth "code" param
	// and "installation_id" param will be set.
	if installationID := r.FormValue("installation_id"); installationID != "" {
		redirected, err := c.handleInstallation(w, r, installationID)
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

// requestAccessToken exchanges an OAuth code for an access token and links the
// access token to either the authenticated group ID or authenticated user ID,
// depending on the state of the OAuth flow.
//
// Note: if the state param is set, it is assumed to be pre-validated against
// the state cookie.
func (c *OAuthHandler) requestAccessToken(r *http.Request, code string) error {
	ctx := r.Context()
	state, err := validateState(r)
	if err != nil {
		return err
	}

	userID := getState(r, userIDCookieName)
	groupID := getState(r, groupIDCookieName)

	u, err := perms.AuthenticatedUser(ctx, c.env)
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
		if err := authutil.AuthorizeGroupRole(u, groupID, role.Admin); err != nil {
			return status.WrapError(err, "failed to link GitHub account: role not authorized")
		}
		log.Infof("Linking GitHub account for group %s", groupID)
		err = c.env.GetDBHandle().DB(ctx).Exec(
			`UPDATE "Groups" SET github_token = ? WHERE group_id = ?`,
			accessToken, groupID).Error
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
		err = c.env.GetDBHandle().DB(ctx).Exec(
			`UPDATE "Users" SET github_token = ? WHERE user_id = ?`,
			accessToken, userID).Error
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
		"https://github.com/login/oauth/access_token?client_id=%s&client_secret=%s&code=%s&state=%s",
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

func (c *OAuthHandler) handleInstallation(w http.ResponseWriter, r *http.Request, rawID string) (redirected bool, err error) {
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

	var err error
	c.tokenLookup.Do(func() {
		err = c.populateTokenIfNecessary(ctx, ownerRepo)
	})
	if err != nil {
		return status.WrapErrorf(err, "failed to populate GitHub token")
	}

	// If we don't have a github token, we can't post a status.
	if c.githubToken == "" {
		return status.UnknownError("failed to populate GitHub token")
	}

	url := fmt.Sprintf("https://api.github.com/repos/%s/statuses/%s", ownerRepo, commitSHA)
	body := new(bytes.Buffer)
	if err := json.NewEncoder(body).Encode(appendStatusNameSuffix(payload)); err != nil {
		return status.UnknownErrorf("failed to encode payload: %s", err)
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return status.InternalErrorf("failed to create request: %s", err)
	}

	req.Header.Set("Authorization", "token "+c.githubToken)
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
	log.CtxInfof(ctx, "Successfully posted GitHub status for %q @ commit %q: %q (%s): %q", ownerRepo, commitSHA, payload.Context, payload.State, payload.Description)
	return nil
}

func (c *GithubClient) getAppInstallationToken(ctx context.Context, ownerRepo string) (string, error) {
	app := c.env.GetGitHubApp()
	if app == nil {
		return "", nil
	}
	parts := strings.Split(ownerRepo, "/")
	if len(parts) != 2 {
		return "", status.InvalidArgumentErrorf("invalid owner/repo %q", ownerRepo)
	}
	return app.GetInstallationTokenForStatusReportingOnly(ctx, parts[0])
}

func (c *GithubClient) populateTokenIfNecessary(ctx context.Context, ownerRepo string) error {
	if c.githubToken != "" {
		return nil
	}

	installationToken, err := c.getAppInstallationToken(ctx, ownerRepo)
	if err != nil {
		log.CtxInfof(ctx, "Failed to look up app installation token; falling back to legacy OAuth lookup: %s", err)
	}
	if installationToken != "" {
		c.githubToken = installationToken
		return nil
	}

	if *accessToken != "" {
		c.githubToken = *accessToken
		return nil
	}

	if !IsLegacyOAuthAppEnabled() {
		return nil
	}

	auth := c.env.GetAuthenticator()
	dbHandle := c.env.GetDBHandle()
	if auth == nil || dbHandle == nil {
		return nil
	}

	userInfo, err := auth.AuthenticatedUser(ctx)
	if userInfo == nil || err != nil {
		return nil
	}

	var group tables.Group
	err = dbHandle.DB(ctx).Raw(`SELECT github_token FROM "Groups" WHERE group_id = ?`,
		userInfo.GetGroupID()).First(&group).Error
	if err != nil {
		return err
	}

	if group.GithubToken != nil {
		c.githubToken = *group.GithubToken
	}
	return nil
}

func appendStatusNameSuffix(p *GithubStatusPayload) *GithubStatusPayload {
	if *statusNameSuffix == "" {
		return p
	}
	name := fmt.Sprintf("%s %s", p.Context, *statusNameSuffix)
	return NewGithubStatusPayload(name, p.TargetURL, p.Description, p.State)
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

func getState(r *http.Request, key string) string {
	if r.FormValue("state") == "" || r.FormValue("state") != getCookie(r, stateCookieName) {
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
	if state != getCookie(r, stateCookieName) {
		return "", status.InvalidArgumentErrorf("OAuth state mismatch: URL param %q does not match cookie value %q", state, getCookie(r, stateCookieName))
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
	req, err := http.NewRequest("GET", "https://api.github.com/user", nil)
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

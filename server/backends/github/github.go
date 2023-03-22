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
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	burl "github.com/buildbuddy-io/buildbuddy/server/util/url"
)

var (
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

	stateCookieName    = "Github-State-Token"
	redirectCookieName = "Github-Redirect-Url"
	groupIDCookieName  = "Github-Linked-Group-ID"
	userIDCookieName   = "Github-Linked-User-ID"
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
	// TODO: If the new GitHub App is enabled, disable user-level token linking
	// for the legacy OAuth app (since the new GitHub App will handle user-level
	// linking).
	return a
}

func legacyClientSecret() string {
	if cs := os.Getenv("BB_GITHUB_CLIENT_SECRET"); cs != "" {
		return cs
	}
	return *clientSecret
}

func IsLegacyOAuthAppEnabled() bool {
	if *clientID == "" && legacyClientSecret() == "" && *accessToken == "" {
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
	// If we don't have a state yet parameter, start oauth flow.
	if r.FormValue("state") == "" {
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
		setCookie(w, redirectCookieName, redirectURL)

		url := fmt.Sprintf(
			"https://github.com/login/oauth/authorize?client_id=%s&state=%s&redirect_uri=%s&scope=%s",
			c.ClientID,
			state,
			url.QueryEscape(build_buddy_url.WithPath(c.Path).String()),
			"repo")
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
		return
	}

	// GitHub redirected back to us with an error.
	if errDesc := r.FormValue("error_description"); errDesc != "" {
		redirectWithError(w, r, status.PermissionDeniedErrorf("GitHub redirected back with error: %s", errDesc))
		return
	}

	// Verify "state" cookie matches
	state := r.FormValue("state")
	if state != getCookie(r, stateCookieName) {
		redirectWithError(w, r, status.PermissionDeniedErrorf("GitHub link state mismatch: %s != %s", r.FormValue("state"), getCookie(r, stateCookieName)))
		return
	}
	redirectURL := r.FormValue("redirect_uri")
	if err := burl.ValidateRedirect(c.env, redirectURL); err != nil {
		redirectWithError(w, r, err)
		return
	}
	code := r.FormValue("code")

	client := &http.Client{}
	url := fmt.Sprintf(
		"https://github.com/login/oauth/access_token?client_id=%s&client_secret=%s&code=%s&state=%s&redirect_uri=%s",
		c.ClientID,
		c.ClientSecret,
		code,
		state,
		url.QueryEscape(redirectURL))

	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		redirectWithError(w, r, status.PermissionDeniedErrorf("Error creating request: %s", err.Error()))
		return
	}

	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		redirectWithError(w, r, status.PermissionDeniedErrorf("Error getting access token: %s", err.Error()))
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		redirectWithError(w, r, status.PermissionDeniedErrorf("Error reading GitHub response: %s", err.Error()))
		return
	}
	if resp.StatusCode != 200 {
		redirectWithError(w, r, status.UnknownErrorf("Error getting access token: HTTP %d: %s", resp.StatusCode, string(body)))
		return
	}

	var accessTokenResponse GithubAccessTokenResponse
	if err := json.Unmarshal(body, &accessTokenResponse); err != nil {
		redirectWithError(w, r, status.WrapError(err, "Failed to unmarshal GitHub access token response"))
		return
	}
	if err := accessTokenResponse.Err(); err != nil {
		redirectWithError(w, r, status.PermissionDeniedErrorf("OAuth token exchange failed: %s", err))
		return
	}
	if accessTokenResponse.RefreshToken != "" {
		// TODO: Support refresh token.
		log.Warningf("GitHub refresh tokens are currently unsupported. To fix this error, opt out of user-to-server token expiration in GitHub app settings.")
		redirectWithError(w, r, status.PermissionDeniedErrorf("OAuth token exchange failed: response included unsupported refresh token"))
		return
	}

	u, err := perms.AuthenticatedUser(r.Context(), c.env)
	if err != nil {
		redirectWithError(w, r, status.WrapError(err, "Failed to link GitHub account: could not authenticate user"))
		return
	}

	dbHandle := c.env.GetDBHandle()
	if dbHandle == nil {
		redirectWithError(w, r, status.PermissionDeniedError("No database configured"))
		return
	}

	// Restore group ID from cookie.
	groupID := getCookie(r, groupIDCookieName)
	// Associate the token with the org (legacy OAuth app only).
	if groupID != "" && c.GroupLinkEnabled {
		if err := authutil.AuthorizeGroupRole(u, groupID, role.Admin); err != nil {
			redirectWithError(w, r, status.WrapError(err, "Failed to link GitHub account: role not authorized"))
			return
		}
		log.Infof("Linking GitHub account for group %s", groupID)
		err = dbHandle.DB(r.Context()).Exec(
			`UPDATE `+"`Groups`"+` SET github_token = ? WHERE group_id = ?`,
			accessTokenResponse.AccessToken, groupID).Error
		if err != nil {
			redirectWithError(w, r, status.PermissionDeniedErrorf("Error linking github account to group: %v", err))
			return
		}
	}

	// Restore user ID from cookie.
	userID := getCookie(r, userIDCookieName)
	if userID != "" && c.UserLinkEnabled {
		if userID != u.GetUserID() {
			redirectWithError(w, r, status.PermissionDeniedErrorf("Invalid user: %v", userID))
			return
		}
		log.Infof("Linking GitHub account for user %s", userID)
		err = dbHandle.DB(r.Context()).Exec(
			`UPDATE `+"`Users`"+` SET github_token = ? WHERE user_id = ?`,
			accessTokenResponse.AccessToken, userID).Error
		if err != nil {
			redirectWithError(w, r, status.PermissionDeniedErrorf("Error linking github account to user: %v", err))
			return
		}
	}

	redirectUrl := getCookie(r, redirectCookieName)
	if redirectUrl == "" {
		redirectUrl = "/"
	}

	http.Redirect(w, r, redirectUrl, http.StatusTemporaryRedirect)
}

func (c *GithubClient) CreateStatus(ctx context.Context, ownerRepo string, commitSHA string, payload *GithubStatusPayload) error {
	if ownerRepo == "" || commitSHA == "" {
		return nil // We can't create a status without an owner/repo and a commit SHA.
	}

	var err error
	c.tokenLookup.Do(func() {
		err = c.populateTokenIfNecessary(ctx)
	})
	if err != nil {
		return nil
	}

	// If we don't have a github token, we can't post a status.
	if c.githubToken == "" {
		return nil
	}

	url := fmt.Sprintf("https://api.github.com/repos/%s/statuses/%s", ownerRepo, commitSHA)
	body := new(bytes.Buffer)
	if err := json.NewEncoder(body).Encode(payload); err != nil {
		return status.UnknownErrorf("failed to encode payload: %s", err)
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "token "+c.githubToken)
	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		b, err := io.ReadAll(res.Body)
		if err != nil {
			return status.UnknownErrorf("HTTP %s: <failed to read response body>", res.Status)
		}
		return status.UnknownErrorf("HTTP %s: %q", res.Status, string(b))
	}
	return nil
}

func (c *GithubClient) populateTokenIfNecessary(ctx context.Context) error {
	if c.githubToken != "" || !IsLegacyOAuthAppEnabled() {
		return nil
	}

	if *accessToken != "" {
		c.githubToken = *accessToken
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
	err = dbHandle.DB(ctx).Raw(`SELECT github_token FROM `+"`Groups`"+` WHERE group_id = ?`,
		userInfo.GetGroupID()).First(&group).Error
	if err != nil {
		return err
	}

	if group.GithubToken != nil {
		c.githubToken = *group.GithubToken
	}
	return nil
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

func redirectWithError(w http.ResponseWriter, r *http.Request, err error) {
	log.Warning(err.Error())
	errorParam := err.Error()
	redirectURL := &url.URL{Path: "/"}
	// Respect the original redirect_url parameter that was set when initiating
	// the flow.
	if r.FormValue("state") == getCookie(r, stateCookieName) {
		if u, err := url.Parse(getCookie(r, redirectCookieName)); err == nil {
			redirectURL = u
		}
	}
	q := redirectURL.Query()
	q.Set("error", errorParam)
	redirectURL.RawQuery = q.Encode()
	http.Redirect(w, r, redirectURL.String(), http.StatusTemporaryRedirect)
}

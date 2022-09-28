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
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	httpfilters "github.com/buildbuddy-io/buildbuddy/server/http/filters"
	burl "github.com/buildbuddy-io/buildbuddy/server/util/url"
)

var (
	clientID     = flag.String("github.client_id", "", "The client ID of your GitHub Oauth App. ** Enterprise only **")
	clientSecret = flagutil.New("github.client_secret", "", "The client secret of your GitHub Oauth App. ** Enterprise only **", flagutil.SecretTag)
	accessToken  = flagutil.New("github.access_token", "", "The GitHub access token used to post GitHub commit statuses. ** Enterprise only **", flagutil.SecretTag)
)

const (
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
	AccessToken string `json:"access_token"`
	Scope       string `json:"scope"`
	TokenType   string `json:"token_type"`
}

type GithubClient struct {
	env         environment.Env
	client      *http.Client
	githubToken string
	tokenLookup sync.Once
}

func Register(env environment.Env) error {
	githubClient := NewGithubClient(env, "")
	env.GetMux().Handle(
		"/auth/github/link/",
		httpfilters.WrapAuthenticatedExternalHandler(env, http.HandlerFunc(githubClient.Link)),
	)
	return nil
}

func NewGithubClient(env environment.Env, token string) *GithubClient {
	return &GithubClient{
		env:         env,
		client:      &http.Client{},
		githubToken: token,
	}
}

func ClientSecret() string {
	if cs := os.Getenv("BB_GITHUB_CLIENT_SECRET"); cs != "" {
		return cs
	}
	return *clientSecret
}

func Enabled() bool {
	if *clientID == "" && *clientSecret == "" && *accessToken == "" {
		return false
	}
	return true
}

func (c *GithubClient) Link(w http.ResponseWriter, r *http.Request) {
	if !Enabled() {
		redirectWithError(w, r, status.PermissionDeniedError("Missing GitHub config"))
		return
	}

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
			*clientID,
			state,
			build_buddy_url.WithPath("/auth/github/link/"),
			"repo")
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
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
		*clientID,
		ClientSecret(),
		code,
		state,
		redirectURL)

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

	var accessTokenResponse GithubAccessTokenResponse
	json.Unmarshal(body, &accessTokenResponse)

	u, err := perms.AuthenticatedUser(r.Context(), c.env)
	if err != nil {
		redirectWithError(w, r, status.WrapError(err, "Failed to link GitHub account"))
		return
	}

	dbHandle := c.env.GetDBHandle()
	if dbHandle == nil {
		redirectWithError(w, r, status.PermissionDeniedError("No database configured"))
		return
	}

	// Restore group ID from cookie.
	groupID := getCookie(r, groupIDCookieName)
	if groupID != "" {
		if err := authutil.AuthorizeGroupRole(u, groupID, role.Admin); err != nil {
			redirectWithError(w, r, status.WrapError(err, "Failed to link GitHub account"))
			return
		}
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
	if userID != "" {
		if userID != u.GetUserID() {
			redirectWithError(w, r, status.PermissionDeniedErrorf("Invalid user: %v", userID))
		}
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

	// If we don't have a github token, we can't post a status.
	if c.githubToken == "" || err != nil {
		return err
	}

	url := fmt.Sprintf("https://api.github.com/repos/%s/statuses/%s", ownerRepo, commitSHA)
	body := new(bytes.Buffer)
	json.NewEncoder(body).Encode(payload)

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "token "+c.githubToken)
	_, err = c.client.Do(req)
	if err != nil {
		log.Warningf("Error posting Github status: %v", err)
	}
	return err
}

func (c *GithubClient) populateTokenIfNecessary(ctx context.Context) error {
	if c.githubToken != "" || !Enabled() {
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
	http.Redirect(w, r, "/?error="+url.QueryEscape(err.Error()), http.StatusTemporaryRedirect)
}

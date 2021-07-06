package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
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

func NewGithubClient(env environment.Env, token string) *GithubClient {
	return &GithubClient{
		env:         env,
		client:      &http.Client{},
		githubToken: token,
	}
}

func (c *GithubClient) Link(w http.ResponseWriter, r *http.Request) {
	githubConfig := c.env.GetConfigurator().GetGithubConfig()
	if githubConfig == nil {
		log.Warningf("Missing GitHub config; redirecting to home page.")
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	// If we don't have a state yet parameter, start oauth flow.
	if r.FormValue("state") == "" {
		state := fmt.Sprintf("%d", random.RandUint64())
		setCookie(w, stateCookieName, state)
		setCookie(w, groupIDCookieName, r.FormValue("group_id"))
		setCookie(w, redirectCookieName, r.FormValue("redirect_url"))

		appURL := c.env.GetConfigurator().GetAppBuildBuddyURL()
		url := fmt.Sprintf(
			"https://github.com/login/oauth/authorize?client_id=%s&state=%s&redirect_uri=%s&scope=%s",
			githubConfig.ClientID,
			state,
			appURL+"/auth/github/link/",
			"repo")
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
		return
	}

	// Verify "state" cookie matches
	if r.FormValue("state") != getCookie(r, stateCookieName) {
		log.Warningf("Github link state mismatch: %s != %s", r.FormValue("state"), getCookie(r, stateCookieName))
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	client := &http.Client{}
	url := fmt.Sprintf(
		"https://github.com/login/oauth/access_token?client_id=%s&client_secret=%s&code=%s&state=%s&redirect_uri=%s",
		githubConfig.ClientID,
		githubConfig.ClientSecret,
		r.FormValue("code"),
		r.FormValue("state"),
		r.FormValue("redirect_uri"))

	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		log.Warningf("Error creating request: %v", err)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Warningf("Error getting access token: %v", err)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warningf("Error reading Github response: %v", err)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	var accessTokenResponse GithubAccessTokenResponse
	json.Unmarshal(body, &accessTokenResponse)

	// Restore group ID from cookie.
	groupID := getCookie(r, groupIDCookieName)
	if err := perms.AuthorizeGroupAccess(r.Context(), c.env, groupID); err != nil {
		log.Warningf("Group auth failed; not linking GitHub account: %s", err)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	dbHandle := c.env.GetDBHandle()
	if dbHandle == nil {
		log.Warningf("No database configured")
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	err = dbHandle.Exec(
		"UPDATE Groups SET github_token = ? WHERE group_id = ?",
		accessTokenResponse.AccessToken, groupID).Error
	if err != nil {
		log.Warningf("Error linking github account to user: %v", err)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
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
	if c.githubToken != "" {
		return nil
	}

	if c.env.GetConfigurator().GetGithubConfig() == nil {
		return nil
	}

	if accessToken := c.env.GetConfigurator().GetGithubConfig().AccessToken; accessToken != "" {
		c.githubToken = accessToken
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
	err = dbHandle.Raw(`SELECT github_token FROM `+"`Groups`"+` WHERE group_id = ?`,
		userInfo.GetGroupID()).First(&group).Error
	if err != nil {
		return err
	}

	c.githubToken = group.GithubToken
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

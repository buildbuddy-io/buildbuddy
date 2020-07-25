package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
)

const (
	stateCookieName    = "Github-State-Token"
	redirectCookieName = "Github-Redirect-Url"
)

type GithubStatusPayload struct {
	State       string `json:"state"`
	TargetURL   string `json:"target_url"`
	Description string `json:"description"`
	Context     string `json:"context"`
}

type GithubAccessTokenResponse struct {
	AccessToken string `json:"access_token"`
	Scope       string `json:"scope"`
	TokenType   string `json:"token_type"`
}

type GithubClient struct {
	env                environment.Env
	client             *http.Client
	githubToken        string
	githubTokenFetched bool
}

func NewGithubClient(env environment.Env) *GithubClient {
	return &GithubClient{
		env:                env,
		client:             &http.Client{},
		githubTokenFetched: false,
	}
}

func (c *GithubClient) Link(w http.ResponseWriter, r *http.Request) {
	githubConfig := c.env.GetConfigurator().GetGithubConfig()
	if githubConfig == nil {
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
	}

	// If we don't have a state yet parameter, start oauth flow.
	if r.FormValue("state") == "" {
		state := fmt.Sprintf("%d", random.RandUint64())
		setCookie(w, stateCookieName, state)
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
		log.Printf("Github link state mismatch: %s != %s", r.FormValue("state"), getCookie(r, stateCookieName))
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
		log.Printf("Error creating request: %v", err)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		log.Printf("Error getting access token: %v", err)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading Github response: %v", err)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	var accessTokenResponse GithubAccessTokenResponse
	json.Unmarshal(body, &accessTokenResponse)

	userInfo, err := c.env.GetAuthenticator().AuthenticatedUser(r.Context())
	if err != nil {
		log.Printf("Error getting authenticated user: %v", err)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	err = c.env.GetDBHandle().Exec(
		"UPDATE Groups SET github_token = ? WHERE group_id IN (?)",
		accessTokenResponse.AccessToken, userInfo.GetAllowedGroups()).Error
	if err != nil {
		log.Printf("Error linking github account to user: %v", err)
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
	err := c.populateTokenFromGroupIfNecessary(ctx)

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
		log.Printf("Error posting Github status: %v", err)
	}
	return err
}

func (c *GithubClient) populateTokenFromGroupIfNecessary(ctx context.Context) error {
	if c.githubTokenFetched || c.env.GetConfigurator().GetGithubConfig() == nil {
		return nil
	}

	userInfo, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if userInfo == nil || err != nil {
		return nil
	}

	var group tables.Group
	err = c.env.GetDBHandle().Raw("SELECT github_token FROM Groups WHERE group_id = ?",
		userInfo.GetGroupID()).First(&group).Error
	if err != nil {
		return err
	}

	c.githubToken = group.GithubToken
	c.githubTokenFetched = true
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

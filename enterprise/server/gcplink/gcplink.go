package gcplink

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/keystore"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/cookie"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/coreos/go-oidc"
	"golang.org/x/oauth2"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	gcpb "github.com/buildbuddy-io/buildbuddy/proto/gcp"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	skpb "github.com/buildbuddy-io/buildbuddy/proto/secrets"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

const (
	Issuer            = "https://accounts.google.com"
	scope             = "https://www.googleapis.com/auth/cloud-platform"
	linkParamName     = "link_gcp_for_group"
	linkCookieName    = "link-gcp-for-group"
	authRedirectParam = "redirect_url"
	stateCookie       = "GCP-State-Token"

	cookieDuration = 1 * time.Hour
	// Refresh token secret that's exchanged for an auth token in workflows
	refreshTokenEnvVariableName = "CLOUDSDK_AUTH_REFRESH_TOKEN"
	// Access token used by the gcloud cli to authenticate
	// Must match: https://cloud.google.com/sdk/docs/authorizing#auth-login
	accessTokenEnvVariableName = "CLOUDSDK_AUTH_ACCESS_TOKEN"
	authTokenURL               = "https://accounts.google.com/o/oauth2/token"
	projectSearchURL           = "https://cloudresourcemanager.googleapis.com/v3/projects:search"
	linkURL                    = "/auth/gcp/link/"
)

var (
	gcpClientId     = flag.String("gcp.client_id", "", "The client id to use for GCP linking.")
	gcpClientSecret = flag.String("gcp.client_secret", "", "The client secret to use for GCP linking.")
)

type GCPService struct {
	env      environment.Env
	provider *oidc.Provider
	config   *oauth2.Config
	options  []oauth2.AuthCodeOption
}

func Register(env *real_environment.RealEnv) error {
	provider, err := oidc.NewProvider(context.TODO(), Issuer)
	if err != nil {
		return err
	}
	config := &oauth2.Config{
		ClientID:     *gcpClientId,
		ClientSecret: *gcpClientSecret,
		RedirectURL:  build_buddy_url.WithPath(linkURL).String(),
		Endpoint:     provider.Endpoint(),
		Scopes:       []string{oidc.ScopeOpenID, "profile", "email", scope},
	}
	options := []oauth2.AuthCodeOption{
		oauth2.AccessTypeOffline,
		oauth2.ApprovalForce,
	}

	env.SetGCPService(&GCPService{
		env:      env,
		provider: provider,
		config:   config,
		options:  options,
	})

	return nil
}

func (g *GCPService) Link(w http.ResponseWriter, r *http.Request) error {
	if r.FormValue("state") == "" {
		err := g.startAuthFlow(w, r)
		if err != nil {
			return err
		}
	}

	err := g.handleAuthRedirect(w, r)
	if err != nil {
		return err
	}

	return nil
}

func (g *GCPService) startAuthFlow(w http.ResponseWriter, r *http.Request) error {
	// Set the "state" cookie which will be returned to us by the authentication
	// provider in the URL. We verify that it matches.
	state := fmt.Sprintf("%d", random.RandUint64())
	cookie.SetCookie(w, stateCookie, state, time.Now().Add(cookieDuration), true /* httpOnly= */)

	redirectURL := r.URL.Query().Get(authRedirectParam)
	if err := build_buddy_url.ValidateRedirect(redirectURL); err != nil {
		return err
	}

	// Redirect to the login provider (and ask for a refresh token).
	authURL := g.config.AuthCodeURL(state, g.options...)

	// Set the redirection URL in a cookie so we can use it after validating
	// the user in our /auth callback.
	cookie.SetCookie(w, cookie.RedirCookie, redirectURL, time.Now().Add(cookieDuration), true /* httpOnly= */)

	cookie.SetCookie(w, linkCookieName, r.URL.Query().Get(linkParamName), time.Now().Add(cookieDuration), true)
	http.Redirect(w, r, authURL, http.StatusTemporaryRedirect)
	return nil
}

func (g *GCPService) handleAuthRedirect(w http.ResponseWriter, r *http.Request) error {
	// Verify "state" cookie match.
	if r.FormValue("state") != cookie.GetCookie(r, stateCookie) {
		return status.PermissionDeniedErrorf("state mismatch: %s != %s", r.FormValue("state"), cookie.GetCookie(r, stateCookie))
	}

	authError := r.URL.Query().Get("error")
	if authError != "" {
		return status.PermissionDeniedErrorf("Authenticator returned error: %s (%s %s)", authError, r.URL.Query().Get("error_desc"), r.URL.Query().Get("error_description"))
	}

	code := r.URL.Query().Get("code")
	oauth2Token, err := g.config.Exchange(r.Context(), code, g.options...)
	if err != nil {
		return status.PermissionDeniedErrorf("Error exchanging code for auth token: %s", code)
	}

	// Extract the ID Token (JWT) from OAuth2 token.
	jwt, ok := oauth2Token.Extra("id_token").(string)
	if !ok {
		return status.PermissionDeniedError("ID Token not present in auth response")
	}

	c := &oidc.Config{
		ClientID:        *gcpClientId,
		SkipExpiryCheck: false,
	}

	_, err = g.provider.Verifier(c).Verify(r.Context(), jwt)
	if err != nil {
		return err
	}

	refreshToken, ok := oauth2Token.Extra("refresh_token").(string)
	if !ok {
		return status.PermissionDeniedError("Refresh token not present in auth response")
	}
	return linkForGroup(g.env, w, r, refreshToken)
}

// Accepts the redirect from the auth provider and stores the results as secrets.
func linkForGroup(env environment.Env, w http.ResponseWriter, r *http.Request, refreshToken string) error {
	if refreshToken == "" {
		return status.PermissionDeniedErrorf("Empty refresh token")
	}
	rc := &ctxpb.RequestContext{
		GroupId: cookie.GetCookie(r, linkCookieName),
	}
	cookie.ClearCookie(w, linkCookieName)
	ctx := requestcontext.ContextWithProtoRequestContext(r.Context(), rc)
	ctx = env.GetAuthenticator().AuthenticatedHTTPContext(w, r.WithContext(ctx))
	secretResponse, err := env.GetSecretService().GetPublicKey(ctx, &skpb.GetPublicKeyRequest{
		RequestContext: rc,
	})
	if err != nil {
		return status.PermissionDeniedErrorf("Error getting public key: %s", err)
	}
	box, err := keystore.NewAnonymousSealedBox(secretResponse.PublicKey.Value, refreshToken)
	if err != nil {
		return status.PermissionDeniedErrorf("Error sealing box: %s", err)
	}
	_, _, err = env.GetSecretService().UpdateSecret(ctx, &skpb.UpdateSecretRequest{
		RequestContext: rc,
		Secret: &skpb.Secret{
			Name:  refreshTokenEnvVariableName,
			Value: box,
		},
	})
	if err != nil {
		return status.PermissionDeniedErrorf("Error updating secret: %s", err)
	}
	http.Redirect(w, r, cookie.GetCookie(r, cookie.RedirCookie), http.StatusTemporaryRedirect)
	return nil
}

// Takes a list of environment variables and exchanges "CLOUDSDK_AUTH_REFRESH_TOKEN" for "CLOUDSDK_AUTH_ACCESS_TOKEN"
func ExchangeRefreshTokenForAuthToken(ctx context.Context, envVars []*repb.Command_EnvironmentVariable, shouldExchangeToken bool) ([]*repb.Command_EnvironmentVariable, error) {
	newEnvVars := []*repb.Command_EnvironmentVariable{}
	for _, e := range envVars {
		if e.GetName() != refreshTokenEnvVariableName {
			newEnvVars = append(newEnvVars, &repb.Command_EnvironmentVariable{
				Name:  e.GetName(),
				Value: e.GetValue(),
			})
			continue
		}
		if !shouldExchangeToken {
			// We'll omit refresh tokens if we're asked not to exchange token.
			continue
		}
		accessToken, err := makeTokenExchangeRequest(e.GetValue())
		if err != nil {
			return nil, err
		}
		newEnvVars = append(newEnvVars, &repb.Command_EnvironmentVariable{
			Name:  accessTokenEnvVariableName,
			Value: accessToken,
		})

	}
	return newEnvVars, nil
}

// Makes an POST request to exchange the given refresh token for an access token.
// https://developers.google.com/identity/protocols/oauth2/web-server#offline
func makeTokenExchangeRequest(refreshToken string) (string, error) {
	data := url.Values{}
	data.Set("refresh_token", refreshToken)
	data.Set("client_id", *gcpClientId)
	data.Set("client_secret", *gcpClientSecret)
	data.Set("grant_type", "refresh_token")
	resp, err := http.Post(authTokenURL, "application/x-www-form-urlencoded", bytes.NewBufferString(data.Encode()))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return "", err
	}
	var response accessTokenResponse
	err = json.Unmarshal(buf.Bytes(), &response)
	if err != nil {
		return "", err
	}
	return response.AccessToken, nil
}

type accessTokenResponse struct {
	AccessToken string `json:"access_token"`
}

func (g *GCPService) GetGCPProject(ctx context.Context, request *gcpb.GetGCPProjectRequest) (*gcpb.GetGCPProjectResponse, error) {
	u, err := g.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	secretService := g.env.GetSecretService()
	if secretService == nil {
		return nil, status.FailedPreconditionError("secret service not available")
	}
	// TODO(siggisim): Add a method for fetching a single env var and use that.
	envVars, err := secretService.GetSecretEnvVars(ctx, u.GetGroupID())
	if err != nil {
		return nil, err
	}
	envVars, err = ExchangeRefreshTokenForAuthToken(ctx, envVars, true)
	if err != nil {
		return nil, err
	}
	accessToken := ""
	for _, envVar := range envVars {
		if envVar.Name == accessTokenEnvVariableName {
			accessToken = envVar.Value
		}
	}
	if accessToken == "" {
		return nil, status.FailedPreconditionError("GCP not linked")
	}

	var projectResponse projectsResponse
	err = getRequest(projectSearchURL, accessToken, &projectResponse)
	if err != nil {
		return nil, err
	}

	proto, err := toProto(projectResponse.Projects)
	if err != nil {
		return nil, err
	}

	return &gcpb.GetGCPProjectResponse{
		Project: proto,
	}, nil
}

func toProto(projects []project) ([]*gcpb.GCPProject, error) {
	protos := []*gcpb.GCPProject{}
	for _, p := range projects {
		createdAt, err := time.Parse(time.RFC3339, p.CreateTime)
		if err != nil {
			return nil, err
		}
		protos = append(protos, &gcpb.GCPProject{
			ResourceName:  p.Name,
			Parent:        p.Parent,
			DisplayName:   p.DisplayName,
			Id:            p.ProjectID,
			State:         p.State,
			CreatedAtUsec: createdAt.UnixMicro(),
			Etag:          p.Etag,
		})
	}
	return protos, nil
}

type projectsResponse struct {
	Projects []project `json:"projects"`
}

type project struct {
	Name        string `json:"name"`
	Parent      string `json:"parent"`
	ProjectID   string `json:"projectId"`
	State       string `json:"state"`
	DisplayName string `json:"displayName"`
	CreateTime  string `json:"createTime"`
	Etag        string `json:"etag"`
}

func getRequest(url, token string, v any) error {
	accessToken := "Bearer " + token
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", accessToken)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(v)
	if err != nil {
		return err
	}
	return nil
}

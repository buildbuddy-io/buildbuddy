// Package gcpoidc exchanges BuildBuddy OIDC tokens for Google Cloud credentials.
package gcpoidc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// AccessTokenEnvVar contains a GCP OAuth access token exchanged from a
	// BuildBuddy OIDC ID token.
	AccessTokenEnvVar = "BUILDBUDDY_GCP_ACCESS_TOKEN"

	// RegistryUsername is the Docker registry username used with OAuth access
	// tokens for Google Artifact Registry and GCR.
	RegistryUsername = "oauth2accesstoken"

	// DefaultSTSEndpoint is the Google Security Token Service token exchange endpoint.
	DefaultSTSEndpoint = "https://sts.googleapis.com/v1/token"

	// DefaultIAMCredentialsEndpoint is the IAM Credentials API base endpoint.
	DefaultIAMCredentialsEndpoint = "https://iamcredentials.googleapis.com/v1"

	cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
)

// Config contains Google Cloud token exchange settings.
type Config struct {
	// Audience is the audience sent to Security Token Service.
	Audience string

	// ServiceAccount is the optional Google Cloud service account email to
	// impersonate.
	ServiceAccount string

	// STSEndpoint overrides the Google Security Token Service endpoint.
	STSEndpoint string

	// IAMCredentialsEndpoint overrides the IAM Credentials API endpoint.
	IAMCredentialsEndpoint string

	// TokenLifetime is the requested lifetime for an impersonated service account
	// access token.
	TokenLifetime time.Duration

	// HTTPClient sends token exchange requests. If nil, Exchange uses
	// http.DefaultClient.
	HTTPClient *http.Client

	// Now is used to calculate STS access token expiration. If zero, Exchange
	// uses time.Now.
	Now time.Time
}

// Credentials contains GCP credentials exchanged from a BuildBuddy OIDC token.
type Credentials struct {
	// Proto contains the env vars and registry credentials to pass to the
	// executor.
	Proto *repb.OIDCCredentials

	// ExpiresAt is the expiration time for the exchanged Google Cloud access
	// token.
	ExpiresAt time.Time
}

type stsResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

type serviceAccountTokenResponse struct {
	AccessToken string `json:"accessToken"`
	ExpireTime  string `json:"expireTime"`
}

// Exchange exchanges idToken for a Google Cloud OAuth access token and
// optionally impersonates cfg.ServiceAccount.
func Exchange(ctx context.Context, cfg Config, idToken string) (*Credentials, error) {
	if cfg.Audience == "" {
		return nil, status.InvalidArgumentError("GCP token audience is required")
	}
	if cfg.STSEndpoint == "" {
		cfg.STSEndpoint = DefaultSTSEndpoint
	}
	if cfg.IAMCredentialsEndpoint == "" {
		cfg.IAMCredentialsEndpoint = DefaultIAMCredentialsEndpoint
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Now.IsZero() {
		cfg.Now = time.Now()
	}

	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange")
	form.Set("audience", cfg.Audience)
	form.Set("scope", cloudPlatformScope)
	form.Set("requested_token_type", "urn:ietf:params:oauth:token-type:access_token")
	form.Set("subject_token_type", "urn:ietf:params:oauth:token-type:id_token")
	form.Set("subject_token", idToken)
	var stsResp stsResponse
	if err := postForm(ctx, cfg.HTTPClient, cfg.STSEndpoint, form, "", &stsResp); err != nil {
		return nil, status.WrapError(err, "exchange GCP token")
	}
	if stsResp.AccessToken == "" || stsResp.ExpiresIn <= 0 {
		return nil, status.UnavailableError("GCP STS response did not include an access token")
	}
	accessToken := stsResp.AccessToken
	expiresAt := cfg.Now.Add(time.Duration(stsResp.ExpiresIn) * time.Second)
	if cfg.ServiceAccount != "" {
		token, tokenExpiresAt, err := impersonateServiceAccount(ctx, cfg, stsResp.AccessToken)
		if err != nil {
			return nil, err
		}
		accessToken = token
		expiresAt = tokenExpiresAt
	}
	creds := &repb.OIDCCredentials{
		SecretEnvVars: []string{
			fmt.Sprintf("%s=%s", AccessTokenEnvVar, accessToken),
		},
		ContainerRegistryUsername: RegistryUsername,
		ContainerRegistryPassword: accessToken,
	}
	return &Credentials{
		Proto:     creds,
		ExpiresAt: expiresAt,
	}, nil
}

func impersonateServiceAccount(ctx context.Context, cfg Config, accessToken string) (string, time.Time, error) {
	lifetimeSeconds := int64(cfg.TokenLifetime.Seconds())
	if lifetimeSeconds <= 0 || lifetimeSeconds > 3600 {
		lifetimeSeconds = 3600
	}
	body := map[string]interface{}{
		"scope":    []string{cloudPlatformScope},
		"lifetime": fmt.Sprintf("%ds", lifetimeSeconds),
	}
	endpoint := strings.TrimRight(cfg.IAMCredentialsEndpoint, "/") + "/projects/-/serviceAccounts/" + url.PathEscape(cfg.ServiceAccount) + ":generateAccessToken"
	var tokenResp serviceAccountTokenResponse
	if err := postJSON(ctx, cfg.HTTPClient, endpoint, body, accessToken, &tokenResp); err != nil {
		return "", time.Time{}, status.WrapError(err, "impersonate GCP service account")
	}
	if tokenResp.AccessToken == "" || tokenResp.ExpireTime == "" {
		return "", time.Time{}, status.UnavailableError("GCP service account response did not include an access token")
	}
	expiresAt, err := time.Parse(time.RFC3339, tokenResp.ExpireTime)
	if err != nil {
		return "", time.Time{}, status.UnavailableErrorf("parse GCP service account token expiration: %s", err)
	}
	return tokenResp.AccessToken, expiresAt, nil
}

func postForm(ctx context.Context, client *http.Client, endpoint string, form url.Values, bearerToken string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	return doJSON(client, req, out)
}

func postJSON(ctx context.Context, client *http.Client, endpoint string, body interface{}, bearerToken string, out interface{}) error {
	var b strings.Builder
	if err := json.NewEncoder(&b).Encode(body); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(b.String()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	return doJSON(client, req, out)
}

func doJSON(client *http.Client, req *http.Request, out interface{}) error {
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return status.UnavailableErrorf("HTTP %d from token exchange endpoint", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return err
	}
	return nil
}

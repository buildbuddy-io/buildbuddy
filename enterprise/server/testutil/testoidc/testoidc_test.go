package testoidc_test

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testoidc"
	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestDiscoveryAndAuthorizationPage(t *testing.T) {
	p := testoidc.Start(t)
	p.AddUser("user-2", "two@example.com")
	p.AddUser("user-1", "one@example.com")

	provider, err := oidc.NewProvider(t.Context(), p.IssuerURL())
	require.NoError(t, err)

	config := oauthConfig(provider, "http://localhost/callback")
	authURL := config.AuthCodeURL("test-state")
	resp, err := http.Get(authURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	html := string(body)
	require.Contains(t, html, `<select name="user">`)
	require.Contains(t, html, `<option value="user-1">one@example.com</option>`)
	require.Contains(t, html, `<option value="user-2">two@example.com</option>`)
	require.Contains(t, html, `<button type="submit">`)
	require.Contains(t, html, `name="redirect_uri" value="http://localhost/callback"`)
	require.Contains(t, html, `name="state" value="test-state"`)
}

func TestCodesAndRefreshTokensAreBoundToSelectedIdentity(t *testing.T) {
	p := testoidc.Start(t)
	p.AddUser("alice-sub", "alice@example.com")
	p.AddUser("bob-sub", "bob@example.com")

	provider, err := oidc.NewProvider(t.Context(), p.IssuerURL())
	require.NoError(t, err)
	config := oauthConfig(provider, "http://localhost/callback?existing=value")
	verifier := provider.Verifier(&oidc.Config{ClientID: testoidc.ClientID})

	aliceCode := authorize(t, config, "alice-sub", "alice-state")
	bobCode := authorize(t, config, "bob-sub", "bob-state")

	aliceToken, err := config.Exchange(t.Context(), aliceCode)
	require.NoError(t, err)
	aliceClaims := verifyClaims(t, verifier, aliceToken)
	require.Equal(t, "alice-sub", aliceClaims.Subject)
	require.Equal(t, "alice@example.com", aliceClaims.Email)

	// Authorization codes are one-time credentials.
	_, err = config.Exchange(t.Context(), aliceCode)
	require.Error(t, err)

	bobToken, err := config.Exchange(t.Context(), bobCode)
	require.NoError(t, err)
	bobClaims := verifyClaims(t, verifier, bobToken)
	require.Equal(t, "bob-sub", bobClaims.Subject)
	require.Equal(t, "bob@example.com", bobClaims.Email)

	// Refreshing Alice after Bob logs in must still produce an Alice token.
	refreshedAliceToken, err := config.TokenSource(t.Context(), &oauth2.Token{
		RefreshToken: aliceToken.RefreshToken,
	}).Token()
	require.NoError(t, err)
	refreshedAliceClaims := verifyClaims(t, verifier, refreshedAliceToken)
	require.Equal(t, "alice-sub", refreshedAliceClaims.Subject)
	require.Equal(t, "alice@example.com", refreshedAliceClaims.Email)
}

type claims struct {
	Subject string `json:"sub"`
	Email   string `json:"email"`
}

func verifyClaims(t testing.TB, verifier *oidc.IDTokenVerifier, token *oauth2.Token) *claims {
	t.Helper()
	rawIDToken, ok := token.Extra("id_token").(string)
	require.True(t, ok)
	idToken, err := verifier.Verify(context.Background(), rawIDToken)
	require.NoError(t, err)
	require.NoError(t, idToken.VerifyAccessToken(token.AccessToken))
	c := &claims{}
	require.NoError(t, idToken.Claims(c))
	return c
}

func oauthConfig(provider *oidc.Provider, redirectURL string) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     testoidc.ClientID,
		ClientSecret: testoidc.ClientSecret,
		Endpoint:     provider.Endpoint(),
		RedirectURL:  redirectURL,
		Scopes:       []string{oidc.ScopeOpenID, "profile", "email", oidc.ScopeOfflineAccess},
	}
}

func authorize(t testing.TB, config *oauth2.Config, subject, state string) string {
	t.Helper()
	authURL, err := url.Parse(config.AuthCodeURL(state))
	require.NoError(t, err)

	form := authURL.Query()
	form.Set("user", subject)
	req, err := http.NewRequest(http.MethodPost, authURL.Scheme+"://"+authURL.Host+authURL.Path, strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusFound, resp.StatusCode)

	location, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	require.Equal(t, state, location.Query().Get("state"))
	require.Equal(t, "value", location.Query().Get("existing"))
	code := location.Query().Get("code")
	require.NotEmpty(t, code)
	return code
}

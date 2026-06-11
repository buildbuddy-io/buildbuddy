package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	authclaims "github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestGenerateRequestEnvAndToken(t *testing.T) {
	key := setSigningKey(t)
	env := testenv.GetTestEnv(t)
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	ctx := authclaims.AuthContext(context.Background(), &authclaims.Claims{
		GroupID: "GR123",
		UserID:  "US456",
	})
	task := &repb.ExecutionTask{
		InvocationId: "INV123",
		ExecutionId:  "EXEC123",
		ExecuteRequest: &repb.ExecuteRequest{
			InstanceName: "remote/instance",
			ActionDigest: &repb.Digest{Hash: "abc123", SizeBytes: 42},
		},
		RequestMetadata: &repb.RequestMetadata{
			ActionId:       "ACTION123",
			TargetId:       "//pkg:target",
			ActionMnemonic: "Genrule",
		},
	}

	envVars, secretEnvVars, err := GenerateRequestEnv(ctx, env, task, "sts.amazonaws.com")
	require.NoError(t, err)
	require.Len(t, envVars, 1)
	require.Len(t, secretEnvVars, 1)
	require.Equal(t, TokenRequestURLEnvVar+"=https://buildbuddy.example.com/oidc/token?api-version=2", envVars[0])
	require.True(t, strings.HasPrefix(secretEnvVars[0], TokenRequestTokenEnvVar+"="))

	requestToken := strings.TrimPrefix(secretEnvVars[0], TokenRequestTokenEnvVar+"=")
	provider, err := NewProvider(env)
	require.NoError(t, err)
	rc, err := provider.parseRequestToken(requestToken)
	require.NoError(t, err)
	require.Equal(t, requestTokenUse, rc.TokenUse)
	require.Equal(t, "sts.amazonaws.com", rc.DefaultAudience)
	require.Equal(t, "buildbuddy:group:GR123", rc.Subject)
	require.Equal(t, "GR123", rc.BuildBuddyGroupID)
	require.Equal(t, "US456", rc.BuildBuddyUserID)
	require.Equal(t, "INV123", rc.InvocationID)
	require.Equal(t, "EXEC123", rc.ExecutionID)
	require.Equal(t, "remote/instance", rc.InstanceName)
	require.Equal(t, "abc123/42", rc.ActionDigest)
	require.Equal(t, "ACTION123", rc.ActionID)
	require.Equal(t, "//pkg:target", rc.TargetID)
	require.Equal(t, "Genrule", rc.ActionMnemonic)

	req := httptest.NewRequest(http.MethodGet, "/oidc/token?audience=override-audience", nil)
	req.Header.Set("Authorization", "bearer "+requestToken)
	rec := httptest.NewRecorder()
	provider.Token(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var tokenResp tokenResponse
	require.NoError(t, jsonDecode(rec.Body.String(), &tokenResp))
	idClaims := &idTokenClaims{}
	parsed, err := jwt.ParseWithClaims(tokenResp.Value, idClaims, func(token *jwt.Token) (interface{}, error) {
		return &key.PublicKey, nil
	})
	require.NoError(t, err)
	require.True(t, parsed.Valid)
	require.Equal(t, issuerURL().String(), idClaims.Issuer)
	require.Equal(t, idTokenUse, idClaims.TokenUse)
	require.Equal(t, "override-audience", idClaims.Audience)
	require.Equal(t, "buildbuddy:group:GR123", idClaims.Subject)
	require.Equal(t, "GR123", idClaims.BuildBuddyGroupID)
	require.Equal(t, "US456", idClaims.BuildBuddyUserID)
	require.Equal(t, "INV123", idClaims.InvocationID)
	require.Equal(t, "EXEC123", idClaims.ExecutionID)
	require.Equal(t, "remote/instance", idClaims.InstanceName)
	require.Equal(t, "abc123/42", idClaims.ActionDigest)
	require.Equal(t, "ACTION123", idClaims.ActionID)
	require.Equal(t, "//pkg:target", idClaims.TargetID)
	require.Equal(t, "Genrule", idClaims.ActionMnemonic)
	require.Equal(t, runnerEnvironment, idClaims.RunnerEnvironment)
}

func TestTokenUsesDefaultAudience(t *testing.T) {
	key := setSigningKey(t)
	env := testenv.GetTestEnv(t)
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	ctx := authclaims.AuthContext(context.Background(), &authclaims.Claims{GroupID: "GR123"})
	requestToken, err := GenerateRequestToken(ctx, env, &repb.ExecutionTask{}, "sts.amazonaws.com")
	require.NoError(t, err)
	provider, err := NewProvider(env)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/oidc/token", nil)
	req.Header.Set("Authorization", "Bearer "+requestToken)
	rec := httptest.NewRecorder()
	provider.Token(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var tokenResp tokenResponse
	require.NoError(t, jsonDecode(rec.Body.String(), &tokenResp))
	idClaims := &idTokenClaims{}
	parsed, err := jwt.ParseWithClaims(tokenResp.Value, idClaims, func(token *jwt.Token) (interface{}, error) {
		return &key.PublicKey, nil
	})
	require.NoError(t, err)
	require.True(t, parsed.Valid)
	require.Equal(t, "sts.amazonaws.com", idClaims.Audience)
}

func TestTokenRejectsInternalRequestTokenAudience(t *testing.T) {
	setSigningKey(t)
	env := testenv.GetTestEnv(t)
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	ctx := authclaims.AuthContext(context.Background(), &authclaims.Claims{GroupID: "GR123"})
	_, err := GenerateRequestToken(ctx, env, &repb.ExecutionTask{}, requestTokenAudience)
	require.Error(t, err)

	requestToken, err := GenerateRequestToken(ctx, env, &repb.ExecutionTask{}, "sts.amazonaws.com")
	require.NoError(t, err)
	provider, err := NewProvider(env)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/oidc/token?audience="+url.QueryEscape(requestTokenAudience), nil)
	req.Header.Set("Authorization", "Bearer "+requestToken)
	rec := httptest.NewRecorder()
	provider.Token(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestIDTokenCannotBeUsedAsRequestToken(t *testing.T) {
	key := setSigningKey(t)
	env := testenv.GetTestEnv(t)
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	provider, err := NewProvider(env)
	require.NoError(t, err)

	now := env.GetClock().Now()
	idClaims := &idTokenClaims{
		StandardClaims: jwt.StandardClaims{
			Audience:  requestTokenAudience,
			ExpiresAt: now.Add(time.Minute).Unix(),
			Issuer:    issuerURL().String(),
			NotBefore: now.Unix(),
			Subject:   subject("GR123"),
		},
		TokenUse:          idTokenUse,
		BuildBuddyGroupID: "GR123",
	}
	idToken, err := sign(key, provider.keyID, idClaims)
	require.NoError(t, err)

	_, err = provider.parseRequestToken(idToken)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid token use")
}

func TestGenerateRequestTokenRequiresAuth(t *testing.T) {
	setSigningKey(t)
	env := testenv.GetTestEnv(t)
	_, err := GenerateRequestToken(context.Background(), env, &repb.ExecutionTask{}, "sts.amazonaws.com")
	require.Error(t, err)
}

func TestWellKnownAndJWKS(t *testing.T) {
	setSigningKey(t)
	env := testenv.GetTestEnv(t)
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	provider, err := NewProvider(env)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	provider.WellKnownOpenIDConfiguration(rec, httptest.NewRequest(http.MethodGet, wellKnownPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var config configurationJSON
	require.NoError(t, jsonDecode(rec.Body.String(), &config))
	require.Equal(t, "https://buildbuddy.example.com/oidc", config.Issuer)
	require.Equal(t, "https://buildbuddy.example.com/oidc/.well-known/jwks", config.JwksURI)
	require.Contains(t, config.IdTokenSigningAlgValuesSupported, "RS256")
	require.Contains(t, config.ClaimsSupported, "sub")

	rec = httptest.NewRecorder()
	provider.JWKS(rec, httptest.NewRequest(http.MethodGet, jwksPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var keySet jwksJSON
	require.NoError(t, jsonDecode(rec.Body.String(), &keySet))
	require.Len(t, keySet.Keys, 1)
	require.Equal(t, "RSA", keySet.Keys[0].Kty)
	require.Equal(t, "sig", keySet.Keys[0].Use)
	require.Equal(t, "RS256", keySet.Keys[0].Alg)
	require.NotEmpty(t, keySet.Keys[0].Kid)
	require.NotEmpty(t, keySet.Keys[0].N)
	require.Equal(t, "AQAB", keySet.Keys[0].E)
}

func setSigningKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der := x509.MarshalPKCS1PrivateKey(key)
	pemBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: der,
	})
	flags.Set(t, "remote_execution.oidc.private_key", string(pemBytes))
	return key
}

func setBuildBuddyURL(t *testing.T, rawURL string) {
	t.Helper()
	u, err := url.Parse(rawURL)
	require.NoError(t, err)
	flags.Set(t, "app.build_buddy_url", *u)
}

func jsonDecode(s string, v interface{}) error {
	return json.NewDecoder(strings.NewReader(s)).Decode(v)
}

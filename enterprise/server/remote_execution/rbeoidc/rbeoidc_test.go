package rbeoidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/golang-jwt/jwt/v4"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestGenerateRequestEnvAndToken(t *testing.T) {
	key := setSigningKey(t)
	env := testenv.GetTestEnv(t)
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	ctx := claims.AuthContext(context.Background(), &claims.Claims{
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
	ctx := claims.AuthContext(context.Background(), &claims.Claims{GroupID: "GR123"})
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
	ctx := claims.AuthContext(context.Background(), &claims.Claims{GroupID: "GR123"})
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

func TestApplyCredentialOverrides_GCPAccessTokenIsCached(t *testing.T) {
	setSigningKey(t)
	resetTokenExchangeState(t)
	env := testenv.GetTestEnv(t)
	now := time.Now()
	env.SetClock(clockwork.NewFakeClockAt(now))
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	ctx := claims.AuthContext(context.Background(), &claims.Claims{GroupID: "GR-GCP-CACHE"})

	var stsRequests atomic.Int64
	var iamRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/token":
			stsRequests.Add(1)
			require.NoError(t, r.ParseForm())
			require.Equal(t, "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider", r.Form.Get("audience"))
			require.NotEmpty(t, r.Form.Get("subject_token"))
			writeJSON(w, &gcpSTSResponse{
				AccessToken: "sts-access-token",
				ExpiresIn:   3600,
				TokenType:   "Bearer",
			})
		case strings.Contains(r.URL.Path, ":generateAccessToken"):
			iamRequests.Add(1)
			require.Equal(t, "Bearer sts-access-token", r.Header.Get("Authorization"))
			writeJSON(w, &gcpServiceAccountTokenResponse{
				AccessToken: "impersonated-access-token",
				ExpireTime:  now.Add(time.Hour).Format(time.RFC3339),
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	gcpSTSEndpoint = server.URL + "/token"
	gcpIAMCredentialsEndpoint = server.URL + "/v1"

	props := &platform.Properties{
		OIDCProvider:          platform.OIDCProviderGCP,
		OIDCTokenAudience:     "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
		OIDCGCPServiceAccount: "rbe@example.iam.gserviceaccount.com",
	}

	creds, err := ExchangeCredentials(ctx, env, nil, props, "" /*registryHost*/)
	require.NoError(t, err)
	require.Empty(t, creds.EnvVars)
	require.Contains(t, creds.SecretEnvVars, AccessTokenEnvVar+"=impersonated-access-token")

	creds, err = ExchangeCredentials(ctx, env, nil, props, "" /*registryHost*/)
	require.NoError(t, err)
	require.Contains(t, creds.SecretEnvVars, AccessTokenEnvVar+"=impersonated-access-token")
	require.Equal(t, int64(1), stsRequests.Load())
	require.Equal(t, int64(1), iamRequests.Load())
}

func TestApplyCredentialOverrides_UsesLeasedCredentials(t *testing.T) {
	key := setSigningKey(t)
	resetTokenExchangeState(t)
	appEnv := testenv.GetTestEnv(t)
	now := time.Now()
	appEnv.SetClock(clockwork.NewFakeClockAt(now))
	ctx := claims.AuthContext(context.Background(), &claims.Claims{
		GroupID: "GR-APP-EXCHANGE",
		UserID:  "US-APP-EXCHANGE",
	})
	task := &repb.ExecutionTask{
		InvocationId: "INV-APP-EXCHANGE",
		ExecutionId:  "EXEC-APP-EXCHANGE",
		Command:      &repb.Command{},
	}
	props := &platform.Properties{
		OIDCProvider:      platform.OIDCProviderGCP,
		OIDCTokenAudience: "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
	}

	var stsRequests atomic.Int64
	stsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stsRequests.Add(1)
		require.Equal(t, "/token", r.URL.Path)
		require.NoError(t, r.ParseForm())
		idClaims := &idTokenClaims{}
		parsed, err := jwt.ParseWithClaims(r.Form.Get("subject_token"), idClaims, func(token *jwt.Token) (interface{}, error) {
			return &key.PublicKey, nil
		})
		require.NoError(t, err)
		require.True(t, parsed.Valid)
		require.Equal(t, "GR-APP-EXCHANGE", idClaims.BuildBuddyGroupID)
		writeJSON(w, &gcpSTSResponse{
			AccessToken: "app-exchanged-access-token",
			ExpiresIn:   3600,
			TokenType:   "Bearer",
		})
	}))
	defer stsServer.Close()
	gcpSTSEndpoint = stsServer.URL + "/token"

	creds, err := GenerateExchangedCredentials(ctx, appEnv, props)
	require.NoError(t, err)
	require.NotNil(t, creds)
	task.OidcCredentials = creds

	executorEnv := testenv.GetTestEnv(t)
	executorEnv.SetClock(clockwork.NewFakeClockAt(now))
	_, exchangedSecretEnvVars, err := ApplyCredentialOverrides(ctx, executorEnv, task, props)
	require.NoError(t, err)
	require.Contains(t, exchangedSecretEnvVars, AccessTokenEnvVar+"=app-exchanged-access-token")
	require.Nil(t, task.GetOidcCredentials())
	require.Equal(t, int64(1), stsRequests.Load())
}

func TestApplyCredentialOverrides_RequiresAppExchangedCredentials(t *testing.T) {
	props := &platform.Properties{
		OIDCProvider:      platform.OIDCProviderGCP,
		OIDCTokenAudience: "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
	}
	_, _, err := ApplyCredentialOverrides(context.Background(), testenv.GetTestEnv(t), &repb.ExecutionTask{}, props)
	require.Error(t, err)
	require.Contains(t, err.Error(), "OIDC credentials were not attached to the leased task")
}

func TestCachedCredentials_RefreshesHalfwayThroughLifetime(t *testing.T) {
	resetTokenExchangeState(t)
	env := testenv.GetTestEnv(t)
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	clock := clockwork.NewFakeClockAt(now)
	env.SetClock(clock)
	key := tokenExchangeCacheKey{
		GroupID:  "GR-CACHE-REFRESH",
		Provider: platform.OIDCProviderGCP,
		Audience: "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
	}

	// Provider credentials stay cached for the first half of their lifetime, so
	// actions sharing a group do not repeat the cloud token exchange.
	storeCachedCredentials(env, key, &CloudCredentials{
		SecretEnvVars: []string{AccessTokenEnvVar + "=cached-token"},
		ExpiresAt:     now.Add(time.Hour),
	})
	clock.Advance(29*time.Minute + 59*time.Second)
	creds := cachedCredentials(env, key)
	require.NotNil(t, creds)
	require.Contains(t, creds.SecretEnvVars, AccessTokenEnvVar+"=cached-token")

	// Once half the lifetime has elapsed, the next action refreshes instead of
	// receiving credentials with less buffer remaining.
	clock.Advance(time.Second)
	require.Nil(t, cachedCredentials(env, key))
}

func TestCachedCredentials_ShortLifetimeRefreshesHalfwayThroughLifetime(t *testing.T) {
	resetTokenExchangeState(t)
	env := testenv.GetTestEnv(t)
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	clock := clockwork.NewFakeClockAt(now)
	env.SetClock(clock)
	key := tokenExchangeCacheKey{
		GroupID:  "GR-CACHE-SHORT",
		Provider: platform.OIDCProviderGCP,
		Audience: "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
	}

	// Short-lived credentials follow the same halfway refresh rule, so a small
	// provider lifetime does not make every cache lookup miss immediately.
	storeCachedCredentials(env, key, &CloudCredentials{
		SecretEnvVars: []string{AccessTokenEnvVar + "=short-token"},
		ExpiresAt:     now.Add(5 * time.Minute),
	})
	creds := cachedCredentials(env, key)
	require.NotNil(t, creds)
	require.Contains(t, creds.SecretEnvVars, AccessTokenEnvVar+"=short-token")

	// The second half of the short lifetime is kept as expiration slack for
	// longer running actions.
	clock.Advance(2*time.Minute + 29*time.Second)
	require.NotNil(t, cachedCredentials(env, key))
	clock.Advance(time.Second)
	require.Nil(t, cachedCredentials(env, key))
}

func TestApplyCredentialOverrides_GCPRegistryCredentials(t *testing.T) {
	setSigningKey(t)
	resetTokenExchangeState(t)
	env := testenv.GetTestEnv(t)
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	env.SetClock(clockwork.NewFakeClockAt(now))
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	ctx := claims.AuthContext(context.Background(), &claims.Claims{GroupID: "GR-GCP-REGISTRY"})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/token", r.URL.Path)
		require.NoError(t, r.ParseForm())
		require.NotEmpty(t, r.Form.Get("subject_token"))
		writeJSON(w, &gcpSTSResponse{
			AccessToken: "gcp-registry-access-token",
			ExpiresIn:   3600,
			TokenType:   "Bearer",
		})
	}))
	defer server.Close()
	gcpSTSEndpoint = server.URL + "/token"

	props := &platform.Properties{
		ContainerImage:              "us-docker.pkg.dev/project/repo/image:tag",
		ContainerRegistryAuthMethod: platform.ContainerRegistryAuthMethodOIDC,
		OIDCProvider:                platform.OIDCProviderGCP,
		OIDCTokenAudience:           "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
	}

	creds, err := ExchangeCredentials(ctx, env, nil, props, imageRegistryHost(props.ContainerImage))
	require.NoError(t, err)
	task := &repb.ExecutionTask{OidcCredentials: credentialsProto(creds)}
	_, secretEnvVars, err := ApplyCredentialOverrides(context.Background(), env, task, props)
	require.NoError(t, err)
	require.Contains(t, secretEnvVars, AccessTokenEnvVar+"=gcp-registry-access-token")
	require.Equal(t, gcpRegistryUsername, props.ContainerRegistryUsername)
	require.Equal(t, "gcp-registry-access-token", props.ContainerRegistryPassword)
}

func TestApplyCredentialOverrides_AWSECRCredentials(t *testing.T) {
	setSigningKey(t)
	resetTokenExchangeState(t)
	env := testenv.GetTestEnv(t)
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	env.SetClock(clockwork.NewFakeClockAt(now))
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	ctx := claims.AuthContext(context.Background(), &claims.Claims{GroupID: "GR-AWS-REGISTRY"})

	registryPassword := "ecr-password"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("X-Amz-Target"), "GetAuthorizationToken") {
			writeJSON(w, map[string]interface{}{
				"authorizationData": []map[string]interface{}{
					{
						"authorizationToken": base64.StdEncoding.EncodeToString([]byte("AWS:" + registryPassword)),
						"expiresAt":          now.Add(time.Hour).Unix(),
					},
				},
			})
			return
		}
		require.NoError(t, r.ParseForm())
		require.Equal(t, "AssumeRoleWithWebIdentity", r.Form.Get("Action"))
		require.NotEmpty(t, r.Form.Get("WebIdentityToken"))
		w.Header().Set("Content-Type", "text/xml")
		fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse>
<AssumeRoleWithWebIdentityResult>
<Credentials>
<AccessKeyId>AKIA_TEST</AccessKeyId>
<SecretAccessKey>secret-access-key</SecretAccessKey>
<SessionToken>session-token</SessionToken>
<Expiration>%s</Expiration>
</Credentials>
</AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>`, now.Add(time.Hour).Format(time.RFC3339))
	}))
	defer server.Close()
	awsSTSEndpoint = server.URL
	awsECREndpoint = server.URL

	props := &platform.Properties{
		ContainerImage:              "123456789012.dkr.ecr.us-west-2.amazonaws.com/repo/image:tag",
		ContainerRegistryAuthMethod: platform.ContainerRegistryAuthMethodOIDC,
		OIDCProvider:                platform.OIDCProviderAWS,
		OIDCAWSRoleARN:              "arn:aws:iam::123456789012:role/buildbuddy-rbe",
	}

	creds, err := ExchangeCredentials(ctx, env, nil, props, imageRegistryHost(props.ContainerImage))
	require.NoError(t, err)
	task := &repb.ExecutionTask{OidcCredentials: credentialsProto(creds)}
	envVars, secretEnvVars, err := ApplyCredentialOverrides(context.Background(), env, task, props)
	require.NoError(t, err)
	require.Contains(t, envVars, awsRegionEnvVar+"=us-west-2")
	require.NotContains(t, secretEnvVars, AccessTokenEnvVar+"=session-token")
	require.Contains(t, secretEnvVars, awsAccessKeyIDEnvVar+"=AKIA_TEST")
	require.Contains(t, secretEnvVars, awsSecretAccessKeyEnvVar+"=secret-access-key")
	require.Contains(t, secretEnvVars, awsSessionTokenEnvVar+"=session-token")
	require.Equal(t, "AWS", props.ContainerRegistryUsername)
	require.Equal(t, registryPassword, props.ContainerRegistryPassword)
}

func TestMaterializeGCloudAccessTokenFile(t *testing.T) {
	tmp := t.TempDir()
	command := &repb.Command{
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: AccessTokenEnvVar, Value: "gcp-access-token"},
		},
	}

	// The runner writes the raw OAuth access token into the action workspace
	// after inputs are downloaded, so gcloud can read its standard token file.
	created, err := MaterializeGCloudAccessTokenFile(command, tmp, "/workspace")
	require.NoError(t, err)
	require.True(t, created)

	tokenPath := filepath.Join(tmp, ".buildbuddy", "gcloud_access_token")
	tokenBytes, err := os.ReadFile(tokenPath)
	require.NoError(t, err)
	require.Equal(t, "gcp-access-token", string(tokenBytes))

	// The action sees the file at the workspace path used by the container, not
	// necessarily the host path where the executor wrote it.
	gcloudTokenPath, ok := commandEnvValue(command, GCloudAccessTokenFileEnvVar)
	require.True(t, ok)
	require.Equal(t, "/workspace/.buildbuddy/gcloud_access_token", gcloudTokenPath)
	accessToken, ok := commandEnvValue(command, AccessTokenEnvVar)
	require.True(t, ok)
	require.Equal(t, "gcp-access-token", accessToken)
}

func TestRemoveGCloudAccessTokenFile(t *testing.T) {
	tmp := t.TempDir()
	command := &repb.Command{
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: AccessTokenEnvVar, Value: "gcp-access-token"},
		},
	}

	// A recycled runner cleans up the token file before the next action's inputs
	// are downloaded, preventing a previous action's token from lingering.
	created, err := MaterializeGCloudAccessTokenFile(command, tmp, tmp)
	require.NoError(t, err)
	require.True(t, created)
	err = RemoveGCloudAccessTokenFile(tmp)
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(tmp, ".buildbuddy", "gcloud_access_token"))
	require.True(t, os.IsNotExist(err))
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
	u, err := url.Parse(rawURL)
	require.NoError(t, err)
	flags.Set(t, "app.build_buddy_url", *u)
}

func jsonDecode(s string, v interface{}) error {
	return json.NewDecoder(strings.NewReader(s)).Decode(v)
}

func resetTokenExchangeState(t *testing.T) {
	oldHTTPClient := httpClient
	oldAWSSTSEndpoint := awsSTSEndpoint
	oldAWSECREndpoint := awsECREndpoint
	oldGCPSTSEndpoint := gcpSTSEndpoint
	oldGCPIAMCredentialsEndpoint := gcpIAMCredentialsEndpoint
	tokenExchangeCacheMu.Lock()
	oldTokenExchangeCache := tokenExchangeCache
	tokenExchangeCache = map[tokenExchangeCacheKey]*CloudCredentials{}
	tokenExchangeCacheMu.Unlock()
	httpClient = http.DefaultClient
	awsSTSEndpoint = ""
	awsECREndpoint = ""
	gcpSTSEndpoint = "https://sts.googleapis.com/v1/token"
	gcpIAMCredentialsEndpoint = "https://iamcredentials.googleapis.com/v1"
	t.Cleanup(func() {
		tokenExchangeCacheMu.Lock()
		tokenExchangeCache = oldTokenExchangeCache
		tokenExchangeCacheMu.Unlock()
		httpClient = oldHTTPClient
		awsSTSEndpoint = oldAWSSTSEndpoint
		awsECREndpoint = oldAWSECREndpoint
		gcpSTSEndpoint = oldGCPSTSEndpoint
		gcpIAMCredentialsEndpoint = oldGCPIAMCredentialsEndpoint
	})
}

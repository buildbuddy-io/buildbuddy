package rbeoidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oidcissuer"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/awsoidc"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/gcpoidc"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
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

	envVars, secretEnvVars, err := GenerateRequestEnvForGroup(env, task, "sts.amazonaws.com", "GR123", "US456")
	require.NoError(t, err)
	require.Len(t, envVars, 1)
	require.Len(t, secretEnvVars, 1)
	require.Equal(t, TokenRequestURLEnvVar+"=https://buildbuddy.example.com/oidc/token?api-version=2", envVars[0])
	require.True(t, strings.HasPrefix(secretEnvVars[0], TokenRequestTokenEnvVar+"="))

	requestToken := strings.TrimPrefix(secretEnvVars[0], TokenRequestTokenEnvVar+"=")
	provider, err := newProvider(env)
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
	provider.token(rec, req)
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
	requestToken, err := generateRequestToken(env, &repb.ExecutionTask{}, "sts.amazonaws.com", "GR123", "")
	require.NoError(t, err)
	provider, err := newProvider(env)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/oidc/token", nil)
	req.Header.Set("Authorization", "Bearer "+requestToken)
	rec := httptest.NewRecorder()
	provider.token(rec, req)
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
	_, err := generateRequestToken(env, &repb.ExecutionTask{}, requestTokenAudience, "GR123", "")
	require.Error(t, err)

	requestToken, err := generateRequestToken(env, &repb.ExecutionTask{}, "sts.amazonaws.com", "GR123", "")
	require.NoError(t, err)
	provider, err := newProvider(env)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/oidc/token?audience="+url.QueryEscape(requestTokenAudience), nil)
	req.Header.Set("Authorization", "Bearer "+requestToken)
	rec := httptest.NewRecorder()
	provider.token(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestIDTokenCannotBeUsedAsRequestToken(t *testing.T) {
	setSigningKey(t)
	env := testenv.GetTestEnv(t)
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	provider, err := newProvider(env)
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
		executionClaims: executionClaims{
			TokenUse:          idTokenUse,
			BuildBuddyGroupID: "GR123",
		},
	}
	idToken, err := provider.issuer.Sign(idClaims)
	require.NoError(t, err)

	_, err = provider.parseRequestToken(idToken)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid token use")
}

func TestGenerateRequestTokenRequiresGroup(t *testing.T) {
	setSigningKey(t)
	env := testenv.GetTestEnv(t)
	_, err := generateRequestToken(env, &repb.ExecutionTask{}, "sts.amazonaws.com", "", "")
	require.Error(t, err)
}

func TestApplyCredentialOverrides_GCPAccessTokenIsCached(t *testing.T) {
	key := setSigningKey(t)
	resetTokenExchangeState(t)
	env := testenv.GetTestEnv(t)
	now := time.Now()
	env.SetClock(clockwork.NewFakeClockAt(now))
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	ctx := context.Background()

	var gcpRequests int
	exchanger := NewExchanger(&fakeCloudTokenExchanger{
		t: t,
		exchangeGCP: func(ctx context.Context, cfg gcpoidc.Config, idToken string) (*gcpoidc.Credentials, error) {
			gcpRequests++
			require.Equal(t, "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider", cfg.Audience)
			require.Equal(t, "rbe@example.iam.gserviceaccount.com", cfg.ServiceAccount)
			requireIDTokenGroup(t, key, idToken, "GR-GCP-CACHE")
			return &gcpoidc.Credentials{
				Proto: &repb.OIDCCredentials{
					SecretEnvVars: []string{gcpoidc.AccessTokenEnvVar + "=impersonated-access-token"},
				},
				ExpiresAt: now.Add(time.Hour),
			}, nil
		},
	})

	props := &platform.Properties{
		OIDCProvider:          platform.OIDCProviderGCP,
		OIDCTokenAudience:     "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
		OIDCGCPServiceAccount: "rbe@example.iam.gserviceaccount.com",
	}

	creds, err := exchanger.GenerateExchangedCredentialsForGroup(ctx, env, props, "GR-GCP-CACHE")
	require.NoError(t, err)
	require.Empty(t, creds.GetEnvVars())
	require.Contains(t, creds.GetSecretEnvVars(), gcpoidc.AccessTokenEnvVar+"=impersonated-access-token")

	creds, err = exchanger.GenerateExchangedCredentialsForGroup(ctx, env, props, "GR-GCP-CACHE")
	require.NoError(t, err)
	require.Contains(t, creds.GetSecretEnvVars(), gcpoidc.AccessTokenEnvVar+"=impersonated-access-token")
	require.Equal(t, 1, gcpRequests)
}

func TestApplyCredentialOverrides_UsesLeasedCredentials(t *testing.T) {
	key := setSigningKey(t)
	resetTokenExchangeState(t)
	appEnv := testenv.GetTestEnv(t)
	now := time.Now()
	appEnv.SetClock(clockwork.NewFakeClockAt(now))
	ctx := context.Background()
	task := &repb.ExecutionTask{
		InvocationId: "INV-APP-EXCHANGE",
		ExecutionId:  "EXEC-APP-EXCHANGE",
		Command:      &repb.Command{},
	}
	props := &platform.Properties{
		OIDCProvider:      platform.OIDCProviderGCP,
		OIDCTokenAudience: "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
	}

	var gcpRequests int
	exchanger := NewExchanger(&fakeCloudTokenExchanger{
		t: t,
		exchangeGCP: func(ctx context.Context, cfg gcpoidc.Config, idToken string) (*gcpoidc.Credentials, error) {
			gcpRequests++
			require.Equal(t, "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider", cfg.Audience)
			requireIDTokenGroup(t, key, idToken, "GR-APP-EXCHANGE")
			return &gcpoidc.Credentials{
				Proto: &repb.OIDCCredentials{
					SecretEnvVars: []string{gcpoidc.AccessTokenEnvVar + "=app-exchanged-access-token"},
				},
				ExpiresAt: now.Add(time.Hour),
			}, nil
		},
	})

	creds, err := exchanger.GenerateExchangedCredentialsForGroup(ctx, appEnv, props, "GR-APP-EXCHANGE")
	require.NoError(t, err)
	require.NotNil(t, creds)
	task.OidcCredentials = creds

	_, exchangedSecretEnvVars, err := ApplyCredentialOverrides(task, props)
	require.NoError(t, err)
	require.Contains(t, exchangedSecretEnvVars, gcpoidc.AccessTokenEnvVar+"=app-exchanged-access-token")
	require.Nil(t, task.GetOidcCredentials())
	require.Equal(t, 1, gcpRequests)
}

func TestGenerateExchangedCredentialsForGroup_RequiresGroupID(t *testing.T) {
	props := &platform.Properties{
		OIDCProvider:      platform.OIDCProviderGCP,
		OIDCTokenAudience: "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
	}

	// OIDC exchange is a group scoped feature, so the app refuses to mint or
	// exchange credentials if the task is not associated with a BuildBuddy group.
	creds, err := GenerateExchangedCredentialsForGroup(context.Background(), testenv.GetTestEnv(t), props, "")
	require.Error(t, err)
	require.Nil(t, creds)
	require.Contains(t, err.Error(), "authenticated BuildBuddy group")
}

func TestApplyCredentialOverrides_RequiresAppExchangedCredentials(t *testing.T) {
	props := &platform.Properties{
		OIDCProvider:      platform.OIDCProviderGCP,
		OIDCTokenAudience: "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
	}
	_, _, err := ApplyCredentialOverrides(&repb.ExecutionTask{}, props)
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
	require.NoError(t, storeCachedCredentials(env, key, &exchangedCredentials{
		credentials: &repb.OIDCCredentials{
			SecretEnvVars: []string{gcpoidc.AccessTokenEnvVar + "=cached-token"},
		},
		expiresAt: now.Add(time.Hour),
	}))
	clock.Advance(29*time.Minute + 59*time.Second)
	creds, err := cachedCredentials(env, key)
	require.NoError(t, err)
	require.NotNil(t, creds)
	require.Contains(t, creds.GetSecretEnvVars(), gcpoidc.AccessTokenEnvVar+"=cached-token")

	// Once half the lifetime has elapsed, the next action refreshes instead of
	// receiving credentials with less buffer remaining.
	clock.Advance(time.Second)
	creds, err = cachedCredentials(env, key)
	require.NoError(t, err)
	require.Nil(t, creds)
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
	require.NoError(t, storeCachedCredentials(env, key, &exchangedCredentials{
		credentials: &repb.OIDCCredentials{
			SecretEnvVars: []string{gcpoidc.AccessTokenEnvVar + "=short-token"},
		},
		expiresAt: now.Add(5 * time.Minute),
	}))
	creds, err := cachedCredentials(env, key)
	require.NoError(t, err)
	require.NotNil(t, creds)
	require.Contains(t, creds.GetSecretEnvVars(), gcpoidc.AccessTokenEnvVar+"=short-token")

	// The second half of the short lifetime is kept as expiration slack for
	// longer running actions.
	clock.Advance(2*time.Minute + 29*time.Second)
	creds, err = cachedCredentials(env, key)
	require.NoError(t, err)
	require.NotNil(t, creds)
	clock.Advance(time.Second)
	creds, err = cachedCredentials(env, key)
	require.NoError(t, err)
	require.Nil(t, creds)
}

func TestApplyCredentialOverrides_GCPRegistryCredentials(t *testing.T) {
	key := setSigningKey(t)
	resetTokenExchangeState(t)
	env := testenv.GetTestEnv(t)
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	env.SetClock(clockwork.NewFakeClockAt(now))
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	ctx := context.Background()

	exchanger := NewExchanger(&fakeCloudTokenExchanger{
		t: t,
		exchangeGCP: func(ctx context.Context, cfg gcpoidc.Config, idToken string) (*gcpoidc.Credentials, error) {
			require.Equal(t, "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider", cfg.Audience)
			requireIDTokenGroup(t, key, idToken, "GR-GCP-REGISTRY")
			return &gcpoidc.Credentials{
				Proto: &repb.OIDCCredentials{
					SecretEnvVars:             []string{gcpoidc.AccessTokenEnvVar + "=gcp-registry-access-token"},
					ContainerRegistryUsername: gcpoidc.RegistryUsername,
					ContainerRegistryPassword: "gcp-registry-access-token",
				},
				ExpiresAt: now.Add(time.Hour),
			}, nil
		},
	})

	props := &platform.Properties{
		ContainerImage:              "us-docker.pkg.dev/project/repo/image:tag",
		ContainerRegistryAuthSource: platform.ContainerRegistryAuthSourceOIDC,
		OIDCProvider:                platform.OIDCProviderGCP,
		OIDCTokenAudience:           "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
	}

	creds, err := exchanger.GenerateExchangedCredentialsForGroup(ctx, env, props, "GR-GCP-REGISTRY")
	require.NoError(t, err)
	task := &repb.ExecutionTask{OidcCredentials: creds}
	_, secretEnvVars, err := ApplyCredentialOverrides(task, props)
	require.NoError(t, err)
	require.Contains(t, secretEnvVars, gcpoidc.AccessTokenEnvVar+"=gcp-registry-access-token")
	require.Equal(t, gcpoidc.RegistryUsername, props.ContainerRegistryUsername)
	require.Equal(t, "gcp-registry-access-token", props.ContainerRegistryPassword)
	// Actions can write the Docker config JSON directly to ~/.docker/config.json
	// when they need to run Docker-compatible commands against the registry.
	requireDockerConfigAuth(t, secretEnvVars, "us-docker.pkg.dev", gcpoidc.RegistryUsername, "gcp-registry-access-token")
}

func TestApplyCredentialOverrides_AWSECRCredentials(t *testing.T) {
	key := setSigningKey(t)
	resetTokenExchangeState(t)
	env := testenv.GetTestEnv(t)
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	env.SetClock(clockwork.NewFakeClockAt(now))
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	ctx := context.Background()

	registryPassword := "ecr-password"
	exchanger := NewExchanger(&fakeCloudTokenExchanger{
		t: t,
		exchangeAWS: func(ctx context.Context, cfg awsoidc.Config, idToken, groupID string) (*awsoidc.Credentials, error) {
			require.Equal(t, "GR-AWS-REGISTRY", groupID)
			require.Equal(t, "arn:aws:iam::123456789012:role/buildbuddy-rbe", cfg.RoleARN)
			require.Equal(t, "us-west-2", cfg.Region)
			require.Equal(t, "123456789012", cfg.RegistryID)
			requireIDTokenGroup(t, key, idToken, "GR-AWS-REGISTRY")
			return &awsoidc.Credentials{
				Proto: &repb.OIDCCredentials{
					EnvVars: []string{awsoidc.RegionEnvVar + "=us-west-2"},
					SecretEnvVars: []string{
						awsoidc.AccessKeyIDEnvVar + "=AKIA_TEST",
						awsoidc.SecretAccessKeyEnvVar + "=secret-access-key",
						awsoidc.SessionTokenEnvVar + "=session-token",
					},
					ContainerRegistryUsername: "AWS",
					ContainerRegistryPassword: registryPassword,
				},
				ExpiresAt: now.Add(time.Hour),
			}, nil
		},
	})

	props := &platform.Properties{
		ContainerImage:              "123456789012.dkr.ecr.us-west-2.amazonaws.com/repo/image:tag",
		ContainerRegistryAuthSource: platform.ContainerRegistryAuthSourceOIDC,
		OIDCProvider:                platform.OIDCProviderAWS,
		OIDCAWSRoleARN:              "arn:aws:iam::123456789012:role/buildbuddy-rbe",
	}

	creds, err := exchanger.GenerateExchangedCredentialsForGroup(ctx, env, props, "GR-AWS-REGISTRY")
	require.NoError(t, err)
	task := &repb.ExecutionTask{OidcCredentials: creds}
	envVars, secretEnvVars, err := ApplyCredentialOverrides(task, props)
	require.NoError(t, err)
	require.Contains(t, envVars, awsoidc.RegionEnvVar+"=us-west-2")
	require.NotContains(t, secretEnvVars, gcpoidc.AccessTokenEnvVar+"=session-token")
	require.Contains(t, secretEnvVars, awsoidc.AccessKeyIDEnvVar+"=AKIA_TEST")
	require.Contains(t, secretEnvVars, awsoidc.SecretAccessKeyEnvVar+"=secret-access-key")
	require.Contains(t, secretEnvVars, awsoidc.SessionTokenEnvVar+"=session-token")
	require.Equal(t, "AWS", props.ContainerRegistryUsername)
	require.Equal(t, registryPassword, props.ContainerRegistryPassword)
	// Actions can write the Docker config JSON directly to ~/.docker/config.json
	// when they need to run Docker-compatible commands against the registry.
	requireDockerConfigAuth(t, secretEnvVars, "123456789012.dkr.ecr.us-west-2.amazonaws.com", "AWS", registryPassword)
}

func TestWellKnownAndJWKS(t *testing.T) {
	signingKey := setSigningKey(t)
	env := testenv.GetTestEnv(t)
	setBuildBuddyURL(t, "https://buildbuddy.example.com")
	provider, err := newProvider(env)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	provider.issuer.ServeWellKnownOpenIDConfiguration(rec, httptest.NewRequest(http.MethodGet, wellKnownPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var config oidcissuer.ProviderMetadata
	require.NoError(t, jsonDecode(rec.Body.String(), &config))
	require.Equal(t, "https://buildbuddy.example.com/oidc", config.Issuer)
	require.Equal(t, "https://buildbuddy.example.com/oidc/.well-known/jwks", config.JWKSURI)
	require.Contains(t, config.IdTokenSigningAlgValuesSupported, "RS256")
	require.Contains(t, config.ClaimsSupported, "sub")

	rec = httptest.NewRecorder()
	provider.issuer.ServeJWKS(rec, httptest.NewRequest(http.MethodGet, jwksPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var keySet oidcissuer.JWKS
	require.NoError(t, jsonDecode(rec.Body.String(), &keySet))
	require.Len(t, keySet.Keys, 1)
	publicKey := keySet.Keys[0]
	require.Equal(t, "RSA", publicKey.Kty)
	require.Equal(t, "sig", publicKey.Use)
	require.Equal(t, "RS256", publicKey.Alg)
	require.NotEmpty(t, publicKey.Kid)
	require.Equal(t, base64.RawURLEncoding.EncodeToString(signingKey.PublicKey.N.Bytes()), publicKey.N)
	require.Equal(t, base64.RawURLEncoding.EncodeToString(big.NewInt(int64(signingKey.PublicKey.E)).Bytes()), publicKey.E)
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

func requireDockerConfigAuth(t *testing.T, envVars []string, registryHost, username, password string) {
	dockerConfig, ok := envOverrideValue(envVars, DockerConfigJSONEnvVar)
	require.True(t, ok)
	var config dockerConfigFile
	require.NoError(t, jsonDecode(dockerConfig, &config))
	auth, ok := config.Auths[registryHost]
	require.True(t, ok)
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte(username+":"+password)), auth.Auth)
}

func envOverrideValue(envVars []string, name string) (string, bool) {
	for i := len(envVars) - 1; i >= 0; i-- {
		n, v, _ := strings.Cut(envVars[i], "=")
		if n == name {
			return v, true
		}
	}
	return "", false
}

type fakeCloudTokenExchanger struct {
	t           *testing.T
	exchangeAWS func(context.Context, awsoidc.Config, string, string) (*awsoidc.Credentials, error)
	exchangeGCP func(context.Context, gcpoidc.Config, string) (*gcpoidc.Credentials, error)
}

func (f *fakeCloudTokenExchanger) ExchangeAWS(ctx context.Context, cfg awsoidc.Config, idToken, groupID string) (*awsoidc.Credentials, error) {
	if f.exchangeAWS == nil {
		f.t.Fatalf("unexpected AWS exchange")
		return nil, nil
	}
	return f.exchangeAWS(ctx, cfg, idToken, groupID)
}

func (f *fakeCloudTokenExchanger) ExchangeGCP(ctx context.Context, cfg gcpoidc.Config, idToken string) (*gcpoidc.Credentials, error) {
	if f.exchangeGCP == nil {
		f.t.Fatalf("unexpected GCP exchange")
		return nil, nil
	}
	return f.exchangeGCP(ctx, cfg, idToken)
}

func requireIDTokenGroup(t *testing.T, key *rsa.PrivateKey, idToken, groupID string) {
	idClaims := &idTokenClaims{}
	parser := jwt.Parser{SkipClaimsValidation: true}
	parsed, err := parser.ParseWithClaims(idToken, idClaims, func(token *jwt.Token) (interface{}, error) {
		return &key.PublicKey, nil
	})
	require.NoError(t, err)
	require.True(t, parsed.Valid)
	require.Equal(t, groupID, idClaims.BuildBuddyGroupID)
}

func resetTokenExchangeState(t *testing.T) {
	oldHTTPClient := httpClient
	oldAWSSTSEndpoint := awsSTSEndpoint
	oldAWSECREndpoint := awsECREndpoint
	oldGCPSTSEndpoint := gcpSTSEndpoint
	oldGCPIAMCredentialsEndpoint := gcpIAMCredentialsEndpoint
	tokenExchangeCacheMu.Lock()
	oldTokenExchangeCache := tokenExchangeCache
	tokenExchangeCache = nil
	tokenExchangeCacheMu.Unlock()
	httpClient = http.DefaultClient
	awsSTSEndpoint = ""
	awsECREndpoint = ""
	gcpSTSEndpoint = gcpoidc.DefaultSTSEndpoint
	gcpIAMCredentialsEndpoint = gcpoidc.DefaultIAMCredentialsEndpoint
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

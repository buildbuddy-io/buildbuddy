// Package rbeoidc implements OpenID Connect identity for BuildBuddy RBE actions.
//
// The package has two related responsibilities. First, it exposes a small OIDC
// issuer under /oidc so relying parties can discover BuildBuddy's issuer URL
// and public signing key. Actions that set oidc-token-audience can receive a
// BuildBuddy request token and exchange it at /oidc/token for an ID token whose
// subject identifies the authenticated BuildBuddy group.
//
// Second, it can use that BuildBuddy-issued ID token as an input to cloud
// provider token exchanges. For AWS, it calls STS AssumeRoleWithWebIdentity and,
// when requested for container image pulls, ECR GetAuthorizationToken. For GCP,
// it calls Security Token Service and can optionally impersonate a service
// account. In the normal lease path, the app performs this exchange right
// before handing the task to an executor, then carries the exchanged credentials
// to the executor on the ExecutionTask so executors do not need the app's OIDC
// signing key and queued tasks do not receive stale credentials. The exchanged
// credentials are injected as secret env vars for the action and can also be
// written back to platform properties as container registry credentials. GCP
// access tokens are also materialized into
// .buildbuddy/gcloud_access_token in the action workspace and exposed to
// gcloud via CLOUDSDK_AUTH_ACCESS_TOKEN_FILE.
//
// Exchanged provider credentials are cached by BuildBuddy group and provider
// configuration until half of their lifetime has elapsed. This avoids excessive
// cloud token exchanges while leaving the second half of the credential lifetime
// as buffer for longer running actions. Token values are intentionally kept out
// of logs and returned as secret env overrides so runner log redaction can scrub
// them from workflow output.
package rbeoidc

import (
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ci_runner_env"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"

	awscreds "github.com/aws/aws-sdk-go/aws/credentials"
	awsecr "github.com/aws/aws-sdk-go/service/ecr"
	awssession "github.com/aws/aws-sdk-go/aws/session"
	awssts "github.com/aws/aws-sdk-go/service/sts"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// TokenRequestURLEnvVar points actions at the BuildBuddy endpoint that can
	// exchange a request token for a BuildBuddy OIDC ID token.
	TokenRequestURLEnvVar = "BUILDBUDDY_OIDC_TOKEN_REQUEST_URL"

	// TokenRequestTokenEnvVar contains the bearer token used with
	// TokenRequestURLEnvVar.
	TokenRequestTokenEnvVar = "BUILDBUDDY_OIDC_TOKEN_REQUEST_TOKEN"

	// AccessTokenEnvVar contains a provider access token exchanged from a
	// BuildBuddy OIDC ID token.
	AccessTokenEnvVar = "BUILDBUDDY_OIDC_ACCESS_TOKEN"

	// GCloudAccessTokenFileEnvVar is the gcloud property environment variable
	// that points gcloud commands at a raw OAuth access token file.
	GCloudAccessTokenFileEnvVar = "CLOUDSDK_AUTH_ACCESS_TOKEN_FILE"

	issuerPath    = "/oidc"
	wellKnownPath = issuerPath + "/.well-known/openid-configuration"
	jwksPath      = issuerPath + "/.well-known/jwks"
	tokenPath     = issuerPath + "/token"

	requestTokenAudience = "buildbuddy-rbe-oidc-token-request"
	runnerEnvironment    = "buildbuddy-rbe"

	requestTokenUse = "buildbuddy-rbe-oidc-request-token"
	idTokenUse      = "id_token"

	awsDefaultAudience = "sts.amazonaws.com"
	awsDefaultRegion   = "us-east-1"

	awsAccessKeyIDEnvVar     = "AWS_ACCESS_KEY_ID"
	awsSecretAccessKeyEnvVar = "AWS_SECRET_ACCESS_KEY"
	awsSessionTokenEnvVar    = "AWS_SESSION_TOKEN"
	awsRegionEnvVar          = "AWS_REGION"

	gcpRegistryUsername      = "oauth2accesstoken"
	gcpCloudPlatformScope    = "https://www.googleapis.com/auth/cloud-platform"
	gcloudAccessTokenRelPath = ".buildbuddy/gcloud_access_token"
)

var (
	signingKeyPEM         = flag.String("remote_execution.oidc.private_key", "", "PEM-encoded RSA private key used by the app to sign OIDC tokens for RBE actions. Configure this on the app to enable the `oidc-token-audience` platform property.", flag.Secret)
	tokenLifetime         = flag.Duration("remote_execution.oidc.token_lifetime", 5*time.Minute, "Lifetime of OIDC ID tokens minted for RBE actions.")
	requestTokenLifetime  = flag.Duration("remote_execution.oidc.request_token_lifetime", 15*time.Minute, "Lifetime of bearer tokens injected into RBE actions for requesting OIDC ID tokens.")
	exchangeTokenLifetime = flag.Duration("remote_execution.oidc.exchange_token_lifetime", time.Hour, "Requested lifetime of cloud provider access tokens exchanged from BuildBuddy RBE OIDC tokens.")

	ecrRegistryHostPattern = regexp.MustCompile(`^([0-9]{12})\.dkr\.ecr(?:-fips)?\.([a-z0-9-]+)\.amazonaws\.com(?:\.cn)?$`)

	tokenExchangeCacheMu sync.Mutex
	tokenExchangeCache   = map[tokenExchangeCacheKey]*CloudCredentials{}
	tokenExchangeGroup   singleflight.Group[tokenExchangeCacheKey, *CloudCredentials]

	signingKeyMu       sync.Mutex
	signingKeyCachePEM string
	signingKeyCacheKey *rsa.PrivateKey
	signingKeyCacheID  string

	httpClient = http.DefaultClient

	awsSTSEndpoint string
	awsECREndpoint string

	gcpSTSEndpoint            = "https://sts.googleapis.com/v1/token"
	gcpIAMCredentialsEndpoint = "https://iamcredentials.googleapis.com/v1"
)

type Provider struct {
	env   environment.Env
	key   *rsa.PrivateKey
	keyID string
}

type configurationJSON struct {
	Issuer                           string   `json:"issuer"`
	JwksURI                          string   `json:"jwks_uri"`
	TokenEndpoint                    string   `json:"token_endpoint,omitempty"`
	SubjectTypesSupported            []string `json:"subject_types_supported"`
	ResponseTypesSupported           []string `json:"response_types_supported"`
	ClaimsSupported                  []string `json:"claims_supported"`
	IdTokenSigningAlgValuesSupported []string `json:"id_token_signing_alg_values_supported"`
	ScopesSupported                  []string `json:"scopes_supported"`
}

type jwksJSON struct {
	Keys []*jwkJSON `json:"keys"`
}

type jwkJSON struct {
	Kty string `json:"kty"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

type tokenResponse struct {
	Value string `json:"value"`
}

// CloudCredentials contains credentials exchanged from a BuildBuddy RBE OIDC token.
type CloudCredentials struct {
	EnvVars          []string
	SecretEnvVars    []string
	RegistryUsername string
	RegistryPassword string
	ExpiresAt        time.Time
	refreshAt        time.Time
}

type tokenExchangeConfig struct {
	Provider          string
	Audience          string
	AWSRoleARN        string
	AWSRegion         string
	AWSRegistryID     string
	GCPServiceAccount string
	RegistryHost      string
}

type tokenExchangeCacheKey struct {
	GroupID           string
	Provider          string
	Audience          string
	AWSRoleARN        string
	AWSRegion         string
	AWSRegistryID     string
	GCPServiceAccount string
	RegistryHost      string
}

type gcpSTSResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

type gcpServiceAccountTokenResponse struct {
	AccessToken string `json:"accessToken"`
	ExpireTime  string `json:"expireTime"`
}

type requestClaims struct {
	jwt.StandardClaims

	TokenUse          string `json:"token_use,omitempty"`
	DefaultAudience   string `json:"default_audience,omitempty"`
	BuildBuddyGroupID string `json:"buildbuddy_group_id,omitempty"`
	BuildBuddyUserID  string `json:"buildbuddy_user_id,omitempty"`
	InvocationID      string `json:"buildbuddy_invocation_id,omitempty"`
	ExecutionID       string `json:"buildbuddy_execution_id,omitempty"`
	InstanceName      string `json:"buildbuddy_instance_name,omitempty"`
	ActionDigest      string `json:"buildbuddy_action_digest,omitempty"`
	ActionID          string `json:"buildbuddy_action_id,omitempty"`
	TargetID          string `json:"buildbuddy_target_id,omitempty"`
	ActionMnemonic    string `json:"buildbuddy_action_mnemonic,omitempty"`
	RunnerEnvironment string `json:"runner_environment,omitempty"`
}

type idTokenClaims struct {
	jwt.StandardClaims

	TokenUse          string `json:"token_use,omitempty"`
	BuildBuddyGroupID string `json:"buildbuddy_group_id,omitempty"`
	BuildBuddyUserID  string `json:"buildbuddy_user_id,omitempty"`
	InvocationID      string `json:"buildbuddy_invocation_id,omitempty"`
	ExecutionID       string `json:"buildbuddy_execution_id,omitempty"`
	InstanceName      string `json:"buildbuddy_instance_name,omitempty"`
	ActionDigest      string `json:"buildbuddy_action_digest,omitempty"`
	ActionID          string `json:"buildbuddy_action_id,omitempty"`
	TargetID          string `json:"buildbuddy_target_id,omitempty"`
	ActionMnemonic    string `json:"buildbuddy_action_mnemonic,omitempty"`
	RunnerEnvironment string `json:"runner_environment,omitempty"`
}

func Register(env environment.Env) error {
	if *signingKeyPEM == "" {
		return nil
	}
	provider, err := NewProvider(env)
	if err != nil {
		return err
	}
	mux := env.GetMux()
	mux.Handle(wellKnownPath, interceptors.SetSecurityHeaders(http.HandlerFunc(provider.WellKnownOpenIDConfiguration)))
	mux.Handle(jwksPath, interceptors.SetSecurityHeaders(http.HandlerFunc(provider.JWKS)))
	mux.Handle(tokenPath, interceptors.SetSecurityHeaders(http.HandlerFunc(provider.Token)))
	return nil
}

func NewProvider(env environment.Env) (*Provider, error) {
	key, keyID, err := signingKey()
	if err != nil {
		return nil, err
	}
	return &Provider{
		env:   env,
		key:   key,
		keyID: keyID,
	}, nil
}

// GenerateRequestEnv returns request-token env vars for the authenticated
// BuildBuddy group in ctx.
func GenerateRequestEnv(ctx context.Context, env environment.Env, task *repb.ExecutionTask, defaultAudience string) ([]string, []string, error) {
	authClaims, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		return nil, nil, status.PermissionDeniedErrorf("OIDC token requests require an authenticated BuildBuddy user: %s", err)
	}
	return GenerateRequestEnvForGroup(ctx, env, task, defaultAudience, authClaims.GetGroupID(), authClaims.GetUserID())
}

// GenerateRequestEnvForGroup returns the request-token env vars for a
// BuildBuddy group. It is used by the app at task lease time, when the lease
// RPC is authenticated as the executor but the task's scheduling metadata still
// identifies the BuildBuddy group that requested the action.
func GenerateRequestEnvForGroup(ctx context.Context, env environment.Env, task *repb.ExecutionTask, defaultAudience, groupID, userID string) ([]string, []string, error) {
	token, err := generateRequestToken(ctx, env, task, defaultAudience, groupID, userID)
	if err != nil {
		return nil, nil, err
	}
	u := tokenEndpoint()
	q := u.Query()
	q.Set("api-version", "2")
	u.RawQuery = q.Encode()
	return []string{
			fmt.Sprintf("%s=%s", TokenRequestURLEnvVar, u.String()),
		}, []string{
			fmt.Sprintf("%s=%s", TokenRequestTokenEnvVar, token),
		}, nil
}

// GenerateExchangedCredentials exchanges cloud credentials on the app.
func GenerateExchangedCredentials(ctx context.Context, env environment.Env, props *platform.Properties) (*repb.OIDCCredentials, error) {
	authClaims, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		return nil, status.PermissionDeniedErrorf("OIDC token exchange requires an authenticated BuildBuddy user: %s", err)
	}
	return GenerateExchangedCredentialsForGroup(ctx, env, props, authClaims.GetGroupID())
}

// GenerateExchangedCredentialsForGroup exchanges cloud credentials for a
// BuildBuddy group and returns the credentials that should be carried to the
// executor with the leased task.
func GenerateExchangedCredentialsForGroup(ctx context.Context, env environment.Env, props *platform.Properties, groupID string) (*repb.OIDCCredentials, error) {
	if !ExchangeEnabled(props) {
		return nil, nil
	}
	registryHost := ""
	if props.ContainerRegistryAuthMethod == platform.ContainerRegistryAuthMethodOIDC {
		registryHost = imageRegistryHost(props.ContainerImage)
	}
	creds, err := exchangeCredentialsForGroup(ctx, env, groupID, props, registryHost)
	if err != nil {
		return nil, err
	}
	return credentialsProto(creds), nil
}

// GenerateRequestToken returns a bearer token that an action can exchange at
// /oidc/token for a BuildBuddy OIDC ID token.
func GenerateRequestToken(ctx context.Context, env environment.Env, task *repb.ExecutionTask, defaultAudience string) (string, error) {
	authClaims, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		return "", status.PermissionDeniedErrorf("OIDC token requests require an authenticated BuildBuddy user: %s", err)
	}
	return generateRequestToken(ctx, env, task, defaultAudience, authClaims.GetGroupID(), authClaims.GetUserID())
}

func generateRequestToken(ctx context.Context, env environment.Env, task *repb.ExecutionTask, defaultAudience, groupID, userID string) (string, error) {
	if defaultAudience == "" {
		return "", status.InvalidArgumentErrorf("%s must be set to a non-empty audience", platform.OIDCTokenAudiencePropertyName)
	}
	if defaultAudience == requestTokenAudience {
		return "", status.InvalidArgumentErrorf("%s cannot be used as an ID token audience", requestTokenAudience)
	}
	key, keyID, err := signingKey()
	if err != nil {
		return "", err
	}
	if groupID == "" {
		return "", status.PermissionDeniedError("OIDC token requests require an authenticated BuildBuddy group")
	}

	now := env.GetClock().Now()
	rc := &requestClaims{
		StandardClaims: jwt.StandardClaims{
			Audience:  requestTokenAudience,
			ExpiresAt: now.Add(*requestTokenLifetime).Unix(),
			Id:        uuid.NewString(),
			IssuedAt:  now.Unix(),
			Issuer:    issuerURL().String(),
			NotBefore: now.Unix(),
			Subject:   subject(groupID),
		},
		TokenUse:          requestTokenUse,
		DefaultAudience:   defaultAudience,
		BuildBuddyGroupID: groupID,
		BuildBuddyUserID:  userID,
		RunnerEnvironment: runnerEnvironment,
	}
	rc.InvocationID = task.GetInvocationId()
	rc.ExecutionID = task.GetExecutionId()
	if req := task.GetExecuteRequest(); req != nil {
		rc.InstanceName = req.GetInstanceName()
		if d := req.GetActionDigest(); d != nil {
			rc.ActionDigest = fmt.Sprintf("%s/%d", d.GetHash(), d.GetSizeBytes())
		}
	}
	if md := task.GetRequestMetadata(); md != nil {
		rc.ActionID = md.GetActionId()
		rc.TargetID = md.GetTargetId()
		rc.ActionMnemonic = md.GetActionMnemonic()
	}

	return sign(key, keyID, rc)
}

// ValidateProperties validates OIDC platform properties without performing any
// token exchange. The app uses this at scheduling time so obvious
// misconfigurations fail before the task sits in the queue.
func ValidateProperties(props *platform.Properties) error {
	if !ExchangeEnabled(props) && props.OIDCTokenAudience == "" {
		return nil
	}
	if _, _, err := signingKey(); err != nil {
		return err
	}
	if !ExchangeEnabled(props) {
		if props.OIDCTokenAudience == "" {
			return status.InvalidArgumentErrorf("%s must be set to a non-empty audience", platform.OIDCTokenAudiencePropertyName)
		}
		if props.OIDCTokenAudience == requestTokenAudience {
			return status.InvalidArgumentErrorf("%s cannot be used as an ID token audience", requestTokenAudience)
		}
		return nil
	}
	registryHost := ""
	if props.ContainerRegistryAuthMethod == platform.ContainerRegistryAuthMethodOIDC {
		registryHost = imageRegistryHost(props.ContainerImage)
	}
	_, err := tokenExchangeConfigFromProperties(props, registryHost)
	return err
}

// ExchangeEnabled reports whether platform properties request cloud token exchange.
func ExchangeEnabled(props *platform.Properties) bool {
	if props.OIDCProvider != "" || props.OIDCAWSRoleARN != "" || props.OIDCGCPServiceAccount != "" {
		return true
	}
	return props.ContainerRegistryAuthMethod == platform.ContainerRegistryAuthMethodOIDC
}

// RequestEnvPresent reports whether the task already includes the app-signed
// request token env vars needed to request a BuildBuddy OIDC ID token.
func RequestEnvPresent(task *repb.ExecutionTask) bool {
	_, hasURL := commandEnvValue(task.GetCommand(), TokenRequestURLEnvVar)
	_, hasToken := commandEnvValue(task.GetCommand(), TokenRequestTokenEnvVar)
	return hasURL && hasToken
}

// ApplyRequestEnv adds OIDC request-token env vars to a command and registers
// secret env vars for runner log redaction.
func ApplyRequestEnv(command *repb.Command, envVars, secretEnvVars []string) error {
	for _, envVar := range append(envVars, secretEnvVars...) {
		name, value, ok := strings.Cut(envVar, "=")
		if !ok {
			return status.InvalidArgumentErrorf("env override %q is missing '='", name)
		}
		setCommandEnvVar(command, name, value)
	}
	if len(secretEnvVars) == 0 {
		return nil
	}
	newNames := make([]string, 0, len(secretEnvVars))
	for _, secretEnvVar := range secretEnvVars {
		name, _, _ := strings.Cut(secretEnvVar, "=")
		newNames = append(newNames, name)
	}
	return addSecretEnvVarNamesForRedaction(command, newNames)
}

// ApplyCredentialOverrides applies cloud credentials attached to the leased
// task and mutates props with registry credentials when requested.
func ApplyCredentialOverrides(_ context.Context, _ environment.Env, task *repb.ExecutionTask, props *platform.Properties) ([]string, []string, error) {
	if !ExchangeEnabled(props) {
		return nil, nil, nil
	}
	if task.GetOidcCredentials() == nil {
		return nil, nil, status.FailedPreconditionError("OIDC credentials were not attached to the leased task; BuildBuddy must exchange OIDC credentials at task lease time")
	}
	creds := cloudCredentialsFromProto(task.GetOidcCredentials())
	task.OidcCredentials = nil
	if props.ContainerRegistryAuthMethod == platform.ContainerRegistryAuthMethodOIDC {
		if creds.RegistryUsername == "" || creds.RegistryPassword == "" {
			return nil, nil, status.InvalidArgumentError("OIDC token exchange did not produce container registry credentials")
		}
		props.ContainerRegistryUsername = creds.RegistryUsername
		props.ContainerRegistryPassword = creds.RegistryPassword
	}
	return creds.EnvVars, creds.SecretEnvVars, nil
}

// MaterializeGCloudAccessTokenFile writes the exchanged GCP OAuth access token
// from the action environment to .buildbuddy/gcloud_access_token in the action
// workspace. It also points the gcloud CLI at the file via
// CLOUDSDK_AUTH_ACCESS_TOKEN_FILE.
func MaterializeGCloudAccessTokenFile(command *repb.Command, hostWorkspacePath, actionWorkspacePath string) (bool, error) {
	accessToken, ok := commandEnvValue(command, AccessTokenEnvVar)
	if !ok || accessToken == "" {
		return false, nil
	}
	if hostWorkspacePath == "" {
		return false, status.InternalError("host workspace path is required")
	}
	if actionWorkspacePath == "" {
		actionWorkspacePath = hostWorkspacePath
	}
	hostTokenPath := filepath.Join(hostWorkspacePath, filepath.FromSlash(gcloudAccessTokenRelPath))
	if err := os.MkdirAll(filepath.Dir(hostTokenPath), 0755); err != nil {
		return false, status.UnavailableErrorf("create gcloud token directory: %s", err)
	}
	if err := os.WriteFile(hostTokenPath, []byte(accessToken), 0644); err != nil {
		return false, status.UnavailableErrorf("write gcloud token file: %s", err)
	}
	actionTokenPath := filepath.Join(actionWorkspacePath, filepath.FromSlash(gcloudAccessTokenRelPath))
	setCommandEnvVar(command, GCloudAccessTokenFileEnvVar, actionTokenPath)
	return true, nil
}

// RemoveGCloudAccessTokenFile removes the gcloud access token file that was
// previously materialized into the action workspace.
func RemoveGCloudAccessTokenFile(hostWorkspacePath string) error {
	if hostWorkspacePath == "" {
		return nil
	}
	hostTokenPath := filepath.Join(hostWorkspacePath, filepath.FromSlash(gcloudAccessTokenRelPath))
	if err := os.Remove(hostTokenPath); err != nil && !os.IsNotExist(err) {
		return status.UnavailableErrorf("remove gcloud token file: %s", err)
	}
	return nil
}

// ExchangeCredentials exchanges a BuildBuddy RBE OIDC token for cloud provider
// credentials. Results are cached until half of the provider credential lifetime
// has elapsed.
func ExchangeCredentials(ctx context.Context, env environment.Env, _ *repb.ExecutionTask, props *platform.Properties, registryHost string) (*CloudCredentials, error) {
	authClaims, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		return nil, status.PermissionDeniedErrorf("OIDC token exchange requires an authenticated BuildBuddy user: %s", err)
	}
	return exchangeCredentialsForGroup(ctx, env, authClaims.GetGroupID(), props, registryHost)
}

func exchangeCredentialsForGroup(ctx context.Context, env environment.Env, groupID string, props *platform.Properties, registryHost string) (*CloudCredentials, error) {
	if groupID == "" {
		return nil, status.PermissionDeniedError("OIDC token exchange requires an authenticated BuildBuddy group")
	}
	cfg, err := tokenExchangeConfigFromProperties(props, registryHost)
	if err != nil {
		return nil, err
	}
	key := tokenExchangeCacheKey{
		GroupID:           groupID,
		Provider:          cfg.Provider,
		Audience:          cfg.Audience,
		AWSRoleARN:        cfg.AWSRoleARN,
		AWSRegion:         cfg.AWSRegion,
		AWSRegistryID:     cfg.AWSRegistryID,
		GCPServiceAccount: cfg.GCPServiceAccount,
	}
	if cfg.Provider == platform.OIDCProviderAWS {
		key.RegistryHost = cfg.RegistryHost
	}
	if creds := cachedCredentials(env, key); creds != nil {
		return creds, nil
	}
	creds, _, err := tokenExchangeGroup.Do(ctx, key, func(ctx context.Context) (*CloudCredentials, error) {
		if creds := cachedCredentials(env, key); creds != nil {
			return creds, nil
		}
		idToken, err := generateGroupIDToken(ctx, env, groupID, cfg.Audience)
		if err != nil {
			return nil, err
		}
		var creds *CloudCredentials
		switch cfg.Provider {
		case platform.OIDCProviderAWS:
			creds, err = exchangeAWS(ctx, cfg, idToken, groupID)
		case platform.OIDCProviderGCP:
			creds, err = exchangeGCP(ctx, env, cfg, idToken)
		default:
			err = status.InvalidArgumentErrorf("unsupported OIDC provider %q", cfg.Provider)
		}
		if err != nil {
			return nil, err
		}
		storeCachedCredentials(env, key, creds)
		return creds.clone(), nil
	})
	if err != nil {
		return nil, err
	}
	return creds, nil
}

func (p *Provider) WellKnownOpenIDConfiguration(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, &configurationJSON{
		Issuer:        issuerURL().String(),
		JwksURI:       jwksEndpoint().String(),
		TokenEndpoint: tokenEndpoint().String(),
		SubjectTypesSupported: []string{
			"public",
		},
		ResponseTypesSupported: []string{
			"id_token",
		},
		ClaimsSupported: []string{
			"sub",
			"aud",
			"exp",
			"iat",
			"iss",
			"jti",
			"nbf",
			"token_use",
			"runner_environment",
			"buildbuddy_group_id",
			"buildbuddy_user_id",
			"buildbuddy_invocation_id",
			"buildbuddy_execution_id",
			"buildbuddy_instance_name",
			"buildbuddy_action_digest",
			"buildbuddy_action_id",
			"buildbuddy_target_id",
			"buildbuddy_action_mnemonic",
		},
		IdTokenSigningAlgValuesSupported: []string{
			"RS256",
		},
		ScopesSupported: []string{
			"openid",
		},
	})
}

func (p *Provider) JWKS(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, &jwksJSON{
		Keys: []*jwkJSON{
			publicJWK(&p.key.PublicKey, p.keyID),
		},
	})
}

func (p *Provider) Token(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	requestToken, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		http.Error(w, "missing bearer token", http.StatusUnauthorized)
		return
	}
	rc, err := p.parseRequestToken(requestToken)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	audience := strings.TrimSpace(r.URL.Query().Get("audience"))
	if audience == "" {
		audience = rc.DefaultAudience
	}
	if audience == "" {
		http.Error(w, "missing audience", http.StatusBadRequest)
		return
	}
	if audience == requestTokenAudience {
		http.Error(w, fmt.Sprintf("%s cannot be used as an ID token audience", requestTokenAudience), http.StatusBadRequest)
		return
	}
	idToken, err := p.idToken(rc, audience)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, &tokenResponse{Value: idToken})
}

func (p *Provider) parseRequestToken(tokenString string) (*requestClaims, error) {
	rc := &requestClaims{}
	token, err := jwt.ParseWithClaims(tokenString, rc, func(token *jwt.Token) (interface{}, error) {
		if token.Method != jwt.SigningMethodRS256 {
			return nil, fmt.Errorf("unexpected signing method %q", token.Method.Alg())
		}
		if kid, _ := token.Header["kid"].(string); kid != "" && kid != p.keyID {
			return nil, fmt.Errorf("unknown key ID %q", kid)
		}
		return &p.key.PublicKey, nil
	})
	if err != nil {
		return nil, err
	}
	if !token.Valid {
		return nil, status.PermissionDeniedError("invalid request token")
	}
	if rc.Issuer != issuerURL().String() {
		return nil, status.PermissionDeniedErrorf("invalid issuer %q", rc.Issuer)
	}
	if rc.Audience != requestTokenAudience {
		return nil, status.PermissionDeniedErrorf("invalid audience %q", rc.Audience)
	}
	if rc.TokenUse != requestTokenUse {
		return nil, status.PermissionDeniedErrorf("invalid token use %q", rc.TokenUse)
	}
	if rc.BuildBuddyGroupID == "" {
		return nil, status.PermissionDeniedError("missing BuildBuddy group ID")
	}
	if rc.Subject == "" {
		rc.Subject = subject(rc.BuildBuddyGroupID)
	}
	return rc, nil
}

func (p *Provider) idToken(rc *requestClaims, audience string) (string, error) {
	now := p.env.GetClock().Now()
	idc := &idTokenClaims{
		StandardClaims: jwt.StandardClaims{
			Audience:  audience,
			ExpiresAt: now.Add(*tokenLifetime).Unix(),
			Id:        uuid.NewString(),
			IssuedAt:  now.Unix(),
			Issuer:    issuerURL().String(),
			NotBefore: now.Unix(),
			Subject:   rc.Subject,
		},
		TokenUse:          idTokenUse,
		BuildBuddyGroupID: rc.BuildBuddyGroupID,
		BuildBuddyUserID:  rc.BuildBuddyUserID,
		InvocationID:      rc.InvocationID,
		ExecutionID:       rc.ExecutionID,
		InstanceName:      rc.InstanceName,
		ActionDigest:      rc.ActionDigest,
		ActionID:          rc.ActionID,
		TargetID:          rc.TargetID,
		ActionMnemonic:    rc.ActionMnemonic,
		RunnerEnvironment: runnerEnvironment,
	}
	return sign(p.key, p.keyID, idc)
}

func tokenExchangeConfigFromProperties(props *platform.Properties, registryHost string) (*tokenExchangeConfig, error) {
	provider := strings.ToLower(props.OIDCProvider)
	audience := props.OIDCTokenAudience
	awsRoleARN := props.OIDCAWSRoleARN
	awsRegion := props.OIDCAWSRegion
	gcpServiceAccount := props.OIDCGCPServiceAccount

	awsRegistryID := ""
	if registryHost != "" && provider != platform.OIDCProviderGCP {
		if id, region, ok := parseECRRegistryHost(registryHost); ok {
			awsRegistryID = id
			if awsRegion == "" {
				awsRegion = region
			}
			if provider == "" {
				provider = platform.OIDCProviderAWS
			}
		}
	}
	if provider == "" {
		switch {
		case awsRoleARN != "" || audience == awsDefaultAudience:
			provider = platform.OIDCProviderAWS
		case gcpServiceAccount != "" || strings.HasPrefix(audience, "//iam.googleapis.com/"):
			provider = platform.OIDCProviderGCP
		default:
			return nil, status.InvalidArgumentErrorf("%s must be set to %q or %q when exchanging OIDC tokens", platform.OIDCProviderPropertyName, platform.OIDCProviderAWS, platform.OIDCProviderGCP)
		}
	}

	switch provider {
	case platform.OIDCProviderAWS:
		if audience == "" {
			audience = awsDefaultAudience
		}
		if awsRoleARN == "" {
			return nil, status.InvalidArgumentErrorf("%s must be set when %s is %q", platform.OIDCAWSRoleARNPropertyName, platform.OIDCProviderPropertyName, platform.OIDCProviderAWS)
		}
		if awsRegion == "" {
			awsRegion = awsDefaultRegion
		}
		if registryHost != "" && awsRegistryID == "" {
			return nil, status.InvalidArgumentErrorf("%s=%q only supports private ECR image pulls for AWS; got registry %q", platform.ContainerRegistryAuthMethodPropertyName, platform.ContainerRegistryAuthMethodOIDC, registryHost)
		}
	case platform.OIDCProviderGCP:
		if audience == "" {
			return nil, status.InvalidArgumentErrorf("%s must be set when %s is %q", platform.OIDCTokenAudiencePropertyName, platform.OIDCProviderPropertyName, platform.OIDCProviderGCP)
		}
	default:
		return nil, status.InvalidArgumentErrorf("unsupported %s %q", platform.OIDCProviderPropertyName, provider)
	}

	return &tokenExchangeConfig{
		Provider:          provider,
		Audience:          audience,
		AWSRoleARN:        awsRoleARN,
		AWSRegion:         awsRegion,
		AWSRegistryID:     awsRegistryID,
		GCPServiceAccount: gcpServiceAccount,
		RegistryHost:      registryHost,
	}, nil
}

func cachedCredentials(env environment.Env, key tokenExchangeCacheKey) *CloudCredentials {
	tokenExchangeCacheMu.Lock()
	defer tokenExchangeCacheMu.Unlock()
	creds := tokenExchangeCache[key]
	if creds == nil {
		return nil
	}
	now := env.GetClock().Now()
	if creds.refreshAt.IsZero() {
		creds.refreshAt = tokenCacheRefreshTime(now, creds.ExpiresAt)
	}
	if !now.Before(creds.refreshAt) {
		delete(tokenExchangeCache, key)
		return nil
	}
	return creds.clone()
}

func storeCachedCredentials(env environment.Env, key tokenExchangeCacheKey, creds *CloudCredentials) {
	tokenExchangeCacheMu.Lock()
	defer tokenExchangeCacheMu.Unlock()
	now := env.GetClock().Now()
	if !now.Before(creds.ExpiresAt) {
		delete(tokenExchangeCache, key)
		return
	}
	clone := creds.clone()
	clone.refreshAt = tokenCacheRefreshTime(now, clone.ExpiresAt)
	tokenExchangeCache[key] = clone
}

func tokenCacheRefreshTime(now, expiresAt time.Time) time.Time {
	lifetime := expiresAt.Sub(now)
	if lifetime <= 0 {
		return now
	}
	return now.Add(lifetime / 2)
}

func credentialsProto(creds *CloudCredentials) *repb.OIDCCredentials {
	return &repb.OIDCCredentials{
		EnvVars:                   append([]string(nil), creds.EnvVars...),
		SecretEnvVars:             append([]string(nil), creds.SecretEnvVars...),
		ContainerRegistryUsername: creds.RegistryUsername,
		ContainerRegistryPassword: creds.RegistryPassword,
	}
}

func cloudCredentialsFromProto(creds *repb.OIDCCredentials) *CloudCredentials {
	return &CloudCredentials{
		EnvVars:          append([]string(nil), creds.GetEnvVars()...),
		SecretEnvVars:    append([]string(nil), creds.GetSecretEnvVars()...),
		RegistryUsername: creds.GetContainerRegistryUsername(),
		RegistryPassword: creds.GetContainerRegistryPassword(),
	}
}

func (c *CloudCredentials) clone() *CloudCredentials {
	return &CloudCredentials{
		EnvVars:          append([]string(nil), c.EnvVars...),
		SecretEnvVars:    append([]string(nil), c.SecretEnvVars...),
		RegistryUsername: c.RegistryUsername,
		RegistryPassword: c.RegistryPassword,
		ExpiresAt:        c.ExpiresAt,
		refreshAt:        c.refreshAt,
	}
}

func commandEnvValue(command *repb.Command, name string) (string, bool) {
	envVars := command.GetEnvironmentVariables()
	for i := len(envVars) - 1; i >= 0; i-- {
		envVar := envVars[i]
		if envVar.GetName() == name {
			return envVar.GetValue(), true
		}
	}
	return "", false
}

func setCommandEnvVar(command *repb.Command, name, value string) {
	envVars := command.GetEnvironmentVariables()
	for i := len(envVars) - 1; i >= 0; i-- {
		envVar := envVars[i]
		if envVar.GetName() == name {
			envVar.Value = value
			return
		}
	}
	command.EnvironmentVariables = append(command.EnvironmentVariables, &repb.Command_EnvironmentVariable{
		Name:  name,
		Value: value,
	})
}

func addSecretEnvVarNamesForRedaction(command *repb.Command, newNames []string) error {
	var existing []string
	for _, envVar := range command.GetEnvironmentVariables() {
		if envVar.GetName() == ci_runner_env.BuildBuddySecretEnvVarNamesForRedaction {
			if err := json.Unmarshal([]byte(envVar.GetValue()), &existing); err != nil {
				return status.InternalErrorf("unmarshal existing %s: %s", ci_runner_env.BuildBuddySecretEnvVarNamesForRedaction, err)
			}
			break
		}
	}
	serializedNames, err := json.Marshal(append(existing, newNames...))
	if err != nil {
		return status.WrapError(err, "marshal secret env var names")
	}
	setCommandEnvVar(command, ci_runner_env.BuildBuddySecretEnvVarNamesForRedaction, string(serializedNames))
	return nil
}

func generateGroupIDToken(ctx context.Context, env environment.Env, groupID string, audience string) (string, error) {
	if audience == "" {
		return "", status.InvalidArgumentErrorf("%s must be set to a non-empty audience", platform.OIDCTokenAudiencePropertyName)
	}
	if audience == requestTokenAudience {
		return "", status.InvalidArgumentErrorf("%s cannot be used as an ID token audience", requestTokenAudience)
	}
	key, keyID, err := signingKey()
	if err != nil {
		return "", err
	}
	now := env.GetClock().Now()
	idc := &idTokenClaims{
		StandardClaims: jwt.StandardClaims{
			Audience:  audience,
			ExpiresAt: now.Add(*tokenLifetime).Unix(),
			Id:        uuid.NewString(),
			IssuedAt:  now.Unix(),
			Issuer:    issuerURL().String(),
			NotBefore: now.Unix(),
			Subject:   subject(groupID),
		},
		TokenUse:          idTokenUse,
		BuildBuddyGroupID: groupID,
		RunnerEnvironment: runnerEnvironment,
	}
	return sign(key, keyID, idc)
}

func exchangeAWS(ctx context.Context, cfg *tokenExchangeConfig, idToken string, groupID string) (*CloudCredentials, error) {
	durationSeconds := int64(exchangeTokenLifetime.Seconds())
	if durationSeconds < 900 {
		durationSeconds = 900
	}
	sess, err := awssession.NewSession(&aws.Config{
		Region: aws.String(cfg.AWSRegion),
	})
	if err != nil {
		return nil, status.UnavailableErrorf("create AWS session: %s", err)
	}
	stsConfig := &aws.Config{}
	if awsSTSEndpoint != "" {
		stsConfig.Endpoint = aws.String(awsSTSEndpoint)
	}
	stsResp, err := awssts.New(sess, stsConfig).AssumeRoleWithWebIdentityWithContext(ctx, &awssts.AssumeRoleWithWebIdentityInput{
		RoleArn:          aws.String(cfg.AWSRoleARN),
		RoleSessionName:  aws.String(awsRoleSessionName(groupID)),
		WebIdentityToken: aws.String(idToken),
		DurationSeconds:  aws.Int64(durationSeconds),
	})
	if err != nil {
		return nil, status.UnavailableErrorf("assume AWS role with web identity: %s", err)
	}
	if stsResp.Credentials == nil || stsResp.Credentials.AccessKeyId == nil || stsResp.Credentials.SecretAccessKey == nil || stsResp.Credentials.SessionToken == nil || stsResp.Credentials.Expiration == nil {
		return nil, status.UnavailableError("AWS STS response did not include complete credentials")
	}
	accessKeyID := aws.StringValue(stsResp.Credentials.AccessKeyId)
	secretAccessKey := aws.StringValue(stsResp.Credentials.SecretAccessKey)
	sessionToken := aws.StringValue(stsResp.Credentials.SessionToken)
	expiresAt := aws.TimeValue(stsResp.Credentials.Expiration)
	creds := &CloudCredentials{
		EnvVars: []string{
			fmt.Sprintf("%s=%s", awsRegionEnvVar, cfg.AWSRegion),
		},
		SecretEnvVars: []string{
			fmt.Sprintf("%s=%s", awsAccessKeyIDEnvVar, accessKeyID),
			fmt.Sprintf("%s=%s", awsSecretAccessKeyEnvVar, secretAccessKey),
			fmt.Sprintf("%s=%s", awsSessionTokenEnvVar, sessionToken),
		},
		ExpiresAt: expiresAt,
	}
	if cfg.RegistryHost != "" {
		registryPassword, registryExpiresAt, err := exchangeAWSECR(ctx, cfg, accessKeyID, secretAccessKey, sessionToken)
		if err != nil {
			return nil, err
		}
		creds.RegistryUsername = "AWS"
		creds.RegistryPassword = registryPassword
		if registryExpiresAt.Before(creds.ExpiresAt) {
			creds.ExpiresAt = registryExpiresAt
		}
	}
	return creds, nil
}

func exchangeAWSECR(ctx context.Context, cfg *tokenExchangeConfig, accessKeyID, secretAccessKey, sessionToken string) (string, time.Time, error) {
	sess, err := awssession.NewSession(&aws.Config{
		Credentials: awscreds.NewStaticCredentials(accessKeyID, secretAccessKey, sessionToken),
		Region:      aws.String(cfg.AWSRegion),
	})
	if err != nil {
		return "", time.Time{}, status.UnavailableErrorf("create AWS ECR session: %s", err)
	}
	ecrConfig := &aws.Config{}
	if awsECREndpoint != "" {
		ecrConfig.Endpoint = aws.String(awsECREndpoint)
	}
	resp, err := awsecr.New(sess, ecrConfig).GetAuthorizationTokenWithContext(ctx, &awsecr.GetAuthorizationTokenInput{
		RegistryIds: []*string{aws.String(cfg.AWSRegistryID)},
	})
	if err != nil {
		return "", time.Time{}, status.UnavailableErrorf("get AWS ECR authorization token: %s", err)
	}
	if len(resp.AuthorizationData) == 0 || resp.AuthorizationData[0] == nil || resp.AuthorizationData[0].AuthorizationToken == nil || resp.AuthorizationData[0].ExpiresAt == nil {
		return "", time.Time{}, status.UnavailableError("AWS ECR response did not include an authorization token")
	}
	decoded, err := base64.StdEncoding.DecodeString(aws.StringValue(resp.AuthorizationData[0].AuthorizationToken))
	if err != nil {
		return "", time.Time{}, status.UnavailableErrorf("decode AWS ECR authorization token: %s", err)
	}
	username, password, ok := strings.Cut(string(decoded), ":")
	if !ok || username == "" || password == "" {
		return "", time.Time{}, status.UnavailableError("AWS ECR authorization token was malformed")
	}
	return password, aws.TimeValue(resp.AuthorizationData[0].ExpiresAt), nil
}

func exchangeGCP(ctx context.Context, env environment.Env, cfg *tokenExchangeConfig, idToken string) (*CloudCredentials, error) {
	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange")
	form.Set("audience", cfg.Audience)
	form.Set("scope", gcpCloudPlatformScope)
	form.Set("requested_token_type", "urn:ietf:params:oauth:token-type:access_token")
	form.Set("subject_token_type", "urn:ietf:params:oauth:token-type:id_token")
	form.Set("subject_token", idToken)
	var stsResp gcpSTSResponse
	if err := postForm(ctx, gcpSTSEndpoint, form, "", &stsResp); err != nil {
		return nil, status.WrapError(err, "exchange GCP token")
	}
	if stsResp.AccessToken == "" || stsResp.ExpiresIn <= 0 {
		return nil, status.UnavailableError("GCP STS response did not include an access token")
	}
	accessToken := stsResp.AccessToken
	expiresAt := env.GetClock().Now().Add(time.Duration(stsResp.ExpiresIn) * time.Second)
	if cfg.GCPServiceAccount != "" {
		token, tokenExpiresAt, err := impersonateGCPServiceAccount(ctx, cfg.GCPServiceAccount, stsResp.AccessToken)
		if err != nil {
			return nil, err
		}
		accessToken = token
		expiresAt = tokenExpiresAt
	}
	creds := &CloudCredentials{
		SecretEnvVars: []string{
			fmt.Sprintf("%s=%s", AccessTokenEnvVar, accessToken),
		},
		RegistryUsername: gcpRegistryUsername,
		RegistryPassword: accessToken,
		ExpiresAt:        expiresAt,
	}
	return creds, nil
}

func impersonateGCPServiceAccount(ctx context.Context, serviceAccount string, accessToken string) (string, time.Time, error) {
	lifetimeSeconds := int64(exchangeTokenLifetime.Seconds())
	if lifetimeSeconds <= 0 || lifetimeSeconds > 3600 {
		lifetimeSeconds = 3600
	}
	body := map[string]interface{}{
		"scope":    []string{gcpCloudPlatformScope},
		"lifetime": fmt.Sprintf("%ds", lifetimeSeconds),
	}
	endpoint := strings.TrimRight(gcpIAMCredentialsEndpoint, "/") + "/projects/-/serviceAccounts/" + url.PathEscape(serviceAccount) + ":generateAccessToken"
	var tokenResp gcpServiceAccountTokenResponse
	if err := postJSON(ctx, endpoint, body, accessToken, &tokenResp); err != nil {
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

func postForm(ctx context.Context, endpoint string, form url.Values, bearerToken string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	return doJSON(req, out)
}

func postJSON(ctx context.Context, endpoint string, body interface{}, bearerToken string, out interface{}) error {
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
	return doJSON(req, out)
}

func doJSON(req *http.Request, out interface{}) error {
	resp, err := httpClient.Do(req)
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

func imageRegistryHost(imageRef string) string {
	imageRef = strings.TrimPrefix(imageRef, platform.DockerPrefix)
	if imageRef == "" {
		return ""
	}
	first, _, _ := strings.Cut(imageRef, "/")
	if strings.Contains(first, ".") || strings.Contains(first, ":") || first == "localhost" {
		return first
	}
	return "index.docker.io"
}

func parseECRRegistryHost(host string) (string, string, bool) {
	matches := ecrRegistryHostPattern.FindStringSubmatch(host)
	if matches == nil {
		return "", "", false
	}
	return matches[1], matches[2], true
}

func awsRoleSessionName(groupID string) string {
	var b strings.Builder
	b.WriteString("buildbuddy-rbe-")
	for _, r := range groupID {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune("_+=,.@-", r) {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	sessionName := b.String()
	if len(sessionName) > 64 {
		sum := sha256.Sum256([]byte(sessionName))
		suffix := base64.RawURLEncoding.EncodeToString(sum[:])[:12]
		sessionName = sessionName[:64-len(suffix)-1] + "-" + suffix
	}
	return sessionName
}

func signingKey() (*rsa.PrivateKey, string, error) {
	pem := *signingKeyPEM
	if pem == "" {
		return nil, "", status.FailedPreconditionError("remote_execution.oidc.private_key must be configured to use RBE OIDC")
	}
	pem = strings.ReplaceAll(pem, `\n`, "\n")
	signingKeyMu.Lock()
	defer signingKeyMu.Unlock()
	if pem == signingKeyCachePEM && signingKeyCacheKey != nil {
		return signingKeyCacheKey, signingKeyCacheID, nil
	}
	key, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(pem))
	if err != nil {
		return nil, "", status.InvalidArgumentErrorf("parse remote_execution.oidc.private_key: %s", err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return nil, "", status.InternalErrorf("marshal OIDC public key: %s", err)
	}
	sum := sha256.Sum256(der)
	keyID := base64.RawURLEncoding.EncodeToString(sum[:])
	signingKeyCachePEM = pem
	signingKeyCacheKey = key
	signingKeyCacheID = keyID
	return key, keyID, nil
}

func sign(key *rsa.PrivateKey, keyID string, c jwt.Claims) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, c)
	token.Header["kid"] = keyID
	token.Header["typ"] = "JWT"
	return token.SignedString(key)
}

func issuerURL() *url.URL {
	return build_buddy_url.WithPath(issuerPath)
}

func jwksEndpoint() *url.URL {
	return build_buddy_url.WithPath(jwksPath)
}

func tokenEndpoint() *url.URL {
	return build_buddy_url.WithPath(tokenPath)
}

func publicJWK(key *rsa.PublicKey, keyID string) *jwkJSON {
	return &jwkJSON{
		Kty: "RSA",
		Use: "sig",
		Kid: keyID,
		Alg: "RS256",
		N:   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		E:   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}
}

func subject(groupID string) string {
	return "buildbuddy:group:" + escapeSubjectValue(groupID)
}

func escapeSubjectValue(s string) string {
	return strings.ReplaceAll(s, ":", "%3A")
}

func bearerToken(header string) (string, bool) {
	scheme, token, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(scheme, "bearer") || strings.TrimSpace(token) == "" {
		return "", false
	}
	return strings.TrimSpace(token), true
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

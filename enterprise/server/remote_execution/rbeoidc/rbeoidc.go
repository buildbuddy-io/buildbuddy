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
// access tokens are exposed to actions as BUILDBUDDY_GCP_ACCESS_TOKEN.
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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oidcissuer"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/awsoidc"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/gcpoidc"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ci_runner_env"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// TokenRequestURLEnvVar points actions at the BuildBuddy endpoint that can
	// exchange a request token for a BuildBuddy OIDC ID token.
	TokenRequestURLEnvVar = "BUILDBUDDY_OIDC_TOKEN_REQUEST_URL"

	// TokenRequestTokenEnvVar contains the bearer token used with
	// TokenRequestURLEnvVar.
	TokenRequestTokenEnvVar = "BUILDBUDDY_OIDC_TOKEN_REQUEST_TOKEN"

	// DockerConfigJSONEnvVar contains a Docker config.json auths object for the
	// registry credentials derived from OIDC token exchange.
	DockerConfigJSONEnvVar = "BUILDBUDDY_DOCKER_CONFIG_JSON"

	issuerPath    = "/oidc"
	wellKnownPath = issuerPath + oidcissuer.ConfigurationPath
	jwksPath      = issuerPath + "/.well-known/jwks"
	tokenPath     = issuerPath + "/token"

	requestTokenAudience = "buildbuddy-rbe-oidc-token-request"
	runnerEnvironment    = "buildbuddy-rbe"

	requestTokenUse = "buildbuddy-rbe-oidc-request-token"
	idTokenUse      = "id_token"

	tokenExchangeCacheMaxEntries = 10000
)

var (
	signingKeyPEM         = flag.String("remote_execution.oidc.private_key", "", "PEM-encoded RSA private key used by the app to sign OIDC tokens for RBE actions. Configure this on the app to enable the `oidc-token-audience` platform property.", flag.Secret)
	tokenLifetime         = flag.Duration("remote_execution.oidc.token_lifetime", 5*time.Minute, "Lifetime of OIDC ID tokens minted for RBE actions.")
	requestTokenLifetime  = flag.Duration("remote_execution.oidc.request_token_lifetime", 15*time.Minute, "Lifetime of bearer tokens injected into RBE actions for requesting OIDC ID tokens.")
	exchangeTokenLifetime = flag.Duration("remote_execution.oidc.exchange_token_lifetime", time.Hour, "Requested lifetime of cloud provider access tokens exchanged from BuildBuddy RBE OIDC tokens.")

	tokenExchangeCacheMu sync.Mutex
	tokenExchangeCache   lru.LRU[*exchangedCredentials]
	tokenExchangeGroup   singleflight.Group[tokenExchangeCacheKey, *repb.OIDCCredentials]

	issuerMu       sync.Mutex
	issuerCachePEM string
	issuerCache    *oidcissuer.Provider

	httpClient = http.DefaultClient

	awsSTSEndpoint            string
	awsECREndpoint            string
	gcpSTSEndpoint            = gcpoidc.DefaultSTSEndpoint
	gcpIAMCredentialsEndpoint = gcpoidc.DefaultIAMCredentialsEndpoint
)

// provider owns the OIDC issuer and serves RBE-specific token endpoints.
type provider struct {
	env    environment.Env
	issuer *oidcissuer.Provider
}

// tokenResponse is the minimal response body returned by /oidc/token.
type tokenResponse struct {
	Value string `json:"value"`
}

// exchangedCredentials contains the credentials exchanged from a BuildBuddy
// OIDC ID token plus local cache metadata that is not sent to executors.
type exchangedCredentials struct {
	credentials *repb.OIDCCredentials
	expiresAt   time.Time
	refreshAt   time.Time
}

// tokenExchangeConfig is the normalized provider config used for token
// exchange after applying defaults inferred from the action platform.
type tokenExchangeConfig struct {
	Provider          string
	Audience          string
	AWSRoleARN        string
	AWSRegion         string
	AWSRegistryID     string
	GCPServiceAccount string
}

// tokenExchangeCacheKey identifies provider credentials that can be reused by
// later actions from the same BuildBuddy group.
type tokenExchangeCacheKey struct {
	GroupID           string
	Provider          string
	Audience          string
	AWSRoleARN        string
	AWSRegion         string
	AWSRegistryID     string
	GCPServiceAccount string
}

func (k tokenExchangeCacheKey) cacheKey() (string, error) {
	b, err := json.Marshal(k)
	if err != nil {
		return "", status.InternalErrorf("marshal OIDC token exchange cache key: %s", err)
	}
	return string(b), nil
}

// executionClaims are the BuildBuddy action metadata carried by both request
// tokens and ID tokens.
type executionClaims struct {
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

// requestClaims are carried by the short lived bearer token injected into an
// action when the user wants to request a BuildBuddy OIDC ID token directly.
type requestClaims struct {
	jwt.StandardClaims
	executionClaims

	DefaultAudience string `json:"default_audience,omitempty"`
}

// idTokenClaims are carried by the BuildBuddy OIDC ID token that actions or
// provider token exchange APIs consume.
type idTokenClaims struct {
	jwt.StandardClaims
	executionClaims
}

// CloudTokenExchanger exchanges BuildBuddy OIDC ID tokens for provider
// credentials.
type CloudTokenExchanger interface {
	// ExchangeAWS exchanges idToken for AWS credentials.
	ExchangeAWS(ctx context.Context, cfg awsoidc.Config, idToken, groupID string) (*awsoidc.Credentials, error)

	// ExchangeGCP exchanges idToken for Google Cloud credentials.
	ExchangeGCP(ctx context.Context, cfg gcpoidc.Config, idToken string) (*gcpoidc.Credentials, error)
}

// Exchanger generates BuildBuddy OIDC ID tokens and exchanges them for cloud
// provider credentials.
type Exchanger struct {
	cloudTokenExchanger CloudTokenExchanger
}

type defaultCloudTokenExchanger struct{}

func Register(env environment.Env) error {
	if *signingKeyPEM == "" {
		return nil
	}
	provider, err := newProvider(env)
	if err != nil {
		return err
	}
	mux := env.GetMux()
	mux.Handle(wellKnownPath, interceptors.SetSecurityHeaders(http.HandlerFunc(provider.issuer.ServeWellKnownOpenIDConfiguration)))
	mux.Handle(jwksPath, interceptors.SetSecurityHeaders(http.HandlerFunc(provider.issuer.ServeJWKS)))
	mux.Handle(tokenPath, interceptors.SetSecurityHeaders(http.HandlerFunc(provider.token)))
	return nil
}

func newProvider(env environment.Env) (*provider, error) {
	issuer, err := newIssuer()
	if err != nil {
		return nil, err
	}
	return &provider{
		env:    env,
		issuer: issuer,
	}, nil
}

func newIssuer() (*oidcissuer.Provider, error) {
	keyPEM := *signingKeyPEM
	if keyPEM == "" {
		return nil, status.FailedPreconditionError("remote_execution.oidc.private_key must be configured to use RBE OIDC")
	}
	issuerMu.Lock()
	defer issuerMu.Unlock()
	if keyPEM == issuerCachePEM && issuerCache != nil {
		return issuerCache, nil
	}
	provider, err := oidcissuer.New(oidcissuer.Config{
		PrivateKeyPEM:          keyPEM,
		IssuerURL:              issuerURL().String(),
		JWKSURI:                build_buddy_url.WithPath(jwksPath).String(),
		TokenEndpoint:          build_buddy_url.WithPath(tokenPath).String(),
		SubjectTypesSupported:  []string{"public"},
		ResponseTypesSupported: []string{"id_token"},
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
		ScopesSupported: []string{"openid"},
	})
	if err != nil {
		return nil, status.InvalidArgumentErrorf("configure RBE OIDC issuer: %s", err)
	}
	issuerCachePEM = keyPEM
	issuerCache = provider
	return provider, nil
}

// NewExchanger returns an Exchanger. If cloudTokenExchanger is nil, the
// Exchanger uses the real AWS and Google Cloud token exchange APIs.
func NewExchanger(cloudTokenExchanger CloudTokenExchanger) *Exchanger {
	if cloudTokenExchanger == nil {
		cloudTokenExchanger = defaultCloudTokenExchanger{}
	}
	return &Exchanger{cloudTokenExchanger: cloudTokenExchanger}
}

// ExchangeAWS exchanges idToken for AWS credentials.
func (defaultCloudTokenExchanger) ExchangeAWS(ctx context.Context, cfg awsoidc.Config, idToken, groupID string) (*awsoidc.Credentials, error) {
	return awsoidc.Exchange(ctx, cfg, idToken, groupID)
}

// ExchangeGCP exchanges idToken for Google Cloud credentials.
func (defaultCloudTokenExchanger) ExchangeGCP(ctx context.Context, cfg gcpoidc.Config, idToken string) (*gcpoidc.Credentials, error) {
	return gcpoidc.Exchange(ctx, cfg, idToken)
}

// GenerateRequestEnvForGroup returns the request-token env vars for a
// BuildBuddy group. It is used by the app at task lease time, when the lease
// RPC is authenticated as the executor but the task's scheduling metadata still
// identifies the BuildBuddy group that requested the action.
func GenerateRequestEnvForGroup(env environment.Env, task *repb.ExecutionTask, defaultAudience, groupID, userID string) ([]string, []string, error) {
	token, err := generateRequestToken(env, task, defaultAudience, groupID, userID)
	if err != nil {
		return nil, nil, err
	}
	u := build_buddy_url.WithPath(tokenPath)
	q := u.Query()
	q.Set("api-version", "2")
	u.RawQuery = q.Encode()
	return []string{
			fmt.Sprintf("%s=%s", TokenRequestURLEnvVar, u.String()),
		}, []string{
			fmt.Sprintf("%s=%s", TokenRequestTokenEnvVar, token),
		}, nil
}

// GenerateExchangedCredentialsForGroup exchanges cloud credentials for a
// BuildBuddy group and returns the credentials that should be carried to the
// executor with the leased task.
func GenerateExchangedCredentialsForGroup(ctx context.Context, env environment.Env, props *platform.Properties, groupID string) (*repb.OIDCCredentials, error) {
	if !ExchangeEnabled(props) {
		return nil, nil
	}
	return NewExchanger(nil).GenerateExchangedCredentialsForGroup(ctx, env, props, groupID)
}

func generateRequestToken(env environment.Env, task *repb.ExecutionTask, defaultAudience, groupID, userID string) (string, error) {
	if defaultAudience == "" {
		return "", status.InvalidArgumentErrorf("%s must be set to a non-empty audience", platform.OIDCTokenAudiencePropertyName)
	}
	if defaultAudience == requestTokenAudience {
		return "", status.InvalidArgumentErrorf("%s cannot be used as an ID token audience", requestTokenAudience)
	}
	issuer, err := newIssuer()
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
		executionClaims: executionClaims{
			TokenUse:          requestTokenUse,
			BuildBuddyGroupID: groupID,
			BuildBuddyUserID:  userID,
			RunnerEnvironment: runnerEnvironment,
		},
		DefaultAudience: defaultAudience,
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

	return issuer.Sign(rc)
}

// ValidateProperties validates OIDC platform properties without performing any
// token exchange. The app uses this at scheduling time so obvious
// misconfigurations fail before the task sits in the queue.
func ValidateProperties(props *platform.Properties) error {
	if !ExchangeEnabled(props) && props.OIDCTokenAudience == "" {
		return nil
	}
	if _, err := newIssuer(); err != nil {
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
	registryHost, err := registryHostFromProperties(props)
	if err != nil {
		return err
	}
	_, err = tokenExchangeConfigFromProperties(props, registryHost)
	return err
}

// ExchangeEnabled reports whether platform properties request cloud token exchange.
func ExchangeEnabled(props *platform.Properties) bool {
	if props.OIDCProvider != "" || props.OIDCAWSRoleARN != "" || props.OIDCGCPServiceAccount != "" {
		return true
	}
	return props.ContainerRegistryAuthSource == platform.ContainerRegistryAuthSourceOIDC
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

// ApplyCredentialOverrides applies cloud credentials attached to the leased
// task and mutates props with registry credentials when requested.
func ApplyCredentialOverrides(task *repb.ExecutionTask, props *platform.Properties) ([]string, []string, error) {
	if !ExchangeEnabled(props) {
		return nil, nil, nil
	}
	if task.GetOidcCredentials() == nil {
		return nil, nil, status.FailedPreconditionError("OIDC credentials were not attached to the leased task; BuildBuddy must exchange OIDC credentials at task lease time")
	}
	creds := task.GetOidcCredentials()
	task.OidcCredentials = nil
	envVars := slices.Clone(creds.GetEnvVars())
	secretEnvVars := slices.Clone(creds.GetSecretEnvVars())
	registryHost, err := registryHostFromProperties(props)
	if err != nil {
		return nil, nil, err
	}
	if registryHost != "" {
		registryUsername := creds.GetContainerRegistryUsername()
		registryPassword := creds.GetContainerRegistryPassword()
		if registryUsername == "" || registryPassword == "" {
			return nil, nil, status.InvalidArgumentError("OIDC token exchange did not produce container registry credentials")
		}
		props.ContainerRegistryUsername = registryUsername
		props.ContainerRegistryPassword = registryPassword
		auth := base64.StdEncoding.EncodeToString([]byte(registryUsername + ":" + registryPassword))
		dockerConfig, err := json.Marshal(&dockerConfigFile{
			Auths: map[string]dockerConfigAuth{registryHost: {Auth: auth}},
		})
		if err != nil {
			return nil, nil, status.WrapError(err, "marshal Docker config")
		}
		secretEnvVars = append(secretEnvVars, fmt.Sprintf("%s=%s", DockerConfigJSONEnvVar, string(dockerConfig)))
	}
	return envVars, secretEnvVars, nil
}

// GenerateExchangedCredentialsForGroup exchanges cloud credentials for a
// BuildBuddy group and returns the credentials that should be carried to the
// executor with the leased task.
func (e *Exchanger) GenerateExchangedCredentialsForGroup(ctx context.Context, env environment.Env, props *platform.Properties, groupID string) (*repb.OIDCCredentials, error) {
	if groupID == "" {
		return nil, status.PermissionDeniedError("OIDC token exchange requires an authenticated BuildBuddy group")
	}
	if !ExchangeEnabled(props) {
		return nil, nil
	}
	var cloudTokenExchanger CloudTokenExchanger = defaultCloudTokenExchanger{}
	if e != nil && e.cloudTokenExchanger != nil {
		cloudTokenExchanger = e.cloudTokenExchanger
	}
	registryHost, err := registryHostFromProperties(props)
	if err != nil {
		return nil, err
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
	creds, err := cachedCredentials(env, key)
	if err != nil {
		return nil, err
	}
	if creds != nil {
		return creds, nil
	}
	creds, _, err = tokenExchangeGroup.Do(ctx, key, func(ctx context.Context) (*repb.OIDCCredentials, error) {
		cachedCreds, err := cachedCredentials(env, key)
		if err != nil {
			return nil, err
		}
		if cachedCreds != nil {
			return cachedCreds, nil
		}
		if cfg.Audience == "" {
			return nil, status.InvalidArgumentErrorf("%s must be set to a non-empty audience", platform.OIDCTokenAudiencePropertyName)
		}
		if cfg.Audience == requestTokenAudience {
			return nil, status.InvalidArgumentErrorf("%s cannot be used as an ID token audience", requestTokenAudience)
		}
		issuer, err := newIssuer()
		if err != nil {
			return nil, err
		}
		now := env.GetClock().Now()
		idc := &idTokenClaims{
			StandardClaims: jwt.StandardClaims{
				Audience:  cfg.Audience,
				ExpiresAt: now.Add(*tokenLifetime).Unix(),
				Id:        uuid.NewString(),
				IssuedAt:  now.Unix(),
				Issuer:    issuerURL().String(),
				NotBefore: now.Unix(),
				Subject:   subject(groupID),
			},
			executionClaims: executionClaims{
				TokenUse:          idTokenUse,
				BuildBuddyGroupID: groupID,
				RunnerEnvironment: runnerEnvironment,
			},
		}
		idToken, err := issuer.Sign(idc)
		if err != nil {
			return nil, err
		}
		var exchangedCreds *exchangedCredentials
		switch cfg.Provider {
		case platform.OIDCProviderAWS:
			awsCreds, err := cloudTokenExchanger.ExchangeAWS(ctx, awsoidc.Config{
				RoleARN:       cfg.AWSRoleARN,
				Region:        cfg.AWSRegion,
				RegistryID:    cfg.AWSRegistryID,
				STSEndpoint:   awsSTSEndpoint,
				ECREndpoint:   awsECREndpoint,
				TokenLifetime: *exchangeTokenLifetime,
			}, idToken, groupID)
			if err != nil {
				return nil, err
			}
			exchangedCreds = &exchangedCredentials{
				credentials: awsCreds.Proto,
				expiresAt:   awsCreds.ExpiresAt,
			}
		case platform.OIDCProviderGCP:
			gcpCreds, err := cloudTokenExchanger.ExchangeGCP(ctx, gcpoidc.Config{
				Audience:               cfg.Audience,
				ServiceAccount:         cfg.GCPServiceAccount,
				STSEndpoint:            gcpSTSEndpoint,
				IAMCredentialsEndpoint: gcpIAMCredentialsEndpoint,
				TokenLifetime:          *exchangeTokenLifetime,
				HTTPClient:             httpClient,
				Now:                    env.GetClock().Now(),
			}, idToken)
			if err != nil {
				return nil, err
			}
			exchangedCreds = &exchangedCredentials{
				credentials: gcpCreds.Proto,
				expiresAt:   gcpCreds.ExpiresAt,
			}
		default:
			return nil, status.InvalidArgumentErrorf("unsupported OIDC provider %q", cfg.Provider)
		}
		if err := storeCachedCredentials(env, key, exchangedCreds); err != nil {
			return nil, err
		}
		return cloneOIDCCredentials(exchangedCreds.credentials), nil
	})
	if err != nil {
		return nil, err
	}
	return creds, nil
}

func (p *provider) token(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	scheme, requestToken, ok := strings.Cut(strings.TrimSpace(r.Header.Get("Authorization")), " ")
	if !ok || !strings.EqualFold(scheme, "bearer") || strings.TrimSpace(requestToken) == "" {
		http.Error(w, "missing bearer token", http.StatusUnauthorized)
		return
	}
	requestToken = strings.TrimSpace(requestToken)
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
		executionClaims: executionClaims{
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
		},
	}
	idToken, err := p.issuer.Sign(idc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, &tokenResponse{Value: idToken})
}

func (p *provider) parseRequestToken(tokenString string) (*requestClaims, error) {
	rc := &requestClaims{}
	token, err := p.issuer.ParseClaims(tokenString, rc)
	if err != nil {
		return nil, err
	}
	if !token.Valid {
		return nil, status.PermissionDeniedError("invalid request token")
	}
	if rc.Issuer != p.issuer.Issuer() {
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

func tokenExchangeConfigFromProperties(props *platform.Properties, registryHost string) (*tokenExchangeConfig, error) {
	provider := strings.ToLower(props.OIDCProvider)
	audience := props.OIDCTokenAudience
	awsRoleARN := props.OIDCAWSRoleARN
	awsRegion := props.OIDCAWSRegion
	gcpServiceAccount := props.OIDCGCPServiceAccount

	awsRegistryID := ""
	if registryHost != "" && provider != platform.OIDCProviderGCP {
		if id, region, ok := awsoidc.ParseECRRegistryHost(registryHost); ok {
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
		case awsRoleARN != "" || audience == awsoidc.DefaultAudience:
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
			audience = awsoidc.DefaultAudience
		}
		if awsRoleARN == "" {
			return nil, status.InvalidArgumentErrorf("%s must be set when %s is %q", platform.OIDCAWSRoleARNPropertyName, platform.OIDCProviderPropertyName, platform.OIDCProviderAWS)
		}
		if awsRegion == "" {
			awsRegion = awsoidc.DefaultRegion
		}
		if registryHost != "" && awsRegistryID == "" {
			return nil, status.InvalidArgumentErrorf("%s=%q only supports private ECR image pulls for AWS; got registry %q", platform.ContainerRegistryAuthSourcePropertyName, platform.ContainerRegistryAuthSourceOIDC, registryHost)
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
	}, nil
}

func registryHostFromProperties(props *platform.Properties) (string, error) {
	if props.ContainerRegistryAuthSource != platform.ContainerRegistryAuthSourceOIDC {
		return "", nil
	}
	imageRef := strings.TrimPrefix(props.ContainerImage, platform.DockerPrefix)
	registryHost := ""
	if imageRef != "" {
		first, _, _ := strings.Cut(imageRef, "/")
		if strings.Contains(first, ".") || strings.Contains(first, ":") || first == "localhost" {
			registryHost = first
		} else {
			registryHost = "index.docker.io"
		}
	}
	if registryHost == "" {
		return "", status.InvalidArgumentErrorf("container-image must be set when %s is %q", platform.ContainerRegistryAuthSourcePropertyName, platform.ContainerRegistryAuthSourceOIDC)
	}
	return registryHost, nil
}

func cachedCredentials(env environment.Env, key tokenExchangeCacheKey) (*repb.OIDCCredentials, error) {
	cache, err := tokenExchangeCredentialsCache(env)
	if err != nil {
		return nil, err
	}
	cacheKey, err := key.cacheKey()
	if err != nil {
		return nil, err
	}
	creds, ok := cache.Get(cacheKey)
	if !ok {
		return nil, nil
	}
	now := env.GetClock().Now()
	if !now.Before(creds.refreshAt) {
		cache.Remove(cacheKey)
		return nil, nil
	}
	return cloneOIDCCredentials(creds.credentials), nil
}

func storeCachedCredentials(env environment.Env, key tokenExchangeCacheKey, creds *exchangedCredentials) error {
	cache, err := tokenExchangeCredentialsCache(env)
	if err != nil {
		return err
	}
	cacheKey, err := key.cacheKey()
	if err != nil {
		return err
	}
	now := env.GetClock().Now()
	if !now.Before(creds.expiresAt) {
		cache.Remove(cacheKey)
		return nil
	}
	refreshAt := now
	if lifetime := creds.expiresAt.Sub(now); lifetime > 0 {
		refreshAt = now.Add(lifetime / 2)
	}
	clone := &exchangedCredentials{
		credentials: cloneOIDCCredentials(creds.credentials),
		expiresAt:   creds.expiresAt,
		refreshAt:   refreshAt,
	}
	cache.Add(cacheKey, clone)
	return nil
}

func tokenExchangeCredentialsCache(env environment.Env) (lru.LRU[*exchangedCredentials], error) {
	tokenExchangeCacheMu.Lock()
	defer tokenExchangeCacheMu.Unlock()
	if tokenExchangeCache != nil {
		return tokenExchangeCache, nil
	}
	ttl := time.Duration(0)
	if *exchangeTokenLifetime > 0 {
		ttl = *exchangeTokenLifetime / 2
	}
	// The LRU TTL is a coarse max age. Each entry still carries refreshAt,
	// since providers can return a shorter lifetime than requested.
	cache, err := lru.New[*exchangedCredentials](&lru.Config[*exchangedCredentials]{
		MaxSize:       tokenExchangeCacheMaxEntries,
		SizeFn:        func(*exchangedCredentials) int64 { return 1 },
		TTL:           ttl,
		ThreadSafe:    true,
		Clock:         env.GetClock(),
		UpdateInPlace: true,
	})
	if err != nil {
		return nil, status.InternalErrorf("initialize OIDC token exchange cache: %s", err)
	}
	tokenExchangeCache = cache
	return tokenExchangeCache, nil
}

func cloneOIDCCredentials(creds *repb.OIDCCredentials) *repb.OIDCCredentials {
	return &repb.OIDCCredentials{
		EnvVars:                   slices.Clone(creds.GetEnvVars()),
		SecretEnvVars:             slices.Clone(creds.GetSecretEnvVars()),
		ContainerRegistryUsername: creds.GetContainerRegistryUsername(),
		ContainerRegistryPassword: creds.GetContainerRegistryPassword(),
	}
}

type dockerConfigFile struct {
	Auths map[string]dockerConfigAuth `json:"auths"`
}

type dockerConfigAuth struct {
	Auth string `json:"auth"`
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

func issuerURL() *url.URL {
	return build_buddy_url.WithPath(issuerPath)
}

func subject(groupID string) string {
	return "buildbuddy:group:" + strings.ReplaceAll(groupID, ":", "%3A")
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

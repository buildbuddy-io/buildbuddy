// Package oidc implements an OpenID Connect provider for BuildBuddy RBE actions.
package oidc

import (
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	TokenRequestURLEnvVar   = "BUILDBUDDY_OIDC_TOKEN_REQUEST_URL"
	TokenRequestTokenEnvVar = "BUILDBUDDY_OIDC_TOKEN_REQUEST_TOKEN"

	issuerPath    = "/oidc"
	wellKnownPath = issuerPath + "/.well-known/openid-configuration"
	jwksPath      = issuerPath + "/.well-known/jwks"
	tokenPath     = issuerPath + "/token"

	requestTokenAudience = "buildbuddy-rbe-oidc-token-request"
	runnerEnvironment    = "buildbuddy-rbe"

	requestTokenUse = "buildbuddy-rbe-oidc-request-token"
	idTokenUse      = "id_token"
)

var (
	signingKeyPEM        = flag.String("remote_execution.oidc.private_key", "", "PEM-encoded RSA private key used to sign OIDC tokens for RBE actions. Configure this on the app and executors to enable the `oidc-token-audience` platform property.", flag.Secret)
	tokenLifetime        = flag.Duration("remote_execution.oidc.token_lifetime", 5*time.Minute, "Lifetime of OIDC ID tokens minted for RBE actions.")
	requestTokenLifetime = flag.Duration("remote_execution.oidc.request_token_lifetime", 15*time.Minute, "Lifetime of bearer tokens injected into RBE actions for requesting OIDC ID tokens.")
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
	if strings.TrimSpace(*signingKeyPEM) == "" {
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

func GenerateRequestEnv(ctx context.Context, env environment.Env, task *repb.ExecutionTask, defaultAudience string) ([]string, []string, error) {
	defaultAudience = strings.TrimSpace(defaultAudience)
	if defaultAudience == "" {
		return nil, nil, status.InvalidArgumentErrorf("%s must be set to a non-empty audience", platform.OIDCTokenAudiencePropertyName)
	}
	token, err := GenerateRequestToken(ctx, env, task, defaultAudience)
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

func GenerateRequestToken(ctx context.Context, env environment.Env, task *repb.ExecutionTask, defaultAudience string) (string, error) {
	defaultAudience = strings.TrimSpace(defaultAudience)
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
	authClaims, err := claims.ClaimsFromContext(ctx)
	if err != nil {
		return "", status.PermissionDeniedErrorf("OIDC token requests require an authenticated BuildBuddy user: %s", err)
	}
	groupID := authClaims.GetGroupID()
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
		BuildBuddyUserID:  authClaims.GetUserID(),
		RunnerEnvironment: runnerEnvironment,
	}
	if task != nil {
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
	}

	return sign(key, keyID, rc)
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

func signingKey() (*rsa.PrivateKey, string, error) {
	pem := strings.TrimSpace(*signingKeyPEM)
	if pem == "" {
		return nil, "", status.FailedPreconditionError("remote_execution.oidc.private_key must be configured to use RBE OIDC")
	}
	pem = strings.ReplaceAll(pem, `\n`, "\n")
	key, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(pem))
	if err != nil {
		return nil, "", status.InvalidArgumentErrorf("parse remote_execution.oidc.private_key: %s", err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return nil, "", status.InternalErrorf("marshal OIDC public key: %s", err)
	}
	sum := sha256.Sum256(der)
	return key, base64.RawURLEncoding.EncodeToString(sum[:]), nil
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

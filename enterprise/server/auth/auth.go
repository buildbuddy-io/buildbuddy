package auth

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang-jwt/jwt"
	"github.com/google/uuid"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	burl "github.com/buildbuddy-io/buildbuddy/server/util/url"
	oidc "github.com/coreos/go-oidc"
)

var (
	adminGroupID = flag.String("auth.admin_group_id", "", "ID of a group whose members can perform actions only accessible to server admins.")
	enableAnonymousUsage = flag.Bool("auth.enable_anonymous_usage", false, "If true, unauthenticated build uploads will still be allowed but won't be associated with your organization.")
	oauthProviders       = []config.OauthProvider{}
	jwtKey               = flag.String("auth.jwt_key", "set_the_jwt_in_config", "The key to use when signing JWT tokens.")
	apiKeyGroupCacheTTL  = flag.Duration("auth.api_key_group_cache_ttl", 0, "Override for the TTL for API Key to Group caching. Set to '0' to disable cache.")
	httpsOnlyCookies     = flag.Bool("auth.https_only_cookies", false, "If true, cookies will only be set over https connections.")
	disableRefreshToken  = flag.Bool("auth.disable_refresh_token", false, "If true, the offline_access scope which requests refresh tokens will not be requested.")
)

func init() {
	flagutil.StructSliceVar(&oauthProviders, "auth.oauth_providers", "")
}

const (
	// The key that the user object is stored under in the
	// context.
	contextUserKey = "auth.user"
	// The key any error is stored under if the user could not be
	// authenticated.
	contextUserErrorKey = "auth.error"

	// The key the JWT token string is stored under.
	// NB: This value must match the value in
	// bb/server/rpc/filters/filters.go which copies/reads this value
	// to/from the outgoing/incoming request contexts.
	contextTokenStringKey = "x-buildbuddy-jwt"

	// The key that the basicAuth object is stored under in the
	// context.
	contextBasicAuthKey = "basicauth.user"

	// The key any error is stored under if the user could not be
	// authenticated.
	contextBasicAuthErrorKey = "basicauth.error"
	authorityHeader          = ":authority"
	basicAuthHeader          = "authorization"

	contextAPIKeyKey = "api.key"
	APIKeyHeader     = "x-buildbuddy-api-key"

	// The name of params read on /login to understand which
	// issuer to use and where to redirect the client after
	// login.
	authRedirectParam = "redirect_url"
	authIssuerParam   = "issuer_url"
	slugParam         = "slug"

	// The name of the auth cookies used to authenticate the
	// client.
	jwtCookie        = "Authorization"
	sessionIDCookie  = "Session-ID"
	authIssuerCookie = "Authorization-Issuer"
	stateCookie      = "State-Token"
	redirCookie      = "Redirect-Url"

	// How long certain cookies last
	tempCookieDuration  = 24 * time.Hour
	loginCookieDuration = 365 * 24 * time.Hour

	// BuildBuddy JWT duration maximum.
	defaultBuildBuddyJWTDuration = 6 * time.Hour

	// Maximum amount of time we will cache Group information for an API key.
	defaultAPIKeyGroupCacheTTL = 5 * time.Minute
	// Maximum number of entries in API Key -> Group cache.
	apiKeyGroupCacheSize = 10000

	// WARNING: app/auth/auth_service.ts depends on these messages matching.
	userNotFoundMsg   = "User not found"
	loggedOutMsg      = "User logged out"
	ExpiredSessionMsg = "User session expired"
)

var (
	apiKeyRegex = regexp.MustCompile(APIKeyHeader + "=([a-zA-Z0-9]*)")
	jwtKey      = []byte("set_the_jwt_in_config") // set via config.
)

func jwtKeyFunc(token *jwt.Token) (interface{}, error) {
	return jwtKey, nil
}

type Claims struct {
	jwt.StandardClaims
	UserID        string `json:"user_id"`
	GroupID       string `json:"group_id"`
	Impersonating bool   `json:"impersonating"`
	// TODO(bduffany): remove this field
	AllowedGroups          []string                      `json:"allowed_groups"`
	GroupMemberships       []*interfaces.GroupMembership `json:"group_memberships"`
	Capabilities           []akpb.ApiKey_Capability      `json:"capabilities"`
	UseGroupOwnedExecutors bool                          `json:"use_group_owned_executors,omitempty"`
}

func (c *Claims) GetUserID() string {
	return c.UserID
}

func (c *Claims) GetGroupID() string {
	return c.GroupID
}

func (c *Claims) IsImpersonating() bool {
	return c.Impersonating
}

func (c *Claims) GetAllowedGroups() []string {
	return c.AllowedGroups
}

func (c *Claims) GetGroupMemberships() []*interfaces.GroupMembership {
	return c.GroupMemberships
}

func (c *Claims) IsAdmin() bool {
	for _, groupID := range c.AllowedGroups {
		if groupID == "admin" {
			return true
		}
	}
	return false
}

func (c *Claims) HasCapability(cap akpb.ApiKey_Capability) bool {
	for _, cc := range c.Capabilities {
		if cap == cc {
			return true
		}
	}
	return false
}

func (c *Claims) GetUseGroupOwnedExecutors() bool {
	return c.UseGroupOwnedExecutors
}

func assembleJWT(ctx context.Context, claims *Claims) (string, error) {
	expirationTime := time.Now().Add(defaultBuildBuddyJWTDuration)
	claims.StandardClaims = jwt.StandardClaims{ExpiresAt: expirationTime.Unix()}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(jwtKey)
	return tokenString, err
}

func SetCookie(env environment.Env, w http.ResponseWriter, name, value string, expiry time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Expires:  expiry,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Path:     "/",
		Secure:   env.GetConfigurator().GetHttpsOnlyCookies(),
	})
}

func ClearCookie(env environment.Env, w http.ResponseWriter, name string) {
	SetCookie(env, w, name, "", time.Now())
}

func GetCookie(r *http.Request, name string) string {
	if c, err := r.Cookie(name); err == nil {
		return c.Value
	}
	return ""
}

func setLoginCookie(env environment.Env, w http.ResponseWriter, jwt, issuer, sessionID string) {
	expiry := time.Now().Add(loginCookieDuration)
	SetCookie(env, w, jwtCookie, jwt, expiry)
	SetCookie(env, w, authIssuerCookie, issuer, expiry)
	SetCookie(env, w, sessionIDCookie, sessionID, expiry)
}

func clearLoginCookie(env environment.Env, w http.ResponseWriter) {
	ClearCookie(env, w, jwtCookie)
	ClearCookie(env, w, authIssuerCookie)
	ClearCookie(env, w, sessionIDCookie)
}

type userToken struct {
	Email      string `json:"email"`
	Sub        string `json:"sub"`
	Name       string `json:"name"`
	GivenName  string `json:"given_name"`
	FamilyName string `json:"family_name"`
	Picture    string `json:"picture"`
	issuer     string
	slug       string
}

func (t *userToken) GetIssuer() string {
	return t.issuer
}

func (t *userToken) GetSubscriber() string {
	return t.Sub
}

func (t *userToken) GetSubID() string {
	return t.issuer + "/" + t.Sub
}

type authenticator interface {
	getSlug() string
	getIssuer() string
	authCodeURL(state string, opts ...oauth2.AuthCodeOption) (string, error)
	exchange(ctx context.Context, code string, opts ...oauth2.AuthCodeOption) (*oauth2.Token, error)
	verifyTokenAndExtractUser(ctx context.Context, jwt string, checkExpiry bool) (*userToken, error)
	checkAccessToken(ctx context.Context, jwt, accessToken string) error
	renewToken(ctx context.Context, refreshToken string) (*oauth2.Token, error)
}

type oidcAuthenticator struct {
	oauth2Config       func() (*oauth2.Config, error)
	cachedOauth2Config *oauth2.Config
	oidcConfig         *oidc.Config
	cachedProvider     *oidc.Provider
	provider           func() (*oidc.Provider, error)
	issuer             string
	slug               string
}

func extractToken(issuer, slug string, idToken *oidc.IDToken) (*userToken, error) {
	ut := &userToken{
		issuer: issuer,
		slug:   slug,
	}
	if err := idToken.Claims(ut); err != nil {
		return nil, err
	}
	return ut, nil
}

func (a *oidcAuthenticator) getIssuer() string {
	return a.issuer
}

func (a *oidcAuthenticator) getSlug() string {
	return a.slug
}

func (a *oidcAuthenticator) authCodeURL(state string, opts ...oauth2.AuthCodeOption) (string, error) {
	oauth2Config, err := a.oauth2Config()
	if err != nil {
		return "", err
	}
	return oauth2Config.AuthCodeURL(state, opts...), nil
}

func (a *oidcAuthenticator) exchange(ctx context.Context, code string, opts ...oauth2.AuthCodeOption) (*oauth2.Token, error) {
	oauth2Config, err := a.oauth2Config()
	if err != nil {
		return nil, err
	}
	return oauth2Config.Exchange(ctx, code, opts...)
}

func (a *oidcAuthenticator) verifyTokenAndExtractUser(ctx context.Context, jwt string, checkExpiry bool) (*userToken, error) {
	conf := a.oidcConfig
	conf.SkipExpiryCheck = !checkExpiry
	provider, err := a.provider()
	if err != nil {
		return nil, err
	}
	validToken, err := provider.Verifier(conf).Verify(ctx, jwt)
	if err != nil {
		return nil, err
	}
	return extractToken(a.issuer, a.slug, validToken)
}

func (a *oidcAuthenticator) checkAccessToken(ctx context.Context, jwt, accessToken string) error {
	provider, err := a.provider()
	if err != nil {
		return err
	}
	conf := a.oidcConfig
	validToken, err := provider.Verifier(conf).Verify(ctx, jwt)
	if err != nil {
		return err
	}
	// at_hash is optional:
	// https://github.com/coreos/go-oidc/blob/d42db69c79f2fa664fd4156e939bf27bba0d2f68/oidc/oidc.go#L373
	if validToken.AccessTokenHash == "" {
		return nil
	}
	return validToken.VerifyAccessToken(accessToken)
}

func (a *oidcAuthenticator) renewToken(ctx context.Context, refreshToken string) (*oauth2.Token, error) {
	oauth2Config, err := a.oauth2Config()
	if err != nil {
		return nil, err
	}
	src := oauth2Config.TokenSource(ctx, &oauth2.Token{RefreshToken: refreshToken})
	t, err := src.Token() // this actually renews the token
	if err != nil {
		return nil, status.PermissionDeniedErrorf("%s: %s", ExpiredSessionMsg, err.Error())
	}
	return t, nil
}

type apiKeyGroupCacheEntry struct {
	data         interfaces.APIKeyGroup
	expiresAfter time.Time
}

// apiKeyGroupCache is a cache for API Key -> Group lookups. A single Bazel invocation
// can generate large bursts of RPCs, each of which needs to be authed.
// There's no need to go to the database for every single request as this data
// rarely changes.
type apiKeyGroupCache struct {
	// Note that even though we base this off an LRU cache, every entry has a hard expiration
	// time to force a refresh of the underlying data.
	lru interfaces.LRU
	ttl time.Duration
	mu  sync.RWMutex
}

func newAPIKeyGroupCache(configurator *config.Configurator) (*apiKeyGroupCache, error) {
	ttl := defaultAPIKeyGroupCacheTTL
	if configurator.GetAuthAPIKeyGroupCacheTTL() != "" {
		configTTL, err := time.ParseDuration(configurator.GetAuthAPIKeyGroupCacheTTL())
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invalid API Key -> Group cache TTL [%s]: %v", configurator.GetAuthAPIKeyGroupCacheTTL(), err)
		}
		ttl = configTTL
	}

	config := &lru.Config{
		MaxSize: apiKeyGroupCacheSize,
		SizeFn:  func(v interface{}) int64 { return 1 },
	}
	lru, err := lru.NewLRU(config)
	if err != nil {
		return nil, status.InternalErrorf("error initializing API Key -> Group cache: %v", err)
	}
	return &apiKeyGroupCache{lru: lru, ttl: ttl}, nil
}

func (c *apiKeyGroupCache) Get(apiKey string) (akg interfaces.APIKeyGroup, ok bool) {
	c.mu.RLock()
	v, ok := c.lru.Get(apiKey)
	c.mu.RUnlock()
	if !ok {
		return nil, ok
	}
	entry, ok := v.(*apiKeyGroupCacheEntry)
	if !ok {
		// Should never happen.
		log.Errorf("Data in cache was of wrong type, got type %T", v)
		return nil, false
	}
	if time.Now().After(entry.expiresAfter) {
		return nil, false
	}
	return entry.data, true
}

func (c *apiKeyGroupCache) Add(apiKey string, apiKeyGroup interfaces.APIKeyGroup) {
	c.mu.Lock()
	c.lru.Add(apiKey, &apiKeyGroupCacheEntry{data: apiKeyGroup, expiresAfter: time.Now().Add(c.ttl)})
	c.mu.Unlock()
}

type OpenIDAuthenticator struct {
	env              environment.Env
	myURL            *url.URL
	apiKeyGroupCache *apiKeyGroupCache
	authenticators   []authenticator
}

func createAuthenticatorsFromConfig(ctx context.Context, env environment.Env, authConfigs []config.OauthProvider, authURL *url.URL) ([]authenticator, error) {
	var authenticators []authenticator
	for _, authConfig := range authConfigs {
		// declare local var that shadows loop var for closure capture
		authConfig := authConfig
		oidcConfig := &oidc.Config{
			ClientID:        authConfig.ClientID,
			SkipExpiryCheck: false,
		}
		authenticator := &oidcAuthenticator{
			slug:       authConfig.Slug,
			issuer:     authConfig.IssuerURL,
			oidcConfig: oidcConfig,
		}

		// initialize provider and oauth2Config.Endpoint on-demand, since our self oauth provider won't be reachable until the server starts
		var oauth2ConfigMutex sync.Mutex
		authenticator.oauth2Config = func() (*oauth2.Config, error) {
			oauth2ConfigMutex.Lock()
			defer oauth2ConfigMutex.Unlock()
			var err error
			if authenticator.cachedOauth2Config == nil {
				var provider *oidc.Provider
				if provider, err = authenticator.provider(); err == nil {
					// "openid" is a required scope for OpenID Connect flows.
					scopes := []string{oidc.ScopeOpenID, "profile", "email"}
					// Google reject the offline_access scope in favor of access_type=offline url param which already gets
					// set in our auth flow thanks to the oauth2.AccessTypeOffline authCodeOption at the top of this file.
					// https://github.com/coreos/go-oidc/blob/v2.2.1/oidc.go#L30
					if authConfig.IssuerURL != "https://accounts.google.com" && !env.GetConfigurator().GetDisableRefreshToken() {
						scopes = append(scopes, oidc.ScopeOfflineAccess)
					}
					// Configure an OpenID Connect aware OAuth2 client.
					authenticator.cachedOauth2Config = &oauth2.Config{
						ClientID:     authConfig.ClientID,
						ClientSecret: authConfig.ClientSecret,
						RedirectURL:  authURL.String(),
						Endpoint:     provider.Endpoint(),
						Scopes:       scopes,
					}
				}
			}
			return authenticator.cachedOauth2Config, err
		}

		var providerMutex sync.Mutex
		authenticator.provider = func() (*oidc.Provider, error) {
			providerMutex.Lock()
			defer providerMutex.Unlock()
			var err error
			if authenticator.cachedProvider == nil {
				if authenticator.cachedProvider, err = oidc.NewProvider(ctx, authConfig.IssuerURL); err != nil {
					log.Errorf("Error Initializing auth: %v", err)
				}
			}
			return authenticator.cachedProvider, err
		}

		authenticators = append(authenticators, authenticator)
	}
	return authenticators, nil
}

func newOpenIDAuthenticator(ctx context.Context, env environment.Env, oauthProviders []config.OauthProvider) (*OpenIDAuthenticator, error) {
	authenticators, err := createAuthenticatorsFromConfig(
		ctx,
		env,
		oauthProviders,
		build_buddy_url.WithPath("/auth/"),
	)
	if err != nil {
		return nil, err
	}

	// Set the JWT key.
	jwtKey = []byte(env.GetConfigurator().GetAuthJWTKey())

	// Initialize API Key -> Group cache unless it's disabled by config.
	var akgCache *apiKeyGroupCache
	if env.GetConfigurator().GetAuthAPIKeyGroupCacheTTL() != "0" {
		akgCache, err = newAPIKeyGroupCache(env.GetConfigurator())
		if err != nil {
			return nil, err
		}
	}

	return &OpenIDAuthenticator{
		env:              env,
		myURL:            build_buddy_url.WithPath(""),
		authenticators:   authenticators,
		apiKeyGroupCache: akgCache,
	}, nil
}

func newForTesting(ctx context.Context, env environment.Env, testAuthenticator authenticator) (*OpenIDAuthenticator, error) {
	oia, err := newOpenIDAuthenticator(ctx, env, nil /*oauthProviders=*/)
	if err != nil {
		return nil, err
	}
	oia.authenticators = append(oia.authenticators, testAuthenticator)
	return oia, nil
}

func NewOpenIDAuthenticator(ctx context.Context, env environment.Env, authConfigs []config.OauthProvider) (*OpenIDAuthenticator, error) {
	if len(authConfigs) == 0 {
		return nil, status.FailedPreconditionErrorf("No auth providers specified in config!")
	}

	a, err := newOpenIDAuthenticator(ctx, env, authConfigs)
	if err != nil {
		alert.UnexpectedEvent("authentication_configuration_failed", "Failed to configure authentication: %s", err)
	}

	return a, err
}

func (a *OpenIDAuthenticator) validateRedirectURL(redirectURL string) error {
	if a.myURL.Host == "" {
		return status.FailedPreconditionError("You must specify a build_buddy_url in your config to enable authentication. For more information, see: https://www.buildbuddy.io/docs/config-app")
	}
	return burl.ValidateRedirect(a.env, redirectURL)
}

func (a *OpenIDAuthenticator) getAuthConfig(issuer string) authenticator {
	for _, a := range a.authenticators {
		if burl.SameHostname(a.getIssuer(), issuer) {
			return a
		}
	}
	return nil
}

func (a *OpenIDAuthenticator) getAuthConfigForSlug(slug string) authenticator {
	for _, a := range a.authenticators {
		if strings.EqualFold(a.getSlug(), slug) {
			return a
		}
	}
	return nil
}

func (a *OpenIDAuthenticator) getAuthCodeOptions(r *http.Request) []oauth2.AuthCodeOption {
	options := []oauth2.AuthCodeOption{}
	if !a.env.GetConfigurator().GetDisableRefreshToken() {
		options = append(options, oauth2.AccessTypeOffline)
	}
	sessionID := GetCookie(r, sessionIDCookie)
	// If a session doesn't already exist, force a consent screen.
	if sessionID == "" {
		options = append(options, oauth2.ApprovalForce)
	}
	return options
}

func (a *OpenIDAuthenticator) lookupAPIKeyGroupFromAPIKey(ctx context.Context, apiKey string) (interfaces.APIKeyGroup, error) {
	if apiKey == "" {
		return nil, status.UnauthenticatedError("missing API key")
	}
	if a.apiKeyGroupCache != nil {
		d, ok := a.apiKeyGroupCache.Get(apiKey)
		if ok {
			return d, nil
		}
	}
	authDB := a.env.GetAuthDB()
	if authDB == nil {
		return nil, status.FailedPreconditionError("AuthDB not configured")
	}
	apkg, err := authDB.GetAPIKeyGroupFromAPIKey(ctx, apiKey)
	if err == nil && a.apiKeyGroupCache != nil {
		a.apiKeyGroupCache.Add(apiKey, apkg)
	}
	return apkg, err
}

func userClaims(u *tables.User, effectiveGroup string) *Claims {
	allowedGroups := make([]string, 0, len(u.Groups))
	groupMemberships := make([]*interfaces.GroupMembership, 0, len(u.Groups))
	for _, g := range u.Groups {
		allowedGroups = append(allowedGroups, g.Group.GroupID)
		groupMemberships = append(groupMemberships, &interfaces.GroupMembership{
			GroupID: g.Group.GroupID,
			Role:    role.Role(g.Role),
		})
	}
	return &Claims{
		UserID:           u.UserID,
		GroupMemberships: groupMemberships,
		AllowedGroups:    allowedGroups,
		GroupID:          effectiveGroup,
	}
}

func groupClaims(akg interfaces.APIKeyGroup) *Claims {
	return &Claims{
		GroupID:       akg.GetGroupID(),
		AllowedGroups: []string{akg.GetGroupID()},
		// For now, API keys are assigned the default role.
		GroupMemberships: []*interfaces.GroupMembership{
			{GroupID: akg.GetGroupID(), Role: role.Default},
		},
		Capabilities:           capabilities.FromInt(akg.GetCapabilities()),
		UseGroupOwnedExecutors: akg.GetUseGroupOwnedExecutors(),
	}
}

func AuthContextWithError(ctx context.Context, err error) context.Context {
	return context.WithValue(ctx, contextUserErrorKey, err)
}

func authContextFromClaims(ctx context.Context, claims *Claims, err error) context.Context {
	if err != nil {
		return AuthContextWithError(ctx, err)
	}
	tokenString, err := assembleJWT(ctx, claims)
	if err != nil {
		return AuthContextWithError(ctx, err)
	}
	ctx = context.WithValue(ctx, contextTokenStringKey, tokenString)
	// Note: we clear the error here in case it was set initially by the
	// authentication handler, but then we want to re-authenticate later on in the
	// request lifecycle, and authentication is successful.
	// Specifically, we do this when we see the API key in the "BuildStarted" event.
	ctx = context.WithValue(ctx, contextUserErrorKey, nil)
	return ctx
}

func (a *OpenIDAuthenticator) ParseAPIKeyFromString(input string) (string, error) {
	matches := apiKeyRegex.FindAllStringSubmatch(input, -1)
	l := len(matches)
	if l == 0 {
		// The api key header is not present
		return "", nil
	}
	lastMatch := matches[l-1]
	if len(lastMatch) != 2 {
		return "", status.UnauthenticatedError("failed to parse API key: invalid input")
	}
	if apiKey := lastMatch[1]; apiKey != "" {
		return apiKey, nil
	}
	return "", status.UnauthenticatedError("failed to parse API key: missing API Key")
}

func (a *OpenIDAuthenticator) AuthContextFromAPIKey(ctx context.Context, apiKey string) context.Context {
	if _, ok := ctx.Value(APIKeyHeader).(string); ok {
		alert.UnexpectedEvent("overwrite_api_key", "Overwriting existing value of %q in context.", APIKeyHeader)
	}
	ctx = context.WithValue(ctx, APIKeyHeader, apiKey)
	claims, err := a.claimsFromAPIKey(ctx, apiKey)
	return authContextFromClaims(ctx, claims, err)
}

func (a *OpenIDAuthenticator) TrustedJWTFromAuthContext(ctx context.Context) string {
	jwt, ok := ctx.Value(contextTokenStringKey).(string)
	if !ok {
		return ""
	}
	return jwt
}

func (a *OpenIDAuthenticator) AuthContextFromTrustedJWT(ctx context.Context, jwt string) context.Context {
	return context.WithValue(ctx, contextTokenStringKey, jwt)
}

func (a *OpenIDAuthenticator) claimsFromAPIKey(ctx context.Context, apiKey string) (*Claims, error) {
	akg, err := a.lookupAPIKeyGroupFromAPIKey(ctx, apiKey)
	if err != nil {
		return nil, err
	}
	return groupClaims(akg), nil
}

func (a *OpenIDAuthenticator) claimsFromBasicAuth(ctx context.Context, login, pass string) (*Claims, error) {
	authDB := a.env.GetAuthDB()
	if authDB == nil {
		return nil, status.FailedPreconditionError("AuthDB not configured")
	}
	akg, err := authDB.GetAPIKeyGroupFromBasicAuth(ctx, login, pass)
	if err != nil {
		return nil, err
	}
	return groupClaims(akg), nil
}

func ClaimsFromSubID(env environment.Env, ctx context.Context, subID string) (*Claims, error) {
	authDB := env.GetAuthDB()
	if authDB == nil {
		return nil, status.FailedPreconditionError("AuthDB not configured")
	}
	u, err := authDB.LookupUserFromSubID(ctx, subID)
	if err != nil {
		return nil, err
	}
	eg := ""
	if c := requestcontext.ProtoRequestContextFromContext(ctx); c != nil && c.GetGroupId() != "" {
		for _, g := range u.Groups {
			if g.Group.GroupID == c.GetGroupId() {
				eg = c.GetGroupId()
			}
		}
	}

	claims := userClaims(u, eg)

	// If the user is trying to impersonate a member of another org and has Admin
	// role within the configured admin group, set their authenticated user to
	// *only* have access to the org being impersonated.
	if c := requestcontext.ProtoRequestContextFromContext(ctx); c != nil && c.GetImpersonatingGroupId() != "" {
		adminGroupID := env.GetConfigurator().GetAuthAdminGroupID()
		for _, membership := range claims.GetGroupMemberships() {
			if membership.GroupID != adminGroupID || membership.Role != role.Admin {
				continue
			}
			u.Groups = []*tables.GroupRole{{
				Group: tables.Group{GroupID: c.GetImpersonatingGroupId()},
				Role:  uint32(role.Admin),
			}}
			claims := userClaims(u, c.GetImpersonatingGroupId())
			claims.Impersonating = true
			return claims, nil
		}
		return nil, status.PermissionDeniedError("You do not have permissions to impersonate group members.")
	}

	return claims, nil
}

func (a *OpenIDAuthenticator) claimsFromAuthorityString(ctx context.Context, authority string) (*Claims, error) {
	loginPass := strings.SplitN(authority, ":", 2)
	if len(loginPass) == 2 {
		return a.claimsFromBasicAuth(ctx, loginPass[0], loginPass[1])
	}
	return a.claimsFromAPIKey(ctx, authority)
}

func (a *OpenIDAuthenticator) AuthenticateGRPCRequest(ctx context.Context) (interfaces.UserInfo, error) {
	return a.authenticateGRPCRequest(ctx, false /* acceptJWT= */)
}

func (a *OpenIDAuthenticator) authenticateGRPCRequest(ctx context.Context, acceptJWT bool) (*Claims, error) {
	p, ok := peer.FromContext(ctx)

	if ok && p != nil && p.AuthInfo != nil {
		certs := p.AuthInfo.(credentials.TLSInfo).State.PeerCertificates
		if len(certs) > 0 && certs[0].Subject.SerialNumber != "" {
			return a.claimsFromAPIKey(ctx, certs[0].Subject.SerialNumber)
		}
	}

	if md, ok := metadata.FromIncomingContext(ctx); ok {
		keys := md.Get(APIKeyHeader)
		if l := len(keys); l > 0 {
			// get the last key
			return a.claimsFromAPIKey(ctx, keys[l-1])
		}

		if keys := md.Get(basicAuthHeader); len(keys) > 0 {
			return a.claimsFromAuthorityString(ctx, keys[0])
		}

		if keys := md.Get(authorityHeader); len(keys) > 0 {
			// Authenticate with :authority header
			lpAndHost := strings.SplitN(keys[0], "@", 2)
			if len(lpAndHost) == 2 {
				return a.claimsFromAuthorityString(ctx, lpAndHost[0])
			}
		}
	}

	if acceptJWT {
		// Check if we're already authenticated from incoming headers.
		if claims, err := a.authenticatedUser(ctx); err == nil {
			return claims, nil
		}
	}

	return nil, status.UnauthenticatedError("gRPC request is missing credentials.")
}

// AuthenticatedGRPCContext attempts to authenticate the gRPC request using peer info,
// API key header, or basic auth headers.
//
// If none of the above information is provided, UnauthenticatedError is returned via the
// `contextUserErrorKey` context value.
func (a *OpenIDAuthenticator) AuthenticatedGRPCContext(ctx context.Context) context.Context {
	claims, err := a.authenticateGRPCRequest(ctx, true /* acceptJWT= */)
	return authContextFromClaims(ctx, claims, err)
}

func (a *OpenIDAuthenticator) AuthenticatedHTTPContext(w http.ResponseWriter, r *http.Request) context.Context {
	claims, userToken, err := a.authenticateUser(w, r)
	ctx := r.Context()
	if userToken != nil {
		// Store the user information in the context even if authentication fails.
		// This information is used in the user creation flow.
		ctx = context.WithValue(ctx, contextUserKey, userToken)
	}
	if err != nil {
		return AuthContextWithError(ctx, err)
	}
	return authContextFromClaims(ctx, claims, err)
}

func (a *OpenIDAuthenticator) authenticateUser(w http.ResponseWriter, r *http.Request) (*Claims, *userToken, error) {
	ctx := r.Context()
	if apiKey := r.Header.Get(APIKeyHeader); apiKey != "" {
		claims, err := a.claimsFromAPIKey(ctx, apiKey)
		return claims, nil, err
	}

	jwt := GetCookie(r, jwtCookie)
	if jwt == "" {
		return nil, nil, status.PermissionDeniedErrorf("%s: no jwt set", loggedOutMsg)
	}
	issuer := GetCookie(r, authIssuerCookie)
	sessionID := GetCookie(r, sessionIDCookie)

	auth := a.getAuthConfig(issuer)
	if auth == nil {
		return nil, nil, status.PermissionDeniedErrorf("No config found for issuer: %s", issuer)
	}

	authDB := a.env.GetAuthDB()
	if authDB == nil {
		return nil, nil, status.FailedPreconditionError("AuthDB not configured")
	}

	// If the token is corrupt for some reason (not just out of date); then
	// bail.
	ut, err := auth.verifyTokenAndExtractUser(ctx, jwt, false /*checkExpiry*/)
	if err != nil {
		return nil, nil, err
	}

	// If the session is not found, bail.
	sesh, err := authDB.ReadSession(ctx, sessionID)
	if err != nil {
		log.Debugf("Session not found: %s", err)
		return nil, ut, status.PermissionDeniedErrorf("%s: session not found", loggedOutMsg)
	}

	if err := auth.checkAccessToken(ctx, jwt, sesh.AccessToken); err != nil {
		log.Debugf("Invalid token: %s", err)
		return nil, ut, status.PermissionDeniedErrorf("%s: invalid token", loggedOutMsg)
	}

	// Now try to verify the token again -- this time we check for expiry.
	// If it succeeds, we're done! Otherwise we fall through to refreshing
	// the token below.
	if ut, err := auth.verifyTokenAndExtractUser(ctx, jwt, true /*=checkExpiry*/); err == nil {
		claims, err := ClaimsFromSubID(a.env, ctx, ut.GetSubID())
		return claims, ut, err
	}

	// WE only refresh the token if:
	//   - there is a valid session
	//   - at_hash matches (so no other update has happened in the mean time)
	//   - token is just out of date.
	// Still here? Token needs a refresh. Do that now.
	newToken, err := auth.renewToken(ctx, sesh.RefreshToken)
	if err != nil {
		return nil, nil, err
	}

	sesh.ExpiryUsec = time.Unix(0, newToken.Expiry.UnixNano()).UnixMicro()
	sesh.AccessToken = newToken.AccessToken

	if err := authDB.InsertOrUpdateUserSession(ctx, sessionID, sesh); err != nil {
		return nil, nil, err
	}
	if jwt, ok := newToken.Extra("id_token").(string); ok {
		setLoginCookie(a.env, w, jwt, issuer, sessionID)
		claims, err := ClaimsFromSubID(a.env, ctx, ut.GetSubID())
		return claims, ut, err
	}
	return nil, nil, status.PermissionDeniedErrorf("%s: could not refresh token", loggedOutMsg)
}

func (a *OpenIDAuthenticator) authenticatedUser(ctx context.Context) (*Claims, error) {
	// If context already contains a JWT, just verify it and return the claims.
	if tokenString, ok := ctx.Value(contextTokenStringKey).(string); ok && tokenString != "" {
		claims := &Claims{}
		_, err := jwt.ParseWithClaims(tokenString, claims, jwtKeyFunc)
		if err != nil {
			return nil, err
		}
		return claims, nil
	}

	// If there's no error or we have an assertion failure; just return a
	// user not found error.
	err, ok := ctx.Value(contextUserErrorKey).(error)
	if !ok || err == nil {
		return nil, status.UnauthenticatedError(userNotFoundMsg)
	}

	// if there was an error set on the context, and it was an
	// Unauthenticated or PermissionDeniedError, then the FE can handle it,
	// so pass it through.
	if status.IsUnauthenticatedError(err) || status.IsPermissionDeniedError(err) {
		return nil, err
	}

	// All other types of errors will be converted into Unauthenticated
	// errors.
	// WARNING: app/auth/auth_service.ts depends on this status being UNAUTHENTICATED.
	return nil, status.UnauthenticatedErrorf("%s: %s", userNotFoundMsg, err.Error())
}

func (a *OpenIDAuthenticator) AuthenticatedUser(ctx context.Context) (interfaces.UserInfo, error) {
	// We don't return directly so that we can return a nil-interface instead of an interface holding a nil *Claims.
	// Callers should be checking err before before accessing the user, but in case they don't this will prevent a nil
	// dereference.
	claims, err := a.authenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	return claims, nil
}

func (a *OpenIDAuthenticator) FillUser(ctx context.Context, user *tables.User) error {
	t, ok := ctx.Value(contextUserKey).(*userToken)
	if !ok {
		// WARNING: app/auth/auth_service.ts depends on this status being UNAUTHENTICATED.
		return status.UnauthenticatedError("No user token available to fill user")
	}

	pk, err := tables.PrimaryKeyForTable("Users")
	if err != nil {
		return err
	}
	user.UserID = pk
	user.SubID = t.GetSubID()
	user.FirstName = t.GivenName
	user.LastName = t.FamilyName
	user.Email = t.Email
	user.ImageURL = t.Picture
	if t.slug != "" {
		user.Groups = []*tables.GroupRole{
			{Group: tables.Group{URLIdentifier: &t.slug}},
		}
	}
	return nil
}

func (a *OpenIDAuthenticator) Login(w http.ResponseWriter, r *http.Request) {
	issuer := GetCookie(r, authIssuerCookie)
	if issuerParam := r.URL.Query().Get(authIssuerParam); issuerParam != "" {
		issuer = issuerParam
	}
	auth := a.getAuthConfig(issuer)

	if slug := r.URL.Query().Get(slugParam); slug != "" {
		auth = a.getAuthConfigForSlug(slug)
		if auth == nil {
			redirectWithError(w, r, status.PermissionDeniedErrorf("No SSO config found for slug: %s", slug))
			return
		}
		issuer = auth.getIssuer()
	}

	if issuer == "" {
		redirectWithError(w, r, status.PermissionDeniedErrorf("No auth issuer set"))
		return
	}

	if auth == nil {
		redirectWithError(w, r, status.PermissionDeniedErrorf("No config found for issuer: %s", issuer))
		return
	}

	// Set the "state" cookie which will be returned to us by tha authentication
	// provider in the URL. We verify that it matches.
	state := fmt.Sprintf("%d", random.RandUint64())
	SetCookie(a.env, w, stateCookie, state, time.Now().Add(tempCookieDuration))

	redirectURL := r.URL.Query().Get(authRedirectParam)
	if err := a.validateRedirectURL(redirectURL); err != nil {
		redirectWithError(w, r, err)
		return
	}

	// Redirect to the login provider (and ask for a refresh token).
	u, err := auth.authCodeURL(state, a.getAuthCodeOptions(r)...)
	if err != nil {
		redirectWithError(w, r, err)
		return
	}

	// Set the redirection URL in a cookie so we can use it after validating
	// the user in our /auth callback.
	SetCookie(a.env, w, redirCookie, redirectURL, time.Now().Add(tempCookieDuration))

	// Set the issuer cookie so we remember which issuer to use when exchanging
	// a token later in our /auth callback.
	SetCookie(a.env, w, authIssuerCookie, issuer, time.Now().Add(tempCookieDuration))

	http.Redirect(w, r, u, http.StatusTemporaryRedirect)
}

func (a *OpenIDAuthenticator) Logout(w http.ResponseWriter, r *http.Request) {
	redir := func() {
		redirURL := r.URL.Query().Get(authRedirectParam)
		if redirURL == "" {
			redirURL = "/" // default to redirecting home.
		}
		http.Redirect(w, r, redirURL, http.StatusTemporaryRedirect)
	}
	clearLoginCookie(a.env, w)
	defer redir()

	// Attempt to mark the user as logged out in the database by clearing
	// their access token.
	jwt := GetCookie(r, jwtCookie)
	if jwt == "" {
		return
	}
	sessionID := GetCookie(r, sessionIDCookie)
	if sessionID == "" {
		return
	}

	if authDB := a.env.GetAuthDB(); authDB != nil {
		if err := authDB.ClearSession(r.Context(), sessionID); err != nil {
			log.Errorf("Error clearing user session on logout: %s", err)
		}
	}
}

func (a *OpenIDAuthenticator) Auth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	authDB := a.env.GetAuthDB()
	if authDB == nil {
		redirectWithError(w, r, status.FailedPreconditionError("AuthDB not configured"))
		return
	}

	// Verify "state" cookie match.
	if r.FormValue("state") != GetCookie(r, stateCookie) {
		redirectWithError(w, r, status.PermissionDeniedErrorf("state mismatch: %s != %s", r.FormValue("state"), GetCookie(r, stateCookie)))
		return
	}

	authError := r.URL.Query().Get("error")
	if authError != "" {
		redirectWithError(w, r, status.PermissionDeniedErrorf("Authenticator returned error: %s (%s %s)", authError, r.URL.Query().Get("error_desc"), r.URL.Query().Get("error_description")))
		return
	}

	// Lookup issuer from the cookie we set in /login.
	issuer := GetCookie(r, authIssuerCookie)
	auth := a.getAuthConfig(issuer)
	if auth == nil {
		redirectWithError(w, r, status.PermissionDeniedErrorf("No config found for issuer: %s", issuer))
		return
	}

	code := r.URL.Query().Get("code")
	oauth2Token, err := auth.exchange(ctx, code, a.getAuthCodeOptions(r)...)
	if err != nil {
		redirectWithError(w, r, status.PermissionDeniedErrorf("Error exchanging code for auth token: %s", code))
		return
	}

	// Extract the ID Token (JWT) from OAuth2 token.
	jwt, ok := oauth2Token.Extra("id_token").(string)
	if !ok {
		redirectWithError(w, r, status.PermissionDeniedError("ID Token not present in auth response"))
		return
	}

	ut, err := auth.verifyTokenAndExtractUser(ctx, jwt /*checkExpiry=*/, true)
	if err != nil {
		redirectWithError(w, r, err)
		return
	}

	guid, err := uuid.NewRandom()
	if err != nil {
		redirectWithError(w, r, err)
		return
	}
	sessionID := guid.String()

	// OK, the token is valid so we will: store the token in our DB for
	// later & set the login cookie so we know this user is logged in.
	setLoginCookie(a.env, w, jwt, issuer, sessionID)

	expireTime := time.Unix(0, oauth2Token.Expiry.UnixNano())
	sesh := &tables.Session{
		SessionID:   sessionID,
		SubID:       ut.GetSubID(),
		AccessToken: oauth2Token.AccessToken,
		ExpiryUsec:  expireTime.UnixMicro(),
	}
	refreshToken, ok := oauth2Token.Extra("refresh_token").(string)
	if ok {
		sesh.RefreshToken = refreshToken
	}
	if err := authDB.InsertOrUpdateUserSession(ctx, sessionID, sesh); err != nil {
		redirectWithError(w, r, err)
		return
	}
	redirURL := GetCookie(r, redirCookie)
	if redirURL == "" {
		redirURL = "/" // default to redirecting home.
	}
	http.Redirect(w, r, redirURL, http.StatusTemporaryRedirect)
}

// Parses the JWT's UserInfo from the context without verifying the JWT.
// Only use this if you know what you're doing and the JWT is coming from a trusted source
// that has already verified its authenticity.
func UserFromTrustedJWT(ctx context.Context) (interfaces.UserInfo, error) {
	if tokenString, ok := ctx.Value(contextTokenStringKey).(string); ok && tokenString != "" {
		claims := &Claims{}
		parser := jwt.Parser{}
		_, _, err := parser.ParseUnverified(tokenString, claims)
		if err != nil {
			return nil, err
		}
		return claims, nil
	}
	// WARNING: app/auth/auth_service.ts depends on this status being UNAUTHENTICATED.
	return nil, status.UnauthenticatedError(userNotFoundMsg)
}

func redirectWithError(w http.ResponseWriter, r *http.Request, err error) {
	log.Warning(err.Error())
	http.Redirect(w, r, "/?error="+url.QueryEscape(err.Error()), http.StatusTemporaryRedirect)
}

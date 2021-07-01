package auth

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"github.com/dgrijalva/jwt-go"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	oidc "github.com/coreos/go-oidc"
)

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

	// The name of the auth cookies used to authenticate the
	// client.
	jwtCookie        = "Authorization"
	authIssuerCookie = "Authorization-Issuer"
	stateCookie      = "State-Token"
	redirCookie      = "Redirect-Url"

	// How long certain cookies last
	tempCookieDuration  = 24 * time.Hour
	loginCookieDuration = 365 * 24 * time.Hour

	// BuildBuddy JWT duration maximum.
	defaultBuildBuddyJWTDuration = 24 * time.Hour

	// Maximum amount of time we will cache Group information for an API key.
	defaultAPIKeyGroupCacheTTL = 5 * time.Minute
	// Maximum number of entries in API Key -> Group cache.
	apiKeyGroupCacheSize = 10000
)

var (
	authCodeOption []oauth2.AuthCodeOption = []oauth2.AuthCodeOption{oauth2.AccessTypeOffline, oauth2.ApprovalForce}
	apiKeyRegex                            = regexp.MustCompile(APIKeyHeader + "=([a-zA-Z0-9]+)")
	jwtKey                                 = []byte("set_the_jwt_in_config") // set via config.
)

func jwtKeyFunc(token *jwt.Token) (interface{}, error) {
	return jwtKey, nil
}

type Claims struct {
	jwt.StandardClaims
	UserID                 string                   `json:"user_id"`
	GroupID                string                   `json:"group_id"`
	AllowedGroups          []string                 `json:"allowed_groups"`
	Capabilities           []akpb.ApiKey_Capability `json:"capabilities"`
	UseGroupOwnedExecutors bool                     `json:"use_group_owned_executors,omitempty"`
}

func (c *Claims) GetUserID() string {
	return c.UserID
}

func (c *Claims) GetGroupID() string {
	return c.GroupID
}

func (c *Claims) GetAllowedGroups() []string {
	return c.AllowedGroups
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
	deadline, ok := ctx.Deadline()
	if ok {
		expirationTime = deadline
	}
	claims.StandardClaims = jwt.StandardClaims{ExpiresAt: expirationTime.Unix()}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(jwtKey)
	return tokenString, err
}

func setCookie(w http.ResponseWriter, name, value string, expiry time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Expires:  expiry,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Path:     "/",
	})
}

func clearCookie(w http.ResponseWriter, name string) {
	setCookie(w, name, "", time.Now())
}

func getCookie(r *http.Request, name string) string {
	if c, err := r.Cookie(name); err == nil {
		return c.Value
	}
	return ""
}

func setLoginCookie(w http.ResponseWriter, jwt, issuer string) {
	expiry := time.Now().Add(loginCookieDuration)
	setCookie(w, jwtCookie, jwt, expiry)
	setCookie(w, authIssuerCookie, issuer, expiry)
}

func clearLoginCookie(w http.ResponseWriter) {
	clearCookie(w, jwtCookie)
	clearCookie(w, authIssuerCookie)
}

type userToken struct {
	Email      string `json:"email"`
	Sub        string `json:"sub"`
	Name       string `json:"name"`
	GivenName  string `json:"given_name"`
	FamilyName string `json:"family_name"`
	Picture    string `json:"picture"`
	issuer     string
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
	getIssuer() string
	authCodeURL(state string, opts ...oauth2.AuthCodeOption) string
	exchange(ctx context.Context, code string, opts ...oauth2.AuthCodeOption) (*oauth2.Token, error)
	verifyTokenAndExtractUser(ctx context.Context, jwt string, checkExpiry bool) (*userToken, error)
	renewToken(ctx context.Context, refreshToken string) (*oauth2.Token, error)
}

type oidcAuthenticator struct {
	oauth2Config *oauth2.Config
	oidcConfig   *oidc.Config
	provider     *oidc.Provider
	issuer       string
}

func extractToken(issuer string, idToken *oidc.IDToken) (*userToken, error) {
	ut := &userToken{
		issuer: issuer,
	}
	if err := idToken.Claims(ut); err != nil {
		return nil, err
	}
	return ut, nil
}

func (a *oidcAuthenticator) getIssuer() string {
	return a.issuer
}

func (a *oidcAuthenticator) authCodeURL(state string, opts ...oauth2.AuthCodeOption) string {
	return a.oauth2Config.AuthCodeURL(state, opts...)
}

func (a *oidcAuthenticator) exchange(ctx context.Context, code string, opts ...oauth2.AuthCodeOption) (*oauth2.Token, error) {
	return a.oauth2Config.Exchange(ctx, code, opts...)
}

func (a *oidcAuthenticator) verifyTokenAndExtractUser(ctx context.Context, jwt string, checkExpiry bool) (*userToken, error) {
	conf := a.oidcConfig
	conf.SkipExpiryCheck = !checkExpiry
	validToken, err := a.provider.Verifier(conf).Verify(ctx, jwt)
	if err != nil {
		return nil, err
	}
	return extractToken(a.issuer, validToken)
}

func (a *oidcAuthenticator) renewToken(ctx context.Context, refreshToken string) (*oauth2.Token, error) {
	src := a.oauth2Config.TokenSource(ctx, &oauth2.Token{RefreshToken: refreshToken})
	return src.Token() // this actually renews the token
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
	lru *lru.LRU
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
		SizeFn:  func(k, v interface{}) int64 { return 1 },
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

func createAuthenticatorsFromConfig(ctx context.Context, authConfigs []config.OauthProvider, authURL *url.URL) ([]authenticator, error) {
	var authenticators []authenticator
	for _, authConfig := range authConfigs {
		provider, err := oidc.NewProvider(ctx, authConfig.IssuerURL)
		if err != nil {
			return nil, err
		}
		oidcConfig := &oidc.Config{
			ClientID:        authConfig.ClientID,
			SkipExpiryCheck: false,
		}
		// Configure an OpenID Connect aware OAuth2 client.
		oauth2Config := &oauth2.Config{
			ClientID:     authConfig.ClientID,
			ClientSecret: authConfig.ClientSecret,
			RedirectURL:  authURL.String(),
			Endpoint:     provider.Endpoint(),
			// "openid" is a required scope for OpenID Connect flows.
			Scopes: []string{oidc.ScopeOpenID, "profile", "email"},
		}
		authenticators = append(authenticators, &oidcAuthenticator{
			issuer:       authConfig.IssuerURL,
			oauth2Config: oauth2Config,
			oidcConfig:   oidcConfig,
			provider:     provider,
		})
	}
	return authenticators, nil
}

func newOpenIDAuthenticator(ctx context.Context, env environment.Env, oauthProviders []config.OauthProvider) (*OpenIDAuthenticator, error) {
	oia := &OpenIDAuthenticator{
		env: env,
	}

	myURL, err := url.Parse(env.GetConfigurator().GetAppBuildBuddyURL())
	if err != nil {
		return nil, err
	}
	authURL, err := myURL.Parse("/auth/")
	if err != nil {
		return nil, err
	}
	oia.myURL = myURL
	oia.authenticators, err = createAuthenticatorsFromConfig(ctx, oauthProviders, authURL)
	if err != nil {
		return nil, err
	}

	// Set the JWT key.
	jwtKey = []byte(env.GetConfigurator().GetAuthJWTKey())

	// Initialize API Key -> Group cache unless it's disabled by config.
	if env.GetConfigurator().GetAuthAPIKeyGroupCacheTTL() != "0" {
		akgCache, err := newAPIKeyGroupCache(env.GetConfigurator())
		if err != nil {
			return nil, err
		}
		oia.apiKeyGroupCache = akgCache
	}
	return oia, nil
}

func newForTesting(ctx context.Context, env environment.Env, testAuthenticator authenticator) (*OpenIDAuthenticator, error) {
	oia, err := newOpenIDAuthenticator(ctx, env, nil /*oauthProviders=*/)
	if err != nil {
		return nil, err
	}
	oia.authenticators = append(oia.authenticators, testAuthenticator)
	return oia, nil
}

func NewOpenIDAuthenticator(ctx context.Context, env environment.Env) (*OpenIDAuthenticator, error) {
	authConfigs := env.GetConfigurator().GetAuthOauthProviders()
	if len(authConfigs) == 0 {
		return nil, status.FailedPreconditionErrorf("No auth providers specified in config!")
	}

	return newOpenIDAuthenticator(ctx, env, authConfigs)
}

func sameHostname(urlStringA, urlStringB string) bool {
	if urlA, err := url.Parse(urlStringA); err == nil {
		if urlB, err := url.Parse(urlStringB); err == nil {
			return urlA.Hostname() == urlB.Hostname()
		}
	}
	return false
}

func (a *OpenIDAuthenticator) validateRedirectURL(redirectURL string) error {
	if a.myURL.Host == "" {
		return status.FailedPreconditionError("You must specify a build_buddy_url in your config to enable authentication. For more information, see: https://www.buildbuddy.io/docs/config-app")
	}

	if !sameHostname(redirectURL, a.myURL.String()) {
		return status.FailedPreconditionErrorf("Redirect url %q was not on this domain %q!", redirectURL, a.myURL.Host)
	}
	return nil
}

func (a *OpenIDAuthenticator) getAuthConfig(issuer string) authenticator {
	for _, a := range a.authenticators {
		if sameHostname(a.getIssuer(), issuer) {
			return a
		}
	}
	return nil
}

func (a *OpenIDAuthenticator) lookupUserFromSubID(ctx context.Context, subID string) (*tables.User, error) {
	dbHandle := a.env.GetDBHandle()
	if dbHandle == nil {
		return nil, status.FailedPreconditionErrorf("No handle to query database")
	}
	user := &tables.User{}
	err := dbHandle.TransactionWithOptions(ctx, db.StaleReadOptions(), func(tx *db.DB) error {
		userRow := tx.Raw(`SELECT * FROM Users WHERE sub_id = ? ORDER BY user_id ASC`, subID)
		if err := userRow.Take(user).Error; err != nil {
			return err
		}
		groupRows, err := tx.Raw(`SELECT g.* FROM `+"`Groups`"+` as g JOIN UserGroups as ug
                                          ON g.group_id = ug.group_group_id
                                          WHERE ug.user_user_id = ?`, user.UserID).Rows()
		if err != nil {
			return err
		}
		defer groupRows.Close()
		for groupRows.Next() {
			g := &tables.Group{}
			if err := tx.ScanRows(groupRows, g); err != nil {
				return err
			}
			user.Groups = append(user.Groups, g)
		}
		return nil
	})
	return user, err
}

func (a *OpenIDAuthenticator) lookupAPIKeyGroupFromAPIKey(ctx context.Context, apiKey string) (interfaces.APIKeyGroup, error) {
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

func userClaims(u *tables.User) *Claims {
	var allowedGroups []string
	for _, g := range u.Groups {
		allowedGroups = append(allowedGroups, g.GroupID)
	}
	return &Claims{
		UserID:        u.UserID,
		AllowedGroups: allowedGroups,
	}
}

func groupClaims(akg interfaces.APIKeyGroup) *Claims {
	return &Claims{
		GroupID:                akg.GetGroupID(),
		AllowedGroups:          []string{akg.GetGroupID()},
		Capabilities:           capabilities.FromInt(akg.GetCapabilities()),
		UseGroupOwnedExecutors: akg.GetUseGroupOwnedExecutors(),
	}
}

func authContextWithError(ctx context.Context, err error) context.Context {
	return context.WithValue(ctx, contextUserErrorKey, err)
}

func authContextFromClaims(ctx context.Context, claims *Claims, err error) context.Context {
	if err != nil {
		return authContextWithError(ctx, err)
	}
	tokenString, err := assembleJWT(ctx, claims)
	if err != nil {
		return authContextWithError(ctx, err)
	}
	ctx = context.WithValue(ctx, contextTokenStringKey, tokenString)
	// Note: we clear the error here in case it was set initially by the
	// authentication handler, but then we want to re-authenticate later on in the
	// request lifecycle, and authentication is successful.
	// Specifically, we do this when we see the API key in the "BuildStarted" event.
	ctx = context.WithValue(ctx, contextUserErrorKey, nil)
	return ctx
}

func (a *OpenIDAuthenticator) ParseAPIKeyFromString(input string) string {
	matches := apiKeyRegex.FindStringSubmatch(input)
	if matches != nil && len(matches) > 1 {
		return matches[1]
	}
	return ""
}

func (a *OpenIDAuthenticator) AuthContextFromAPIKey(ctx context.Context, apiKey string) context.Context {
	claims, err := a.claimsFromAPIKey(ctx, apiKey)
	return authContextFromClaims(ctx, claims, err)
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

func (a *OpenIDAuthenticator) claimsFromSubID(ctx context.Context, subID string) (*Claims, error) {
	u, err := a.lookupUserFromSubID(ctx, subID)
	if err != nil {
		return nil, err
	}
	return userClaims(u), nil
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
		if keys := md.Get(APIKeyHeader); len(keys) > 0 {
			return a.claimsFromAPIKey(ctx, keys[0])
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
		return authContextWithError(ctx, err)
	}
	err = a.authenticateGroup(ctx, claims)
	if err != nil {
		return authContextWithError(ctx, err)
	}
	return authContextFromClaims(ctx, claims, err)
}

func (a *OpenIDAuthenticator) authenticateGroup(ctx context.Context, claims *Claims) error {
	reqCtx := requestcontext.ProtoRequestContextFromContext(ctx)
	// If no group ID was provided in context then we'll fall back to a default.
	if reqCtx == nil || reqCtx.GetGroupId() == "" {
		return nil
	}
	groupID := reqCtx.GetGroupId()
	for _, allowedGroupID := range claims.GetAllowedGroups() {
		if groupID == allowedGroupID {
			return nil
		}
	}
	return status.PermissionDeniedError("User does not have access to the requested group.")
}

func (a *OpenIDAuthenticator) authenticateUser(w http.ResponseWriter, r *http.Request) (*Claims, *userToken, error) {
	ctx := r.Context()
	if apiKey := r.Header.Get(APIKeyHeader); apiKey != "" {
		claims, err := a.claimsFromAPIKey(ctx, apiKey)
		return claims, nil, err
	}

	jwt := getCookie(r, jwtCookie)
	if jwt == "" {
		return nil, nil, status.PermissionDeniedErrorf("No jwt set")
	}
	issuer := getCookie(r, authIssuerCookie)
	auth := a.getAuthConfig(issuer)
	if auth == nil {
		return nil, nil, status.PermissionDeniedErrorf("No config found for issuer: %s", issuer)
	}

	ut, err := auth.verifyTokenAndExtractUser(ctx, jwt /*checkExpiry=*/, false)
	if err != nil {
		return nil, nil, err
	}

	// Now try to verify the token again -- this time we check for expiry.
	// If it succeeds, we're done! Otherwise we fall through to refreshing
	// the token below.
	if ut, err := auth.verifyTokenAndExtractUser(ctx, jwt /*checkExpiry=*/, true); err == nil {
		claims, err := a.claimsFromSubID(ctx, ut.GetSubID())
		return claims, ut, err
	}

	// Now attempt to refresh the token.
	if authDB := a.env.GetAuthDB(); authDB != nil {
		tt, err := authDB.ReadToken(ctx, ut.GetSubID())
		if err != nil {
			return nil, nil, err
		}
		newToken, err := auth.renewToken(ctx, tt.RefreshToken)
		if err != nil {
			return nil, nil, err
		}
		if err := authDB.InsertOrUpdateUserToken(ctx, ut.GetSubID(), tt); err != nil {
			return nil, nil, err
		}
		if jwt, ok := newToken.Extra("id_token").(string); ok {
			setLoginCookie(w, jwt, issuer)
			claims, err := a.claimsFromSubID(ctx, ut.GetSubID())
			return claims, ut, err
		}
	}
	return nil, nil, status.PermissionDeniedError("could not refresh token")
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
	// NB: DO NOT CHANGE THIS ERROR MESSAGE. The client app matches it in
	// order to trigger the CreateUser flow.
	return nil, status.PermissionDeniedError("User not found")
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
		return status.FailedPreconditionErrorf("No user token available to fill user")
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
	return nil
}

func (a *OpenIDAuthenticator) Login(w http.ResponseWriter, r *http.Request) {
	issuer := r.URL.Query().Get(authIssuerParam)
	auth := a.getAuthConfig(issuer)
	if auth == nil {
		err := status.PermissionDeniedErrorf("No config found for issuer: %s", issuer)
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	// Set the "state" cookie which will be returned to us by tha authentication
	// provider in the URL. We verify that it matches.
	state := fmt.Sprintf("%d", random.RandUint64())
	setCookie(w, stateCookie, state, time.Now().Add(tempCookieDuration))

	redirectURL := r.URL.Query().Get(authRedirectParam)
	if err := a.validateRedirectURL(redirectURL); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	// Set the redirection URL in a cookie so we can use it after validating
	// the user in our /auth callback.
	setCookie(w, redirCookie, redirectURL, time.Now().Add(tempCookieDuration))

	// Set the issuer cookie so we remember which issuer to use when exchanging
	// a token later in our /auth callback.
	setCookie(w, authIssuerCookie, issuer, time.Now().Add(tempCookieDuration))

	// Redirect to the login provider (and ask for a refresh token).
	u := auth.authCodeURL(state, authCodeOption...)
	http.Redirect(w, r, u, http.StatusTemporaryRedirect)
}

func (a *OpenIDAuthenticator) Logout(w http.ResponseWriter, r *http.Request) {
	clearLoginCookie(w)

	redirURL := r.URL.Query().Get(authRedirectParam)
	if redirURL == "" {
		redirURL = "/" // default to redirecting home.
	}
	http.Redirect(w, r, redirURL, http.StatusTemporaryRedirect)
}

func (a *OpenIDAuthenticator) Auth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Verify "state" cookie match.
	if r.FormValue("state") != getCookie(r, stateCookie) {
		log.Printf("state mismatch: %s != %s", r.FormValue("state"), getCookie(r, stateCookie))
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	authError := r.URL.Query().Get("error")
	if authError != "" {
		authErrorDesc := r.URL.Query().Get("error_desc")
		authErrorDescription := r.URL.Query().Get("error_description")
		log.Warningf("Authenticator returned error: %s (%s %s)", authError, authErrorDesc, authErrorDescription)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	// Lookup issuer from the cookie we set in /login.
	issuer := getCookie(r, authIssuerCookie)
	auth := a.getAuthConfig(issuer)
	if auth == nil {
		err := status.PermissionDeniedErrorf("No config found for issuer: %s", issuer)
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	code := r.URL.Query().Get("code")
	oauth2Token, err := auth.exchange(ctx, code, authCodeOption...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	// Extract the ID Token (JWT) from OAuth2 token.
	jwt, ok := oauth2Token.Extra("id_token").(string)
	if !ok {
		err := status.PermissionDeniedErrorf("ID Token not present in auth response")
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	ut, err := auth.verifyTokenAndExtractUser(ctx, jwt /*checkExpiry=*/, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return

	}

	// OK, the token is valid so we will: store the refresh token in our DB
	// for later & set the login cookie so we know this user is logged in.
	setLoginCookie(w, jwt, issuer)

	refreshToken, ok := oauth2Token.Extra("refresh_token").(string)
	if ok {
		tt := &tables.Token{
			SubID:        ut.GetSubID(),
			AccessToken:  oauth2Token.AccessToken,
			RefreshToken: refreshToken,
			ExpiryUsec:   oauth2Token.Expiry.UnixNano() / 1000,
		}
		if authDB := a.env.GetAuthDB(); authDB != nil {
			if err := authDB.InsertOrUpdateUserToken(ctx, ut.GetSubID(), tt); err != nil {
				// If this write fails then we are unable to silently refresh
				// the user's token and they will need to login again in an hour.
				log.Printf("Failed to save refresh token: %s", err)
			}
		}
	}

	redirURL := getCookie(r, redirCookie)
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
	return nil, status.PermissionDeniedError("User not found")
}

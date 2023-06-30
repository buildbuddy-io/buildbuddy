package claims

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang-jwt/jwt"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

const (
	// Maximum number of entries in JWT -> Claims cache.
	claimsCacheSize = 10_00

	// BuildBuddy JWT duration maximum.
	defaultBuildBuddyJWTDuration = 6 * time.Hour

	// The key the Claims are stored under in the context.
	// If unset, the JWT can be used to reconstitute the claims.
	contextClaimsKey = "auth.claims"
)

var (
	jwtKey = flagutil.New("auth.jwt_key", "set_the_jwt_in_config", "The key to use when signing JWT tokens.", flagutil.SecretTag)
)

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
	CacheEncryptionEnabled bool                          `json:"cache_encryption_enabled,omitempty"`
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

func (c *Claims) GetCapabilities() []akpb.ApiKey_Capability {
	return c.Capabilities
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
		if cap&cc > 0 {
			return true
		}
	}
	return false
}

func (c *Claims) GetUseGroupOwnedExecutors() bool {
	return c.UseGroupOwnedExecutors
}

func (c *Claims) GetCacheEncryptionEnabled() bool {
	return c.CacheEncryptionEnabled
}

func ParseClaims(token string) (*Claims, error) {
	claims := &Claims{}
	_, err := jwt.ParseWithClaims(token, claims, jwtKeyFunc)
	if err != nil {
		return nil, err
	}
	return claims, nil
}

func APIKeyGroupClaims(akg interfaces.APIKeyGroup) *Claims {
	return &Claims{
		UserID:        akg.GetUserID(),
		GroupID:       akg.GetGroupID(),
		AllowedGroups: []string{akg.GetGroupID()},
		// For now, API keys are assigned the default role.
		GroupMemberships: []*interfaces.GroupMembership{
			{GroupID: akg.GetGroupID(), Role: role.Default},
		},
		Capabilities:           capabilities.FromInt(akg.GetCapabilities()),
		UseGroupOwnedExecutors: akg.GetUseGroupOwnedExecutors(),
		CacheEncryptionEnabled: akg.GetCacheEncryptionEnabled(),
	}
}

func ClaimsFromSubID(ctx context.Context, env environment.Env, subID string) (*Claims, error) {
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
		for _, membership := range claims.GetGroupMemberships() {
			if membership.GroupID != env.GetAuthenticator().AdminGroupID() || membership.Role != role.Admin {
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

func assembleJWT(ctx context.Context, c *Claims) (string, error) {
	expirationTime := time.Now().Add(defaultBuildBuddyJWTDuration)
	expiresAt := expirationTime.Unix()
	// Round expiration times down to the nearest minute to improve stability
	// of JWTs for caching purposes.
	expiresAt -= (expiresAt % 60)
	c.StandardClaims = jwt.StandardClaims{ExpiresAt: expiresAt}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
	tokenString, err := token.SignedString([]byte(*jwtKey))
	return tokenString, err
}

func AuthContextFromClaims(ctx context.Context, c *Claims, err error) context.Context {
	if err != nil {
		return authutil.AuthContextWithError(ctx, err)
	}
	tokenString, err := assembleJWT(ctx, c)
	if err != nil {
		return authutil.AuthContextWithError(ctx, err)
	}
	ctx = context.WithValue(ctx, authutil.ContextTokenStringKey, tokenString)
	ctx = context.WithValue(ctx, contextClaimsKey, c)
	// Note: we clear the error here in case it was set initially by the
	// authentication handler, but then we want to re-authenticate later on in the
	// request lifecycle, and authentication is successful.
	// Specifically, we do this when we see the API key in the "BuildStarted" event.
	return authutil.AuthContextWithError(ctx, nil)
}

func ClaimsFromContext(ctx context.Context) (*Claims, error) {
	// If the context already contains trusted Claims, return them directly
	// instead of re-parsing the JWT (which is expensive).
	if claims, ok := ctx.Value(contextClaimsKey).(*Claims); ok && claims != nil {
		return claims, nil
	}

	// If context already contains a JWT, just verify it and return the claims.
	if tokenString, ok := ctx.Value(authutil.ContextTokenStringKey).(string); ok && tokenString != "" {
		claims, err := ParseClaims(tokenString)
		if err != nil {
			return nil, err
		}
		return claims, nil
	}

	// If there's no error or we have an assertion failure; just return a
	// user not found error.
	err, ok := authutil.AuthErrorFromContext(ctx)
	if !ok || err == nil {
		return nil, authutil.AnonymousUserError(authutil.UserNotFoundMsg)
	}

	// if there was an error set on the context, and it was an
	// Unauthenticated or PermissionDeniedError, then the FE can handle it,
	// so pass it through. This includes anonymous user errors.
	if status.IsUnauthenticatedError(err) || status.IsPermissionDeniedError(err) {
		return nil, err
	}

	// All other types of errors will be converted into Unauthenticated
	// errors.
	// WARNING: app/auth/auth_service.ts depends on this status being UNAUTHENTICATED.
	return nil, status.UnauthenticatedErrorf("%s: %s", authutil.UserNotFoundMsg, err.Error())
}

// ClaimsCache helps reduce CPU overhead due to JWT parsing by caching parsed
// and verified JWT claims.
//
// The JWTs used with this cache should have Expiration times rounded down to
// the nearest minute, so that their cache key doesn't change as often and can
// therefore be cached for longer.
type ClaimsCache struct {
	ttl time.Duration

	mu  sync.Mutex
	lru interfaces.LRU
}

func NewClaimsCache(ctx context.Context, ttl time.Duration) (*ClaimsCache, error) {
	config := &lru.Config{
		MaxSize: claimsCacheSize,
		SizeFn:  func(v interface{}) int64 { return 1 },
	}
	lru, err := lru.NewLRU(config)
	if err != nil {
		return nil, err
	}
	return &ClaimsCache{ttl: ttl, lru: lru}, nil
}

func (c *ClaimsCache) Get(token string) (*Claims, error) {
	c.mu.Lock()
	v, ok := c.lru.Get(token)
	c.mu.Unlock()

	if ok {
		if claims := v.(*Claims); claims.ExpiresAt > time.Now().Unix() {
			return claims, nil
		}
	}

	claims, err := ParseClaims(token)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.lru.Add(token, claims)
	c.mu.Unlock()

	return claims, nil
}

func jwtKeyFunc(token *jwt.Token) (interface{}, error) {
	return []byte(*jwtKey), nil
}

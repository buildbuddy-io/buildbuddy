package claims

import (
	"context"
	"crypto/rsa"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/golang-jwt/jwt/v4"
	"google.golang.org/grpc/metadata"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

const (
	// Maximum number of entries in JWT -> Claims cache.
	claimsCacheSize = 1_000

	// The key the Claims are stored under in the context.
	// If unset, the JWT can be used to reconstitute the claims.
	contextClaimsKey = "auth.claims"
)

var (
	jwtKey             = flag.String("auth.jwt_key", "set_the_jwt_in_config", "The key to use when signing JWT tokens.", flag.Secret)
	newJwtKey          = flag.String("auth.new_jwt_key", "", "If set, JWT verifications will try both this and the old JWT key.", flag.Secret)
	signUsingNewJwtKey = flag.Bool("auth.sign_using_new_jwt_key", false, "If true, new JWTs will be signed using the new JWT key.")
	claimsCacheTTL     = flag.Duration("auth.jwt_claims_cache_ttl", 15*time.Second, "TTL for JWT string to parsed claims caching. Set to '0' to disable cache.")
	jwtDuration        = flag.Duration("auth.jwt_duration", 6*time.Hour, "Maximum lifetime of the generated JWT.")

	jwtRSAPublicKey     = flag.String("auth.jwt_rsa_public_key", "", "PEM-encoded RSA public key used to verify JWTs.", flag.Secret)
	jwtRSAPrivateKey    = flag.String("auth.jwt_rsa_private_key", "", "PEM-encoded RSA private key used to sign JWTs.", flag.Secret)
	newJWTRSAPublicKey  = flag.String("auth.new_jwt_rsa_public_key", "", "PEM-encoded RSA public key used to verify new JWTs during key rotation.", flag.Secret)
	newJWTRSAPrivateKey = flag.String("auth.new_jwt_rsa_private_key", "", "PEM-encoded RSA private key used to sign new JWTs during key rotation.", flag.Secret)

	serverAdminGroupID = flag.String("auth.admin_group_id", "", "ID of a group whose members can perform actions only accessible to server admins.")
)

var (
	// Generic permission denied error.
	errPermissionDenied = status.PermissionDeniedError("permission denied")
)

type Claims struct {
	jwt.StandardClaims
	APIKeyID                   string `json:"api_key_id,omitempty"`
	UserID                     string `json:"user_id"`
	GroupID                    string `json:"group_id"`
	ExperimentTargetingGroupID string `json:"experiment_targeting_group_id"`
	// APIKeyOwnerGroupID identifies the group that owns the API key used
	// for authentication. Will be empty if authentication was not performed
	// using an API key.
	// The GroupID field above should be used in most cases!
	APIKeyOwnerGroupID string `json:"api_key_owner_group_id,omitempty"`
	Impersonating      bool   `json:"impersonating"`
	// TODO(bduffany): remove this field
	AllowedGroups          []string                      `json:"allowed_groups"`
	GroupMemberships       []*interfaces.GroupMembership `json:"group_memberships"`
	Capabilities           []cappb.Capability            `json:"capabilities"`
	UseGroupOwnedExecutors bool                          `json:"use_group_owned_executors,omitempty"`
	CacheEncryptionEnabled bool                          `json:"cache_encryption_enabled,omitempty"`
	EnforceIPRules         bool                          `json:"enforce_ip_rules,omitempty"`
	GroupStatus            grpb.Group_GroupStatus        `json:"group_status,omitempty"`
	// TODO(vadim): remove this field
	SAML        bool `json:"saml,omitempty"`
	CustomerSSO bool `json:"customer_sso,omitempty"`
}

func (c *Claims) GetAPIKeyInfo() interfaces.APIKeyInfo {
	return interfaces.APIKeyInfo{
		ID: c.APIKeyID, OwnerGroupID: c.APIKeyOwnerGroupID,
	}
}

func (c *Claims) GetUserID() string {
	return c.UserID
}

func (c *Claims) GetGroupID() string {
	return c.GroupID
}

func (c *Claims) GetExperimentTargetingGroupID() string {
	if c.ExperimentTargetingGroupID == "" {
		return c.GroupID
	}
	return c.ExperimentTargetingGroupID
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

func (c *Claims) GetCapabilities() []cappb.Capability {
	return c.Capabilities
}

func (c *Claims) HasCapability(cap cappb.Capability) bool {
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

func (c *Claims) GetEnforceIPRules() bool {
	return c.EnforceIPRules
}

func (c *Claims) GetGroupStatus() grpb.Group_GroupStatus {
	return grpb.Group_GroupStatus(c.GroupStatus)
}

func (c *Claims) IsSAML() bool {
	return c.SAML
}

func (c *Claims) IsCustomerSSO() bool {
	return c.SAML || c.CustomerSSO
}

func ParseClaims(token string) (*Claims, error) {
	keys := []string{*jwtKey}
	if *newJwtKey != "" {
		// Try the new key first.
		keys = []string{*newJwtKey, *jwtKey}
	}

	var lastErr error
	claims := &Claims{}
	for _, key := range keys {
		_, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
			return []byte(key), nil
		})
		if err == nil {
			return claims, nil
		}
		lastErr = err

		var validationErr *jwt.ValidationError
		if errors.As(err, &validationErr) && validationErr.Errors&jwt.ValidationErrorSignatureInvalid != 0 {
			continue
		}
		return nil, err
	}
	return nil, lastErr
}

func APIKeyGroupClaims(ctx context.Context, akg interfaces.APIKeyGroup) (*Claims, error) {
	allowedGroups := []string{akg.GetGroupID()}
	groupMemberships := []*interfaces.GroupMembership{{
		GroupID:      akg.GetGroupID(),
		Capabilities: capabilities.FromInt(akg.GetCapabilities()),
	}}
	for _, cg := range akg.GetChildGroupIDs() {
		allowedGroups = append(allowedGroups, cg)
		groupMemberships = append(groupMemberships, &interfaces.GroupMembership{
			GroupID:      cg,
			Capabilities: capabilities.FromInt(akg.GetCapabilities()),
		})
	}

	requestContext := requestcontext.ProtoRequestContextFromContext(ctx)
	effectiveGroup := akg.GetGroupID()
	if requestContext.GetGroupId() != "" {
		if slices.Contains(allowedGroups, requestContext.GetGroupId()) {
			effectiveGroup = requestContext.GetGroupId()
		} else {
			return nil, status.PermissionDeniedErrorf("invalid group id %s", requestContext.GetGroupId())
		}
	}

	experimentTargetingGroup := ""
	if v := metadata.ValueFromIncomingContext(ctx, "x-buildbuddy-experiment.group_id"); len(v) > 0 {
		// Only server admins can set this header. Note: can't use
		// AuthorizeServerAdmin directly, since the claims aren't in the ctx yet
		// (this function is building the claims).
		if !isAdminOfServerAdminGroup(groupMemberships) {
			return nil, errPermissionDenied
		}
		experimentTargetingGroup = v[len(v)-1]
	}

	return &Claims{
		APIKeyID:                   akg.GetAPIKeyID(),
		UserID:                     akg.GetUserID(),
		GroupID:                    effectiveGroup,
		ExperimentTargetingGroupID: experimentTargetingGroup,
		APIKeyOwnerGroupID:         akg.GetGroupID(),
		AllowedGroups:              allowedGroups,
		GroupMemberships:           groupMemberships,
		Capabilities:               capabilities.FromInt(akg.GetCapabilities()),
		UseGroupOwnedExecutors:     akg.GetUseGroupOwnedExecutors(),
		CacheEncryptionEnabled:     akg.GetCacheEncryptionEnabled(),
		EnforceIPRules:             akg.GetEnforceIPRules(),
		GroupStatus:                akg.GetGroupStatus(),
	}, nil
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

	requestContext := requestcontext.ProtoRequestContextFromContext(ctx)
	// TODO(https://github.com/buildbuddy-io/buildbuddy-internal/issues/4191):
	// return an error here once we have a better understanding of why the
	// request context can be missing.
	if requestContext == nil {
		log.CtxInfof(ctx, "Request is missing request context")
	} else if requestContext.GetGroupId() == "" {
		log.CtxInfof(ctx, "Request context group ID is empty")
	}

	eg := ""
	if requestContext.GetGroupId() != "" {
		for _, g := range u.Groups {
			if g.Group.GroupID == requestContext.GetGroupId() {
				eg = requestContext.GetGroupId()
			}
		}
	}

	claims, err := userClaims(u, eg)
	if err != nil {
		return nil, err
	}

	// If the user is trying to impersonate a member of another org and has Admin
	// role within the configured admin group, set their authenticated user to
	// *only* have access to the org being impersonated.
	if requestContext.GetImpersonatingGroupId() != "" {
		for _, membership := range claims.GetGroupMemberships() {
			if membership.GroupID != ServerAdminGroupID() || !slices.Contains(membership.Capabilities, cappb.Capability_ORG_ADMIN) {
				continue
			}

			ig, err := env.GetUserDB().GetGroupByID(ctx, requestContext.GetImpersonatingGroupId())
			if err != nil {
				return nil, err
			}

			// If the user requested impersonation but the subdomain doesn't
			// match the impersonation target then don't enable impersonation.
			if sd := subdomain.Get(ctx); sd != "" && sd != ig.URLIdentifier {
				return claims, nil
			}

			u.Groups = []*tables.GroupRole{{
				Group:        *ig,
				Capabilities: role.AdminCapabilities,
			}}
			claims, err := userClaims(u, requestContext.GetImpersonatingGroupId())
			if err != nil {
				return nil, err
			}
			claims.Impersonating = true
			return claims, nil
		}
		return nil, status.PermissionDeniedError("You do not have permissions to impersonate group members.")
	}

	return claims, nil
}

func userClaims(u *tables.User, effectiveGroup string) (*Claims, error) {
	allowedGroups := make([]string, 0, len(u.Groups))
	groupMemberships := make([]*interfaces.GroupMembership, 0, len(u.Groups))
	cacheEncryptionEnabled := false
	enforceIPRules := false
	groupStatus := grpb.Group_UNKNOWN_GROUP_STATUS
	var capabilities []cappb.Capability
	for _, g := range u.Groups {
		allowedGroups = append(allowedGroups, g.Group.GroupID)
		groupMemberships = append(groupMemberships, &interfaces.GroupMembership{
			GroupID:      g.Group.GroupID,
			Capabilities: g.Capabilities,
		})
		if g.Group.GroupID == effectiveGroup {
			// TODO: move these fields into u.GroupMemberships
			cacheEncryptionEnabled = g.Group.CacheEncryptionEnabled
			enforceIPRules = g.Group.EnforceIPRules
			groupStatus = g.Group.Status
			capabilities = g.Capabilities
		}
	}
	return &Claims{
		UserID:                 u.UserID,
		GroupID:                effectiveGroup,
		GroupMemberships:       groupMemberships,
		AllowedGroups:          allowedGroups,
		Capabilities:           capabilities,
		CacheEncryptionEnabled: cacheEncryptionEnabled,
		EnforceIPRules:         enforceIPRules,
		GroupStatus:            groupStatus,
	}, nil
}

func AssembleJWT(c *Claims, method jwt.SigningMethod) (string, error) {
	expirationTime := time.Now().Add(*jwtDuration)
	expiresAt := expirationTime.Unix()
	// Round expiration times down to the nearest minute to improve stability
	// of JWTs for caching purposes.
	expiresAt -= (expiresAt % 60)
	c.StandardClaims = jwt.StandardClaims{ExpiresAt: expiresAt}
	token := jwt.NewWithClaims(method, c)
	if method == jwt.SigningMethodHS256 {
		return assembleHS256JWT(token)
	} else if method == jwt.SigningMethodRS256 {
		return assembleRS256JWT(token)
	}
	return "", status.InternalError("Unsupported JWT signing method")
}

func assembleHS256JWT(token *jwt.Token) (string, error) {
	key := *jwtKey
	if *newJwtKey != "" && *signUsingNewJwtKey {
		key = *newJwtKey
	}
	return token.SignedString([]byte(key))
}

var (
	rsaPrivateKeyOnce sync.Once
	rsaPrivateKey     *rsa.PrivateKey
	rsaPrivateKeyErr  error

	newRSAPrivateKeyOnce sync.Once
	newRSAPrivateKey     *rsa.PrivateKey
	newRSAPrivateKeyErr  error
)

func getRSAPrivateKey() (*rsa.PrivateKey, error) {
	if *newJWTRSAPrivateKey != "" {
		newRSAPrivateKeyOnce.Do(func() {
			newRSAPrivateKey, newRSAPrivateKeyErr = jwt.ParseRSAPrivateKeyFromPEM([]byte(*newJWTRSAPrivateKey))
		})
		return newRSAPrivateKey, newRSAPrivateKeyErr
	}

	rsaPrivateKeyOnce.Do(func() {
		rsaPrivateKey, rsaPrivateKeyErr = jwt.ParseRSAPrivateKeyFromPEM([]byte(*jwtRSAPrivateKey))
	})
	return rsaPrivateKey, rsaPrivateKeyErr
}

func assembleRS256JWT(token *jwt.Token) (string, error) {
	privateKey, err := getRSAPrivateKey()
	if err != nil {
		return "", err
	}
	return token.SignedString(privateKey)
}

// Returns a context containing auth state for the provided Claims and auth
// error. Note that this function assembles a JWT out of the provided Claims
// and sets that in the context as well, so it should only be used in cases
// where that is necessary.
func AuthContextWithJWT(ctx context.Context, c *Claims, err error) context.Context {
	if err != nil {
		return authutil.AuthContextWithError(ctx, err)
	}
	tokenString, err := AssembleJWT(c, jwt.SigningMethodHS256)
	if err != nil {
		return authutil.AuthContextWithError(ctx, err)
	}
	ctx = context.WithValue(ctx, authutil.ContextTokenStringKey, tokenString)
	return AuthContext(ctx, c)
}

// Returns a Context containing auth state for the the provided Claims.
func AuthContext(ctx context.Context, c *Claims) context.Context {
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

// AuthorizeServerAdmin checks whether the authenticated user is a server admin
// (a member of the server admin group, with ORG_ADMIN capability).
func AuthorizeServerAdmin(ctx context.Context) error {
	u, err := ClaimsFromContext(ctx)
	if err != nil {
		return err
	}

	// If impersonation is in effect, it implies the user is an admin.
	// Can't check group membership because impersonation modifies
	// group information.
	if u.IsImpersonating() {
		return nil
	}

	if isAdminOfServerAdminGroup(u.GetGroupMemberships()) {
		return nil
	}

	return errPermissionDenied
}

func isAdminOfServerAdminGroup(memberships []*interfaces.GroupMembership) bool {
	serverAdminGID := *serverAdminGroupID
	if serverAdminGID == "" {
		return false
	}
	for _, m := range memberships {
		if m.GroupID == serverAdminGID && slices.Contains(m.Capabilities, cappb.Capability_ORG_ADMIN) {
			return true
		}
	}
	return false
}

// ServerAdminGroupID returns the ID of the server admin group.
// For auth checks, prefer using AuthorizeServerAdmin instead.
func ServerAdminGroupID() string {
	return *serverAdminGroupID
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
	lru interfaces.LRU[*Claims]
}

// Returns a ClaimsCache if the claims cache is enabled, or nil otherwise, or
// an error if there's an error constructing the cache.
//
// Note: this function can return (nil, nil)!
func NewClaimsCache() (*ClaimsCache, error) {
	if *claimsCacheTTL <= 0 {
		return nil, nil
	}

	config := &lru.Config[*Claims]{
		MaxSize: claimsCacheSize,
		SizeFn:  func(v *Claims) int64 { return 1 },
	}
	lru, err := lru.NewLRU[*Claims](config)
	if err != nil {
		return nil, err
	}
	return &ClaimsCache{ttl: *claimsCacheTTL, lru: lru}, nil
}

func (c *ClaimsCache) Get(token string) (*Claims, error) {
	c.mu.Lock()
	v, ok := c.lru.Get(token)
	c.mu.Unlock()

	if ok {
		if claims := v; claims.ExpiresAt > time.Now().Unix() {
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

func GetRSAPublicKeys() []string {
	var keys []string
	if *newJWTRSAPublicKey != "" {
		keys = append(keys, *newJWTRSAPublicKey)
	}
	if *jwtRSAPublicKey != "" {
		keys = append(keys, *jwtRSAPublicKey)
	}
	return keys
}

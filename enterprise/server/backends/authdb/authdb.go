package authdb

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/crypto/chacha20"
	"gorm.io/gorm/clause"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

const (
	apiKeyLength = 20

	// For encrypted keys, the first N characters serve as the nonce.
	// This cannot be changed without a migration.
	apiKeyNonceLength = 6

	apiKeyEncryptionBackfillBatchSize = 100

	impersonationAPIKeyDuration = 1 * time.Hour
)

const (
	// Maximum number of entries in API Key -> Group cache.
	apiKeyGroupCacheSize = 10_000
)

var (
	userOwnedKeysEnabled = flag.Bool("app.user_owned_keys_enabled", false, "If true, enable user-owned API keys.")
	apiKeyGroupCacheTTL  = flag.Duration("auth.api_key_group_cache_ttl", 5*time.Minute, "TTL for API Key to Group caching. Set to '0' to disable cache.")
	apiKeyEncryptionKey  = flag.String("auth.api_key_encryption.key", "", "Base64-encoded 256-bit encryption key for API keys.", flag.Secret)
	encryptNewKeys       = flag.Bool("auth.api_key_encryption.encrypt_new_keys", false, "If enabled, all new API keys will be written in an encrypted format.")
	encryptOldKeys       = flag.Bool("auth.api_key_encryption.encrypt_old_keys", false, "If enabled, all existing unencrypted keys will be encrypted on startup. The unencrypted keys will remain in the database and will need to be cleared manually after verifying the success of the migration.")
)

type apiKeyGroupCacheEntry struct {
	data         interfaces.APIKeyGroup
	expiresAfter time.Time
}

// apiKeyGroupCache is a cache for API Key -> Group lookups. A single Bazel
// invocation can generate large bursts of RPCs, each of which needs to be
// authed.
// There's no need to go to the database for every single request as this data
// rarely changes.
type apiKeyGroupCache struct {
	// Note that even though we base this off an LRU cache, every entry has a
	// hard expiration time to force a refresh of the underlying data.
	lru interfaces.LRU[*apiKeyGroupCacheEntry]
	ttl time.Duration
	mu  sync.Mutex
}

func newAPIKeyGroupCache() (*apiKeyGroupCache, error) {
	config := &lru.Config[*apiKeyGroupCacheEntry]{
		MaxSize: apiKeyGroupCacheSize,
		SizeFn:  func(v *apiKeyGroupCacheEntry) int64 { return 1 },
	}
	lru, err := lru.NewLRU[*apiKeyGroupCacheEntry](config)
	if err != nil {
		return nil, status.InternalErrorf("error initializing API Key -> Group cache: %v", err)
	}
	return &apiKeyGroupCache{lru: lru, ttl: *apiKeyGroupCacheTTL}, nil
}

func (c *apiKeyGroupCache) Get(apiKey string) (akg interfaces.APIKeyGroup, ok bool) {
	c.mu.Lock()
	entry, ok := c.lru.Get(apiKey)
	c.mu.Unlock()
	if !ok {
		return nil, ok
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

type AuthDB struct {
	env environment.Env
	h   interfaces.DBHandle

	apiKeyGroupCache *apiKeyGroupCache

	// Nil if API key encryption is not enabled.
	apiKeyEncryptionKey []byte
}

func NewAuthDB(env environment.Env, h interfaces.DBHandle) (interfaces.AuthDB, error) {
	adb := &AuthDB{env: env, h: h}
	if *apiKeyGroupCacheTTL != 0 {
		akgCache, err := newAPIKeyGroupCache()
		if err != nil {
			return nil, err
		}
		adb.apiKeyGroupCache = akgCache
	}
	if *apiKeyEncryptionKey != "" {
		key, err := base64.StdEncoding.DecodeString(*apiKeyEncryptionKey)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("could not decode API Key encryption key: %s", err)
		}
		if len(key) != chacha20.KeySize {
			return nil, status.InvalidArgumentError("API Key encryption key does not have expected length")
		}
		adb.apiKeyEncryptionKey = key
		if err := adb.backfillUnencryptedKeys(); err != nil {
			return nil, err
		}
	}
	return adb, nil
}

func (d *AuthDB) backfillUnencryptedKeys() error {
	if d.apiKeyEncryptionKey == nil {
		return nil
	}
	if !*encryptOldKeys {
		return nil
	}

	ctx := d.env.GetServerContext()
	dbh := d.env.GetDBHandle()

	for {
		query := fmt.Sprintf(`SELECT * FROM "APIKeys" WHERE encrypted_value = '' LIMIT %d`, apiKeyEncryptionBackfillBatchSize)
		rq := dbh.NewQuery(ctx, "authdb_select_keys_to_backfill").Raw(query)
		keysToUpdate, err := db.ScanAll(rq, &tables.APIKey{})
		if err != nil {
			return err
		}

		if len(keysToUpdate) == 0 {
			break
		}

		rowsUpdated := 0
		for _, apk := range keysToUpdate {
			encrypted, err := d.encryptAPIKey(apk.Value)
			if err != nil {
				return err
			}
			if err := dbh.NewQuery(ctx, "authdb_backfill_encrypted_value").Raw(`
				UPDATE "APIKeys"
				SET encrypted_value = ?
				WHERE api_key_id = ?`,
				encrypted, apk.APIKeyID,
			).Exec().Error; err != nil {
				return err
			}
			rowsUpdated++
		}
		if rowsUpdated == 0 {
			break
		}
	}

	return nil
}

type apiKeyGroup struct {
	APIKeyID               string
	UserID                 string
	GroupID                string
	Capabilities           int32
	UseGroupOwnedExecutors bool
	CacheEncryptionEnabled bool
	EnforceIPRules         bool
}

func (g *apiKeyGroup) GetAPIKeyID() string {
	return g.APIKeyID
}

func (g *apiKeyGroup) GetUserID() string {
	return g.UserID
}

func (g *apiKeyGroup) GetGroupID() string {
	return g.GroupID
}

func (g *apiKeyGroup) GetCapabilities() int32 {
	return g.Capabilities
}

func (g *apiKeyGroup) GetUseGroupOwnedExecutors() bool {
	return g.UseGroupOwnedExecutors
}

func (g *apiKeyGroup) GetCacheEncryptionEnabled() bool {
	return g.CacheEncryptionEnabled
}

func (g *apiKeyGroup) GetEnforceIPRules() bool {
	return g.EnforceIPRules
}

func (d *AuthDB) InsertOrUpdateUserSession(ctx context.Context, sessionID string, session *tables.Session) error {
	session.SessionID = sessionID
	// Note: this could be one query, but it's likely too complicated to be worth
	// the slightly lower QPS.
	if result := d.h.GORM(ctx, "authdb_create_session").Clauses(clause.OnConflict{DoNothing: true}).Create(session); result.Error != nil {
		return result.Error
	} else if result.RowsAffected != 0 {
		return nil
	}
	return d.h.NewQuery(ctx, "authdb_update_session").Update(session)
}

func (d *AuthDB) ReadSession(ctx context.Context, sessionID string) (*tables.Session, error) {
	s := &tables.Session{}
	rq := d.h.NewQuery(ctx, "authdb_get_session").Raw(
		`SELECT * FROM "Sessions" WHERE session_id = ?`, sessionID)
	if err := rq.Take(s); err != nil {
		return nil, err
	}
	return s, nil
}

func (d *AuthDB) ClearSession(ctx context.Context, sessionID string) error {
	return d.h.NewQuery(ctx, "authdb_delete_session").Raw(
		`DELETE FROM "Sessions" WHERE session_id = ?`, sessionID).Exec().Error
}

// encryptAPIkey encrypts apiKey using chacha20 using the following process:
//
// We take the first apiKeyNonceLength bytes of the key, pad it with zeroes to
// chacha20 nonce size and use it as the nonce input. This part of the key
// remains non-secret inside the database.
//
// The remainder of the key is encrypted and kept secret.
//
// The final result includes both the nonce and the encrypted portion of the
// key, encoded into a hex string. The result is prefixed with
// apiKeyEncryptedValuePrefix to make it possible to differentiate encrypted
// and non-encrypted values in the database.
func (d *AuthDB) encryptAPIKey(apiKey string) (string, error) {
	if len(apiKey) != apiKeyLength {
		return "", status.FailedPreconditionErrorf("Invalid API key %q", redactInvalidAPIKey(apiKey))
	}
	if d.apiKeyEncryptionKey == nil {
		return "", status.FailedPreconditionError("API key encryption is not enabled")
	}

	nonce := apiKey[:apiKeyNonceLength]
	paddedNonce := make([]byte, chacha20.NonceSize)
	copy(paddedNonce, nonce)
	ciph, err := chacha20.NewUnauthenticatedCipher(d.apiKeyEncryptionKey, paddedNonce)
	if err != nil {
		return "", status.InternalErrorf("could not create API key cipher: %s", err)
	}

	data := []byte(apiKey[apiKeyNonceLength:])
	ciph.XORKeyStream(data, data)
	data = append([]byte(apiKey[:apiKeyNonceLength]), data...)
	return hex.EncodeToString(data), nil
}

// decryptAPIKey retrieves the plaintext representation of the API key. It is
// intended to be used when it is necessary to display the API key to the user,
// but not in the authentication process.
//
// After hex-decoding the key, we take the first apiKeyNonceLength bytes to be
// used as the nonce into the decryption function. The rest of key is fed into
// the cipher to retrieve the plaintext. The nonce and the decrypted value are
// combined to form the decrypted API key.
func (d *AuthDB) decryptAPIKey(encryptedAPIKey string) (string, error) {
	if d.apiKeyEncryptionKey == nil {
		return "", status.FailedPreconditionError("API key encryption is not enabled")
	}
	decoded, err := hex.DecodeString(encryptedAPIKey)
	if err != nil {
		return "", err
	}
	if len(decoded) < apiKeyNonceLength {
		return "", status.FailedPreconditionError("Encrypted API key too short")
	}
	nonce := make([]byte, chacha20.NonceSize)
	copy(nonce, decoded[:apiKeyNonceLength])
	data := decoded[apiKeyNonceLength:]
	ciph, err := chacha20.NewUnauthenticatedCipher(d.apiKeyEncryptionKey, nonce)
	if err != nil {
		return "", status.InternalErrorf("could not create API key cipher: %s", err)
	}
	ciph.XORKeyStream(data, data)
	return string(decoded[:apiKeyNonceLength]) + string(data), nil
}

func (d *AuthDB) fillDecryptedAPIKey(ak *tables.APIKey) error {
	if d.apiKeyEncryptionKey == nil || ak.EncryptedValue == "" {
		return nil
	}
	key, err := d.decryptAPIKey(ak.EncryptedValue)
	if err != nil {
		return err
	}
	ak.Value = key
	return nil
}

func (d *AuthDB) GetAPIKeyGroupFromAPIKey(ctx context.Context, apiKey string) (interfaces.APIKeyGroup, error) {
	apiKey = strings.TrimSpace(apiKey)
	if strings.Contains(apiKey, " ") || len(apiKey) != apiKeyLength {
		return nil, status.UnauthenticatedErrorf("Invalid API key %q", redactInvalidAPIKey(apiKey))
	}

	cacheKey := apiKey
	sd := subdomain.Get(ctx)
	if sd != "" {
		cacheKey = sd + "," + apiKey
	}

	if d.apiKeyGroupCache != nil {
		d, ok := d.apiKeyGroupCache.Get(cacheKey)
		if ok {
			metrics.APIKeyLookupCount.With(prometheus.Labels{metrics.APIKeyLookupStatus: "cache_hit"}).Inc()
			return d, nil
		}
	}

	akg := &apiKeyGroup{}
	qb := d.newAPIKeyGroupQuery(sd, true /*=allowUserOwnedKeys*/)
	keyClauses := query_builder.OrClauses{}
	if !*encryptOldKeys {
		keyClauses.AddOr("ak.value = ?", apiKey)
	}
	if d.apiKeyEncryptionKey != nil {
		encryptedAPIKey, err := d.encryptAPIKey(apiKey)
		if err != nil {
			return nil, err
		}
		keyClauses.AddOr("ak.encrypted_value = ?", encryptedAPIKey)
	}
	keyQuery, keyArgs := keyClauses.Build()
	qb.AddWhereClause(keyQuery, keyArgs...)
	q, args := qb.Build()

	err := d.h.NewQueryWithOpts(
		ctx,
		"authdb_get_api_key_group_by_key",
		db.Opts().WithStaleReads(),
	).Raw(
		q, args...,
	).Take(akg)

	if err != nil {
		if db.IsRecordNotFound(err) {
			if d.apiKeyGroupCache != nil {
				metrics.APIKeyLookupCount.With(prometheus.Labels{metrics.APIKeyLookupStatus: "invalid_key"}).Inc()
			}
			return nil, status.UnauthenticatedErrorf("Invalid API key %q", redactInvalidAPIKey(apiKey))
		}
		return nil, err
	}
	if d.apiKeyGroupCache != nil {
		metrics.APIKeyLookupCount.With(prometheus.Labels{metrics.APIKeyLookupStatus: "cache_miss"}).Inc()
		d.apiKeyGroupCache.Add(cacheKey, akg)
	}
	return akg, nil
}

func (d *AuthDB) GetAPIKeyGroupFromAPIKeyID(ctx context.Context, apiKeyID string) (interfaces.APIKeyGroup, error) {
	cacheKey := apiKeyID
	sd := subdomain.Get(ctx)
	if sd != "" {
		cacheKey = sd + "," + apiKeyID
	}
	if d.apiKeyGroupCache != nil {
		d, ok := d.apiKeyGroupCache.Get(cacheKey)
		if ok {
			return d, nil
		}
	}
	akg := &apiKeyGroup{}
	qb := d.newAPIKeyGroupQuery(sd, true /*=allowUserOwnedKeys*/)
	qb.AddWhereClause(`ak.api_key_id = ?`, apiKeyID)
	q, args := qb.Build()

	err := d.h.NewQueryWithOpts(
		ctx,
		"authdb_get_api_key_group_by_id",
		db.Opts().WithStaleReads(),
	).Raw(
		q,
		args...,
	).Take(akg)

	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.UnauthenticatedErrorf("Invalid API key ID %q", redactInvalidAPIKey(apiKeyID))
		}
		return nil, err
	}
	if d.apiKeyGroupCache != nil {
		d.apiKeyGroupCache.Add(cacheKey, akg)
	}
	return akg, nil
}

func (d *AuthDB) LookupUserFromSubID(ctx context.Context, subID string) (*tables.User, error) {
	rq := d.h.NewQueryWithOpts(ctx, "authdb_lookup_user_groups", db.Opts().WithStaleReads()).Raw(`
		SELECT u.*, g.*, ug.*
		FROM (
			SELECT * FROM "Users" 
			WHERE sub_id = ?
			ORDER BY user_id ASC
			LIMIT 1
		) AS u
			LEFT JOIN "UserGroups" AS ug
				ON u.user_id = ug.user_user_id
			LEFT JOIN "Groups" AS g
				ON ug.group_group_id = g.group_id
		AND (ug.membership_status = ? OR ug.user_user_id IS NULL)
		ORDER BY u.user_id, g.group_id ASC
		`, subID, int32(grpb.GroupMembershipStatus_MEMBER),
	)
	ugr, err := db.ScanAll(rq, &struct {
		tables.User
		*tables.Group
		*tables.UserGroup
	}{})
	if err != nil {
		return nil, err
	}
	if len(ugr) == 0 {
		return nil, status.NotFoundErrorf("Sub id %s was not found in LookupUserFromSubID.", subID)
	}
	user := &ugr[0].User
	if ugr[0].UserGroup == nil {
		// no user groups matched this user ID
		return user, nil
	}
	for _, v := range ugr {
		if v.Group == nil {
			// no group matched the user group (this shouldn't really happen)
			log.CtxWarningf(ctx, "In LookupUserFromSubID, the UserGroup row User: %s Group %s did not match a group with that ID.", v.UserGroup.UserUserID, v.UserGroup.GroupGroupID)
			continue
		}
		user.Groups = append(user.Groups, &tables.GroupRole{Group: *v.Group, Role: v.UserGroup.Role})
	}
	return user, nil
}

func (d *AuthDB) newAPIKeyGroupQuery(subDomain string, allowUserOwnedKeys bool) *query_builder.Query {
	qb := query_builder.NewQuery(`
		SELECT
			ak.capabilities,
			ak.api_key_id,
			ak.user_id,
			g.group_id,
			g.use_group_owned_executors,
			g.cache_encryption_enabled,
			g.enforce_ip_rules
		FROM "Groups" AS g,
		"APIKeys" AS ak
	`)
	qb.AddWhereClause(`ak.group_id = g.group_id`)

	if subDomain != "" {
		qb.AddWhereClause("url_identifier = ?", subDomain)
	}

	if *userOwnedKeysEnabled && allowUserOwnedKeys {
		// Note: the org can disable user-owned keys at any time, and the
		// predicate here ensures that existing keys are effectively deactivated
		// (but not deleted).
		qb.AddWhereClause(`(
			g.user_owned_keys_enabled
			OR ak.user_id = ''
			OR ak.user_id IS NULL
		)`)
	} else {
		qb.AddWhereClause(`(
			ak.user_id = ''
			OR ak.user_id IS NULL
		)`)
	}
	return qb
}

func redactInvalidAPIKey(key string) string {
	if len(key) < 8 {
		return "***"
	}
	return key[:1] + "***" + key[len(key)-1:]
}

func (d *AuthDB) createAPIKey(ctx context.Context, db interfaces.DB, ak tables.APIKey) (*tables.APIKey, error) {
	key, err := newAPIKeyToken()
	if err != nil {
		return nil, err
	}
	nonce := key[:apiKeyNonceLength]

	pk, err := tables.PrimaryKeyForTable("APIKeys")
	if err != nil {
		return nil, err
	}
	keyPerms := int32(0)
	if ak.UserID == "" {
		keyPerms = perms.GROUP_READ | perms.GROUP_WRITE
	} else {
		keyPerms = perms.OWNER_READ | perms.OWNER_WRITE
	}
	ak.Perms = keyPerms

	value := key
	var encryptedValue string
	if d.apiKeyEncryptionKey != nil && *encryptNewKeys {
		ek, err := d.encryptAPIKey(key)
		if err != nil {
			return nil, err
		}
		encryptedValue = ek
		value = ""
	}
	err = db.NewQuery(ctx, "authdb_create_api_key").Raw(`
		INSERT INTO "APIKeys" (
			api_key_id,
			user_id,
			group_id,
			perms,
			capabilities,
			value,
			encrypted_value,
			nonce,
			label,
			visible_to_developers,
			impersonation,
			expiry_usec
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		pk,
		ak.UserID,
		ak.GroupID,
		keyPerms,
		ak.Capabilities,
		value,
		encryptedValue,
		nonce,
		ak.Label,
		ak.VisibleToDevelopers,
		ak.Impersonation,
		ak.ExpiryUsec,
	).Exec().Error
	if err != nil {
		return nil, err
	}
	ak.APIKeyID = pk
	ak.Value = key
	return &ak, nil
}

func newAPIKeyToken() (string, error) {
	return random.RandomString(apiKeyLength)
}

func (d *AuthDB) authorizeGroupAdminRole(ctx context.Context, groupID string) error {
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	return authutil.AuthorizeOrgAdmin(u, groupID)
}

func (d *AuthDB) CreateAPIKey(ctx context.Context, groupID string, label string, caps []akpb.ApiKey_Capability, visibleToDevelopers bool) (*tables.APIKey, error) {
	if groupID == "" {
		return nil, status.InvalidArgumentError("Group ID cannot be nil.")
	}

	// Authorize org-level key creation (authenticated user must be a
	// group admin).
	if err := d.authorizeGroupAdminRole(ctx, groupID); err != nil {
		return nil, err
	}

	ak := tables.APIKey{
		GroupID:             groupID,
		Label:               label,
		Capabilities:        capabilities.ToInt(caps),
		VisibleToDevelopers: visibleToDevelopers,
	}
	return d.createAPIKey(ctx, d.h, ak)
}

func (d *AuthDB) CreateImpersonationAPIKey(ctx context.Context, groupID string) (*tables.APIKey, error) {
	if groupID == "" {
		return nil, status.InvalidArgumentError("Group ID cannot be nil.")
	}

	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	// If impersonation is in effect, it implies the user is an admin.
	// Can't check group membership because impersonation modifies
	// group information.
	if !u.IsImpersonating() {
		adminGroupID := d.env.GetAuthenticator().AdminGroupID()
		if adminGroupID == "" {
			return nil, status.PermissionDeniedError("You do not have access to the requested organization")
		}
		if err := authutil.AuthorizeOrgAdmin(u, adminGroupID); err != nil {
			return nil, err
		}
	}
	ak := tables.APIKey{
		GroupID: groupID,
		Label:   fmt.Sprintf("Impersonation key generated by %s", u.GetUserID()),
		// Read-only API key.
		Capabilities:  capabilities.ToInt(nil),
		Impersonation: true,
		ExpiryUsec:    time.Now().Add(impersonationAPIKeyDuration).UnixMicro(),
	}
	return d.createAPIKey(ctx, d.h, ak)
}

func (d *AuthDB) CreateAPIKeyWithoutAuthCheck(ctx context.Context, tx interfaces.DB, groupID string, label string, caps []akpb.ApiKey_Capability, visibleToDevelopers bool) (*tables.APIKey, error) {
	if groupID == "" {
		return nil, status.InvalidArgumentError("Group ID cannot be nil.")
	}
	ak := tables.APIKey{
		GroupID:             groupID,
		Label:               label,
		Capabilities:        capabilities.ToInt(caps),
		VisibleToDevelopers: visibleToDevelopers,
	}
	return d.createAPIKey(ctx, tx, ak)
}

func (d *AuthDB) authorizeNewAPIKeyCapabilities(ctx context.Context, userID, groupID string, caps []akpb.ApiKey_Capability) error {
	userCapabilities, err := capabilities.ForAuthenticatedUserGroup(ctx, d.env, groupID)
	if err != nil {
		return err
	}

	if userID != "" {
		// Capabilities assigned to the user-level key should not exceed the
		// capabilities of the currently authenticated user.
		requestedCapabilities := capabilities.ToInt(caps)
		if requestedCapabilities&capabilities.ToInt(userCapabilities) != requestedCapabilities {
			return status.PermissionDeniedError("user does not have permission to assign these API key capabilities")
		}

		// Additionally, respect our list of capabilities that can be assigned
		// to user-level keys.
		if requestedCapabilities&capabilities.UserAPIKeyCapabilitiesMask != requestedCapabilities {
			return status.PermissionDeniedError("the requested API key capabilities are not allowed for user-level keys")
		}

		return nil
	}
	return d.authorizeGroupAdminRole(ctx, groupID)
}

func (d *AuthDB) CreateUserAPIKey(ctx context.Context, groupID, label string, caps []akpb.ApiKey_Capability) (*tables.APIKey, error) {
	if !*userOwnedKeysEnabled {
		return nil, status.UnimplementedError("not implemented")
	}
	if groupID == "" {
		return nil, status.InvalidArgumentError("Group ID cannot be nil.")
	}

	if err := authutil.AuthorizeGroupAccess(ctx, d.env, groupID); err != nil {
		return nil, err
	}

	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	if err := d.authorizeNewAPIKeyCapabilities(ctx, u.GetUserID(), groupID, caps); err != nil {
		return nil, err
	}

	// Check that the group has user-owned keys enabled.
	g := &tables.Group{}
	err = d.h.NewQuery(ctx, "authdb_check_user_owned_keys_enabled").Raw(
		`SELECT user_owned_keys_enabled FROM "Groups" WHERE group_id = ?`,
		groupID,
	).Take(g)
	if err != nil {
		return nil, status.InternalErrorf("group lookup failed: %s", err)
	}
	if !g.UserOwnedKeysEnabled {
		return nil, status.PermissionDeniedErrorf("group %q does not have user-owned keys enabled", groupID)
	}

	ak := tables.APIKey{
		UserID:       u.GetUserID(),
		GroupID:      u.GetGroupID(),
		Label:        label,
		Capabilities: capabilities.ToInt(caps),
	}
	return d.createAPIKey(ctx, d.h, ak)
}

func (d *AuthDB) getAPIKey(ctx context.Context, h interfaces.DB, apiKeyID string) (*tables.APIKey, error) {
	if apiKeyID == "" {
		return nil, status.InvalidArgumentError("API key ID cannot be empty.")
	}
	rq := h.NewQuery(ctx, "authdb_get_api_key_by_id").Raw(
		`SELECT * FROM "APIKeys" WHERE api_key_id = ? AND (expiry_usec = 0 OR expiry_usec > ?)`, apiKeyID, time.Now().UnixMicro())
	key := &tables.APIKey{}
	if err := rq.Take(key); err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("The requested API key was not found.")
		}
		return nil, err
	}
	return key, nil
}

func (d *AuthDB) GetAPIKey(ctx context.Context, apiKeyID string) (*tables.APIKey, error) {
	user, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	key, err := d.getAPIKey(ctx, d.h, apiKeyID)
	if err != nil {
		return nil, err
	}
	acl := perms.ToACLProto(&uidpb.UserId{Id: key.UserID}, key.GroupID, key.Perms)
	if err := perms.AuthorizeRead(user, acl); err != nil {
		return nil, err
	}
	if err := d.fillDecryptedAPIKey(key); err != nil {
		return nil, err
	}
	return key, nil
}

// GetAPIKeyForInternalUseOnly returns any API key for the group. It is only to
// be used in situations where the user has a pre-authorized grant to access
// resources on behalf of the org, such as a publicly shared invocation. The
// returned API key must only be used to access internal resources and must
// not be returned to the caller.
func (d *AuthDB) GetAPIKeyForInternalUseOnly(ctx context.Context, groupID string) (*tables.APIKey, error) {
	if groupID == "" {
		return nil, status.InvalidArgumentError("Group ID cannot be empty.")
	}
	key := &tables.APIKey{}
	rq := d.h.NewQuery(ctx, "authdb_get_api_key_for_group").Raw(`
		SELECT * FROM "APIKeys"
		WHERE group_id = ?
		AND (user_id IS NULL OR user_id = '')
		AND impersonation = false
		AND expiry_usec = 0
		ORDER BY label ASC LIMIT 1
	`, groupID)
	if err := rq.Take(key); err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("no API keys were found for the requested group")
		}
		return nil, err
	}
	if err := d.fillDecryptedAPIKey(key); err != nil {
		return nil, err
	}
	return key, nil
}

// GetAPIKeys returns group-level API keys that the user is authorized to
// access.
func (d *AuthDB) GetAPIKeys(ctx context.Context, groupID string) ([]*tables.APIKey, error) {
	if groupID == "" {
		return nil, status.InvalidArgumentError("Group ID cannot be empty.")
	}
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if err := authutil.AuthorizeGroupAccess(ctx, d.env, groupID); err != nil {
		return nil, err
	}
	q := query_builder.NewQuery(`SELECT * FROM "APIKeys"`)
	// Select group-owned keys only
	q.AddWhereClause(`user_id IS NULL OR user_id = ''`)
	q.AddWhereClause(`group_id = ?`, groupID)
	if err := authutil.AuthorizeOrgAdmin(u, groupID); err != nil {
		// If we're not an admin, restrict to keys that have only been made
		// visible to non-admins. Note: the visible_to_developers field means "visible to
		// non-admins" now that we have reader/writer roles.
		q.AddWhereClause("visible_to_developers = ?", true)
	}
	q.AddWhereClause(`impersonation = false`)
	q.AddWhereClause(`expiry_usec = 0 OR expiry_usec > ?`, time.Now().UnixMicro())
	q.SetOrderBy("label", true /*ascending*/)
	queryStr, args := q.Build()
	rq := d.h.NewQuery(ctx, "authdb_get_api_keys").Raw(queryStr, args...)

	keys := make([]*tables.APIKey, 0)
	err = db.ScanEach(rq, func(ctx context.Context, k *tables.APIKey) error {
		if err := d.fillDecryptedAPIKey(k); err != nil {
			return err
		}
		keys = append(keys, k)
		return nil
	})
	return keys, err
}

func (d *AuthDB) authorizeAPIKeyWrite(ctx context.Context, h interfaces.DB, apiKeyID string) (*tables.APIKey, error) {
	if apiKeyID == "" {
		return nil, status.InvalidArgumentError("API key ID is required")
	}
	user, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	key, err := d.getAPIKey(ctx, h, apiKeyID)
	if err != nil {
		return nil, err
	}
	acl := perms.ToACLProto(&uidpb.UserId{Id: key.UserID}, key.GroupID, key.Perms)
	if err := perms.AuthorizeWrite(&user, acl); err != nil {
		return nil, err
	}
	// Only group admins can write to group-level API keys.
	if key.UserID == "" {
		if err := d.authorizeGroupAdminRole(ctx, key.GroupID); err != nil {
			return nil, err
		}
	}
	return key, nil
}

func (d *AuthDB) UpdateAPIKey(ctx context.Context, key *tables.APIKey) error {
	// TODO (zoey): could make this one query
	if key == nil {
		return status.InvalidArgumentError("API key cannot be nil.")
	}
	existingKey, err := d.authorizeAPIKeyWrite(ctx, d.h, key.APIKeyID)
	if err != nil {
		return err
	}
	if existingKey.UserID != "" && key.VisibleToDevelopers {
		return status.InvalidArgumentError(`"visible_to_developers" field should not be set for user-owned keys`)
	}
	// When updating capabilities, make sure the user has the appropriate
	// permissions to set them.
	if err := d.authorizeNewAPIKeyCapabilities(ctx, existingKey.UserID, existingKey.GroupID, capabilities.FromInt(key.Capabilities)); err != nil {
		return err
	}
	return d.h.NewQuery(ctx, "authdb_update_api_key").Raw(`
		UPDATE "APIKeys"
		SET
			label = ?,
			capabilities = ?,
			visible_to_developers = ?
		WHERE
			api_key_id = ?`,
		key.Label,
		key.Capabilities,
		key.VisibleToDevelopers,
		key.APIKeyID,
	).Exec().Error
}

func (d *AuthDB) DeleteAPIKey(ctx context.Context, apiKeyID string) error {
	// TODO (zoey): could make this one query
	if _, err := d.authorizeAPIKeyWrite(ctx, d.h, apiKeyID); err != nil {
		return err
	}
	return d.h.NewQuery(ctx, "authdb_delete_api_key").Raw(
		`DELETE FROM "APIKeys" WHERE api_key_id = ?`, apiKeyID).Exec().Error
}

func (d *AuthDB) GetUserAPIKeys(ctx context.Context, groupID string) ([]*tables.APIKey, error) {
	if !*userOwnedKeysEnabled {
		return nil, status.UnimplementedError("not implemented")
	}
	if groupID == "" {
		return nil, status.InvalidArgumentError("Group ID cannot be empty.")
	}
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if err := authutil.AuthorizeGroupAccess(ctx, d.env, groupID); err != nil {
		return nil, err
	}

	// Validate that user-level keys are enabled
	g := &tables.Group{}
	err = d.h.NewQuery(ctx, "authdb_check_user_api_keys_enabled").Raw(
		`SELECT user_owned_keys_enabled FROM "Groups" WHERE group_id = ?`,
		groupID,
	).Take(g)
	if err != nil {
		return nil, status.InternalErrorf("failed to look up user-owned keys setting: %s", err)
	}
	if !g.UserOwnedKeysEnabled {
		return nil, status.PermissionDeniedError("user-owned keys are not enabled for this group")
	}

	q := query_builder.NewQuery(`SELECT * FROM "APIKeys"`)
	q.AddWhereClause(`user_id = ?`, u.GetUserID())
	q.AddWhereClause(`group_id = ?`, groupID)
	q.AddWhereClause(`impersonation = false`)
	q.AddWhereClause(`expiry_usec = 0 OR expiry_usec > ?`, time.Now().UnixMicro())
	q.SetOrderBy("label", true /*=ascending*/)
	queryStr, args := q.Build()

	rq := d.h.NewQuery(ctx, "authdb_get_user_api_keys").Raw(queryStr, args...)
	var keys []*tables.APIKey
	err = db.ScanEach(rq, func(ctx context.Context, k *tables.APIKey) error {
		if err := d.fillDecryptedAPIKey(k); err != nil {
			return err
		}
		keys = append(keys, k)
		return nil
	})
	return keys, err
}

func (d *AuthDB) GetUserOwnedKeysEnabled() bool {
	return *userOwnedKeysEnabled
}

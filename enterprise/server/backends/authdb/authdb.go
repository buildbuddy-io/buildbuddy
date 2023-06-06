package authdb

import (
	"context"
	"flag"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

const (
	apiKeyLength = 20
)

var (
	userOwnedKeysEnabled = flag.Bool("app.user_owned_keys_enabled", false, "If true, enable user-owned API keys.")
)

type AuthDB struct {
	env environment.Env
	h   interfaces.DBHandle
}

func NewAuthDB(env environment.Env, h interfaces.DBHandle) *AuthDB {
	return &AuthDB{env: env, h: h}
}

type apiKeyGroup struct {
	UserID                 string
	GroupID                string
	Capabilities           int32
	UseGroupOwnedExecutors bool
	CacheEncryptionEnabled bool
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

func (d *AuthDB) InsertOrUpdateUserSession(ctx context.Context, sessionID string, session *tables.Session) error {
	session.SessionID = sessionID
	return d.h.Transaction(ctx, func(tx *db.DB) error {
		var existing tables.Session
		if err := tx.Where("session_id = ?", sessionID).First(&existing).Error; err != nil {
			if db.IsRecordNotFound(err) {
				return tx.Create(session).Error
			}
			return err
		}
		return tx.Model(&existing).Where("session_id = ?", sessionID).Updates(session).Error
	})
}

func (d *AuthDB) ReadSession(ctx context.Context, sessionID string) (*tables.Session, error) {
	s := &tables.Session{}
	existingRow := d.h.DB(ctx).Raw(`SELECT * FROM "Sessions" WHERE session_id = ?`, sessionID)
	if err := existingRow.Take(s).Error; err != nil {
		return nil, err
	}
	return s, nil
}

func (d *AuthDB) ClearSession(ctx context.Context, sessionID string) error {
	err := d.h.Transaction(ctx, func(tx *db.DB) error {
		res := tx.Exec(`DELETE FROM "Sessions" WHERE session_id = ?`, sessionID)
		return res.Error
	})
	return err
}

func (d *AuthDB) GetAPIKeyGroupFromAPIKey(ctx context.Context, apiKey string) (interfaces.APIKeyGroup, error) {
	if strings.Contains(strings.TrimSpace(apiKey), " ") || len(strings.TrimSpace(apiKey)) > apiKeyLength {
		return nil, status.UnauthenticatedErrorf("Invalid API key %q", redactInvalidAPIKey(apiKey))
	}

	akg := &apiKeyGroup{}
	err := d.h.TransactionWithOptions(ctx, db.Opts().WithStaleReads(), func(tx *db.DB) error {
		qb := d.newAPIKeyGroupQuery(true /*=allowUserOwnedKeys*/)
		qb.AddWhereClause(`ak.value = ?`, apiKey)
		q, args := qb.Build()
		existingRow := tx.Raw(q, args...)
		return existingRow.Take(akg).Error
	})
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.UnauthenticatedErrorf("Invalid API key %q", redactInvalidAPIKey(apiKey))
		}
		return nil, err
	}
	return akg, nil
}

func (d *AuthDB) GetAPIKeyGroupFromAPIKeyID(ctx context.Context, apiKeyID string) (interfaces.APIKeyGroup, error) {
	akg := &apiKeyGroup{}
	err := d.h.TransactionWithOptions(ctx, db.Opts().WithStaleReads(), func(tx *db.DB) error {
		qb := d.newAPIKeyGroupQuery(true /*=allowUserOwnedKeys*/)
		qb.AddWhereClause(`ak.api_key_id = ?`, apiKeyID)
		q, args := qb.Build()
		existingRow := tx.Raw(q, args...)
		return existingRow.Take(akg).Error
	})
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.UnauthenticatedErrorf("Invalid API key ID %q", redactInvalidAPIKey(apiKeyID))
		}
		return nil, err
	}
	return akg, nil
}

func (d *AuthDB) GetAPIKeyGroupFromBasicAuth(ctx context.Context, login, pass string) (interfaces.APIKeyGroup, error) {
	akg := &apiKeyGroup{}
	err := d.h.TransactionWithOptions(ctx, db.Opts().WithStaleReads(), func(tx *db.DB) error {
		// User-owned keys are disallowed here, since the group-level write
		// token should not grant access to user-level keys.
		qb := d.newAPIKeyGroupQuery(false /*=allowUserOwnedKeys*/)
		qb.AddWhereClause(`g.group_id = ?`, login)
		qb.AddWhereClause(`g.write_token = ?`, pass)
		q, args := qb.Build()
		existingRow := tx.Raw(q, args...)
		return existingRow.Take(akg).Error
	})
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.UnauthenticatedErrorf("User/Group specified by %s:*** not found", login)
		}
		return nil, err
	}
	log.Infof("Group %q successfully authed using write_token", login)
	return akg, nil
}

func (d *AuthDB) LookupUserFromSubID(ctx context.Context, subID string) (*tables.User, error) {
	user := &tables.User{}
	err := d.h.TransactionWithOptions(ctx, db.Opts().WithStaleReads(), func(tx *db.DB) error {
		userRow := tx.Raw(`SELECT * FROM "Users" WHERE sub_id = ? ORDER BY user_id ASC`, subID)
		if err := userRow.Take(user).Error; err != nil {
			return err
		}
		groupRows, err := tx.Raw(`
			SELECT
				g.user_id,
				g.group_id,
				g.url_identifier,
				g.name,
				g.owned_domain,
				g.github_token,
				g.sharing_enabled,
				g.user_owned_keys_enabled,
				g.use_group_owned_executors,
				g.cache_encryption_enabled,
				g.saml_idp_metadata_url,
				ug.role
			FROM "Groups" AS g, "UserGroups" AS ug
			WHERE g.group_id = ug.group_group_id
			AND ug.membership_status = ?
			AND ug.user_user_id = ?
			`, int32(grpb.GroupMembershipStatus_MEMBER), user.UserID,
		).Rows()
		if err != nil {
			return err
		}
		defer groupRows.Close()
		for groupRows.Next() {
			gr := &tables.GroupRole{}
			err := groupRows.Scan(
				&gr.Group.UserID,
				&gr.Group.GroupID,
				&gr.Group.URLIdentifier,
				&gr.Group.Name,
				&gr.Group.OwnedDomain,
				&gr.Group.GithubToken,
				&gr.Group.SharingEnabled,
				&gr.Group.UserOwnedKeysEnabled,
				&gr.Group.UseGroupOwnedExecutors,
				&gr.Group.CacheEncryptionEnabled,
				&gr.Group.SamlIdpMetadataUrl,
				&gr.Role,
			)
			if err != nil {
				return err
			}
			user.Groups = append(user.Groups, gr)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return user, nil
}

func (d *AuthDB) newAPIKeyGroupQuery(allowUserOwnedKeys bool) *query_builder.Query {
	qb := query_builder.NewQuery(`
		SELECT
			ak.capabilities,
			ak.user_id,
			g.group_id,
			g.use_group_owned_executors,
			g.cache_encryption_enabled
		FROM "Groups" AS g,
		"APIKeys" AS ak
	`)
	qb.AddWhereClause(`ak.group_id = g.group_id`)

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

func createAPIKey(db *db.DB, userID, groupID, value, label string, caps []akpb.ApiKey_Capability, visibleToDevelopers bool) (*tables.APIKey, error) {
	pk, err := tables.PrimaryKeyForTable("APIKeys")
	if err != nil {
		return nil, err
	}
	keyPerms := int32(0)
	if userID == "" {
		keyPerms = perms.GROUP_READ | perms.GROUP_WRITE
	} else {
		keyPerms = perms.OWNER_READ | perms.OWNER_WRITE
	}
	err = db.Exec(`
		INSERT INTO "APIKeys" (
			api_key_id,
			user_id,
			group_id,
			perms,
			capabilities,
			value,
			label,
			visible_to_developers
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		pk,
		userID,
		groupID,
		keyPerms,
		capabilities.ToInt(caps),
		value,
		label,
		visibleToDevelopers,
	).Error
	if err != nil {
		return nil, err
	}
	return &tables.APIKey{
		APIKeyID:            pk,
		UserID:              userID,
		GroupID:             groupID,
		Value:               value,
		Label:               label,
		Perms:               keyPerms,
		Capabilities:        capabilities.ToInt(caps),
		VisibleToDevelopers: visibleToDevelopers,
	}, nil
}

func randomToken(length int) string {
	// NB: Keep in sync with BuildBuddyServer#redactAPIKeys, which relies on this exact impl.
	token, err := random.RandomString(length)
	if err != nil {
		token = "bUiLdBuDdy"
	}
	return token
}

func newAPIKeyToken() string {
	return randomToken(apiKeyLength)
}

func (d *AuthDB) authorizeGroupAdminRole(ctx context.Context, groupID string) error {
	u, err := perms.AuthenticatedUser(ctx, d.env)
	if err != nil {
		return err
	}
	return authutil.AuthorizeGroupRole(u, groupID, role.Admin)
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

	return createAPIKey(d.h.DB(ctx), "" /*=userID*/, groupID, newAPIKeyToken(), label, caps, visibleToDevelopers)
}

func (d *AuthDB) CreateAPIKeyWithoutAuthCheck(tx *db.DB, groupID string, label string, caps []akpb.ApiKey_Capability, visibleToDevelopers bool) (*tables.APIKey, error) {
	if groupID == "" {
		return nil, status.InvalidArgumentError("Group ID cannot be nil.")
	}
	return createAPIKey(tx, "" /*=userID*/, groupID, newAPIKeyToken(), label, caps, visibleToDevelopers)
}

// Returns whether the given capabilities list contains any capabilities that
// requires Admin role in order to assign. We are opinionated here and let
// developers read and write to CAS; other capabilities require Admin role.
func hasAdminOnlyCapabilities(capabilities []akpb.ApiKey_Capability) bool {
	for _, c := range capabilities {
		if c != akpb.ApiKey_CAS_WRITE_CAPABILITY {
			return true
		}
	}
	return false
}

func (d *AuthDB) authorizeNewAPIKeyCapabilities(ctx context.Context, userID, groupID string, caps []akpb.ApiKey_Capability) error {
	if userID != "" {
		if capabilities.ToInt(caps)&int32(akpb.ApiKey_REGISTER_EXECUTOR_CAPABILITY) > 0 {
			return status.PermissionDeniedError("user-owned API keys cannot be used to register executors")
		}
	}

	if !hasAdminOnlyCapabilities(caps) {
		return nil
	}
	return d.authorizeGroupAdminRole(ctx, groupID)
}

func (d *AuthDB) CreateUserAPIKey(ctx context.Context, groupID, label string, capabilities []akpb.ApiKey_Capability) (*tables.APIKey, error) {
	if !*userOwnedKeysEnabled {
		return nil, status.UnimplementedError("not implemented")
	}
	if groupID == "" {
		return nil, status.InvalidArgumentError("Group ID cannot be nil.")
	}

	if err := perms.AuthorizeGroupAccess(ctx, d.env, groupID); err != nil {
		return nil, err
	}

	u, err := perms.AuthenticatedUser(ctx, d.env)
	if err != nil {
		return nil, err
	}

	if err := d.authorizeNewAPIKeyCapabilities(ctx, u.GetUserID(), groupID, capabilities); err != nil {
		return nil, err
	}

	var createdKey *tables.APIKey
	err = d.h.Transaction(ctx, func(tx *db.DB) error {
		// Check that the group has user-owned keys enabled.
		g := &tables.Group{}
		res := tx.Raw(
			`SELECT user_owned_keys_enabled FROM "Groups" WHERE group_id = ?`,
			groupID,
		).Take(g)
		if res.Error != nil {
			return status.InternalErrorf("group lookup failed: %s", res.Error)
		}
		if !g.UserOwnedKeysEnabled {
			return status.PermissionDeniedErrorf("group %q does not have user-owned keys enabled", groupID)
		}

		key, err := createAPIKey(tx, u.GetUserID(), groupID, newAPIKeyToken(), label, capabilities, false /*=visibleToDevelopers*/)
		if err != nil {
			return err
		}
		createdKey = key
		return nil
	})
	if err != nil {
		return nil, err
	}
	return createdKey, nil
}

func (d *AuthDB) getAPIKey(tx *db.DB, apiKeyID string) (*tables.APIKey, error) {
	if apiKeyID == "" {
		return nil, status.InvalidArgumentError("API key ID cannot be empty.")
	}
	query := tx.Raw(`SELECT * FROM "APIKeys" WHERE api_key_id = ?`, apiKeyID)
	key := &tables.APIKey{}
	if err := query.Take(key).Error; err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("The requested API key was not found.")
		}
		return nil, err
	}
	return key, nil
}

func (d *AuthDB) GetAPIKey(ctx context.Context, apiKeyID string) (*tables.APIKey, error) {
	return d.getAPIKey(d.h.DB(ctx), apiKeyID)
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
	query := d.h.DB(ctx).Raw(`
		SELECT * FROM "APIKeys"
		WHERE group_id = ?
		AND (user_id IS NULL OR user_id = '')
		ORDER BY label ASC LIMIT 1
	`, groupID)
	if err := query.Take(key).Error; err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("no API keys were found for the requested group")
		}
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
	u, err := perms.AuthenticatedUser(ctx, d.env)
	if err != nil {
		return nil, err
	}
	if err := perms.AuthorizeGroupAccess(ctx, d.env, groupID); err != nil {
		return nil, err
	}
	q := query_builder.NewQuery(`SELECT * FROM "APIKeys"`)
	// Select group-owned keys only
	q.AddWhereClause(`user_id IS NULL OR user_id = ''`)
	q.AddWhereClause(`group_id = ?`, groupID)
	if err := authutil.AuthorizeGroupRole(u, groupID, role.Admin); err != nil {
		q.AddWhereClause("visible_to_developers = ?", true)
	}
	q.SetOrderBy("label", true /*ascending*/)
	queryStr, args := q.Build()
	query := d.h.DB(ctx).Raw(queryStr, args...)
	rows, err := query.Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make([]*tables.APIKey, 0)
	for rows.Next() {
		k := &tables.APIKey{}
		if err := d.h.DB(ctx).ScanRows(rows, k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, nil
}

func (d *AuthDB) authorizeAPIKeyWrite(ctx context.Context, tx *db.DB, apiKeyID string) (*tables.APIKey, error) {
	if apiKeyID == "" {
		return nil, status.InvalidArgumentError("API key ID is required")
	}
	user, err := perms.AuthenticatedUser(ctx, d.env)
	if err != nil {
		return nil, err
	}
	key, err := d.getAPIKey(tx, apiKeyID)
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
	if key == nil {
		return status.InvalidArgumentError("API key cannot be nil.")
	}
	return d.h.DB(ctx).Transaction(func(tx *db.DB) error {
		existingKey, err := d.authorizeAPIKeyWrite(ctx, tx, key.APIKeyID)
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
		return tx.Exec(`
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
		).Error
	})
}

func (d *AuthDB) DeleteAPIKey(ctx context.Context, apiKeyID string) error {
	return d.h.DB(ctx).Transaction(func(tx *db.DB) error {
		if _, err := d.authorizeAPIKeyWrite(ctx, tx, apiKeyID); err != nil {
			return err
		}
		return tx.Exec(`DELETE FROM "APIKeys" WHERE api_key_id = ?`, apiKeyID).Error
	})
}

func (d *AuthDB) GetUserAPIKeys(ctx context.Context, groupID string) ([]*tables.APIKey, error) {
	if !*userOwnedKeysEnabled {
		return nil, status.UnimplementedError("not implemented")
	}
	if groupID == "" {
		return nil, status.InvalidArgumentError("Group ID cannot be empty.")
	}
	u, err := perms.AuthenticatedUser(ctx, d.env)
	if err != nil {
		return nil, err
	}
	if err := perms.AuthorizeGroupAccess(ctx, d.env, groupID); err != nil {
		return nil, err
	}

	// Validate that user-level keys are enabled
	g := &tables.Group{}
	err = d.h.DB(ctx).Raw(
		`SELECT user_owned_keys_enabled FROM "Groups" WHERE group_id = ?`,
		groupID,
	).Take(g).Error
	if err != nil {
		return nil, status.InternalErrorf("failed to look up user-owned keys setting: %s", err)
	}
	if !g.UserOwnedKeysEnabled {
		return nil, status.PermissionDeniedError("user-owned keys are not enabled for this group")
	}

	q := query_builder.NewQuery(`SELECT * FROM "APIKeys"`)
	q.AddWhereClause(`user_id = ?`, u.GetUserID())
	q.AddWhereClause(`group_id = ?`, groupID)
	q.SetOrderBy("label", true /*=ascending*/)
	queryStr, args := q.Build()

	rows, err := d.h.DB(ctx).Raw(queryStr, args...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []*tables.APIKey
	for rows.Next() {
		k := &tables.APIKey{}
		if err := d.h.DB(ctx).ScanRows(rows, k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}

	return keys, nil
}

func (d *AuthDB) GetUserOwnedKeysEnabled() bool {
	return *userOwnedKeysEnabled
}

package authdb

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
)

type AuthDB struct {
	h interfaces.DBHandle
}

func NewAuthDB(h interfaces.DBHandle) *AuthDB {
	return &AuthDB{h: h}
}

type apiKeyGroup struct {
	GroupID                string
	Capabilities           int32
	UseGroupOwnedExecutors bool
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
	existingRow := d.h.DB(ctx).Raw(`SELECT * FROM Sessions WHERE session_id = ?`, sessionID)
	if err := existingRow.Take(s).Error; err != nil {
		return nil, err
	}
	return s, nil
}

func (d *AuthDB) ClearSession(ctx context.Context, sessionID string) error {
	err := d.h.Transaction(ctx, func(tx *db.DB) error {
		res := tx.Exec(`DELETE FROM Sessions WHERE session_id = ?`, sessionID)
		return res.Error
	})
	return err
}

func (d *AuthDB) GetAPIKeyGroupFromAPIKey(ctx context.Context, apiKey string) (interfaces.APIKeyGroup, error) {
	akg := &apiKeyGroup{}
	err := d.h.TransactionWithOptions(ctx, db.Opts().WithStaleReads(), func(tx *db.DB) error {
		qb := newAPIKeyGroupQuery()
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
		qb := newAPIKeyGroupQuery()
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
		qb := newAPIKeyGroupQuery()
		qb.AddWhereClause(`g.group_id = ?`, login)
		qb.AddWhereClause(`g.write_token = ?`, pass)
		q, args := qb.Build()
		existingRow := tx.Raw(q, args...)
		return existingRow.Scan(akg).Error
	})
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.UnauthenticatedErrorf("User/Group specified by %s:%s not found", login, pass)
		}
		return nil, err
	}
	return akg, nil

}

func (d *AuthDB) LookupUserFromSubID(ctx context.Context, subID string) (*tables.User, error) {
	user := &tables.User{}
	err := d.h.TransactionWithOptions(ctx, db.Opts().WithStaleReads(), func(tx *db.DB) error {
		userRow := tx.Raw(`SELECT * FROM Users WHERE sub_id = ? ORDER BY user_id ASC`, subID)
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
				g.use_group_owned_executors,
				g.saml_idp_metadata_url,
				ug.role
			FROM `+"`Groups`"+` AS g, UserGroups AS ug
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
				&gr.Group.UseGroupOwnedExecutors,
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
	return user, err
}

func newAPIKeyGroupQuery() *query_builder.Query {
	qb := query_builder.NewQuery(`
		SELECT
			ak.capabilities,
			g.group_id,
			g.use_group_owned_executors
		FROM ` + "`Groups`" + ` AS g,
		APIKeys AS ak
	`)
	qb.AddWhereClause(`ak.group_id = g.group_id`)
	return qb
}

func redactInvalidAPIKey(key string) string {
	if len(key) < 8 {
		return "***"
	}
	return key[:1] + "***" + key[len(key)-1:]
}

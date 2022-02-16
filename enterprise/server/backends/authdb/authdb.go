package authdb

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
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

func (d *AuthDB) InsertOrUpdateUserToken(ctx context.Context, subID string, token *tables.Token) error {
	token.SubID = subID
	return d.h.Transaction(ctx, func(tx *db.DB) error {
		var existing tables.Token
		if err := tx.Where("sub_id = ?", subID).First(&existing).Error; err != nil {
			if db.IsRecordNotFound(err) {
				return tx.Create(token).Error
			}
			return err
		}
		return tx.Model(&existing).Where("sub_id = ?", subID).Updates(token).Error
	})
}

func (d *AuthDB) ReadToken(ctx context.Context, subID string) (*tables.Token, error) {
	ti := &tables.Token{}
	existingRow := d.h.DB(ctx).Raw(`SELECT * FROM Tokens WHERE sub_id = ?`, subID)
	if err := existingRow.Take(ti).Error; err != nil {
		return nil, err
	}
	return ti, nil
}

func (d *AuthDB) ClearToken(ctx context.Context, subID string) error {
	err := d.h.Transaction(ctx, func(tx *db.DB) error {
		res := tx.Exec(`UPDATE Tokens SET access_token = "" WHERE sub_id = ?`, subID)
		return res.Error
	})
	return err
}

func (d *AuthDB) GetAPIKeyGroupFromAPIKey(ctx context.Context, apiKey string) (interfaces.APIKeyGroup, error) {
	akg := &apiKeyGroup{}
	err := d.h.TransactionWithOptions(ctx, db.Opts().WithStaleReads(), func(tx *db.DB) error {
		existingRow := tx.Raw(`
			SELECT ak.capabilities, g.group_id, g.use_group_owned_executors
			FROM `+"`Groups`"+` AS g, APIKeys AS ak
			WHERE g.group_id = ak.group_id AND ak.value = ?`,
			apiKey)
		return existingRow.Take(akg).Error
	})
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.UnauthenticatedErrorf("Invalid API key %q", apiKey)
		}
		return nil, err
	}
	return akg, nil
}

func (d *AuthDB) GetAPIKeyGroupFromBasicAuth(ctx context.Context, login, pass string) (interfaces.APIKeyGroup, error) {
	akg := &apiKeyGroup{}
	err := d.h.TransactionWithOptions(ctx, db.Opts().WithStaleReads(), func(tx *db.DB) error {
		existingRow := tx.Raw(`
			SELECT ak.capabilities, g.group_id, g.use_group_owned_executors
			FROM `+"`Groups`"+` AS g, APIKeys AS ak
			WHERE g.group_id = ? AND g.write_token = ? AND g.group_id = ak.group_id`,
			login, pass)
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
		// Ensure the access token is set. If it's not, return an error
		// that the frontend can recognize to trigger login again.
		at := &struct{ AccessToken string }{}
		err := tx.Raw(`SELECT access_token FROM Tokens WHERE sub_id = ?`, subID).Take(at).Error
		if err != nil {
			return status.UnauthenticatedError(err.Error())
		}
		// For now, we don't need to validate this token -- it's enough
		// to ensure it was not cleared. If it was, that would indicate
		// that the user had logged out (or been logged out by us).
		if at.AccessToken == "" {
			return status.PermissionDeniedError("user not logged in")
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

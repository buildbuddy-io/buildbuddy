package authdb

import (
	"context"
	"errors"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"gorm.io/gorm"
)

type AuthDB struct {
	h *db.DBHandle
}

func NewAuthDB(h *db.DBHandle) *AuthDB {
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
	return d.h.Transaction(func(tx *gorm.DB) error {
		var existing tables.Token
		if err := tx.Where("sub_id = ?", subID).First(&existing).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return tx.Create(token).Error
			}
			return err
		}
		return tx.Model(&existing).Where("sub_id = ?", subID).Updates(token).Error
	})
}

func (d *AuthDB) ReadToken(ctx context.Context, subID string) (*tables.Token, error) {
	ti := &tables.Token{}
	existingRow := d.h.Raw(`SELECT * FROM Tokens as t
                               WHERE t.sub_id = ?`, subID)
	if err := existingRow.Take(ti).Error; err != nil {
		return nil, err
	}
	return ti, nil
}

func (d *AuthDB) GetAPIKeyGroupFromAPIKey(apiKey string) (interfaces.APIKeyGroup, error) {
	akg := &apiKeyGroup{}
	err := d.h.TransactionWithOptions(db.StaleReadOptions(), func(tx *gorm.DB) error {
		existingRow := tx.Raw(`
			SELECT ak.capabilities, g.group_id, g.use_group_owned_executors
			FROM `+"`Groups`"+` AS g, APIKeys AS ak
			WHERE g.group_id = ak.group_id AND ak.value = ?`,
			apiKey)
		return existingRow.Take(akg).Error
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.UnauthenticatedErrorf("Invalid API key %s", apiKey)
		}
		return nil, err
	}
	return akg, nil
}

func (d *AuthDB) GetAPIKeyGroupFromBasicAuth(login, pass string) (interfaces.APIKeyGroup, error) {
	akg := &apiKeyGroup{}
	err := d.h.TransactionWithOptions(db.StaleReadOptions(), func(tx *gorm.DB) error {
		existingRow := tx.Raw(`
			SELECT ak.capabilities, g.group_id, g.use_group_owned_executors
			FROM `+"`Groups`"+` AS g, APIKeys AS ak
			WHERE g.group_id = ? AND g.write_token = ? AND g.group_id = ak.group_id`,
			login, pass)
		return existingRow.Scan(akg).Error
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.UnauthenticatedErrorf("User/Group specified by %s:%s not found", login, pass)
		}
		return nil, err
	}
	return akg, nil

}

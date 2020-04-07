package invocationdb

import (
	"context"
	"log"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jinzhu/gorm"
)

type InvocationDB struct {
	env environment.Env
	h   *db.DBHandle
}

func NewInvocationDB(env environment.Env, h *db.DBHandle) *InvocationDB {
	return &InvocationDB{
		env: env,
		h:   h,
	}
}

func (d *InvocationDB) createInvocation(tx *gorm.DB, ctx context.Context, ti *tables.Invocation) error {
	permissions := perms.AnonymousUserPermissions()
	if auth := d.env.GetAuthenticator(); auth != nil {
		bat, err := auth.GetBasicAuthToken(ctx)
		if err == nil && bat != nil {
			userDB := d.env.GetUserDB()
			if userDB == nil {
				return status.FailedPreconditionError("UserDB not configured -- can't authorize request")
			}
			// Attempt to lookup this group by auth token.
			g, err := userDB.GetGroupForAuthToken(ctx, bat)
			if err != nil {
				return status.UnauthenticatedError("Basic auth credentials were not valid.")
			}
			permissions = perms.GroupAuthPermissions(g)
		}
	}
	ti.UserID = permissions.UserID
	ti.GroupID = permissions.GroupID
	ti.Perms = permissions.Perms
	return tx.Create(ti).Error
}

func (d *InvocationDB) InsertOrUpdateInvocation(ctx context.Context, ti *tables.Invocation) error {
	return d.h.Transaction(func(tx *gorm.DB) error {
		var existing tables.Invocation
		if err := tx.Where("invocation_id = ?", ti.InvocationID).First(&existing).Error; err != nil {
			if gorm.IsRecordNotFoundError(err) {
				return d.createInvocation(tx, ctx, ti)
			}
		} else {
			log.Printf("updating invocation")
			tx.Model(&existing).Where("invocation_id = ?", ti.InvocationID).Updates(ti)
		}
		return nil
	})
}

func (d *InvocationDB) LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error) {
	ti := &tables.Invocation{}
	existingRow := d.h.Raw(`SELECT * FROM Invocations as i
                               WHERE i.invocation_id = ?`, invocationID)
	if err := existingRow.Scan(ti).Error; err != nil {
		return nil, err
	}
	return ti, nil
}

func (d *InvocationDB) LookupExpiredInvocations(ctx context.Context, cutoffTime time.Time, limit int) ([]*tables.Invocation, error) {
	cutoffUsec := cutoffTime.UnixNano() / 1000
	rows, err := d.h.Raw(`SELECT * FROM Invocations as i
                                   WHERE i.created_at_usec < ?
                                   LIMIT ?`, cutoffUsec, limit).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	invocations := make([]*tables.Invocation, 0)
	var ti tables.Invocation
	for rows.Next() {
		if err := d.h.ScanRows(rows, &ti); err != nil {
			return nil, err
		}
		i := ti
		invocations = append(invocations, &i)
	}
	return invocations, nil
}

func (d *InvocationDB) DeleteInvocation(ctx context.Context, invocationID string) error {
	ti := &tables.Invocation{InvocationID: invocationID}
	return d.h.Delete(ti).Error
}

func (d *InvocationDB) RawQueryInvocations(ctx context.Context, sql string, values ...interface{}) ([]*tables.Invocation, error) {
	rows, err := d.h.Raw(sql, values...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	invocations := make([]*tables.Invocation, 0)
	var ti tables.Invocation
	for rows.Next() {
		if err := d.h.ScanRows(rows, &ti); err != nil {
			return nil, err
		}
		i := ti
		invocations = append(invocations, &i)
	}
	return invocations, nil
}

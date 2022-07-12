package invocationdb

import (
	"context"
	"errors"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"gorm.io/gorm"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

type InvocationDB struct {
	env environment.Env
	h   interfaces.DBHandle
}

func NewInvocationDB(env environment.Env, h interfaces.DBHandle) *InvocationDB {
	return &InvocationDB{
		env: env,
		h:   h,
	}
}

func getACL(i *tables.Invocation) *aclpb.ACL {
	return perms.ToACLProto(&uidpb.UserId{Id: i.UserID}, i.GroupID, i.Perms)
}

func (d *InvocationDB) registerInvocationAttempt(ctx context.Context, ti *tables.Invocation) (bool, error) {
	ti.Attempt = 1
	created := false
	err := d.h.TransactionWithOptions(ctx, db.Opts().WithQueryName("upsert_invocation"), func(tx *db.DB) error {
		// First, try inserting the invocation. This will work for first attempts.
		err := tx.Create(ti).Error
		if err == nil {
			// Insert worked; we're done.
			created = true
			return nil
		}
		if !d.h.IsDuplicateKeyError(err) {
			return err
		}

		// Insert failed due to conflict; update the existing row instead.
		err = tx.Raw(`
				SELECT attempt FROM Invocations
				WHERE invocation_id = ? AND invocation_status <> ? AND updated_at_usec > ? 
				`+d.h.SelectForUpdateModifier(),
			ti.InvocationID,
			int64(inpb.Invocation_COMPLETE_INVOCATION_STATUS),
			time.Now().Add(time.Hour*-4).UnixMicro(),
		).Take(ti).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// The invocation either succeeded or is more than 4 hours old. It may
				// not be re-attempted.
				return nil
			}
			return err
		}

		// ti had Attempt populated with the previous attempt value, so update it.
		if ti.Attempt == 0 {
			// This invocation was attempted before we added Attempt count, this is at
			// least the second attempt.
			ti.Attempt = 2
		} else {
			ti.Attempt += 1
		}
		result := tx.Updates(ti)
		created = result.RowsAffected > 0
		return result.Error
	})
	return created, err
}

func (d *InvocationDB) CreateInvocation(ctx context.Context, ti *tables.Invocation) (bool, error) {
	var permissions *perms.UserGroupPerm
	var caps []akpb.ApiKey_Capability
	if auth := d.env.GetAuthenticator(); auth != nil {
		u, err := auth.AuthenticatedUser(ctx)
		if err == nil {
			if u.GetGroupID() != "" {
				permissions = perms.GroupAuthPermissions(u.GetGroupID())
			}
			caps = u.GetCapabilities()
		} else {
			if perms.IsAnonymousUserError(err) && auth.AnonymousUsageEnabled() {
				caps = capabilities.DefaultAuthenticatedUserCapabilities
			}
		}
	}

	if permissions == nil && d.env.GetAuthenticator().AnonymousUsageEnabled() {
		permissions = perms.AnonymousUserPermissions()
	} else if permissions == nil {
		return false, status.PermissionDeniedErrorf("Anonymous access disabled, permission denied.")
	}

	ti.UserID = permissions.UserID
	ti.GroupID = permissions.GroupID
	ti.Perms = ti.Perms | permissions.Perms
	ti.CreatedWithCapabilities = capabilities.ToInt(caps)
	return d.registerInvocationAttempt(ctx, ti)
}

// UpdateInvocation updates an existing invocation with the given
// id and attempt number. It returns whether a row was updated.
func (d *InvocationDB) UpdateInvocation(ctx context.Context, ti *tables.Invocation) (bool, error) {
	updated := false
	var err error
	for r := retry.DefaultWithContext(ctx); r.Next(); {
		err = d.h.TransactionWithOptions(ctx, db.Opts().WithQueryName("update_invocation"), func(tx *db.DB) error {
			result := tx.Where("`invocation_id` = ? AND `attempt` = ?", ti.InvocationID, ti.Attempt).Updates(ti)
			updated = result.RowsAffected > 0
			return result.Error
		})
		if d.h.IsDeadlockError(err) {
			log.Warningf("Encountered deadlock when attempting to update invocation table for invocation %s, attempt %d of %d", ti.InvocationID, r.AttemptNumber(), r.MaxAttempts())
			continue
		}
		break
	}
	return updated, err
}

func (d *InvocationDB) UpdateInvocationACL(ctx context.Context, authenticatedUser *interfaces.UserInfo, invocationID string, acl *aclpb.ACL) error {
	p, err := perms.FromACL(acl)
	if err != nil {
		return err
	}
	return d.h.Transaction(ctx, func(tx *db.DB) error {
		var in tables.Invocation
		if err := tx.Raw(`SELECT user_id, group_id, perms FROM Invocations WHERE invocation_id = ?`, invocationID).Take(&in).Error; err != nil {
			return err
		}
		var group tables.Group
		if err := tx.Raw(`SELECT sharing_enabled FROM `+"`Groups`"+` WHERE group_id = ?`, in.GroupID).Take(&group).Error; err != nil {
			return err
		}
		if !group.SharingEnabled {
			return status.PermissionDeniedError("Your organization does not allow this action.")
		}

		if err := perms.AuthorizeWrite(authenticatedUser, getACL(&in)); err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE Invocations SET perms = ? WHERE invocation_id = ?`, p, invocationID).Error; err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE Executions SET perms = ? WHERE invocation_id = ?`, p, invocationID).Error; err != nil {
			return err
		}
		return nil
	})
}

func (d *InvocationDB) LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error) {
	ti := &tables.Invocation{}
	if err := d.h.DB(ctx).Raw(`SELECT * FROM Invocations WHERE invocation_id = ?`, invocationID).Take(ti).Error; err != nil {
		return nil, err
	}
	if ti.Perms&perms.OTHERS_READ == 0 {
		u, err := perms.AuthenticatedUser(ctx, d.env)
		if err != nil {
			return nil, err
		}
		if err := perms.AuthorizeRead(&u, getACL(ti)); err != nil {
			return nil, err
		}
	}
	return ti, nil
}

func (d *InvocationDB) LookupGroupFromInvocation(ctx context.Context, invocationID string) (*tables.Group, error) {
	ti := &tables.Group{}
	q := query_builder.NewQuery(`SELECT * FROM ` + "`Groups`" + ` as g JOIN Invocations as i ON g.group_id = i.group_id`)
	q = q.AddWhereClause(`i.invocation_id = ?`, invocationID)
	if err := perms.AddPermissionsCheckToQueryWithTableAlias(ctx, d.env, q, "i"); err != nil {
		return nil, err
	}
	queryStr, args := q.Build()
	existingRow := d.h.DB(ctx).Raw(queryStr, args...)
	if err := existingRow.Take(ti).Error; err != nil {
		return nil, err
	}
	return ti, nil
}

func (d *InvocationDB) LookupGroupIDFromInvocation(ctx context.Context, invocationID string) (string, error) {
	in := &tables.Invocation{}
	err := d.h.DB(ctx).Raw(
		`SELECT group_id FROM Invocations WHERE invocation_id = ?`, invocationID,
	).Take(in).Error
	if err != nil {
		return "", err
	}
	return in.GroupID, nil
}

func (d *InvocationDB) LookupExpiredInvocations(ctx context.Context, cutoffTime time.Time, limit int) ([]*tables.Invocation, error) {
	cutoffUsec := cutoffTime.UnixMicro()
	rows, err := d.h.DB(ctx).Raw(`SELECT * FROM Invocations as i
                                      WHERE i.created_at_usec < ?
                                      LIMIT ?`, cutoffUsec, limit).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	invocations := make([]*tables.Invocation, 0)
	var ti tables.Invocation
	for rows.Next() {
		if err := d.h.DB(ctx).ScanRows(rows, &ti); err != nil {
			return nil, err
		}
		i := ti
		invocations = append(invocations, &i)
	}
	return invocations, nil
}

func (d *InvocationDB) FillCounts(ctx context.Context, stat *telpb.TelemetryStat) error {
	counts := d.h.DB(ctx).Raw(`
		SELECT 
			COUNT(DISTINCT invocation_id) as invocation_count,
			COUNT(DISTINCT host) as bazel_host_count,
			COUNT(DISTINCT user) as bazel_user_count
		FROM Invocations as i
		WHERE 
			i.created_at_usec >= ? AND
			i.created_at_usec < ?`,
		time.Now().Truncate(24*time.Hour).Add(-24*time.Hour).UnixMicro(),
		time.Now().Truncate(24*time.Hour).UnixMicro())

	if err := counts.Take(stat).Error; err != nil {
		return err
	}
	return nil
}

func (d *InvocationDB) DeleteInvocation(ctx context.Context, invocationID string) error {
	ti := &tables.Invocation{InvocationID: invocationID}
	return d.h.DB(ctx).Delete(ti).Error
}

func (d *InvocationDB) DeleteInvocationWithPermsCheck(ctx context.Context, authenticatedUser *interfaces.UserInfo, invocationID string) error {
	return d.h.Transaction(ctx, func(tx *db.DB) error {
		var in tables.Invocation
		if err := tx.Raw(`SELECT user_id, group_id, perms FROM Invocations WHERE invocation_id = ?`, invocationID).Take(&in).Error; err != nil {
			return err
		}
		acl := perms.ToACLProto(&uidpb.UserId{Id: in.UserID}, in.GroupID, in.Perms)
		if err := perms.AuthorizeWrite(authenticatedUser, acl); err != nil {
			return err
		}
		if err := tx.Exec(`DELETE FROM Invocations WHERE invocation_id = ?`, invocationID).Error; err != nil {
			return err
		}
		if err := tx.Exec(`DELETE FROM Executions WHERE invocation_id = ?`, invocationID).Error; err != nil {
			return err
		}
		return nil
	})
}

func (d *InvocationDB) SetNowFunc(now func() time.Time) {
	d.h.SetNowFunc(now)
}

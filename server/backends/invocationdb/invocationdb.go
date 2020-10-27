package invocationdb

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jinzhu/gorm"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
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
	var permissions *perms.UserGroupPerm
	if auth := d.env.GetAuthenticator(); auth != nil {
		if u, err := auth.AuthenticatedUser(ctx); err == nil && u.GetGroupID() != "" {
			permissions = perms.GroupAuthPermissions(u.GetGroupID())
		}
	}

	if permissions == nil && d.env.GetConfigurator().GetAnonymousUsageEnabled() {
		permissions = perms.AnonymousUserPermissions()
	} else if permissions == nil {
		return status.PermissionDeniedErrorf("Anonymous access disabled, permission denied.")
	}

	ti.UserID = permissions.UserID
	ti.GroupID = permissions.GroupID
	ti.Perms = ti.Perms | permissions.Perms
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
			tx.Model(&existing).Where("invocation_id = ?", ti.InvocationID).Updates(ti)
		}
		return nil
	})
}

func (d *InvocationDB) UpdateInvocationACL(ctx context.Context, invocationID string, acl *aclpb.ACL) error {
	p, err := perms.FromACL(acl)
	if err != nil {
		return err
	}
	return d.h.Transaction(func(tx *gorm.DB) error {
		var in tables.Invocation
		if err := tx.Raw("SELECT user_id, group_id, perms FROM Invocations WHERE invocation_id = ?", invocationID).Scan(&in).Error; err != nil {
			return err
		}
		var group tables.Group
		if err := tx.Raw("SELECT sharing_enabled FROM Groups WHERE group_id = ?", in.GroupID).Scan(&group).Error; err != nil {
			return err
		}
		if !group.SharingEnabled {
			return status.PermissionDeniedError("Your organization does not allow this action.")
		}

		currentACL := perms.ToACLProto(&uidpb.UserId{Id: in.UserID}, in.GroupID, in.Perms)
		if err := perms.AuthorizeWrite(ctx, currentACL); err != nil {
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
	q := query_builder.NewQuery(`SELECT * FROM Invocations as i`)
	q = q.AddWhereClause(`i.invocation_id = ?`, invocationID)
	if err := perms.AddPermissionsCheckToQueryWithTableAlias(ctx, d.env, q, "i"); err != nil {
		return nil, err
	}
	queryStr, args := q.Build()
	existingRow := d.h.Raw(queryStr, args...)
	if err := existingRow.Scan(ti).Error; err != nil {
		return nil, err
	}
	return ti, nil
}

func (d *InvocationDB) LookupGroupFromInvocation(ctx context.Context, invocationID string) (*tables.Group, error) {
	ti := &tables.Group{}
	q := query_builder.NewQuery(`SELECT * FROM Groups as g JOIN Invocations as i ON g.group_id = i.group_id`)
	q = q.AddWhereClause(`i.invocation_id = ?`, invocationID)
	if err := perms.AddPermissionsCheckToQueryWithTableAlias(ctx, d.env, q, "i"); err != nil {
		return nil, err
	}
	queryStr, args := q.Build()
	existingRow := d.h.Raw(queryStr, args...)
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

func (d *InvocationDB) FillCounts(ctx context.Context, stat *telpb.TelemetryStat) error {
	counts := d.h.Raw(`
		SELECT 
			COUNT(DISTINCT invocation_id) as invocation_count,
			COUNT(DISTINCT host) as bazel_host_count,
			COUNT(DISTINCT user) as bazel_user_count
		FROM Invocations as i
		WHERE 
			i.created_at_usec >= ? AND
			i.created_at_usec < ?`,
		int64(time.Now().Truncate(24*time.Hour).Add(-24*time.Hour).UnixNano()/1000),
		int64(time.Now().Truncate(24*time.Hour).UnixNano()/1000))

	if err := counts.Scan(stat).Error; err != nil {
		return err
	}
	return nil
}

func (d *InvocationDB) DeleteInvocation(ctx context.Context, invocationID string) error {
	ti := &tables.Invocation{InvocationID: invocationID}
	return d.h.Delete(ti).Error
}

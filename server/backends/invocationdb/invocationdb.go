package invocationdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jinzhu/gorm"

	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
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

func (d *InvocationDB) addPermissionsCheckToQuery(ctx context.Context, q *query_builder.Query) error {
	o := query_builder.OrClauses{}
	o.AddOr("(i.perms & ? != 0)", perms.OTHERS_READ)

	hasUser := false
	if auth := d.env.GetAuthenticator(); auth != nil {
		if u, err := auth.AuthenticatedUser(ctx); err == nil {
			hasUser = true
			if u.GetGroupID() != "" {
				groupArgs := []interface{}{
					perms.GROUP_READ,
					u.GetGroupID(),
				}
				o.AddOr("(i.perms & ? != 0 AND i.group_id = ?)", groupArgs...)
			} else if u.GetUserID() != "" {
				groupArgs := []interface{}{
					perms.GROUP_READ,
				}
				groupParams := make([]string, 0)
				for _, groupID := range u.GetAllowedGroups() {
					groupArgs = append(groupArgs, groupID)
					groupParams = append(groupParams, "?")
				}
				groupParamString := "(" + strings.Join(groupParams, ", ") + ")"
				groupQueryStr := fmt.Sprintf("(i.perms & ? != 0 AND i.group_id IN %s)", groupParamString)
				o.AddOr(groupQueryStr, groupArgs...)
				o.AddOr("(i.perms & ? != 0 AND i.user_id = ?)", perms.OWNER_READ, u.GetUserID())
			}
		}
	}

	if !hasUser && !d.env.GetConfigurator().GetAnonymousUsageEnabled() {
		return status.PermissionDeniedErrorf("Anonymous access disabled, permission denied.")
	}

	orQuery, orArgs := o.Build()
	q = q.AddWhereClause("("+orQuery+")", orArgs...)
	return nil
}

func (d *InvocationDB) LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error) {
	ti := &tables.Invocation{}
	q := query_builder.NewQuery(`SELECT * FROM Invocations as i`)
	q = q.AddWhereClause(`i.invocation_id = ?`, invocationID)
	if err := d.addPermissionsCheckToQuery(ctx, q); err != nil {
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
	if err := d.addPermissionsCheckToQuery(ctx, q); err != nil {
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
	defer rows.Close()
	if err != nil {
		return nil, err
	}

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

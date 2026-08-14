package invocationdb

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/error_tracking"
	"github.com/buildbuddy-io/buildbuddy/server/features"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

// invocationReconnectWindow is how long after an incomplete invocation's
// last DB update the invocation may still be retried. An incomplete
// invocation whose row has not been updated within this window is assumed to
// be abandoned and may not be retried.
const invocationReconnectWindow = 4 * time.Hour

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
	// First, try inserting the invocation. This will work for first attempts.
	result := d.h.GORM(ctx, "invocationdb_insert_invocation").Clauses(clause.OnConflict{DoNothing: true}).Create(ti)
	if result.Error != nil {
		return false, result.Error
	} else if result.RowsAffected > 0 {
		// Insert worked; we're done.
		return true, nil
	}
	// Insert failed due to conflict; update the existing row instead.
	created := false
	err := d.h.Transaction(ctx, func(tx interfaces.DB) error {
		err := tx.NewQuery(ctx, "invocationdb_find_existing_attempt").Raw(`
				SELECT attempt, created_at_usec, error_tracking_incarnation FROM "Invocations"
				WHERE invocation_id = ? AND invocation_status <> ? AND updated_at_usec > ? 
				`+d.h.SelectForUpdateModifier(),
			ti.InvocationID,
			int64(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
			tx.NowFunc().Add(-invocationReconnectWindow).UnixMicro(),
		).Take(ti)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// The invocation either succeeded or is past the reconnect
				// window. It may not be re-attempted.
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
		if ti.ErrorTrackingIncarnation == "" {
			ti.ErrorTrackingIncarnation = uuid.New()
		}
		result = tx.GORM(ctx, "invocationdb_update_invocation_attempt").
			Omit("error_acl_version", "error_occurrences_state", "group_id", "user_id", "perms").
			Updates(ti)
		created = result.RowsAffected > 0
		return result.Error
	})
	return created, err
}

func (d *InvocationDB) CreateInvocation(ctx context.Context, ti *tables.Invocation) (bool, error) {
	permissions, err := perms.ForAuthenticatedGroup(ctx, d.env)
	if err != nil {
		return false, err
	}

	caps, err := capabilities.ForAuthenticatedUser(ctx, d.env.GetAuthenticator())
	if err != nil {
		// Set empty capabilities by default
		caps = []cappb.Capability{}
	}

	ti.UserID = permissions.UserID
	ti.GroupID = permissions.GroupID
	ti.Perms = ti.Perms | permissions.Perms
	ti.CreatedWithCapabilities = capabilities.ToInt(caps)
	ti.ErrorOccurrencesState = error_tracking.ErrorOccurrencesNone
	ti.ErrorTrackingIncarnation = uuid.New()
	return d.registerInvocationAttempt(ctx, ti)
}

func (d *InvocationDB) hasErrorOccurrences(ctx context.Context, in *tables.Invocation) (bool, error) {
	if d.env.GetOLAPDBHandle() == nil {
		return false, nil
	}
	switch in.ErrorOccurrencesState {
	case error_tracking.ErrorOccurrencesPresent:
		return true, nil
	case error_tracking.ErrorOccurrencesNone:
		return false, nil
	}
	// Legacy rows predate the occurrence-state column and read as unknown. When
	// the experimental feature is disabled, do not make ordinary ACL or delete
	// operations depend on ClickHouse just to discover whether such a row has
	// occurrences. Rows explicitly marked present still take the cleanup path.
	if !*features.ErrorTrackingEnabled {
		return false, nil
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	version, err := d.env.GetOLAPDBHandle().GetMaxErrorInvocationACLVersion(lookupCtx, in.GroupID, in.InvocationID)
	return version > 0, err
}

// ensureErrorTrackingIncarnation lazily backfills rows created before the
// incarnation column was added. Callers must hold the invocation's primary DB
// write lock so that every derived ClickHouse write for this row observes the
// same durable identity.
func (d *InvocationDB) ensureErrorTrackingIncarnation(ctx context.Context, tx interfaces.DB, in *tables.Invocation) error {
	if in.ErrorTrackingIncarnation != "" {
		return nil
	}
	incarnation := uuid.New()
	result := tx.NewQuery(ctx, "invocationdb_backfill_error_tracking_incarnation").Raw(
		`UPDATE "Invocations" SET error_tracking_incarnation = ? WHERE invocation_id = ? AND COALESCE(error_tracking_incarnation, '') = ''`,
		incarnation, in.InvocationID,
	).Exec()
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return status.AbortedErrorf("invocation %s changed while backfilling error tracking incarnation", in.InvocationID)
	}
	in.ErrorTrackingIncarnation = incarnation
	return nil
}

// UpdateInvocation updates an existing invocation with the given
// id and attempt number. It returns whether a row was updated.
func (d *InvocationDB) UpdateInvocation(ctx context.Context, ti *tables.Invocation) (bool, error) {
	return retry.Do(ctx, retry.DefaultOptions(), func(ctx context.Context) (bool, error) {
		updated := false
		err := d.h.Transaction(ctx, func(tx interfaces.DB) error {
			q := tx.GORM(ctx, "invocationdb_update_invocation").
				Where("invocation_id = ? AND attempt = ?", ti.InvocationID, ti.Attempt)
			if ti.ErrorTrackingIncarnation != "" {
				q = q.Where("error_tracking_incarnation = ?", ti.ErrorTrackingIncarnation)
			}
			result := q.Omit("error_acl_version", "error_occurrences_state", "error_tracking_incarnation", "group_id", "user_id", "perms").Updates(ti)
			updated = result.RowsAffected > 0
			if result.Error != nil {
				return result.Error
			}
			// The initial invocation row is created before BES metadata has been
			// accumulated, so VISIBILITY=PUBLIC can first appear on a later update.
			// Apply that initial monotonic grant only while no explicit ACL generation
			// exists; user-driven ACL changes continue to own perms after that point.
			if ti.Perms&perms.OTHERS_READ != 0 {
				publicQuery := tx.GORM(ctx, "invocationdb_update_initial_public_visibility").
					Model(&tables.Invocation{}).
					Where("invocation_id = ? AND attempt = ?", ti.InvocationID, ti.Attempt).
					Where("COALESCE(error_acl_version, 0) = 0")
				if ti.ErrorTrackingIncarnation != "" {
					publicQuery = publicQuery.Where("error_tracking_incarnation = ?", ti.ErrorTrackingIncarnation)
				}
				result = publicQuery.Update("perms", gorm.Expr("perms | ?", perms.OTHERS_READ))
				updated = updated || result.RowsAffected > 0
				return result.Error
			}
			return nil
		})
		if err != nil {
			updated = false
		}
		if d.h.IsDeadlockError(err) {
			return updated, status.UnavailableErrorf("update invocation %s: deadlock: %s", ti.InvocationID, err)
		} else if err != nil {
			// Don't retry non-deadlock errors.
			return updated, retry.NonRetryableError(err)
		}
		return updated, nil
	})
}

// repairErrorACLAfterRollback advances the canonical generation and publishes
// the unchanged primary ACL above a pre-commit ClickHouse restriction or
// tombstone whose SQL transaction rolled back.
func (d *InvocationDB) repairErrorACLAfterRollback(ctx context.Context, invocationID, expectedIncarnation string) error {
	if d.env.GetOLAPDBHandle() == nil {
		return nil
	}
	repairCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	var repaired tables.Invocation
	err := d.h.Transaction(repairCtx, func(tx interfaces.DB) error {
		if err := tx.NewQuery(repairCtx, "invocationdb_lock_invocation_for_error_acl_repair").Raw(
			`UPDATE "Invocations" SET perms = perms WHERE invocation_id = ? AND error_tracking_incarnation = ?`, invocationID, expectedIncarnation,
		).Exec().Error; err != nil {
			return err
		}
		if err := tx.NewQuery(repairCtx, "invocationdb_get_invocation_for_error_acl_repair").Raw(
			`SELECT invocation_id, user_id, group_id, perms, error_acl_version, error_occurrences_state, error_tracking_incarnation, created_at_usec FROM "Invocations" WHERE invocation_id = ? AND error_tracking_incarnation = ? `+d.h.SelectForUpdateModifier(), invocationID, expectedIncarnation,
		).Take(&repaired); err != nil {
			if db.IsRecordNotFound(err) {
				return nil
			}
			return err
		}
		// Skip past both the generation that rolled back and any pre-commit
		// tombstone at that generation. Delete writes a committed version for
		// generation N+1 before SQL commit, so repairing at N+1 would create two
		// conflicting rows with the same ClickHouse replacement key.
		repaired.ErrorACLVersion += 2
		return tx.NewQuery(repairCtx, "invocationdb_advance_error_acl_repair_generation").Raw(
			`UPDATE "Invocations" SET error_acl_version = ? WHERE invocation_id = ? AND error_tracking_incarnation = ?`, repaired.ErrorACLVersion, invocationID, expectedIncarnation,
		).Exec().Error
	})
	if err != nil || repaired.InvocationID == "" {
		return err
	}
	hasErrors, err := d.hasErrorOccurrences(repairCtx, &repaired)
	if err != nil || !hasErrors {
		return err
	}
	_, err = error_tracking.FlushInvocationACLStateForIncarnation(repairCtx, d.env, invocationID, expectedIncarnation, repaired.Perms, error_tracking.CommittedACLVersion(repaired.ErrorACLVersion), false)
	return err
}

func (d *InvocationDB) repairErrorACLAfterRollbackIfNeeded(ctx context.Context, invocationID, expectedIncarnation string, transactionErr error, wrotePrecommitState bool) error {
	if transactionErr == nil || !wrotePrecommitState {
		return transactionErr
	}
	if repairErr := d.repairErrorACLAfterRollback(ctx, invocationID, expectedIncarnation); repairErr != nil {
		return errors.Join(transactionErr, status.WrapErrorf(repairErr, "repair error ACL after primary transaction rollback"))
	}
	return transactionErr
}

func (d *InvocationDB) UpdateInvocationACL(ctx context.Context, authenticatedUser *interfaces.UserInfo, invocationID string, acl *aclpb.ACL) error {
	p, err := perms.FromACL(acl)
	if err != nil {
		return err
	}
	var committedACL tables.Invocation
	repairIncarnation := ""
	wrotePrecommitState := false
	err = d.h.Transaction(ctx, func(tx interfaces.DB) error {
		var in tables.Invocation
		if err := tx.NewQuery(ctx, "invocationdb_get_invocation_for_update_acl").Raw(
			`SELECT invocation_id, user_id, group_id, perms, error_acl_version, error_occurrences_state, error_tracking_incarnation, created_at_usec FROM "Invocations" WHERE invocation_id = ? `+d.h.SelectForUpdateModifier(), invocationID).Take(&in); err != nil {
			return err
		}
		var group tables.Group
		if err := tx.NewQuery(ctx, "invocationdb_get_group_for_update_acl").Raw(
			`SELECT sharing_enabled FROM "Groups" WHERE group_id = ?`, in.GroupID).Take(&group); err != nil {
			return err
		}
		if !group.SharingEnabled {
			return status.PermissionDeniedError("Your organization does not allow this action.")
		}

		if err := perms.AuthorizeWrite(authenticatedUser, getACL(&in)); err != nil {
			return err
		}
		if err := tx.NewQuery(ctx, "invocationdb_lock_invocation_for_update_acl").Raw(
			`UPDATE "Invocations" SET perms = perms WHERE invocation_id = ?`, invocationID,
		).Exec().Error; err != nil {
			return err
		}
		// Re-read after acquiring SQLite's write lock; another transaction may
		// have committed between the authorization read and the lock attempt.
		if err := tx.NewQuery(ctx, "invocationdb_refresh_invocation_for_update_acl").Raw(
			`SELECT invocation_id, user_id, group_id, perms, error_acl_version, error_occurrences_state, error_tracking_incarnation, created_at_usec FROM "Invocations" WHERE invocation_id = ?`, invocationID).Take(&in); err != nil {
			return err
		}
		if err := perms.AuthorizeWrite(authenticatedUser, getACL(&in)); err != nil {
			return err
		}
		if err := d.ensureErrorTrackingIncarnation(ctx, tx, &in); err != nil {
			return err
		}
		repairIncarnation = in.ErrorTrackingIncarnation
		// Keep derived error rows synchronized with the current invocation ACL.
		// Remove visibility in ClickHouse before changing the primary DB, then
		// add any new visibility after commit. For mixed changes, the intermediate
		// ACL is the intersection, so neither cross-database failure order can
		// leave stale permissive error rows.
		readBits := int32(perms.OWNER_READ | perms.GROUP_READ | perms.OTHERS_READ)
		oldReadBits, newReadBits := in.Perms&readBits, p&readBits
		removedReadBits := oldReadBits &^ newReadBits
		nextGeneration := in.ErrorACLVersion + 1
		hasErrors, err := d.hasErrorOccurrences(ctx, &in)
		if err != nil {
			return err
		}
		if removedReadBits != 0 && hasErrors {
			intermediatePerms := (p &^ readBits) | (oldReadBits & newReadBits)
			// A returned timeout/error is ambiguous: ClickHouse may have accepted
			// the fail-closed state. Always repair after any transaction failure
			// once the write is attempted.
			wrotePrecommitState = true
			if err := error_tracking.FlushInvocationACLStateWithTimeout(ctx, d.env, &in, intermediatePerms, error_tracking.PendingACLVersion(nextGeneration), false); err != nil {
				return err
			}
		}
		if err := tx.NewQuery(ctx, "invocationdb_update_invocation_acl").Raw(
			`UPDATE "Invocations" SET perms = ?, error_acl_version = ? WHERE invocation_id = ?`, p, nextGeneration, invocationID).Exec().Error; err != nil {
			return err
		}
		if err := tx.NewQuery(ctx, "invocationdb_update_execution_acl").Raw(
			`UPDATE "Executions" SET perms = ? WHERE invocation_id = ?`, p, invocationID).Exec().Error; err != nil {
			return err
		}
		committedACL = in
		committedACL.Perms = p
		committedACL.ErrorACLVersion = nextGeneration
		return nil
	})
	if err != nil {
		return d.repairErrorACLAfterRollbackIfNeeded(ctx, invocationID, repairIncarnation, err, wrotePrecommitState)
	}
	// Publish the committed state after the primary DB update. Visibility
	// expansions therefore fail closed, while restrictions were already made
	// safe by the even-versioned intermediate state above.
	if d.env.GetOLAPDBHandle() != nil {
		hasErrors, err := d.hasErrorOccurrences(ctx, &committedACL)
		if err != nil {
			return err
		}
		if !hasErrors {
			return nil
		}
		_, err = error_tracking.FlushInvocationACLStateForIncarnation(ctx, d.env, invocationID, committedACL.ErrorTrackingIncarnation, committedACL.Perms, error_tracking.CommittedACLVersion(committedACL.ErrorACLVersion), false)
		return err
	}
	return nil
}

func (d *InvocationDB) LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error) {
	ti := &tables.Invocation{}
	if err := d.h.NewQuery(ctx, "invocationdb_get_invocation").Raw(
		`SELECT * FROM "Invocations" WHERE invocation_id = ?`, invocationID).Take(ti); err != nil {
		return nil, err
	}
	if ti.Perms&perms.OTHERS_READ == 0 {
		u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
		if err != nil {
			return nil, err
		}
		if err := perms.AuthorizeRead(u, getACL(ti)); err != nil {
			return nil, err
		}
	}
	return ti, nil
}

func (d *InvocationDB) LookupChildInvocations(ctx context.Context, parentRunID string) ([]string, error) {
	u, err := d.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	rq := d.h.NewQuery(ctx, "invocationdb_get_child_invocations").Raw(
		`SELECT invocation_id FROM "Invocations" WHERE parent_run_id = ? AND group_id = ? ORDER BY created_at_usec`, parentRunID, u.GetGroupID())
	iids := make([]string, 0)
	err = db.ScanEach(rq, func(ctx context.Context, inv *tables.Invocation) error {
		iids = append(iids, inv.InvocationID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return iids, nil
}

func (d *InvocationDB) LookupGroupFromInvocation(ctx context.Context, invocationID string) (*tables.Group, error) {
	ti := &tables.Group{}
	q := query_builder.NewQuery(`SELECT * FROM "Groups" as g JOIN "Invocations" as i ON g.group_id = i.group_id`)
	q = q.AddWhereClause(`i.invocation_id = ?`, invocationID)
	if err := perms.AddPermissionsCheckToQueryWithTableAlias(ctx, d.env, q, "i"); err != nil {
		return nil, err
	}
	queryStr, args := q.Build()
	existingRow := d.h.NewQuery(ctx, "invocationdb_group_for_invocation").Raw(queryStr, args...)
	if err := existingRow.Take(ti); err != nil {
		return nil, err
	}
	return ti, nil
}

func (d *InvocationDB) LookupGroupIDFromInvocation(ctx context.Context, invocationID string) (string, error) {
	in := &tables.Invocation{}
	err := d.h.NewQuery(ctx, "invocationdb_groupd_for_invocation").Raw(
		`SELECT group_id FROM "Invocations" WHERE invocation_id = ?`, invocationID,
	).Take(in)
	if err != nil {
		return "", err
	}
	return in.GroupID, nil
}

func (d *InvocationDB) LookupExpiredInvocations(ctx context.Context, cutoffTime time.Time, limit int) ([]*tables.Invocation, error) {
	cutoffUsec := cutoffTime.UnixMicro()
	rq := d.h.NewQuery(ctx, "invocationdb_get_expired_invocations").Raw(
		`SELECT * FROM "Invocations" as i
             WHERE i.created_at_usec < ?
             LIMIT ?`, cutoffUsec, limit)
	return db.ScanAll(rq, &tables.Invocation{})
}

func (d *InvocationDB) FillCounts(ctx context.Context, stat *telpb.TelemetryStat) error {
	counts := d.h.NewQuery(ctx, "invocationdb_get_counts").Raw(`
		SELECT 
			COUNT(DISTINCT invocation_id) as invocation_count,
			COUNT(DISTINCT host) as bazel_host_count,
			COUNT(DISTINCT user) as bazel_user_count
		FROM "Invocations" as i
		WHERE 
			i.created_at_usec >= ? AND
			i.created_at_usec < ?`,
		time.Now().Truncate(24*time.Hour).Add(-24*time.Hour).UnixMicro(),
		time.Now().Truncate(24*time.Hour).UnixMicro())

	if err := counts.Take(stat); err != nil {
		return err
	}
	return nil
}

func (d *InvocationDB) DeleteInvocation(ctx context.Context, invocationID string) error {
	repairIncarnation := ""
	wrotePrecommitState := false
	err := d.h.Transaction(ctx, func(tx interfaces.DB) error {
		if err := tx.NewQuery(ctx, "invocationdb_lock_invocation_for_delete").Raw(
			`UPDATE "Invocations" SET perms = perms WHERE invocation_id = ?`, invocationID,
		).Exec().Error; err != nil {
			return err
		}
		var in tables.Invocation
		if err := tx.NewQuery(ctx, "invocationdb_get_invocation_for_delete").Raw(
			`SELECT invocation_id, user_id, group_id, error_acl_version, error_occurrences_state, error_tracking_incarnation, created_at_usec FROM "Invocations" WHERE invocation_id = ? `+d.h.SelectForUpdateModifier(), invocationID).Take(&in); err != nil {
			if db.IsRecordNotFound(err) {
				return d.deleteInvocation(ctx, tx, invocationID)
			}
			return err
		}
		if err := d.ensureErrorTrackingIncarnation(ctx, tx, &in); err != nil {
			return err
		}
		repairIncarnation = in.ErrorTrackingIncarnation
		hasErrors, err := d.hasErrorOccurrences(ctx, &in)
		if err != nil {
			return err
		}
		if hasErrors {
			wrotePrecommitState = true
			if err := error_tracking.FlushInvocationACLStateWithTimeout(ctx, d.env, &in, 0, error_tracking.CommittedACLVersion(in.ErrorACLVersion+1), true); err != nil {
				return err
			}
		}
		return d.deleteInvocation(ctx, tx, invocationID)
	})
	if err != nil {
		return d.repairErrorACLAfterRollbackIfNeeded(ctx, invocationID, repairIncarnation, err, wrotePrecommitState)
	}
	// The ACL tombstone makes occurrences immediately unreadable. Let the table
	// TTL purge them in merge batches rather than issuing one synchronous full
	// ClickHouse mutation for every invocation deleted by the janitor.
	return nil
}

func (d *InvocationDB) DeleteInvocationWithPermsCheck(ctx context.Context, authenticatedUser *interfaces.UserInfo, invocationID string) error {
	if authenticatedUser == nil {
		return status.InvalidArgumentError("authenticatedUser cannot be nil.")
	}
	u := *authenticatedUser

	repairIncarnation := ""
	wrotePrecommitState := false
	err := d.h.Transaction(ctx, func(tx interfaces.DB) error {
		var in tables.Invocation
		qb := query_builder.NewQuery(`SELECT invocation_id, user_id, group_id, perms, error_acl_version, error_occurrences_state, error_tracking_incarnation, created_at_usec FROM "Invocations"`)
		qb.AddWhereClause("invocation_id = ?", invocationID)
		if err := perms.AddPermissionsCheckToQuery(ctx, d.env, qb); err != nil {
			return err
		}
		q, args := qb.Build()
		q += " " + d.h.SelectForUpdateModifier()
		if err := tx.NewQuery(ctx, "invocationdb_get_invocation_for_delete").Raw(q, args...).Take(&in); err != nil {
			if db.IsRecordNotFound(err) {
				return status.NotFoundErrorf("No invocation with id %s exists that user %s has write permissions on.", invocationID, u.GetUserID())
			}
			return err
		}
		if err := tx.NewQuery(ctx, "invocationdb_lock_invocation_for_delete").Raw(
			`UPDATE "Invocations" SET perms = perms WHERE invocation_id = ?`, invocationID,
		).Exec().Error; err != nil {
			return err
		}
		// Re-run the permission-filtered read after acquiring SQLite's write lock.
		if err := tx.NewQuery(ctx, "invocationdb_refresh_invocation_for_delete").Raw(q, args...).Take(&in); err != nil {
			if db.IsRecordNotFound(err) {
				return status.NotFoundErrorf("No invocation with id %s exists that user %s has write permissions on.", invocationID, u.GetUserID())
			}
			return err
		}
		if err := d.ensureErrorTrackingIncarnation(ctx, tx, &in); err != nil {
			return err
		}
		repairIncarnation = in.ErrorTrackingIncarnation
		// Append a higher-versioned tombstone before deleting the canonical
		// invocation. Delayed BES ACL states have lower versions and cannot make
		// old error rows visible again.
		hasErrors, err := d.hasErrorOccurrences(ctx, &in)
		if err != nil {
			return err
		}
		if hasErrors {
			wrotePrecommitState = true
			if err := error_tracking.FlushInvocationACLStateWithTimeout(ctx, d.env, &in, 0, error_tracking.CommittedACLVersion(in.ErrorACLVersion+1), true); err != nil {
				return err
			}
		}
		if err := tx.NewQuery(ctx, "invocationdb_delete_invocation_with_perms_check").Raw(
			`DELETE FROM "Invocations" WHERE invocation_id = ?`, invocationID).Exec().Error; err != nil {
			return err
		}
		if err := tx.NewQuery(ctx, "invocationdb_delete_executions").Raw(
			`DELETE FROM "Executions" WHERE invocation_id = ?`, invocationID).Exec().Error; err != nil {
			return err
		}
		if err := tx.NewQuery(ctx, "invocationdb_delete_execution_links").Raw(
			`DELETE FROM "InvocationExecutions" WHERE invocation_id = ?`, invocationID).Exec().Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return d.repairErrorACLAfterRollbackIfNeeded(ctx, invocationID, repairIncarnation, err, wrotePrecommitState)
	}
	// The ACL tombstone makes occurrences immediately unreadable; table TTL
	// handles physical removal in batches.
	return nil
}

func (d *InvocationDB) deleteInvocation(ctx context.Context, tx interfaces.DB, invocationID string) error {
	if err := tx.NewQuery(ctx, "invocationdb_delete_invocation").Raw(
		`DELETE FROM "Invocations" WHERE invocation_id = ?`, invocationID).Exec().Error; err != nil {
		return err
	}
	if err := tx.NewQuery(ctx, "invocationdb_delete_executions").Raw(
		`DELETE FROM "Executions" WHERE invocation_id = ?`, invocationID).Exec().Error; err != nil {
		return err
	}
	if err := tx.NewQuery(ctx, "invocationdb_delete_execution_links").Raw(
		`DELETE FROM "InvocationExecutions" WHERE invocation_id = ?`, invocationID).Exec().Error; err != nil {
		return err
	}
	return nil
}

func (d *InvocationDB) SetNowFunc(now func() time.Time) {
	d.h.SetNowFunc(now)
}

func (d *InvocationDB) GetInvocationReconnectWindow() time.Duration {
	return invocationReconnectWindow
}

func TableInvocationToProto(i *tables.Invocation) *inpb.Invocation {
	out := &inpb.Invocation{}
	out.InvocationId = i.InvocationID // Required.
	out.Success = i.Success
	out.User = i.User
	out.DurationUsec = i.DurationUsec
	out.Host = i.Host
	out.RepoUrl = i.RepoURL
	out.BranchName = i.BranchName
	out.CommitSha = i.CommitSHA
	out.Role = i.Role
	out.Command = i.Command
	if i.Pattern != "" {
		out.Pattern = strings.Split(i.Pattern, ", ")
	}
	out.ActionCount = i.ActionCount
	// BlobID is not present in output client proto.
	out.InvocationStatus = inspb.InvocationStatus(i.InvocationStatus)
	out.CreatedAtUsec = i.Model.CreatedAtUsec
	out.UpdatedAtUsec = i.Model.UpdatedAtUsec
	if i.Perms&perms.OTHERS_READ > 0 {
		out.ReadPermission = inpb.InvocationPermission_PUBLIC
	} else {
		out.ReadPermission = inpb.InvocationPermission_GROUP
	}
	out.CreatedWithCapabilities = capabilities.FromInt(i.CreatedWithCapabilities)
	out.Acl = perms.ToACLProto(&uidpb.UserId{Id: i.UserID}, i.GroupID, i.Perms)
	out.CacheStats = &capb.CacheStats{
		ActionCacheHits:                   i.ActionCacheHits,
		ActionCacheMisses:                 i.ActionCacheMisses,
		ActionCacheUploads:                i.ActionCacheUploads,
		CasCacheHits:                      i.CasCacheHits,
		CasCacheMisses:                    i.CasCacheMisses,
		CasCacheUploads:                   i.CasCacheUploads,
		TotalDownloadSizeBytes:            i.TotalDownloadSizeBytes,
		TotalDownloadTransferredSizeBytes: i.TotalDownloadTransferredSizeBytes,
		TotalUploadSizeBytes:              i.TotalUploadSizeBytes,
		TotalUploadTransferredSizeBytes:   i.TotalUploadTransferredSizeBytes,
		TotalDownloadUsec:                 i.TotalDownloadUsec,
		TotalUploadUsec:                   i.TotalUploadUsec,
		TotalCachedActionExecUsec:         i.TotalCachedActionExecUsec,
		TotalUncachedActionExecUsec:       i.TotalUncachedActionExecUsec,
		DownloadThroughputBytesPerSecond:  i.DownloadThroughputBytesPerSecond,
		UploadThroughputBytesPerSecond:    i.UploadThroughputBytesPerSecond,
	}
	out.LastChunkId = i.LastChunkId
	if i.LastChunkId != "" {
		out.HasChunkedEventLogs = true
	}
	out.Attempt = i.Attempt
	out.BazelExitCode = i.BazelExitCode
	out.DownloadOutputsOption = inpb.DownloadOutputsOption(i.DownloadOutputsOption)
	out.RemoteExecutionEnabled = i.RemoteExecutionEnabled
	out.UploadLocalResultsEnabled = i.UploadLocalResultsEnabled
	// Don't bother with validation here; just give the user whatever the DB
	// claims the tags are.
	out.Tags, _ = invocation_format.SplitAndTrimAndDedupeTags(i.Tags, false)
	out.ParentRunId = i.ParentRunID
	out.RunId = i.RunID
	out.RunStatus = inspb.OverallStatus(i.RunStatus)
	return out
}

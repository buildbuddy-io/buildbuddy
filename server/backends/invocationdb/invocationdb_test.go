package invocationdb_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	"github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/error_tracking"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testolapdb"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/require"
)

var (
	userMap = testauth.TestUsers("user1", "group1")
)

func getEnvAuthAndCtx(t *testing.T) (*testenv.TestEnv, *testauth.TestAuthenticator, context.Context) {
	te := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, userMap)
	te.SetAuthenticator(ta)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	if err != nil {
		t.Errorf("error attaching user prefix: %v", err)
	}
	return te, ta, ctx
}

func TestCreateReadUpdateDelete(t *testing.T) {
	env, authenticator, ctx := getEnvAuthAndCtx(t)

	// Authenticate as user1.
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	dbh := env.GetDBHandle()
	idb := invocationdb.NewInvocationDB(env, dbh)

	for i := 0; i < 10; i++ {
		iid := fmt.Sprintf("invocation-%d", i)
		pattern := fmt.Sprintf("//pattern:%d", i)

		created, err := idb.CreateInvocation(ctx, &tables.Invocation{
			InvocationID: iid,
			Pattern:      pattern,
		})
		require.NoError(t, err)
		require.True(t, created)

		err = dbh.NewQuery(ctx, "insert").Raw(`
			INSERT INTO "InvocationExecutions" (invocation_id, execution_id)
			VALUES (?, ?)`, iid, iid+"-execution").Exec().Error
		require.NoError(t, err)
	}

	// Delete invocation 0 then look up again; should not be found.
	err = idb.DeleteInvocation(ctx, "invocation-0")
	require.NoError(t, err)
	inv, err := idb.LookupInvocation(ctx, "invocation-0")
	require.Nil(t, inv)
	require.True(t, db.IsRecordNotFound(err), "expected RecordNotFound, got: %v", err)
	err = dbh.NewQuery(ctx, "get_invocation_executions").Raw(
		`SELECT * FROM "InvocationExecutions" WHERE invocation_id = ?`,
		"invocation-0",
	).Take(&tables.InvocationExecution{})
	require.True(t, db.IsRecordNotFound(err))

	// Update invocation 1 (attempt 1) then look up again, should be updated.
	updated, err := idb.UpdateInvocation(ctx,
		&tables.Invocation{InvocationID: "invocation-1", Attempt: 1, Pattern: "//updated"})
	require.True(t, updated)
	require.NoError(t, err)
	inv, err = idb.LookupInvocation(ctx, "invocation-1")
	require.NoError(t, err)
	require.Equal(t, "//updated", inv.Pattern)

	// Get invocation 2, should not have changed.
	inv, err = idb.LookupInvocation(ctx, "invocation-2")
	require.NoError(t, err)
	require.Equal(t, "//pattern:2", inv.Pattern)
	require.Equal(t, "user1", inv.UserID)
	require.Equal(t, "group1", inv.GroupID)
	ie := &tables.InvocationExecution{}
	err = dbh.NewQuery(ctx, "get_invocation_executions").Raw(
		`SELECT * FROM "InvocationExecutions" WHERE invocation_id = ?`,
		"invocation-2",
	).Take(ie)
	require.NoError(t, err)
	require.Equal(t, "invocation-2-execution", ie.ExecutionID)
}

func TestUpdateInvocationRejectsReusedIDWithStaleIncarnation(t *testing.T) {
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())
	const invocationID = "stale-background-update"

	original := &tables.Invocation{InvocationID: invocationID}
	created, err := idb.CreateInvocation(ctx, original)
	require.NoError(t, err)
	require.True(t, created)
	require.NotEmpty(t, original.ErrorTrackingIncarnation)
	require.NoError(t, idb.DeleteInvocation(ctx, invocationID))
	replacement := &tables.Invocation{InvocationID: invocationID, Pattern: "//replacement"}
	created, err = idb.CreateInvocation(ctx, replacement)
	require.NoError(t, err)
	require.True(t, created)
	require.NotEqual(t, original.ErrorTrackingIncarnation, replacement.ErrorTrackingIncarnation)

	updated, err := idb.UpdateInvocation(ctx, &tables.Invocation{
		InvocationID:             invocationID,
		Attempt:                  original.Attempt,
		ErrorTrackingIncarnation: original.ErrorTrackingIncarnation,
		Pattern:                  "//stale",
		Perms:                    perms.OTHERS_READ,
	})
	require.NoError(t, err)
	require.False(t, updated)
	got, err := idb.LookupInvocation(ctx, invocationID)
	require.NoError(t, err)
	require.Equal(t, "//replacement", got.Pattern)
	require.Zero(t, got.Perms&perms.OTHERS_READ)
}

func TestDelayedErrorACLWriteRejectsReusedInvocationID(t *testing.T) {
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	olapDB := testolapdb.NewHandle()
	env.SetOLAPDBHandle(olapDB)
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())
	const invocationID = "stale-delayed-error-acl"

	original := &tables.Invocation{InvocationID: invocationID}
	created, err := idb.CreateInvocation(ctx, original)
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, idb.DeleteInvocation(ctx, invocationID))
	replacement := &tables.Invocation{InvocationID: invocationID}
	created, err = idb.CreateInvocation(ctx, replacement)
	require.NoError(t, err)
	require.True(t, created)

	matched, err := error_tracking.FlushInvocationACLStateForIncarnation(
		ctx, env, invocationID, original.ErrorTrackingIncarnation,
		perms.OTHERS_READ, error_tracking.CommittedACLVersion(100), false,
	)
	require.NoError(t, err)
	require.False(t, matched)
	require.Nil(t, olapDB.GetErrorInvocationACL(invocationID))
	got, err := idb.LookupInvocation(ctx, invocationID)
	require.NoError(t, err)
	require.Equal(t, replacement.ErrorTrackingIncarnation, got.ErrorTrackingIncarnation)
	require.Zero(t, got.ErrorACLVersion)
}

func TestUpdateInvocationACLBackfillsLegacyErrorTrackingIncarnation(t *testing.T) {
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	olapDB := testolapdb.NewHandle()
	env.SetOLAPDBHandle(olapDB)
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())
	require.NoError(t, env.GetDBHandle().GORM(ctx, "insert_legacy_acl_group").Create(&tables.Group{
		GroupID: "group1", UserID: "user1", SharingEnabled: true,
	}).Error)
	const invocationID = "legacy-empty-error-tracking-incarnation"
	created, err := idb.CreateInvocation(ctx, &tables.Invocation{InvocationID: invocationID})
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "simulate_legacy_invocation").Raw(`
		UPDATE "Invocations"
		SET error_tracking_incarnation = '', error_occurrences_state = ?
		WHERE invocation_id = ?`, error_tracking.ErrorOccurrencesPresent, invocationID).Exec().Error)

	user, err := authenticator.AuthenticatedUser(ctx)
	require.NoError(t, err)
	publicPerms := int32(perms.OWNER_READ | perms.OWNER_WRITE | perms.GROUP_READ | perms.OTHERS_READ)
	publicACL := perms.ToACLProto(
		&uidpb.UserId{Id: "user1"}, "group1",
		publicPerms,
	)
	require.NoError(t, idb.UpdateInvocationACL(ctx, &user, invocationID, publicACL))

	var got tables.Invocation
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "get_backfilled_invocation").Raw(`
		SELECT error_tracking_incarnation, error_acl_version FROM "Invocations" WHERE invocation_id = ?`,
		invocationID,
	).Take(&got))
	require.NotEmpty(t, got.ErrorTrackingIncarnation)
	require.Equal(t, int64(1), got.ErrorACLVersion)
	acl := olapDB.GetErrorInvocationACL(invocationID)
	require.NotNil(t, acl)
	require.Equal(t, publicPerms, acl.Perms)
	require.Equal(t, error_tracking.CommittedACLVersion(got.ErrorACLVersion), acl.ACLVersion)
}

func TestReconnectPreservesCanonicalACLState(t *testing.T) {
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())
	const invocationID = "reconnect-preserves-acl"

	created, err := idb.CreateInvocation(ctx, &tables.Invocation{InvocationID: invocationID})
	require.NoError(t, err)
	require.True(t, created)
	canonicalPerms := int32(perms.OWNER_READ | perms.OWNER_WRITE)
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "set_private_invocation_acl").Raw(`
		UPDATE "Invocations"
		SET perms = ?, error_acl_version = 7
		WHERE invocation_id = ?`, canonicalPerms, invocationID).Exec().Error)

	// CreateInvocation takes the reconnect path and carries default group perms
	// from the new request. Those stale values must not overwrite ACL-owned
	// columns or reuse the existing error ACL generation with different perms.
	created, err = idb.CreateInvocation(ctx, &tables.Invocation{InvocationID: invocationID})
	require.NoError(t, err)
	require.True(t, created)
	var got tables.Invocation
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "get_reconnected_invocation_acl").Raw(`
		SELECT perms, error_acl_version FROM "Invocations" WHERE invocation_id = ?`, invocationID).Take(&got))
	require.Equal(t, canonicalPerms, got.Perms)
	require.Equal(t, int64(7), got.ErrorACLVersion)
}

func TestUpdateInvocationAppliesInitialBESPublicVisibility(t *testing.T) {
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())
	const invocationID = "initial-bes-public-visibility"

	created, err := idb.CreateInvocation(ctx, &tables.Invocation{InvocationID: invocationID})
	require.NoError(t, err)
	require.True(t, created)
	updated, err := idb.UpdateInvocation(ctx, &tables.Invocation{
		InvocationID: invocationID,
		Attempt:      1,
		Perms:        perms.OTHERS_READ,
	})
	require.NoError(t, err)
	require.True(t, updated)

	var got tables.Invocation
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "get_public_bes_invocation").Raw(`
		SELECT perms, error_acl_version FROM "Invocations" WHERE invocation_id = ?`, invocationID).Take(&got))
	require.NotZero(t, got.Perms&perms.OTHERS_READ)
	require.Zero(t, got.ErrorACLVersion)

	// Once an explicit ACL generation exists, later BES updates cannot restore
	// public visibility over the user's canonical choice.
	privatePerms := int32(perms.OWNER_READ | perms.OWNER_WRITE)
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "set_explicit_private_acl").Raw(`
		UPDATE "Invocations" SET perms = ?, error_acl_version = 7 WHERE invocation_id = ?`, privatePerms, invocationID).Exec().Error)
	updated, err = idb.UpdateInvocation(ctx, &tables.Invocation{
		InvocationID: invocationID,
		Attempt:      1,
		Perms:        perms.OTHERS_READ,
	})
	require.NoError(t, err)
	require.True(t, updated)
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "get_private_bes_invocation").Raw(`
		SELECT perms, error_acl_version FROM "Invocations" WHERE invocation_id = ?`, invocationID).Take(&got))
	require.Equal(t, privatePerms, got.Perms)
	require.Equal(t, int64(7), got.ErrorACLVersion)
}

func TestUpdateInvocationRollsBackMetadataWhenInitialPublicGrantFails(t *testing.T) {
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	if !strings.Contains(env.GetDBHandle().DialectName(), "sqlite") {
		t.Skip("failure injection is SQLite-specific")
	}
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())
	const invocationID = "public-visibility-rollback"

	created, err := idb.CreateInvocation(ctx, &tables.Invocation{InvocationID: invocationID})
	require.NoError(t, err)
	require.True(t, created)
	var before tables.Invocation
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "get_invocation_before_public_failure").Raw(`
		SELECT invocation_status, perms FROM "Invocations" WHERE invocation_id = ?`, invocationID).Take(&before))
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "install_public_visibility_failure_trigger").Raw(`
		CREATE TRIGGER fail_public_visibility
		BEFORE UPDATE OF perms ON Invocations
		WHEN NEW.invocation_id = '`+invocationID+`'
		BEGIN
			SELECT RAISE(FAIL, 'injected public visibility failure');
		END`).Exec().Error)

	updated, err := idb.UpdateInvocation(ctx, &tables.Invocation{
		InvocationID:     invocationID,
		Attempt:          1,
		InvocationStatus: int64(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
		Perms:            perms.OTHERS_READ,
	})
	require.Error(t, err)
	require.False(t, updated)
	var after tables.Invocation
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "get_invocation_after_public_failure").Raw(`
		SELECT invocation_status, perms FROM "Invocations" WHERE invocation_id = ?`, invocationID).Take(&after))
	require.Equal(t, before.InvocationStatus, after.InvocationStatus)
	require.Equal(t, before.Perms, after.Perms)
}

func TestUpdateInvocationACLRollbackRepublishesCanonicalState(t *testing.T) {
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	if !strings.Contains(env.GetDBHandle().DialectName(), "sqlite") {
		t.Skip("failure injection is SQLite-specific")
	}
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	olapDB := testolapdb.NewHandle()
	env.SetOLAPDBHandle(olapDB)
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())
	require.NoError(t, env.GetDBHandle().GORM(ctx, "insert_acl_rollback_group").Create(&tables.Group{
		GroupID: "group1", UserID: "user1", SharingEnabled: true,
	}).Error)
	const invocationID = "acl-rollback-repair"
	created, err := idb.CreateInvocation(ctx, &tables.Invocation{InvocationID: invocationID})
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "mark_acl_rollback_has_errors").Raw(
		`UPDATE "Invocations" SET error_occurrences_state = ? WHERE invocation_id = ?`, error_tracking.ErrorOccurrencesPresent, invocationID,
	).Exec().Error)

	var before tables.Invocation
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "get_acl_before_rollback").Raw(`
		SELECT perms FROM "Invocations" WHERE invocation_id = ?`, invocationID).Take(&before))
	// Force the primary transaction to fail after the restrictive ClickHouse
	// state has been written.
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "drop_executions_for_acl_rollback").Raw(`DROP TABLE "Executions"`).Exec().Error)
	user, err := authenticator.AuthenticatedUser(ctx)
	require.NoError(t, err)
	ownerOnly := perms.ToACLProto(&uidpb.UserId{Id: "user1"}, "group1", perms.OWNER_READ|perms.OWNER_WRITE)
	require.Error(t, idb.UpdateInvocationACL(ctx, &user, invocationID, ownerOnly))

	var after tables.Invocation
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "get_acl_after_rollback").Raw(`
		SELECT perms, error_acl_version FROM "Invocations" WHERE invocation_id = ?`, invocationID).Take(&after))
	require.Equal(t, before.Perms, after.Perms)
	require.Equal(t, int64(2), after.ErrorACLVersion)
	acl := olapDB.GetErrorInvocationACL(invocationID)
	require.NotNil(t, acl)
	require.Equal(t, before.Perms, acl.Perms)
	require.Equal(t, int64(5), acl.ACLVersion)
}

func TestUpdateInvocationACLAmbiguousPrecommitFailureRepairsCanonicalState(t *testing.T) {
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	olapDB := testolapdb.NewHandle()
	env.SetOLAPDBHandle(olapDB)
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())
	require.NoError(t, env.GetDBHandle().GORM(ctx, "insert_ambiguous_acl_group").Create(&tables.Group{
		GroupID: "group1", UserID: "user1", SharingEnabled: true,
	}).Error)
	const invocationID = "ambiguous-acl-write-repair"
	created, err := idb.CreateInvocation(ctx, &tables.Invocation{InvocationID: invocationID})
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "mark_ambiguous_acl_has_errors").Raw(
		`UPDATE "Invocations" SET error_occurrences_state = ? WHERE invocation_id = ?`, error_tracking.ErrorOccurrencesPresent, invocationID,
	).Exec().Error)

	var before tables.Invocation
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "get_acl_before_ambiguous_write").Raw(`
		SELECT perms FROM "Invocations" WHERE invocation_id = ?`, invocationID).Take(&before))
	olapDB.SetNextErrorACLUpdateError(context.DeadlineExceeded)
	user, err := authenticator.AuthenticatedUser(ctx)
	require.NoError(t, err)
	ownerOnly := perms.ToACLProto(&uidpb.UserId{Id: "user1"}, "group1", perms.OWNER_READ|perms.OWNER_WRITE)
	require.Error(t, idb.UpdateInvocationACL(ctx, &user, invocationID, ownerOnly))

	var after tables.Invocation
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "get_acl_after_ambiguous_write").Raw(`
		SELECT perms, error_acl_version FROM "Invocations" WHERE invocation_id = ?`, invocationID).Take(&after))
	require.Equal(t, before.Perms, after.Perms)
	require.Equal(t, int64(2), after.ErrorACLVersion)
	acl := olapDB.GetErrorInvocationACL(invocationID)
	require.NotNil(t, acl)
	require.Equal(t, before.Perms, acl.Perms)
	require.Equal(t, int64(5), acl.ACLVersion)
}

func TestDeleteInvocationRollbackRepublishesAboveTombstone(t *testing.T) {
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	if !strings.Contains(env.GetDBHandle().DialectName(), "sqlite") {
		t.Skip("failure injection is SQLite-specific")
	}
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	olapDB := testolapdb.NewHandle()
	env.SetOLAPDBHandle(olapDB)
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())
	require.NoError(t, env.GetDBHandle().GORM(ctx, "insert_delete_rollback_group").Create(&tables.Group{
		GroupID: "group1", UserID: "user1", SharingEnabled: true,
	}).Error)
	const invocationID = "delete-rollback-repair"
	created, err := idb.CreateInvocation(ctx, &tables.Invocation{InvocationID: invocationID})
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "mark_delete_rollback_has_errors").Raw(
		`UPDATE "Invocations" SET error_occurrences_state = ? WHERE invocation_id = ?`, error_tracking.ErrorOccurrencesPresent, invocationID,
	).Exec().Error)

	var before tables.Invocation
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "get_delete_before_rollback").Raw(`
		SELECT perms FROM "Invocations" WHERE invocation_id = ?`, invocationID).Take(&before))
	// Force deletion to fail after its ClickHouse tombstone has been written.
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "drop_executions_for_delete_rollback").Raw(`DROP TABLE "Executions"`).Exec().Error)
	require.Error(t, idb.DeleteInvocation(ctx, invocationID))

	var after tables.Invocation
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "get_delete_after_rollback").Raw(`
		SELECT perms, error_acl_version FROM "Invocations" WHERE invocation_id = ?`, invocationID).Take(&after))
	require.Equal(t, before.Perms, after.Perms)
	require.Equal(t, int64(2), after.ErrorACLVersion)
	acl := olapDB.GetErrorInvocationACL(invocationID)
	require.NotNil(t, acl)
	require.Equal(t, before.Perms, acl.Perms)
	require.False(t, acl.Deleted)
	require.Equal(t, int64(5), acl.ACLVersion)
}

func TestDeleteInvocationWithoutErrorsDoesNotWriteOLAPACL(t *testing.T) {
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	olapDB := testolapdb.NewHandle()
	env.SetOLAPDBHandle(olapDB)
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())
	const invocationID = "invocation-without-errors"
	created, err := idb.CreateInvocation(ctx, &tables.Invocation{InvocationID: invocationID})
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, idb.DeleteInvocation(ctx, invocationID))
	require.Nil(t, olapDB.GetErrorInvocationACL(invocationID))
}

func TestDeleteLegacyInvocationWhenErrorTrackingDisabledSkipsOLAPLookup(t *testing.T) {
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	olapDB := testolapdb.NewHandle()
	olapDB.SetMaxErrorInvocationACLVersionError(errors.New("clickhouse unavailable"))
	env.SetOLAPDBHandle(olapDB)
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())
	const invocationID = "legacy-invocation-while-disabled"
	created, err := idb.CreateInvocation(ctx, &tables.Invocation{InvocationID: invocationID})
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, env.GetDBHandle().NewQuery(ctx, "mark_legacy_error_state_unknown").Raw(
		`UPDATE "Invocations" SET error_occurrences_state = ? WHERE invocation_id = ?`, error_tracking.ErrorOccurrencesUnknown, invocationID,
	).Exec().Error)

	require.NoError(t, idb.DeleteInvocation(ctx, invocationID))
	_, err = idb.LookupInvocation(ctx, invocationID)
	require.True(t, db.IsRecordNotFound(err), "expected RecordNotFound, got: %v", err)
}

func TestAttemptLogic(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	dbh := env.GetDBHandle()
	idb := invocationdb.NewInvocationDB(env, dbh)

	i := 1
	iid := fmt.Sprintf("invocation-%d", i)
	pattern := fmt.Sprintf("//pattern:%d", i)

	dbh.SetNowFunc(func() time.Time { return time.Unix(0, 0) })

	ti1 := &tables.Invocation{InvocationID: iid, Pattern: pattern}
	created, err := idb.CreateInvocation(ctx, ti1)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, uint64(1), ti1.Attempt)

	dbh.SetNowFunc(func() time.Time { return time.Unix(int64((time.Hour * 4).Seconds()), 0) })

	ti2 := &tables.Invocation{InvocationID: iid, Pattern: pattern}
	created, err = idb.CreateInvocation(ctx, ti2)
	require.NoError(t, err)
	require.False(t, created)

	dbh.SetNowFunc(func() time.Time { return time.Unix(int64((time.Hour*4).Seconds()-1), 0) })

	ti3 := &tables.Invocation{InvocationID: iid, Pattern: pattern}
	created, err = idb.CreateInvocation(ctx, ti3)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, uint64(2), ti3.Attempt)

	dbh.SetNowFunc(func() time.Time { return time.Unix(int64((time.Hour*4).Seconds()+1), 0) })

	ti4 := &tables.Invocation{InvocationID: iid, Pattern: pattern, InvocationStatus: int64(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS)}
	created, err = idb.CreateInvocation(ctx, ti4)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, uint64(3), ti4.Attempt)

	dbh.SetNowFunc(func() time.Time { return time.Unix(int64((time.Hour*4).Seconds()+2), 0) })

	ti5 := &tables.Invocation{InvocationID: iid, Pattern: pattern}
	created, err = idb.CreateInvocation(ctx, ti5)
	require.NoError(t, err)
	require.False(t, created)
}

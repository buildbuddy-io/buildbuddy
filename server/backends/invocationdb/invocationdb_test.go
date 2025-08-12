package invocationdb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	"github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
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

	for i := range 10 {
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

func TestDeleteInvocations(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	dbh := env.GetDBHandle()
	idb := invocationdb.NewInvocationDB(env, dbh)

	for _, invocationID := range []string{"delete-1", "delete-2", "keep"} {
		executionID := invocationID + "-execution"
		require.NoError(t, dbh.NewQuery(ctx, "insert_invocation").Raw(
			`INSERT INTO "Invocations" (invocation_id) VALUES (?)`, invocationID).Exec().Error)
		require.NoError(t, dbh.NewQuery(ctx, "insert_execution").Raw(
			`INSERT INTO "Executions" (execution_id, invocation_id) VALUES (?, ?)`, executionID, invocationID).Exec().Error)
		require.NoError(t, dbh.NewQuery(ctx, "insert_invocation_execution").Raw(
			`INSERT INTO "InvocationExecutions" (invocation_id, execution_id) VALUES (?, ?)`, invocationID, executionID).Exec().Error)
	}

	// Duplicate IDs must not affect the rows that are not selected for deletion.
	require.NoError(t, idb.DeleteInvocations(ctx, []string{"delete-1", "delete-2", "delete-1"}))
	for _, table := range []string{"Invocations", "Executions", "InvocationExecutions"} {
		var count int
		require.NoError(t, dbh.NewQuery(ctx, "count_remaining_rows").Raw(
			fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE invocation_id = ?`, table), "keep").Take(&count))
		require.Equal(t, 1, count, "table %s", table)
		require.NoError(t, dbh.NewQuery(ctx, "count_deleted_rows").Raw(
			fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE invocation_id IN (?, ?)`, table), "delete-1", "delete-2").Take(&count))
		require.Zero(t, count, "table %s", table)
	}

	// An empty batch is deliberately a no-op.
	require.NoError(t, idb.DeleteInvocations(ctx, nil))
	var count int
	require.NoError(t, dbh.NewQuery(ctx, "count_remaining_invocations").Raw(
		`SELECT COUNT(*) FROM "Invocations" WHERE invocation_id = ?`, "keep").Take(&count))
	require.Equal(t, 1, count)

	// Overlapping cleanup batches may race across app pods. Already-deleted and
	// missing IDs must be harmless and must not affect unrelated rows.
	require.NoError(t, idb.DeleteInvocations(ctx, []string{"delete-1", "missing"}))
	require.NoError(t, dbh.NewQuery(ctx, "count_remaining_after_overlapping_batch").Raw(
		`SELECT COUNT(*) FROM "Invocations" WHERE invocation_id = ?`, "keep").Take(&count))
	require.Equal(t, 1, count)
}

func TestDeleteInvocationsRollsBackOnFailure(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	dbh := env.GetDBHandle()
	idb := invocationdb.NewInvocationDB(env, dbh)

	require.NoError(t, dbh.NewQuery(ctx, "insert_invocation").Raw(
		`INSERT INTO "Invocations" (invocation_id) VALUES (?)`, "rollback").Exec().Error)
	require.NoError(t, dbh.NewQuery(ctx, "insert_execution").Raw(
		`INSERT INTO "Executions" (execution_id, invocation_id) VALUES (?, ?)`, "rollback-execution", "rollback").Exec().Error)
	require.NoError(t, dbh.NewQuery(ctx, "drop_invocation_executions").Raw(
		`DROP TABLE "InvocationExecutions"`).Exec().Error)

	require.Error(t, idb.DeleteInvocations(ctx, []string{"rollback"}))
	for _, table := range []string{"Invocations", "Executions"} {
		var count int
		require.NoError(t, dbh.NewQuery(ctx, "count_rows_after_rollback").Raw(
			fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE invocation_id = ?`, table), "rollback").Take(&count))
		require.Equal(t, 1, count, "table %s", table)
	}
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

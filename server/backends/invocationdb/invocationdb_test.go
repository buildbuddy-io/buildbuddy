package invocationdb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	"github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/stretchr/testify/require"
)

func TestCreateReadUpdateDelete(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
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

		err = dbh.DB(ctx).Exec(`
			INSERT INTO "InvocationExecutions" (invocation_id, execution_id)
			VALUES (?, ?)`, iid, iid+"-execution").Error
		require.NoError(t, err)
	}

	// Delete invocation 0 then look up again; should not be found.
	err := idb.DeleteInvocation(ctx, "invocation-0")
	require.NoError(t, err)
	inv, err := idb.LookupInvocation(ctx, "invocation-0")
	require.Nil(t, inv)
	require.True(t, db.IsRecordNotFound(err), "expected RecordNotFound, got: %v", err)
	err = dbh.DB(ctx).Raw(
		`SELECT * FROM "InvocationExecutions" WHERE invocation_id = ?`,
		"invocation-0",
	).Take(&tables.InvocationExecution{}).Error
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
	ie := &tables.InvocationExecution{}
	err = dbh.DB(ctx).Raw(
		`SELECT * FROM "InvocationExecutions" WHERE invocation_id = ?`,
		"invocation-2",
	).Take(ie).Error
	require.NoError(t, err)
	require.Equal(t, "invocation-2-execution", ie.ExecutionID)
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

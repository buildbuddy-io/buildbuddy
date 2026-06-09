package invocationdb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	"github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testolapdb"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
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

func makeExperimentProvider(t *testing.T, defaultVariant string) interfaces.ExperimentFlagProvider {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		"olap.write_all_invocations": {
			State:          memprovider.Enabled,
			DefaultVariant: defaultVariant,
			Variants: map[string]any{
				"true":  true,
				"false": false,
			},
		},
	})
	require.NoError(t, openfeature.SetNamedProviderAndWait(t.Name(), testProvider))
	fp, err := experiments.NewFlagProvider(t.Name())
	require.NoError(t, err)
	return fp
}

func TestDualWriteToAllInvocations(t *testing.T) {
	flags.Set(t, "app.enable_write_all_invocations_to_olap_db", true)
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	olap := testolapdb.NewHandle()
	env.SetOLAPDBHandle(olap)
	env.SetExperimentFlagProvider(makeExperimentProvider(t, "true"))
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())

	iid := "inv-enrolled"
	created, err := idb.CreateInvocation(ctx, &tables.Invocation{InvocationID: iid, Pattern: "//x"})
	require.NoError(t, err)
	require.True(t, created)

	// Enrolled groups dual-write the full canonical row to AllInvocations.
	got := olap.GetAllInvocation(iid)
	require.NotNil(t, got, "expected enrolled invocation to be dual-written to AllInvocations")
	require.Equal(t, iid, got.InvocationID)
	require.Equal(t, "group1", got.GroupID, "dual-written row should be the full canonical row (with group_id)")
}

func TestDualWriteToAllInvocations_NotEnrolled(t *testing.T) {
	flags.Set(t, "app.enable_write_all_invocations_to_olap_db", true)
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	olap := testolapdb.NewHandle()
	env.SetOLAPDBHandle(olap)
	env.SetExperimentFlagProvider(makeExperimentProvider(t, "false"))
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())

	iid := "inv-not-enrolled"
	_, err = idb.CreateInvocation(ctx, &tables.Invocation{InvocationID: iid})
	require.NoError(t, err)
	require.Nil(t, olap.GetAllInvocation(iid), "un-enrolled group must not dual-write")
}

func TestDualWriteToAllInvocations_GlobalFlagOff(t *testing.T) {
	// The global flag defaults to false; leave it unset.
	env, authenticator, ctx := getEnvAuthAndCtx(t)
	ctx, err := authenticator.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)
	olap := testolapdb.NewHandle()
	env.SetOLAPDBHandle(olap)
	env.SetExperimentFlagProvider(makeExperimentProvider(t, "true"))
	idb := invocationdb.NewInvocationDB(env, env.GetDBHandle())

	iid := "inv-flag-off"
	_, err = idb.CreateInvocation(ctx, &tables.Invocation{InvocationID: iid})
	require.NoError(t, err)
	require.Nil(t, olap.GetAllInvocation(iid), "global flag off must disable dual-write")
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

package tenantquery_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/tenantquery"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tqpb "github.com/buildbuddy-io/buildbuddy/proto/tenant_query"
)

// setup starts ClickHouse with tenant queries enabled, and inserts two
// invocations and an execution for group GR1, and an invocation for group GR2.
func setup(t *testing.T) *real_environment.RealEnv {
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers()))
	testclickhouse.GetOrStart(t)
	flags.Set(t, "olap_database.tenant_queries.data_source", testclickhouse.TenantDataSource(t, "bb_tenant", "tenant-password"))
	testclickhouse.Configure(t, env)

	db := env.GetOLAPDBHandle().GORM(t.Context(), "test_insert")
	err := db.Exec(`INSERT INTO Invocations (group_id, updated_at_usec, invocation_uuid, user) VALUES
		('GR1', 1, 'a', 'alice'), ('GR1', 2, 'b', 'alice'), ('GR2', 3, 'c', 'bob')`).Error
	require.NoError(t, err)
	err = db.Exec(`INSERT INTO Executions (group_id, updated_at_usec, invocation_uuid, execution_uuid, worker) VALUES
		('GR1', 1, 'a', '00000000-0000-0000-0000-000000000001', 'executor-1')`).Error
	require.NoError(t, err)
	return env
}

func groupContext(t *testing.T, groupID string) context.Context {
	return testauth.WithAuthenticatedUserInfo(t.Context(), testauth.User("US1", groupID))
}

func TestQuery_ReadsOnlyRowsOfAuthenticatedGroup(t *testing.T) {
	env := setup(t)
	service := env.GetTenantQueryService()
	req := &tqpb.QueryRequest{Query: "SELECT group_id, count() AS n FROM Invocations GROUP BY group_id"}

	rsp, err := service.Query(groupContext(t, "GR1"), req)
	require.NoError(t, err)
	assert.Equal(t, "group_id\tn\nGR1\t2\n", rsp.GetOutput())
	assert.Greater(t, rsp.GetReadRows(), int64(0))
	assert.Greater(t, rsp.GetReadBytes(), int64(0))

	rsp, err = service.Query(groupContext(t, "GR2"), req)
	require.NoError(t, err)
	assert.Equal(t, "group_id\tn\nGR2\t1\n", rsp.GetOutput())

	// The row policy must not apply to the OLAP DB handle's user.
	var count int64
	err = env.GetOLAPDBHandle().GORM(t.Context(), "test_count").Raw("SELECT count() FROM Invocations").Scan(&count).Error
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

func TestQuery_CannotChangeGroup(t *testing.T) {
	env := setup(t)
	service := env.GetTenantQueryService()
	req := &tqpb.QueryRequest{Query: "SELECT count() FROM Invocations SETTINGS SQL_group_id = 'GR2'"}

	_, err := service.Query(groupContext(t, "GR1"), req)

	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied, got %v", err)
	assert.Contains(t, err.Error(), "readonly")
}

func TestQuery_ReadsOnlyTenantFields(t *testing.T) {
	env := setup(t)
	service := env.GetTenantQueryService()
	ctx := groupContext(t, "GR1")

	// ClickHouse only lists the tables and columns that the user can read.
	rsp, err := service.Query(ctx, &tqpb.QueryRequest{Query: "SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name"})
	require.NoError(t, err)
	assert.Equal(t, "name\nExecutions\nInvocations\nTestTargetStatuses\n", rsp.GetOutput())
	rsp, err = service.Query(ctx, &tqpb.QueryRequest{Query: "SELECT name FROM system.columns WHERE table = 'Executions' AND name IN ('target_label', 'worker')"})
	require.NoError(t, err)
	assert.Equal(t, "name\ntarget_label\n", rsp.GetOutput())

	for _, query := range []string{
		"SELECT worker FROM Executions",
		"SELECT count() FROM AuditLogs",
		"SELECT count() FROM RawUsage",
		"SELECT count() FROM Usage",
		// All tenant queries run as the same ClickHouse user, so this table
		// would show the queries of other groups.
		"SELECT query FROM system.processes",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := service.Query(ctx, &tqpb.QueryRequest{Query: query})
			require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied, got %v", err)
		})
	}
}

func TestQuery_InvalidSQL(t *testing.T) {
	env := setup(t)
	service := env.GetTenantQueryService()

	_, err := service.Query(groupContext(t, "GR1"), &tqpb.QueryRequest{Query: "SELECT FROM WHERE"})

	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got %v", err)
	assert.Contains(t, err.Error(), "Syntax error")
	assert.NotContains(t, err.Error(), "(version")
}

func TestQuery_QuotaIsPerGroup(t *testing.T) {
	flags.Set(t, "olap_database.tenant_queries.max_queries_per_hour", int64(5))
	env := setup(t)
	service := env.GetTenantQueryService()
	req := &tqpb.QueryRequest{Query: "SELECT count() FROM Invocations"}

	for range 5 {
		_, err := service.Query(groupContext(t, "GR1"), req)
		require.NoError(t, err)
	}
	_, err := service.Query(groupContext(t, "GR1"), req)
	require.True(t, status.IsResourceExhaustedError(err), "expected ResourceExhausted, got %v", err)
	_, err = service.Query(groupContext(t, "GR2"), req)
	require.NoError(t, err)

	// Running the migrations again must not reset the usage of GR1.
	err = tenantquery.RunMigrations(env.GetOLAPDBHandle().GORM(t.Context(), "test_migrate"))
	require.NoError(t, err)
	_, err = service.Query(groupContext(t, "GR1"), req)
	require.True(t, status.IsResourceExhaustedError(err), "expected ResourceExhausted, got %v", err)
}

func TestRunMigrations_AppliesLimits(t *testing.T) {
	env := setup(t)
	service := env.GetTenantQueryService()
	ctx := groupContext(t, "GR1")
	req := &tqpb.QueryRequest{Query: "SELECT getSetting('max_execution_time') AS max_execution_time, getSetting('max_threads') AS max_threads"}

	rsp, err := service.Query(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "max_execution_time\tmax_threads\n60\t4\n", rsp.GetOutput())

	flags.Set(t, "olap_database.tenant_queries.max_execution_time", 30*time.Second)
	flags.Set(t, "olap_database.tenant_queries.max_threads", int64(2))
	err = tenantquery.RunMigrations(env.GetOLAPDBHandle().GORM(t.Context(), "test_migrate"))
	require.NoError(t, err)

	rsp, err = service.Query(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "max_execution_time\tmax_threads\n30\t2\n", rsp.GetOutput())
}

func TestRunMigrations_RevokesFieldsThatAreNotTenantFields(t *testing.T) {
	env := setup(t)
	service := env.GetTenantQueryService()
	ctx := groupContext(t, "GR1")
	req := &tqpb.QueryRequest{Query: "SELECT worker FROM Executions"}
	db := env.GetOLAPDBHandle().GORM(t.Context(), "test_grant")
	err := db.Exec("GRANT SELECT(worker) ON Executions TO bb_tenant_query").Error
	require.NoError(t, err)
	rsp, err := service.Query(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "worker\nexecutor-1\n", rsp.GetOutput())

	err = tenantquery.RunMigrations(db)
	require.NoError(t, err)

	_, err = service.Query(ctx, req)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied, got %v", err)
}

func TestQuery_RequiresAuthenticatedGroup(t *testing.T) {
	env := testenv.GetTestEnv(t)
	flags.Set(t, "olap_database.tenant_queries.data_source", "http://bb_tenant:tenant-password@127.0.0.1:8123/buildbuddy_test")
	err := tenantquery.Register(env)
	require.NoError(t, err)

	_, err = env.GetTenantQueryService().Query(t.Context(), &tqpb.QueryRequest{Query: "SELECT 1"})

	require.True(t, status.IsUnauthenticatedError(err), "expected Unauthenticated, got %v", err)
}

func TestRegister_InvalidDataSource(t *testing.T) {
	env := testenv.GetTestEnv(t)
	for _, testCase := range []struct {
		name       string
		dataSource string
	}{
		{name: "NativeProtocol", dataSource: "clickhouse://bb_tenant:tenant-password@127.0.0.1:9000/buildbuddy_test"},
		{name: "NoUser", dataSource: "http://127.0.0.1:8123/buildbuddy_test"},
		{name: "UserNeedsQuoting", dataSource: "http://bb-tenant:tenant-password@127.0.0.1:8123/buildbuddy_test"},
		{name: "NoPassword", dataSource: "http://bb_tenant@127.0.0.1:8123/buildbuddy_test"},
		{name: "NoDatabase", dataSource: "http://bb_tenant:tenant-password@127.0.0.1:8123"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			flags.Set(t, "olap_database.tenant_queries.data_source", testCase.dataSource)

			err := tenantquery.Register(env)

			require.Error(t, err)
			assert.Nil(t, env.GetTenantQueryService())
		})
	}
}

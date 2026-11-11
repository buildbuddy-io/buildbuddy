package tenantquery_test

import (
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/tenantquery"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	chproto "github.com/ClickHouse/ch-go/proto"
)

// setup starts ClickHouse, runs the migrations with a tenant data source, and
// inserts two invocations and an execution for group GR1, and an invocation
// for group GR2. It returns the env and the tenant data source.
func setup(t *testing.T) (*real_environment.RealEnv, string) {
	env := testenv.GetTestEnv(t)
	dataSource := testclickhouse.GetOrStart(t).TenantDataSource("bb_tenant", "tenant-password")
	flags.Set(t, "olap_database.tenant_queries.data_source", dataSource)
	testclickhouse.Configure(t, env)

	db := env.GetOLAPDBHandle().GORM(t.Context(), "test_insert")
	err := db.Exec(`INSERT INTO Invocations (group_id, updated_at_usec, invocation_uuid, user) VALUES
		('GR1', 1, 'a', 'alice'), ('GR1', 2, 'b', 'alice'), ('GR2', 3, 'c', 'bob')`).Error
	require.NoError(t, err)
	err = db.Exec(`INSERT INTO Executions (group_id, updated_at_usec, invocation_uuid, execution_uuid, worker) VALUES
		('GR1', 1, 'a', '00000000-0000-0000-0000-000000000001', 'executor-1')`).Error
	require.NoError(t, err)
	return env, dataSource
}

func TestRunMigrations_TenantReadsOnlyRowsOfItsGroup(t *testing.T) {
	env, dataSource := setup(t)
	query := "SELECT group_id, count() AS n FROM Invocations GROUP BY group_id"

	output, code := testclickhouse.TenantQuery(t, dataSource, "GR1", query)
	require.Zero(t, code, output)
	assert.Equal(t, "group_id\tn\nGR1\t2\n", output)

	output, code = testclickhouse.TenantQuery(t, dataSource, "GR2", query)
	require.Zero(t, code, output)
	assert.Equal(t, "group_id\tn\nGR2\t1\n", output)

	// ClickHouse must not apply the row policy to the OLAP DB handle's user.
	var count int64
	err := env.GetOLAPDBHandle().GORM(t.Context(), "test_count").Raw("SELECT count() FROM Invocations").Scan(&count).Error
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

func TestRunMigrations_TenantCannotChangeGroup(t *testing.T) {
	_, dataSource := setup(t)

	output, code := testclickhouse.TenantQuery(t, dataSource, "GR1", "SELECT count() FROM Invocations SETTINGS SQL_group_id = 'GR2'")

	assert.Equal(t, chproto.ErrReadonly, code, output)
}

func TestRunMigrations_TenantCannotChangeGroupInSubquery(t *testing.T) {
	_, dataSource := setup(t)

	output, code := testclickhouse.TenantQuery(t, dataSource, "GR1", "SELECT group_id, invocation_uuid FROM (SELECT group_id, invocation_uuid FROM Invocations SETTINGS SQL_group_id = 'GR2') ORDER BY invocation_uuid")

	require.Zero(t, code, output)
	assert.Equal(t, "group_id\tinvocation_uuid\nGR1\ta\nGR1\tb\n", output)
}

func TestRunMigrations_TenantCannotResetSettingsToDefault(t *testing.T) {
	_, dataSource := setup(t)
	for _, setting := range []string{"readonly", "max_execution_time", "max_memory_usage", "max_result_rows"} {
		t.Run(setting, func(t *testing.T) {
			output, code := testclickhouse.TenantQuery(t, dataSource, "GR1", "SELECT getSetting('"+setting+"') SETTINGS "+setting+" = DEFAULT")

			assert.Equal(t, chproto.ErrReadonly, code, output)
		})
	}
}

func TestRunMigrations_TenantCannotLiftLimitsInSubquery(t *testing.T) {
	_, dataSource := setup(t)
	for _, testCase := range []struct {
		name    string
		setting string
		value   string
		// ClickHouse ignores the subquery's SETTINGS clause, so the query sees
		// the value from the settings profile.
		want string
	}{
		{name: "ResetTimeout", setting: "max_execution_time", value: "DEFAULT", want: "60"},
		{name: "RemoveTimeout", setting: "max_execution_time", value: "0", want: "60"},
		{name: "ResetMemoryLimit", setting: "max_memory_usage", value: "DEFAULT", want: "4000000000"},
		{name: "RemoveMemoryLimit", setting: "max_memory_usage", value: "0", want: "4000000000"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			query := "SELECT * FROM (SELECT getSetting('" + testCase.setting + "') AS v SETTINGS " + testCase.setting + " = " + testCase.value + ")"

			output, code := testclickhouse.TenantQuery(t, dataSource, "GR1", query)

			require.Zero(t, code, output)
			assert.Equal(t, "v\n"+testCase.want+"\n", output)
		})
	}
}

func TestRunMigrations_TenantReadsOnlyVisibleFields(t *testing.T) {
	_, dataSource := setup(t)

	// ClickHouse only lists the tables and columns that the user can read.
	output, code := testclickhouse.TenantQuery(t, dataSource, "GR1", "SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name")
	require.Zero(t, code, output)
	assert.Equal(t, "name\nExecutions\nInvocations\nTestTargetStatuses\n", output)
	output, code = testclickhouse.TenantQuery(t, dataSource, "GR1", "SELECT name FROM system.columns WHERE table = 'Executions' AND name IN ('target_label', 'worker')")
	require.Zero(t, code, output)
	assert.Equal(t, "name\ntarget_label\n", output)

	for _, query := range []string{
		"SELECT worker FROM Executions",
		"SELECT count() FROM AuditLogs",
		"SELECT count() FROM RawUsage",
		"SELECT count() FROM Usage",
		// Every group queries as the same ClickHouse user, so tenants could
		// see the queries of other groups in system.processes.
		"SELECT query FROM system.processes",
	} {
		t.Run(query, func(t *testing.T) {
			output, code := testclickhouse.TenantQuery(t, dataSource, "GR1", query)
			assert.Equal(t, chproto.ErrAccessDenied, code, output)
		})
	}
}

func TestRunMigrations_AppliesLimits(t *testing.T) {
	env, dataSource := setup(t)
	query := "SELECT getSetting('max_execution_time') AS max_execution_time, getSetting('max_threads') AS max_threads"

	output, code := testclickhouse.TenantQuery(t, dataSource, "GR1", query)
	require.Zero(t, code, output)
	assert.Equal(t, "max_execution_time\tmax_threads\n60\t4\n", output)

	flags.Set(t, "olap_database.tenant_queries.max_execution_time", 30*time.Second)
	flags.Set(t, "olap_database.tenant_queries.max_threads", int64(2))
	err := tenantquery.RunMigrations(env.GetOLAPDBHandle().GORM(t.Context(), "test_migrate"))
	require.NoError(t, err)

	output, code = testclickhouse.TenantQuery(t, dataSource, "GR1", query)
	require.Zero(t, code, output)
	assert.Equal(t, "max_execution_time\tmax_threads\n30\t2\n", output)
}

func TestRunMigrations_RevokesFieldsThatAreNotVisible(t *testing.T) {
	env, dataSource := setup(t)
	query := "SELECT worker FROM Executions"
	db := env.GetOLAPDBHandle().GORM(t.Context(), "test_grant")
	err := db.Exec("GRANT SELECT(worker) ON Executions TO bb_tenant_query").Error
	require.NoError(t, err)
	output, code := testclickhouse.TenantQuery(t, dataSource, "GR1", query)
	require.Zero(t, code, output)
	require.Equal(t, "worker\nexecutor-1\n", output)

	err = tenantquery.RunMigrations(db)
	require.NoError(t, err)

	output, code = testclickhouse.TenantQuery(t, dataSource, "GR1", query)
	assert.Equal(t, chproto.ErrAccessDenied, code, output)
}

func TestRunMigrations_InvalidDataSource(t *testing.T) {
	env := testenv.GetTestEnv(t)
	testclickhouse.Configure(t, env)
	db := env.GetOLAPDBHandle().GORM(t.Context(), "test_migrate")
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

			err := tenantquery.RunMigrations(db)

			require.Error(t, err)
		})
	}
}

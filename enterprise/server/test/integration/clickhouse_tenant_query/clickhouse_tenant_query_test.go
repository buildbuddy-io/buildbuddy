package clickhouse_tenant_query_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testclickhousecluster"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	chproto "github.com/ClickHouse/ch-go/proto"
)

// The migrations run on the first server of the cluster. This test sends
// tenant queries to the second server, in order to check that the migrations
// limit the access of the tenant user on every server.
func TestRunMigrations_ReplicatedCluster(t *testing.T) {
	dataSource := testclickhousecluster.Start(t).DSN()
	tenantDataSource := "http://bb_tenant:tenant-password@127.0.0.1:8226/default"
	flags.Set(t, "olap_database.data_source", dataSource)
	flags.Set(t, "olap_database.enable_data_replication", true)
	flags.Set(t, "olap_database.tenant_queries.data_source", tenantDataSource)
	env := testenv.GetTestEnv(t)
	err := clickhouse.Register(env)
	require.NoError(t, err)
	err = env.GetOLAPDBHandle().GORM(t.Context(), "test_insert").Exec(`
		INSERT INTO Invocations (group_id, updated_at_usec, invocation_uuid)
		VALUES ('GR1', 1, 'a'), ('GR2', 2, 'b')
	`).Error
	require.NoError(t, err)
	// Wait for the second server to replicate the rows.
	rsp, err := http.Post("http://127.0.0.1:8226", "text/plain", strings.NewReader("SYSTEM SYNC REPLICA default.Invocations"))
	require.NoError(t, err)
	defer rsp.Body.Close()
	body, err := io.ReadAll(rsp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rsp.StatusCode, "sync replica: %s", body)

	output, code := testclickhouse.TenantQuery(t, tenantDataSource, "GR1", "SELECT group_id, invocation_uuid FROM Invocations")
	require.Zero(t, code, output)
	assert.Equal(t, "group_id\tinvocation_uuid\nGR1\ta\n", output)

	output, code = testclickhouse.TenantQuery(t, tenantDataSource, "GR1", "SELECT count() FROM AuditLogs")
	assert.Equal(t, chproto.ErrAccessDenied, code, output)
}

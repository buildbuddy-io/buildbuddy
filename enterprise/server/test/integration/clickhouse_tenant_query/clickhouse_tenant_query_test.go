package clickhouse_tenant_query_test

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	chproto "github.com/ClickHouse/ch-go/proto"
	chgo "github.com/ClickHouse/clickhouse-go/v2"
)

// Set via x_defs in the BUILD file.
var clickhouseClusterRlocationpath string

// The migrations run on the first server of the cluster. This test sends
// tenant queries to the second server, in order to check that the migrations
// limit the access of the tenant user on every server.
func TestRunMigrations_ReplicatedCluster(t *testing.T) {
	startCluster(t)
	tenantDataSource := "http://bb_tenant:tenant-password@127.0.0.1:8226/default"
	flags.Set(t, "olap_database.data_source", "clickhouse://127.0.0.1:9201/default")
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

// startCluster runs the clickhouse_cluster tool, which starts two servers
// that replicate the "default" database, and waits for both servers to accept
// connections.
func startCluster(t *testing.T) {
	binary, err := runfiles.Rlocation(clickhouseClusterRlocationpath)
	require.NoError(t, err)
	cmd := exec.Command(binary)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start(), "start clickhouse_cluster")
	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Logf("signal clickhouse_cluster: %s", err)
			}
		}
		select {
		case <-done:
		case <-time.After(40 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("cluster shutdown timed out")
		}
		require.NoError(t, waitErr, "stop clickhouse_cluster")
	})
	for _, addr := range []string{"127.0.0.1:9201", "127.0.0.1:9202"} {
		require.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
			}
			conn, err := chgo.Open(&chgo.Options{Addr: []string{addr}, DialTimeout: time.Second})
			if err != nil {
				return false
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			return conn.Ping(ctx) == nil
		}, 2*time.Minute, 20*time.Millisecond)
		select {
		case <-done:
			t.Fatalf("cluster exited during startup: %v", waitErr)
		default:
		}
	}
}

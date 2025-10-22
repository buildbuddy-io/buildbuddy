// Basic tests for the clickhouse_backup CLI tool to make sure it isn't totally
// broken. This is not intended to replace regular testing of data backup and
// restore procedures.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/require"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
)

// Set via x_defs in the BUILD file.
var (
	clickhouseClusterRlocationpath string
	clickhouseBackupRlocationpath  string
)

func TestClickHouseBackupAndRestore(t *testing.T) {
	// Configure a local backup disk for testing.
	backupDir := testfs.MakeTempDir(t)
	configDir := testfs.MakeTempDir(t)
	configPath := testfs.WriteFile(t, configDir, "disk_backup.xml", `
<clickhouse>
	<storage_configuration>
		<disks>
			<backups>
				<type>local</type>
				<path>/backups/</path>
			</backups>
		</disks>
	</storage_configuration>
	<backups>
		<allowed_disk>backups</allowed_disk>
		<allowed_path>/backups/</allowed_path>
	</backups>
</clickhouse>
`)

	// Run a ClickHouse cluster with the configured backup disk.
	dsn := startCluster(t, "--config_file", configPath, "--volume", backupDir+`:/backups/:rw`)

	// Set up the OLAP DB handle.
	flags.Set(t, "olap_database.data_source", dsn)
	flags.Set(t, "olap_database.enable_data_replication", true)
	env := testenv.GetTestEnv(t)
	err := clickhouse.Register(env)
	require.NoError(t, err)
	// Create a test invocation.
	ctx := t.Context()
	iid := uuid.New()
	err = env.GetOLAPDBHandle().GORM(ctx, "test_create_invocation").Create(&schema.Invocation{InvocationUUID: iid}).Error
	require.NoError(t, err)

	var count int64
	// There should now be one invocation.
	err = env.GetOLAPDBHandle().GORM(ctx, "test_count_invocations").Model(&schema.Invocation{}).Count(&count).Error
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	// Create a backup.
	runBackupTool(
		t,
		"--olap_database.data_source", dsn,
		"--storage.disk.root_directory", backupDir,
		"create",
		"--database=default",
		"--backup_disk_name=backups",
	)

	// Accidentally delete all invocations (using TRUNCATE for immediate
	// deletion).
	err = env.GetOLAPDBHandle().GORM(ctx, "test_delete_invocations").Exec(`
		TRUNCATE TABLE Invocations
	`).Error
	require.NoError(t, err)

	// We should now see 0 invocations.
	err = env.GetOLAPDBHandle().GORM(ctx, "test_count_invocations").Model(&schema.Invocation{}).Count(&count).Error
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	// Restore invocations from the backup.
	time.Sleep(1 * time.Second)
	runBackupTool(
		t,
		"--olap_database.data_source", dsn,
		"--storage.disk.root_directory", backupDir,
		"restore",
		"--backup_disk_name=backups",
		"--backup_database=default",
		"--destination_database=default",
		"--table=Invocations",
	)

	// There should now be one invocation.
	err = env.GetOLAPDBHandle().GORM(ctx, "test_count_invocations").Model(&schema.Invocation{}).Count(&count).Error
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func runBackupTool(t *testing.T, args ...string) {
	clickhouseBackupPath, err := runfiles.Rlocation(clickhouseBackupRlocationpath)
	require.NoError(t, err)
	cmd := exec.Command(clickhouseBackupPath, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	require.NoError(t, err, "run clickhouse_backup")
}

func startCluster(t *testing.T, args ...string) (dsn string) {
	// TODO: remove docker-compose dependency.
	ensureDockerComposeInstalled(t)

	clickhouseClusterPath, err := runfiles.Rlocation(clickhouseClusterRlocationpath)
	require.NoError(t, err)
	cmd := exec.Command(clickhouseClusterPath, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	err = cmd.Start()
	require.NoError(t, err, "start clickhouse_cluster")
	t.Cleanup(func() {
		cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	})
	addr := "localhost:9201"
	dsn = fmt.Sprintf("clickhouse://%s/default", addr)
	options, err := chgo.ParseDSN(dsn)
	require.NoError(t, err)
	// Wait for the cluster to be ready.
	require.Eventually(t, func() bool {
		conn, err := chgo.Open(options)
		if err != nil {
			return false
		}
		defer conn.Close()
		if err := conn.Ping(t.Context()); err != nil {
			return false
		}
		return true
	}, 2*time.Minute, 10*time.Millisecond)

	return dsn
}

func ensureDockerComposeInstalled(t *testing.T) {
	if _, err := exec.LookPath("docker-compose"); err == nil {
		return
	}
	if os.Getuid() == 0 {
		b, err := exec.Command("sh", "-c", `apt update && apt install -y docker-compose`).CombinedOutput()
		require.NoError(t, err, "install docker-compose", string(b))
	}
}

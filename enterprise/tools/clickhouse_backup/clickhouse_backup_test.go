// Basic tests for the clickhouse_backup CLI tool to make sure it isn't totally
// broken. This is not intended to replace regular testing of data backup and
// restore procedures.
package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
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
	backupDir := filepath.Join(testfs.MakeTempDir(t), "backups with spaces")
	require.NoError(t, os.Mkdir(backupDir, 0700))
	configDir := testfs.MakeTempDir(t)
	configPath := testfs.WriteFile(t, configDir, "disk_backup.xml", `
<clickhouse>
	<storage_configuration>
		<disks>
			<backups>
				<type>local</type>
				<path from_env="BB_CLICKHOUSE_TEST_BACKUP_DIR"/>
			</backups>
		</disks>
	</storage_configuration>
	<backups>
		<allowed_disk>backups</allowed_disk>
		<allowed_path from_env="BB_CLICKHOUSE_TEST_BACKUP_DIR"/>
	</backups>
</clickhouse>
`)

	envFile := testfs.WriteFile(t, configDir, "backup.env", "BB_CLICKHOUSE_TEST_BACKUP_DIR="+backupDir+"/\n")
	dsn := startCluster(t, "--config_file", configPath, "--env_file", envFile)

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
	binary, err := runfiles.Rlocation(clickhouseClusterRlocationpath)
	require.NoError(t, err)
	cmd := exec.Command(binary, args...)
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
	return "clickhouse://127.0.0.1:9201/default"
}

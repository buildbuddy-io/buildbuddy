package testclickhouse

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/dockerutil"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"
)

const (
	clickhouseVersion   = "22.3.18"
	containerNamePrefix = "buildbuddy-test-clickhouse-"
)

var (
	targets = map[testing.TB]string{}
)

// GetOrStart starts a new instance for the given test if one is not already
// running; otherwise it returns the existing target.
func GetOrStart(t testing.TB, reuseServer bool) string {
	target := targets[t]
	if target != "" {
		return target
	}
	target = Start(t, reuseServer)
	targets[t] = target
	t.Cleanup(func() {
		delete(targets, t)
	})
	return target
}

func Start(t testing.TB, reuseServer bool) string {
	const dbName = "buildbuddy_test"

	var port int
	var containerName string
	if reuseServer {
		port, containerName = dockerutil.FindServerContainer(t, containerNamePrefix)
		if containerName != "" {
			log.Debugf("Reusing existing clickhouse DB container %s", containerName)
		}
	}

	if port == 0 {
		port = testport.FindFree(t)
		containerName = fmt.Sprintf("%s%d", containerNamePrefix, port)
		log.Debug("Starting ClickHouse DB...")

		cmd := exec.Command(
			"docker", "run", "--rm", "--detach",
			"--env", "CLICKHOUSE_DB="+dbName,
			"--publish", fmt.Sprintf("%d:9000", port),
			"--name", containerName,
			fmt.Sprintf("clickhouse/clickhouse-server:%s", clickhouseVersion))

		cmd.Stderr = &logWriter{"docker run clickhouse"}
		err := cmd.Run()
		require.NoError(t, err)

	}

	if !reuseServer {
		t.Cleanup(func() {
			cmd := exec.Command("docker", "kill", containerName)
			cmd.Stderr = &logWriter{"docker kill " + containerName}
			err := cmd.Run()
			require.NoError(t, err)
		})
	}
	dsn := fmt.Sprintf("clickhouse://default:@127.0.0.1:%d/%s", port, dbName)
	options, err := clickhouse.ParseDSN(dsn)
	options.Debug = true
	require.NoError(t, err)
	conn, err := clickhouse.Open(options)
	require.NoError(t, err)

	defer conn.Close()
	for {
		ctx := context.Background()
		if err := conn.Ping(ctx); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// Drop the DB from the old test and let it be re-created via GORM
		// auto-migration.
		if reuseServer {
			require.NoError(t, err)
			err = conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
			require.NoError(t, err)
			err = conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
			require.NoError(t, err)
		}

		return dsn
	}
}

type logWriter struct {
	tag string
}

func (w *logWriter) Write(b []byte) (int, error) {
	lines := strings.Split(string(b), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		log.Infof("[%s] %s", w.tag, line)
	}
	return len(b), nil
}

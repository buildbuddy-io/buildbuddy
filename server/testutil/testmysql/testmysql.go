package testmysql

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

var (
	targets = map[testing.TB]string{}

	reuseServer = flag.Bool("testmysql.reuse_server", false, "If true, reuse mysql server between tests.")
)

// GetOrStart starts a new instance for the given test if one is not already
// running; otherwise it returns the existing target.
func GetOrStart(t testing.TB) string {
	target := targets[t]
	if target != "" {
		return target
	}
	target = Start(t)
	targets[t] = target
	t.Cleanup(func() {
		delete(targets, t)
	})
	return target
}

// Start starts a test-scoped MySQL DB and returns the DB target.
//
// Currently requires Docker to be available in the test execution environment.
func Start(t testing.TB) string {
	const dbName = "buildbuddy-test"

	var port int
	if *reuseServer {
		// List running server processes by name, and if we find one that looks
		// like `buildbuddy-test-mysql-$PORT`, then return a connection directly
		// to that port
		cmd := exec.Command("docker", "ps", "--filter=name=buildbuddy-test-mysql-", "--format={{.Names}}")
		b, err := cmd.CombinedOutput()
		require.NoError(t, err)
		lines := strings.Split(string(b), "\n")
		for _, containerName := range lines {
			if containerName == "" {
				continue
			}
			var err error
			port, err = strconv.Atoi(strings.TrimPrefix(containerName, "buildbuddy-test-mysql-"))
			require.NoError(t, err, "failed to parse container port from %q", containerName)
			log.Debugf("Reusing existing mysql DB container %s", containerName)
			break
		}
	}

	if port == 0 {
		port = testport.FindFree(t)
		containerName := fmt.Sprintf("buildbuddy-test-mysql-%d", port)

		log.Debug("Starting mysql DB...")

		cmd := exec.Command(
			"docker", "run", "--rm", "--detach",
			"--env", "MYSQL_ROOT_PASSWORD=root",
			"--env", "MYSQL_DATABASE="+dbName,
			"--publish", fmt.Sprintf("%d:3306", port),
			"--name", containerName,
			"mysql:8.0",
		)
		cmd.Stderr = &logWriter{"docker run mysql"}
		err := cmd.Run()
		require.NoError(t, err)
	}

	if !*reuseServer {
		t.Cleanup(func() {
			containerName := fmt.Sprintf("buildbuddy-test-mysql-%d", port)
			cmd := exec.Command("docker", "kill", containerName)
			cmd.Stderr = &logWriter{"docker kill " + containerName}
			err := cmd.Run()
			require.NoError(t, err)
		})
	}

	// Wait for the DB to start up.
	dsn := fmt.Sprintf("root:root@tcp(127.0.0.1:%d)/%s", port, dbName)
	// Ignore errors logged by mysql driver when attempting to ping the DB before it is ready.
	mysql.SetLogger(&discardLogger{})
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	for {
		if err := db.Ping(); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Drop the DB from the old test and let it be re-created via GORM
		// auto-migration.
		if *reuseServer {
			ctx := context.Background()
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()
			_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
			require.NoError(t, err)
		}

		return "mysql://" + dsn
	}
}

type discardLogger struct{}

func (d *discardLogger) Print(args ...interface{}) {}

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

package testpostgres

import (
	"database/sql"
	"fmt"
	"net"
	"net/url"

	"os/exec"
	"strconv"
	"strings"
	"testing"

	"time"

	// "github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"

	_ "github.com/jackc/pgx/v5"
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

// Start starts a test-scoped Postgres DB and returns the DB target.
//
// Currently requires Docker to be available in the test execution environment.
func Start(t testing.TB, reuseServer bool) string {
	const dbName = "buildbuddy-test"

	var port int
	if reuseServer {
		// List running server processes by name, and if we find one that looks
		// like `buildbuddy-test-postgres-$PORT`, then return a connection directly
		// to that port
		cmd := exec.Command("docker", "ps", "--filter=name=buildbuddy-test-postgres-", "--format={{.Names}}")
		b, err := cmd.CombinedOutput()
		require.NoError(t, err)
		lines := strings.Split(string(b), "\n")
		for _, containerName := range lines {
			if containerName == "" {
				continue
			}
			var err error
			port, err = strconv.Atoi(strings.TrimPrefix(containerName, "buildbuddy-test-postgres-"))
			require.NoError(t, err, "failed to parse container port from %q", containerName)
			log.Debugf("Reusing existing postgres DB container %s", containerName)
			break
		}
	}

	containerName := fmt.Sprintf("buildbuddy-test-postgres-%d", port)
	if port == 0 {
		port = testport.FindFree(t)
		containerName = fmt.Sprintf("buildbuddy-test-postgres-%d", port)

		log.Debug("Starting postgres DB...")
		cmd := exec.Command(
			"docker", "run", "--detach",
			"--env", "POSTGRES_USER=postgres",
			"--env", "POSTGRES_PASSWORD=postgres",
			"--env", "POSTGRES_DB="+dbName,
			"--env", "POSTGRES_HOST_AUTH_METHOD=password",
			"--publish", fmt.Sprintf("%d:5432", port),
			"--name", containerName,
			"postgres:15.3",
		)
		cmd.Stderr = &logWriter{"docker run postgres"}
		err := cmd.Run()
		require.NoError(t, err)
	}

	if !reuseServer {
		t.Cleanup(func() {
			containerName := fmt.Sprintf("buildbuddy-test-postgres-%d", port)
			cmd := exec.Command("docker", "kill", containerName)
			cmd.Stderr = &logWriter{"docker kill " + containerName}
			err := cmd.Run()
			require.NoError(t, err)
		})
	}

	// Wait for the DB to start up.
	dsn := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("postgres", "postgres"),
		Host:     net.JoinHostPort("127.0.0.1", strconv.Itoa(port)),
		Path:     dbName,
		RawQuery: url.Values(map[string][]string{"sslmode": {"disable"}}).Encode(),
	}
	db, err := sql.Open("pgx", dsn.String())
	require.NoError(t, err)
	defer db.Close()
	for {
		if err := db.Ping(); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// Drop the DB from the old test and let it be re-created via GORM
		// auto-migration.
		if reuseServer {
			log.Debug("Attempting to reset database.")

			cmd := exec.Command(
				"docker", "exec",
				"-u", "postgres",
				containerName,
				"dropdb", "-f", dbName,
			)
			cmd.Stderr = &logWriter{"docker exec dropdb"}
			err := cmd.Run()
			require.NoError(t, err)

			cmd = exec.Command(
				"docker", "exec",
				"-u", "postgres",
				containerName,
				"createdb", dbName,
			)
			cmd.Stderr = &logWriter{"docker exec createdb"}
			err = cmd.Run()
			require.NoError(t, err)

			log.Debug("Reset database.")
		}

		return dsn.String()
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

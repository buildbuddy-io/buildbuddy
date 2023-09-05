package testpostgres

import (
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/dockerutil"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"

	_ "github.com/jackc/pgx/v5"
)

var (
	targets = map[testing.TB]string{}
)

const (
	containerNamePrefix = "buildbuddy-test-postgres-"
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
	var containerName string
	if reuseServer {
		port, containerName = dockerutil.FindServerContainer(t, containerNamePrefix)
		if containerName != "" {
			log.Debugf("Reusing existing postgres DB container %s", containerName)
		}
	}

	if port == 0 {
		log.Debug("Starting sshd...")
		// if strings.HasPrefix("A", "B") {
		{
			cmd := exec.Command("bash", "-c", `
				set -e
				set -x
				cd /
				export PATH="$PATH:/sbin/:/usr/sbin/"
				# Set the root password to 'root'
				printf 'root:root' | chpasswd
				# Install openssh-server (with apt, assuming the VM is running ubuntu)
				export DEBIAN_FRONTEND=noninteractive
				apt update && apt install -y openssh-server
				# Permit SSH root login
				echo >>/etc/ssh/sshd_config 'PermitRootLogin yes'
				# Start sshd
				echo 'Starting sshd...'
				/etc/init.d/ssh start
				echo 'sshd started'
			`)
			cmd.Stdout = os.Stderr
			cmd.Stderr = os.Stderr
			err := cmd.Run()
			require.NoError(t, err)
		}

		port = testport.FindFree(t)
		containerName = fmt.Sprintf("%s%d", containerNamePrefix, port)

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

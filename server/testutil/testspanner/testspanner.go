package testspanner

import (
	dbadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"context"
	"database/sql"
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/dockerutil"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	_ "github.com/googleapis/go-sql-spanner"
	_ "github.com/jackc/pgx/v5"
)

var (
	targets = map[testing.TB]string{}
)

const (
	containerNamePrefix = "buildbuddy-test-spanner-"
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

// Start starts a test-scoped spanner DB and returns the DB target.
//
// Currently requires Docker to be available in the test execution environment.
func Start(t testing.TB, reuseServer bool) string {
	const dbName = "projects/no-project-for-test/instances/theinstance/databases/thedatabase"

	var port int
	var containerName string
	if reuseServer {
		port, containerName = dockerutil.FindServerContainer(t, containerNamePrefix)
		if containerName != "" {
			log.Debugf("Reusing existing postgres DB container %s", containerName)
		}
	}

	if port == 0 {
		port = testport.FindFree(t)
		containerName = fmt.Sprintf("%s%d", containerNamePrefix, port)

		log.Debugf("Starting spanner DB [%d]...", port)
		cmd := exec.Command(
			"docker", "run", "--detach",
			"-p", fmt.Sprintf("%d:9010", port),
			"-p", fmt.Sprintf("%d:9020", testport.FindFree(t)),
			"--name", containerName,
			"gcr.io/cloud-spanner-emulator/emulator@sha256:636fdfc528824bae5f0ea2eca6ae307fe81092f05ec21038008bc0d6100e52fc",
		)
		cmd.Stderr = &logWriter{"docker run spanner"}
		err := cmd.Run()
		require.NoError(t, err)
	}

	ctx := context.Background()

	//dialCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	//defer cancel()
	// Setup connection to the emulator server. Note that we use an insecure
	// connection as the emulator does not use https.
	conn, err := grpc.NewClient("localhost:"+strconv.Itoa(port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Dialing in-memory fake: %v", err)
	}

	if !reuseServer {
		t.Cleanup(func() {
			cmd := exec.Command("docker", "kill", containerName)
			cmd.Stderr = &logWriter{"docker kill " + containerName}
			err := cmd.Run()
			require.NoError(t, err)
		})
	}

	instanceClient, err := instance.NewInstanceAdminClient(ctx, option.WithGRPCConn(conn))
	require.NoError(t, err)
	defer instanceClient.Close()

	if _, err := instanceClient.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: "projects/no-project-for-test/instances/theinstance"}); err != nil {
		instanceOp, err := instanceClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
			Parent:     "projects/no-project-for-test",
			InstanceId: "theinstance"})
		require.NoError(t, err)
		_, err = instanceOp.Wait(ctx)
		require.NoError(t, err)
	}

	// Create an admin client that connects to the emulator.
	adminClient, err := dbadmin.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Connecting to in-memory fake DB admin: %v", err)
	}
	defer adminClient.Close()
	if _, err = adminClient.GetDatabase(ctx, &databasepb.GetDatabaseRequest{Name: dbName}); err != nil {
		dbOp, err := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{Parent: "projects/no-project-for-test/instances/theinstance",
			CreateStatement: "create database thedatabase"})
		require.NoError(t, err)
		_, err = dbOp.Wait(ctx)
		require.NoError(t, err)
	}

	// Wait for the DB to start up.
	fullPath := fmt.Sprintf("localhost:%d/%s", port, dbName)
	db, err := sql.Open("spanner", fullPath)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Ping())

	return "spanner://" + fullPath
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

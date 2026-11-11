// Package testclickhousecluster runs a local replicated ClickHouse cluster for
// tests, using the clickhouse_cluster tool.
package testclickhousecluster

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/stretchr/testify/require"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
)

// Set via x_defs in the BUILD file.
var clickhouseClusterRlocationpath string

// Start runs the clickhouse_cluster tool with the given args, and waits until
// both of the tool's default two servers accept connections. It stops the
// cluster when the test finishes. It returns the data source of the first
// server.
func Start(t testing.TB, args ...string) string {
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

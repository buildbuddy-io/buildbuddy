package testredis

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

const (
	redisLinuxBinRunfilePath = "enterprise/server/test/bin/redis/redis-server-linux-x86_64"

	startupTimeout      = 10 * time.Second
	startupPingInterval = 5 * time.Millisecond
)

// Start spawns a Redis server for the given test and returns a Redis target
// that points to it.
func Start(t testing.TB) string {
	var redisBinPath string
	osArchKey := runtime.GOOS + "_" + runtime.GOARCH
	switch osArchKey {
	case "linux_amd64":
		redisBinPath = redisLinuxBinRunfilePath
	default:
		// Skip the test on unsupported platforms until we have mac binary in place.
		t.SkipNow()
		return ""
	}
	redisBinPath, err := bazel.Runfile(redisLinuxBinRunfilePath)
	if err != nil {
		assert.FailNow(t, "redis binary not found in runfiles", err.Error())
	}

	redisPort := app.FreePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	args := []string{"--port", strconv.Itoa(redisPort)}
	// Disable persistence, not useful for testing.
	args = append(args, "--save", "")
	// Set a precautionary limit, tests should not reach it...
	args = append(args, "--maxmemory", "1gb")
	// ... but do break things if we reach the limit.
	args = append(args, "--maxmemory-policy", "noeviction")
	cmd := exec.CommandContext(ctx, redisBinPath, args...)
	log.Printf("Starting redis server: %s", cmd)
	cmd.Stdout = &logWriter{}
	cmd.Stderr = &logWriter{}
	err = cmd.Start()
	if err != nil {
		assert.FailNowf(t, "redis binary could not be started", err.Error())
	}
	var killed atomic.Value
	killed.Store(false)
	go func() {
		if err := cmd.Wait(); err != nil && killed.Load() != true {
			log.Warningf("redis server did not exit cleanly: %v", err)
		}
	}()
	t.Cleanup(func() {
		log.Info("Shutting down Redis server.")
		killed.Store(true)
		cancel()
	})
	target := fmt.Sprintf("localhost:%d", redisPort)
	waitUntilHealthy(t, target)
	return target
}

func waitUntilHealthy(t testing.TB, target string) {
	start := time.Now()
	ctx := context.Background()
	r := redis.NewClient(redisutil.TargetToOptions(target))
	hc := redisutil.HealthChecker{Rdb: r}
	for {
		err := hc.Check(ctx)
		if err == nil {
			return
		}
		if time.Since(start) > startupTimeout {
			assert.FailNowf(t, "Failed to connect to redis", "Health check still failing after %s: %s", startupTimeout, err)
		}
		time.Sleep(startupPingInterval)
	}
}

type logWriter struct{}

func (w *logWriter) Write(b []byte) (int, error) {
	lines := strings.Split(string(b), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		log.Infof("[redis server] %s", line)
	}
	return len(b), nil
}

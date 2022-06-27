package testredis

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	redisLinuxBinRunfilePath  = "enterprise/server/test/bin/redis/redis-server-linux-x86_64"
	redisDarwinBinRunfilePath = "enterprise/server/test/bin/redis/redis-server-darwin-arm64"

	startupTimeout      = 10 * time.Second
	startupPingInterval = 5 * time.Millisecond
)

type Handle struct {
	Target     string
	port       int
	socketPath string

	t      testing.TB
	cancel context.CancelFunc
	done   chan struct{}
}

func (h *Handle) start() {
	var redisBinPath string
	osArchKey := runtime.GOOS + "_" + runtime.GOARCH
	switch osArchKey {
	case "linux_amd64":
		redisBinPath = redisLinuxBinRunfilePath
	case "darwin_arm64":
		redisBinPath = redisDarwinBinRunfilePath
	default:
		// Skip the test on unsupported platforms until we have binaries in place.
		log.Warningf("No redis binary found for platform %q. Tests are skipped.", osArchKey)
		h.t.SkipNow()
		return
	}

	redisBinPath, err := bazel.Runfile(redisBinPath)
	if err != nil {
		assert.FailNow(h.t, "redis binary not found in runfiles", err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = make(chan struct{})

	args := []string{}
	if h.port != 0 {
		args = append(args, "--port", strconv.Itoa(h.port))
	}
	if h.socketPath != "" {
		args = append(args, "--port", "0")
		args = append(args, "--unixsocket", h.socketPath)
		args = append(args, "--unixsocketperm", "700")
	}
	// Disable persistence, not useful for testing.
	args = append(args, "--save", "")
	// Set a precautionary limit, tests should not reach it...
	args = append(args, "--maxmemory", "1gb")
	// ... but do break things if we reach the limit.
	args = append(args, "--maxmemory-policy", "noeviction")
	cmd := exec.CommandContext(ctx, redisBinPath, args...)
	log.Infof("Starting redis server: %s", cmd)
	cmd.Stdout = &logWriter{}
	cmd.Stderr = &logWriter{}
	err = cmd.Start()
	if err != nil {
		assert.FailNowf(h.t, "redis binary could not be started", err.Error())
	}
	go func() {
		if err := cmd.Wait(); err != nil {
			close(h.done)
			select {
			case <-ctx.Done():
			default:
				log.Warningf("redis server did not exit cleanly: %v", err)
			}
		}
	}()
}

// Restart restarts Redis.
// Blocks until Redis is reachable.
func (h *Handle) Restart() {
	h.cancel()
	<-h.done
	h.start()
	waitUntilHealthy(h.t, h.Target)
}

func (h *Handle) Client() redis.UniversalClient {
	return redis.NewClient(redisutil.TargetToOptions(h.Target))
}

func (h *Handle) KeyCount(pattern string) int {
	keys, err := h.Client().Keys(context.Background(), pattern).Result()
	require.NoError(h.t, err)
	return len(keys)
}

// Start spawns a Redis server for the given test and returns a handle to the running process.
func Start(t testing.TB) *Handle {
	// redis socket must be in /tmp, redis won't read socket files in arbitrary locations
	socketPath := testfs.MakeSocket(t, "redis.sock")
	target := fmt.Sprintf("unix://%s", socketPath)

	handle := &Handle{
		Target:     target,
		socketPath: socketPath,
		t:          t,
	}
	handle.start()
	waitUntilHealthy(t, target)

	return handle
}

// StartTCP spawns a Redis server listening on a TCP port.
// Most uses should prefer the Start function. This function should be used for cases where a unix socket is not
// acceptable (i.e. Ring client).
func StartTCP(t testing.TB) *Handle {
	port := testport.FindFree(t)
	target := fmt.Sprintf("localhost:%d", port)

	handle := &Handle{
		Target: target,
		port:   port,
		t:      t,
	}
	handle.start()
	waitUntilHealthy(t, target)

	return handle
}

func waitUntilHealthy(t testing.TB, target string) {
	start := time.Now()
	ctx := context.Background()
	r := redis.NewClient(redisutil.TargetToOptions(target))
	for {
		err := r.Ping(ctx).Err()
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

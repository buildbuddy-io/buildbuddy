package sidecar

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/google/shlex"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/sidecar"
)

const (
	windowsOSName        = "windows"
	windowsFileExtension = ".exe"
	sockPrefix           = "sidecar-"

	// Number of attempts to restart and reconnect to the sidecar.
	numConnectionAttempts = 2
)

func hashStrings(in []string) string {
	data := []byte{}
	for _, i := range in {
		data = append(data, []byte(i)...)
	}
	u := crc32.ChecksumIEEE(data)
	return fmt.Sprintf("%d", u)
}

func pathExists(p string) bool {
	_, err := os.Stat(p)
	return !os.IsNotExist(err)
}

func restartSidecarIfNecessary(ctx context.Context, bbCacheDir string, args []string) (string, error) {
	// Forward args from BB_SIDECAR_ARGS env var (useful for setting debug log
	// level, etc.)
	rawExtraArgs := os.Getenv("BB_SIDECAR_ARGS")
	extraArgs, err := shlex.Split(rawExtraArgs)
	if err != nil {
		return "", err
	}
	args = append(args, extraArgs...)

	// A sidecar instance is identified by the args passed to it as well as its
	// version.
	//
	// Note: During development, the version string will be "unknown".
	// To get the sidecar to restart, you can shut it down manually with
	// `kill -INT <sidecar_pid>`, then re-run the CLI.
	sidecarID := hashStrings(append([]string{version.String()}, args...))
	sockName := sockPrefix + sidecarID + ".sock"
	sockPath := filepath.Join(os.TempDir(), sockName)

	// Check if a process is already running with this sock.
	// If one is, we're all done!
	if pathExists(sockPath) {
		log.Debugf("Sidecar socket %q exists.", sockPath)
		return sockPath, nil
	}

	logPath := filepath.Join(bbCacheDir, "sidecar-"+sidecarID+".log")
	f, err := os.Create(logPath)
	if err != nil {
		return "", fmt.Errorf("failed to create sidecar log file: %s", err)
	}
	// Note: Not closing f since the sidecar writes to it.

	// This is where we'll listen for bazel traffic
	args = append(args, fmt.Sprintf("--listen_addr=unix://%s", sockPath))
	// Re-invoke ourselves in sidecar mode.
	c := exec.Command(os.Args[0], append(args, "--sidecar=1")...)
	// Start the sidecar in its own process group so that when we send Ctrl+C,
	// the sidecar can keep running in the background.
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.Stdout = f
	c.Stderr = f
	log.Debugf("Running sidecar cmd: %s", c.String())
	log.Debugf("Sidecar will write logs to: %s", logPath)
	if err := c.Start(); err != nil {
		return "", err
	}
	return sockPath, nil
}

func ConfigureSidecar(args []string) []string {
	log.Debugf("Configuring sidecar")

	ctx := context.Background()

	cacheDir, err := storage.CacheDir()
	if err != nil {
		log.Warnf("Sidecar could not be initialized, continuing without sidecar: %s", err)
		return args
	}

	// Re(Start) the sidecar if the flags set don't match.
	sidecarArgs := []string{}
	besBackendFlag := arg.Get(args, "bes_backend")
	remoteCacheFlag := arg.Get(args, "remote_cache")
	remoteExecFlag := arg.Get(args, "remote_executor")

	if besBackendFlag != "" {
		sidecarArgs = append(sidecarArgs, "--bes_backend="+besBackendFlag)
	}
	if remoteCacheFlag != "" && remoteExecFlag == "" {
		sidecarArgs = append(sidecarArgs, "--remote_cache="+remoteCacheFlag)
		// Also specify a disk cache directory.
		// TODO: Prevent multiple sidecar instances from clobbering each others'
		// disk caches.
		diskCacheDir := filepath.Join(cacheDir, "filecache")
		sidecarArgs = append(sidecarArgs, fmt.Sprintf("--cache_dir=%s", diskCacheDir))
	}

	if len(sidecarArgs) == 0 {
		return args
	}

	sidecarArgs = append(sidecarArgs, []string{
		// Allow the sidecar's cache proxy to handle ZSTD streams.
		// Note that if the remote cache backend doesn't support ZSTD, then
		// this transcoding functionality will go unused as bazel will see that
		// compression is not enabled in the remote capabilities.
		// TODO: Have the sidecar store artifacts compressed on disk once we
		// support it, to avoid any local CPU overhead due to transcoding.
		"--cache.zstd_transcoding_enabled=true",
		// Use a much higher BEP proxy size than the default, since we typically
		// only need to handle a small number of concurrent invocations and
		// we don't want to drop events just because we're proxying.
		fmt.Sprintf("--build_event_proxy.buffer_size=%d", 500_000),
	}...)

	var connectionErr error
	for i := 0; i < numConnectionAttempts; i++ {
		sidecarSocket, err := restartSidecarIfNecessary(ctx, cacheDir, sidecarArgs)
		if err != nil {
			log.Warnf("Sidecar could not be initialized, continuing without sidecar: %s", err)
			return args
		}
		if err := keepaliveSidecar(ctx, sidecarSocket); err != nil {
			// If we fail to connect, the sidecar might have been abruptly
			// killed before this CLI invocation. Attempt to remove the socket
			// and restart the sidecar before the next connection attempt.
			if err := os.Remove(sidecarSocket); err != nil {
				log.Debugf("Failed to remove sidecar socket: %s", err)
			}
			connectionErr = err
			log.Debugf("Sidecar connection error (retryable): %s", err)
			continue
		}
		if besBackendFlag != "" {
			args = append(args, fmt.Sprintf("--bes_backend=unix://%s", sidecarSocket))
		}
		if remoteCacheFlag != "" && remoteExecFlag == "" {
			args = append(args, fmt.Sprintf("--remote_cache=unix://%s", sidecarSocket))
			// Set bytestream URI prefix to match the actual remote cache
			// backend, rather than the sidecar socket.
			instanceName := arg.Get(args, "remote_instance_name")
			args = append(args, fmt.Sprintf("--remote_bytestream_uri_prefix=%s", bytestreamURIPrefix(remoteCacheFlag, instanceName)))
		}
		return args
	}
	log.Warnf("Could not connect to sidecar, continuing without sidecar: %s", connectionErr)
	return args
}

func bytestreamURIPrefix(cacheTarget, instanceName string) string {
	prefix := stripProtocol(cacheTarget)
	if instanceName != "" {
		prefix += "/" + instanceName
	}
	return prefix
}

func stripProtocol(target string) string {
	for _, protocol := range []string{"grpc://", "grpcs://", "http://", "https://"} {
		if strings.HasPrefix(target, protocol) {
			return strings.TrimPrefix(target, protocol)
		}
	}
	return target
}

// keepaliveSidecar validates the connection to the sidecar and keeps the
// sidecar alive as long as this process is alive by issuing background ping
// requests.
func keepaliveSidecar(ctx context.Context, sidecarSocket string) error {
	conn, err := grpc_client.DialTarget("unix://" + sidecarSocket)
	if err != nil {
		return err
	}
	s := scpb.NewSidecarClient(conn)
	connected := make(chan struct{})
	timedOut := make(chan struct{})
	go func() {
		connectionValidated := false
		pingInterval := 10 * time.Millisecond
		for {
			_, err := s.Ping(ctx, &scpb.PingRequest{})
			if connectionValidated && err != nil {
				log.Debugf("sidecar did not respond to ping request: %s\n", err)
				return
			}
			if !connectionValidated && err == nil {
				log.Debugf("Established connection to sidecar.")
				close(connected)
				connectionValidated = true
				pingInterval = 1 * time.Second
			}
			select {
			case <-timedOut:
				return
			case <-ctx.Done():
				return
			case <-time.After(pingInterval):
			}
		}
	}()
	select {
	case <-connected:
		return nil
	case <-time.After(5 * time.Second):
		close(timedOut)
		return fmt.Errorf("timed out waiting for sidecar connection")
	}
}

package sidecar

import (
	"bufio"
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/config"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
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

// configureLocalCache initializes the local disk cache based on the given
// configuration and returns the required sidecar args to use the local cache.
func configureLocalCache(ctx context.Context, cliCacheDir, remoteCache string, configFile *config.File) (args []string, ok bool) {
	diskCacheDir := filepath.Join(cliCacheDir, "filecache")

	// If there's a buildbuddy.yaml file, respect the cache config from that.
	var cfg config.LocalCacheConfig
	if configFile != nil && configFile.LocalCache != nil {
		cfg = *configFile.LocalCache
	}

	// Check whether the local cache is explicitly disabled
	if cfg.Enabled != nil && !*cfg.Enabled {
		return nil, false
	}

	if cfg.RootDirectory != "" {
		diskCacheDir = cfg.RootDirectory
	}

	// Eagerly create the disk cache dir and disable disk cache if we
	// fail to do so. We also need this in order to determine the
	// capacity of the filesystem where the disk cache will live.
	if err := os.MkdirAll(diskCacheDir, 0755); err != nil {
		log.Warnf("Failed to create local cache directory: %s", err)
		return nil, false
	}

	// Determine max size of disk cache from config.
	// If this is unset (0), the sidecar will choose a default size.
	var maxSize int64
	if cfg.MaxSize != nil {
		s, err := config.ParseDiskCapacityBytes(cfg.MaxSize, diskCacheDir)
		if err != nil {
			log.Warnf("Invalid local_cache.max_size value %q: %s", s, err)
		} else {
			maxSize = s
		}
	}

	return []string{
		"--remote_cache=" + remoteCache,
		"--cache_dir=" + diskCacheDir,
		"--cache_max_size_bytes=" + fmt.Sprintf("%d", maxSize),
	}, true
}

func restartSidecarIfNecessary(ctx context.Context, bbCacheDir string, args []string) (*Instance, error) {
	// Forward args from BB_SIDECAR_ARGS env var (useful for setting debug log
	// level, etc.)
	rawExtraArgs := os.Getenv("BB_SIDECAR_ARGS")
	extraArgs, err := shlex.Split(rawExtraArgs)
	if err != nil {
		return nil, err
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

	logPath := filepath.Join(bbCacheDir, "sidecar-"+sidecarID+".log")
	instance := &Instance{
		SockPath: sockPath,
		LogPath:  logPath,
	}

	// Check if a process is already running with this sock.
	// If one is, we're all done!
	if pathExists(sockPath) {
		log.Debugf("Sidecar socket %q exists.", sockPath)
		return instance, nil
	}

	f, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create sidecar log file: %s", err)
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
		return nil, err
	}
	return instance, nil
}

func isFlagTrue(flag string) bool {
	if flag == "1" || strings.EqualFold(flag, "true") {
		return true
	}
	return false
}

func isCI(args []string) bool {
	if isFlagTrue(os.Getenv("CI")) {
		return true
	}
	for _, md := range arg.GetMulti(args, "build_metadata") {
		if md == "ROLE=CI" {
			return true
		}
	}
	return false
}

// Instance holds information about the running sidecar instance.
type Instance struct {
	// SockPath is the path to the sidecar socket.
	SockPath string
	// LogPath is the path to the logs for the sidecar.
	LogPath string
}

func (i *Instance) PrintLogsSince(t time.Time) {
	// TODO: only look at tail of logs, to limit IO
	f, err := os.Open(i.LogPath)
	if err != nil {
		return
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	var lines []string
	for s.Scan() {
		line := s.Text()
		lt, ok := parseLogTimestamp(line)
		if !ok || !lt.After(t) {
			continue
		}
		lines = append(lines, line)
	}
	if len(lines) > 0 {
		os.Stderr.WriteString("---\n")
		log.Printf("BuildBuddy CLI sidecar logs for this build:")
		for _, line := range lines {
			fmt.Print(line)
			if !strings.HasSuffix(line, "\n") {
				fmt.Println()
			}
		}
	}
}

var logTimestampRegexp = regexp.MustCompile(`\d+/\d+/\d+ \d+:\d+:\d+\.\d+`)
var logTimestampFormat = "2006/01/02 15:04:05.000"

func parseLogTimestamp(line string) (time.Time, bool) {
	// TODO: maybe use structured logging to avoid this parsing and to allow
	// the CLI to present sidecar errors in its own style.
	m := logTimestampRegexp.FindString(line)
	if m == "" {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation(logTimestampFormat, m, time.Local)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func ConfigureSidecar(args []string) ([]string, *Instance) {
	originalArgs := args

	// Disable sidecar on CI for now since the async upload behavior can cause
	// problems if the CI runner terminates before the uploads have completed.
	if isCI(args) {
		log.Debugf("CI build detected.")
		syncFlag := arg.Get(args, "sync")
		if !isFlagTrue(syncFlag) {
			log.Debugf("CI build detected. add --sync=true")
			args = append(args, "--sync=true")
		}
	}

	log.Debugf("Configuring sidecar")

	ctx := context.Background()

	cacheDir, err := storage.CacheDir()
	if err != nil {
		log.Warnf("Sidecar could not be initialized, continuing without sidecar: %s", err)
		return args, nil
	}

	// Re(Start) the sidecar if the flags set don't match.
	sidecarArgs := []string{}
	besBackendFlag := arg.Get(args, "bes_backend")
	remoteCacheFlag := arg.Get(args, "remote_cache")
	remoteExecFlag := arg.Get(args, "remote_executor")
	synchronousWriteFlag, args := arg.Pop(args, "sync")

	// Read config YAML.
	ws, err := workspace.Path()
	if err != nil {
		// Not in a bazel workspace
		return args, nil
	}
	cf, err := config.LoadFile(filepath.Join(ws, config.WorkspaceRelativeConfigPath))
	if err != nil {
		log.Warnf("Failed to load buildbuddy.yaml: %s", err)
		return args, nil
	}

	sidecarBESEnabled := false
	if besBackendFlag != "" {
		sidecarBESEnabled = true
		sidecarArgs = append(sidecarArgs, "--bes_backend="+besBackendFlag)
	}
	sidecarCacheEnabled := remoteCacheFlag != "" && remoteExecFlag == ""
	if sidecarCacheEnabled {
		cacheArgs, ok := configureLocalCache(ctx, cacheDir, remoteCacheFlag, cf)
		if !ok {
			sidecarCacheEnabled = false
		} else {
			sidecarArgs = append(sidecarArgs, cacheArgs...)
		}
	}

	if !sidecarBESEnabled && !sidecarCacheEnabled {
		// Sidecar is not needed for this invocation; don't start it.
		return args, nil
	}

	if synchronousWriteFlag == "1" || synchronousWriteFlag == "true" {
		sidecarArgs = append(sidecarArgs, "--local_cache_proxy.synchronous_write")
		sidecarArgs = append(sidecarArgs, "--bes_synchronous")
		args = append(args, "--bes_upload_mode=wait_for_upload_complete")
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

	log.Debugf("Sidecar arguments: %v", sidecarArgs)

	var connectionErr error
	for i := 0; i < numConnectionAttempts; i++ {
		instance, err := restartSidecarIfNecessary(ctx, cacheDir, sidecarArgs)
		if err != nil {
			log.Warnf("Sidecar could not be initialized, continuing without sidecar: %s", err)
			return originalArgs, nil
		}
		if err := keepaliveSidecar(ctx, instance.SockPath); err != nil {
			// If we fail to connect, the sidecar might have been abruptly
			// killed before this CLI invocation. Attempt to remove the socket
			// and restart the sidecar before the next connection attempt.
			if err := os.Remove(instance.SockPath); err != nil {
				log.Debugf("Failed to remove sidecar socket: %s", err)
			}
			connectionErr = err
			log.Debugf("Sidecar connection error (retryable): %s", err)
			continue
		}
		if sidecarBESEnabled {
			args = append(args, fmt.Sprintf("--bes_backend=unix://%s", instance.SockPath))
		}
		if sidecarCacheEnabled {
			args = append(args, fmt.Sprintf("--remote_cache=unix://%s", instance.SockPath))
			// Set bytestream URI prefix to match the actual remote cache
			// backend, rather than the sidecar socket.
			instanceName := arg.Get(args, "remote_instance_name")
			args = append(args, fmt.Sprintf("--remote_bytestream_uri_prefix=%s", bytestreamURIPrefix(remoteCacheFlag, instanceName)))
		}
		return args, instance
	}
	log.Warnf("Could not connect to sidecar, continuing without sidecar: %s", connectionErr)
	return originalArgs, nil
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
	conn, err := grpc_client.DialSimple("unix://" + sidecarSocket)
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
				log.Warnf("BuildBuddy CLI sidecar did not respond to ping request: %s", err)
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

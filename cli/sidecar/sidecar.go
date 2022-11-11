package sidecar

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
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
		log.Debugf("sidecar with args %s is already listening at %q.", args, sockPath)
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

	cacheDir, err := storage.CacheDir()
	ctx := context.Background()
	if err != nil {
		log.Printf("Sidecar could not be initialized, continuing without sidecar: %s", err)
	}

	// Re(Start) the sidecar if the flags set don't match.
	sidecarArgs := make([]string, 0)
	besBackendFlag := arg.Get(args, "bes_backend")
	remoteCacheFlag := arg.Get(args, "remote_cache")
	remoteExecFlag := arg.Get(args, "remote_executor")

	if besBackendFlag != "" {
		sidecarArgs = append(sidecarArgs, "--bes_backend="+besBackendFlag)
	}
	if remoteCacheFlag != "" && remoteExecFlag == "" {
		sidecarArgs = append(sidecarArgs, "--remote_cache="+remoteCacheFlag)
		// Also specify as disk cache directory.
		diskCacheDir := filepath.Join(cacheDir, "filecache")
		sidecarArgs = append(sidecarArgs, fmt.Sprintf("--cache_dir=%s", diskCacheDir))
	}

	if len(sidecarArgs) == 0 {
		return args
	}

	sidecarSocket, err := restartSidecarIfNecessary(ctx, cacheDir, sidecarArgs)
	if err != nil {
		log.Printf("Sidecar could not be initialized, continuing without sidecar: %s", err)
		return args
	}
	if err := keepaliveSidecar(ctx, sidecarSocket); err != nil {
		log.Printf("Could not connect to sidecar, continuing without sidecar: %s", err)
		return args
	}
	if besBackendFlag != "" {
		args = append(args, fmt.Sprintf("--bes_backend=unix://%s", sidecarSocket))
	}
	if remoteCacheFlag != "" && remoteExecFlag == "" {
		args = append(args, fmt.Sprintf("--remote_cache=unix://%s", sidecarSocket))
	}
	return args
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
	go func() {
		connectionValidated := false
		for {
			_, err := s.Ping(ctx, &scpb.PingRequest{})
			if connectionValidated && err != nil {
				log.Debugf("sidecar did not respond to ping request: %s\n", err)
				return
			}
			if !connectionValidated && err == nil {
				close(connected)
				connectionValidated = true
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
		}
	}()
	select {
	case <-connected:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("could not connect to sidecar")
	}
}

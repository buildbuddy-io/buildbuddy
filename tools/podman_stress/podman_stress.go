package main

import (
	"context"
	"flag"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/qps"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"golang.org/x/sync/errgroup"
)

// Runs several podman containers concurrently.
// Instructions:
/*
bazel build tools/podman_stress
# Find a pod to run on:
POD=$(kubectl get pods -n executor-dev -l app=executor -o json | jq -r '.items[0].metadata.name')
# Copy to the pod:
kubectl -n executor-dev ./bazel-bin/tools/podman_stress/podman_stress_/podman_stress $POD:/usr/local/bin/podman-stress
# Run it:
kubectl exec -it -n executor-dev $POD -- podman-stress -jobs=500
*/

var (
	jobs     = flag.Int("jobs", runtime.NumCPU(), "Number of concurrent podman jobs")
	filesize = flag.String("filesize", "16M", "Size of the file to be written & read back by each job")
	tmpdb    = flag.Bool("tmp_db", false, "Use tmpfs for podman db")
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	flag.Parse()

	tmpDir := "/run/containers/storage/_tmp/"
	defer os.RemoveAll(tmpDir)

	if *tmpdb {
		cleanup, err := mountTmpDb()
		if err != nil {
			return err
		}
		defer cleanup()
	}

	cmd := exec.Command("podman", "--transient-store", "pull", "mirror.gcr.io/library/ubuntu:20.04")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}

	ctx := context.Background()
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var eg errgroup.Group
	eg.SetLimit(*jobs)

	log.Infof("Starting stressors: jobs=%d filesize=%s", *jobs, *filesize)

	qps := qps.NewCounter(10 * time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				log.Infof("Rate: %.1f/sec", qps.Get())
			}
		}
	}()

	for {
		eg.Go(func() error {
			cmd, err := podmanCmd(tmpDir)
			if err != nil {
				return err
			}
			if err := cmd.Start(); err != nil {
				return err
			}
			var status error
			exited := make(chan struct{})
			go func() {
				status = cmd.Wait()
				qps.Inc()
				close(exited)
			}()
			select {
			case <-exited:
				return status
			case <-ctx.Done():
			}
			// Command was Ctrl+C'd, graceful kill.
			const gracePeriod = 1 * time.Second
			cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-exited:
				return nil
			case <-time.After(gracePeriod):
				cmd.Process.Signal(syscall.SIGKILL)
				<-exited
				return nil
			}
		})
		if ctx.Err() != nil {
			break
		}
	}
	return eg.Wait()
}

func mountTmpDb() (cleanup func() error, err error) {
	var cleanupTasks []func() error
	cleanup = func() error {
		for i := len(cleanupTasks) - 1; i >= 0; i-- {
			if err := cleanupTasks[i](); err != nil {
				return err
			}
		}
		return nil
	}
	defer func() {
		if err != nil {
			cleanup()
		}
	}()
	tmpfs, err := os.MkdirTemp("", "")
	if err != nil {
		return nil, err
	}
	cleanupTasks = append(cleanupTasks, func() error { return os.RemoveAll(tmpfs) })
	if err := syscall.Mount("", tmpfs, "tmpfs", 0, "size=16M"); err != nil {
		return nil, err
	}
	cleanupTasks = append(cleanupTasks, func() error { return syscall.Unmount(tmpfs, 0) })
	if err := os.WriteFile(filepath.Join(tmpfs, "db.sql"), nil, 0644); err != nil {
		return nil, err
	}
	if err := syscall.Mount(filepath.Join(tmpfs, "db.sql"), "/run/containers/storage/db.sql", "", syscall.MS_BIND, ""); err != nil {
		return nil, err
	}
	cleanupTasks = append(cleanupTasks, func() error {
		for {
			if err := syscall.Unmount("/run/containers/storage/db.sql", 0); err != nil {
				log.Warningf("unmount tmpfs failed; retrying...")
				time.Sleep(500 * time.Millisecond)
				continue
			}
			return nil
		}
	})
	return cleanup, nil
}

func podmanCmd(tmpDir string) (*exec.Cmd, error) {
	workspace := tmpDir + uuid.New()
	if err := os.MkdirAll(workspace, 0755); err != nil {
		return nil, err
	}
	cmd := exec.Command(
		"podman", "run",
		"--rm", "--transient-store", "--net=none",
		"--volume="+workspace+":/workspace",
		"mirror.gcr.io/library/ubuntu:20.04",
		"sh", "-c", `
			dd if=/dev/zero of=/workspace/zeros.bin bs=`+*filesize+` count=1 status=none
			cat /workspace/zeros.bin >/dev/null
		`,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd, nil
}

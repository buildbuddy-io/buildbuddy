//go:build linux && !android

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vbd"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
)

var (
	childCgroupsEnabled = flag.Bool("executor.child_cgroups_enabled", false, "On startup, sets up separate child cgroups for the executor process and any action processes that it starts. When using this flag, the executor's starting cgroup must not have any other processes besides the executor. In particular, the executor cannot be run under tini when using this flag - use 'executor.run_under_init' instead.")
	runUnderInit        = flag.Bool("executor.run_under_init", false, "Have the executor respawn itself under a lightweight init process on startup. This helps prevent buildup of zombie processes.")
)

const (
	// cgroup name for the executor process as well as any child processes that
	// aren't explicitly run in separate cgroups (e.g. executable tools).
	executorCgroupName = "buildbuddy.executor"

	// cgroup name for the parent cgroup for action executions.
	taskCgroupName = "buildbuddy.executor.tasks"
)

// setupCgroups moves the executor process to its own child cgroup, and sets up
// a separate cgroup that will be a parent cgroup for all action cgroups.
//
// The resulting cgroup hierarchy might look like this:
//
//	/sys/fs/cgroup/ # cgroup2 root
//		/kubepods.slice/pod-abc/container-123/ # k8s-provisioned cgroup
//			/buildbuddy.executor/ # executor process cgroup
//			/buildbuddy.executor.tasks/ # tasks parent cgroup
//				# Per-task cgroups:
//				/abc123/
//				/def456/
//				...
//
// This function returns the cgroup where tasks are placed, relative to
// the cgroup root. In the above example, this would be
// "kubepods.slice/pod-abc/container-123/buildbuddy.executor.tasks"
func setupCgroups() (string, error) {
	if !*childCgroupsEnabled {
		return "", nil
	}

	// Get the cgroup that the executor process was originally started in.
	// On k8s this will be something like "kubepods.slice/pod-abc/container-123"
	startingCgroup, err := cgroup.GetCurrent()
	if err != nil {
		if errors.Is(err, cgroup.ErrV1NotSupported) {
			log.Warningf("Note: executor is running under cgroup v1, which has limited support. Some functionality may not work as expected.")
			return "", nil
		}
	}

	// When running under tini, we will have already moved to the new cgroup,
	// before re-exec'ing under tini. Just return the paths that we already
	// set up.
	if os.Getenv(isRunningUnderInitEnvVarName) != "" {
		// We've been moved to the executor child cgroup already, so the
		// original starting cgroup is the parent of the current cgroup.
		startingCgroup = filepath.Dir(startingCgroup)
		return filepath.Join(startingCgroup, taskCgroupName), nil
	}

	// Create the executor cgroup and move the executor process to it.
	executorCgroupPath := filepath.Join(cgroup.RootPath, startingCgroup, executorCgroupName)
	if err := os.MkdirAll(executorCgroupPath, 0755); err != nil {
		return "", fmt.Errorf("create executor cgroup %s: %w", executorCgroupPath, err)
	}
	if err := os.WriteFile(filepath.Join(executorCgroupPath, "cgroup.procs"), []byte(strconv.Itoa(os.Getpid())), 0); err != nil {
		return "", fmt.Errorf("add executor to cgroup %s: %w", executorCgroupPath, err)
	}
	log.Infof("Set up executor cgroup at %s", executorCgroupPath)

	// Create the cgroup for action execution.
	taskCgroupPath := filepath.Join(cgroup.RootPath, startingCgroup, taskCgroupName)
	if err := os.MkdirAll(taskCgroupPath, 0755); err != nil {
		return "", fmt.Errorf("create task cgroup %s: %w", taskCgroupPath, err)
	}
	log.Infof("Set up task cgroup at %s", taskCgroupPath)

	// Enable the same controllers for the child cgroups that were enabled
	// for the starting cgroup.
	if err := cgroup.DelegateControllers(filepath.Join(cgroup.RootPath, startingCgroup)); err != nil {
		return "", fmt.Errorf("inherit subtree control: %w", err)
	}

	taskCgroupRelpath := filepath.Join(startingCgroup, taskCgroupName)
	return taskCgroupRelpath, nil
}

func execUnderInitProcess() error {
	if !*runUnderInit {
		return nil
	}
	if os.Getenv(isRunningUnderInitEnvVarName) != "" {
		// Already running as child of init.
		return nil
	}
	// Sanity check: make sure we're pid 1, otherwise there might already be an
	// init process running. We could technically run tini as a subreaper
	// process, but it's just not necessary, assuming pid 1 is already an init
	// process.
	if os.Getpid() != 1 {
		return fmt.Errorf("run_under_init: executor must be pid 1 to use this flag")
	}

	os.Setenv(isRunningUnderInitEnvVarName, "1")

	var tiniPath string
	// Look for tini in / (where it should be found in the executor image) and
	// in PATH.
	// TODO: consolidate this logic with ociruntime.
	_, err := os.Stat("/tini")
	if err == nil {
		tiniPath = "/tini"
	} else {
		tiniPath, err = exec.LookPath("tini")
		if err != nil {
			return fmt.Errorf("run_under_init: could not find tini in PATH")
		}
	}

	cmd := append([]string{tiniPath, "--"}, os.Args...)
	return syscall.Exec(cmd[0], cmd, os.Environ())
}

func setupNetworking(rootContext context.Context) {
	// Clean up net namespaces in case vestiges remain from a previous executor.
	if !networking.PreserveExistingNetNamespaces() {
		if err := networking.DeleteNetNamespaces(rootContext); err != nil {
			log.Debugf("Error cleaning up old net namespaces:  %s", err)
		}
	}
	if err := networking.Configure(rootContext); err != nil {
		fmt.Printf("Error configuring secondary network: %s", err)
		os.Exit(1)
	}
}

func cleanupFUSEMounts() {
	if err := vbd.CleanStaleMounts(); err != nil {
		log.Warningf("Failed to cleanup Virtual Block Device mounts from previous runs: %s", err)
	}
}

func cleanBuildRoot(ctx context.Context, buildRoot string) error {
	return disk.CleanDirectory(ctx, buildRoot)
}

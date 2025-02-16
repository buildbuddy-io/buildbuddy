//go:build linux && !android

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vbd"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
)

var (
	childCgroupsEnabled = flag.Bool("executor.child_cgroups_enabled", false, "On startup, sets up separate child cgroups for the executor process and any action processes that it starts. When using this flag, the executor's starting cgroup must not have any other processes besides the executor. In particular, the executor cannot be run under tini when using this flag.")
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

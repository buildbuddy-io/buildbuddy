//go:build linux && !android

package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vbd"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
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
	// Get the cgroup that the executor process was originally started in. On
	// k8s this will be something like "kubepods.slice/pod-abc/container-123".
	//
	// When running locally as a non-root user this is typically something like
	// "user.slice/user-{uid}.slice/user@{uid}.service/app.slice/{terminal-app-name}-{terminal-app-pid}.scope"
	// or "user.slice/user-{uid}.slice/user@{uid}.service/session-{sid}.scope"
	startingCgroup, err := cgroup.GetCurrent()
	if err != nil {
		if errors.Is(err, cgroup.ErrV1NotSupported) {
			log.Warningf("Note: executor is running under cgroup v1, which has limited support. Some functionality may not work as expected.")
			return "", nil
		}
	}

	if !*childCgroupsEnabled {
		return startingCgroup, nil
	}

	return cgroup.SetupChildCgroups(executorCgroupName, taskCgroupName)
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

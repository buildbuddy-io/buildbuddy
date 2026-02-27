//go:build linux && !android

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vbd"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	childCgroupsEnabled  = flag.Bool("executor.child_cgroups_enabled", false, "On startup, sets up separate child cgroups for the executor process and any action processes that it starts. When using this flag, the executor's starting cgroup must not have any other processes besides the executor.")
	childCgroupsMoveTini = flag.Bool("executor.child_cgroups_move_tini", false, "If true, and child_cgroups_enabled is true, and the parent process is tini (pid 1), move tini into the child cgroup as well. This is needed to avoid violating the 'no internal process constraint' of cgroups when running the executor under tini.")
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
	executorCgroup := filepath.Join(startingCgroup, executorCgroupName)
	executorCgroupPath := filepath.Join(cgroup.RootPath, executorCgroup)
	if err := os.MkdirAll(executorCgroupPath, 0755); err != nil {
		return "", fmt.Errorf("create executor cgroup %s: %w", executorCgroupPath, err)
	}
	if err := os.WriteFile(filepath.Join(executorCgroupPath, "cgroup.procs"), []byte(strconv.Itoa(os.Getpid())), 0); err != nil {
		return "", fmt.Errorf("add executor to cgroup %s: %w", executorCgroupPath, err)
	}
	log.Infof("Set up executor cgroup at %s", executorCgroupPath)

	if *childCgroupsMoveTini {
		// Move tini to the executor cgroup. This must be done before we
		// delegate controllers, otherwise it violates the "no internal
		// processes constraint".
		if err := moveTiniToExecutorCgroup(executorCgroupPath); err != nil {
			return "", fmt.Errorf("move tini to cgroup %s: %w", executorCgroupPath, err)
		}
	}

	// Create the cgroup for action execution.
	taskCgroup := filepath.Join(startingCgroup, taskCgroupName)
	taskCgroupPath := filepath.Join(cgroup.RootPath, taskCgroup)
	if err := os.MkdirAll(taskCgroupPath, 0755); err != nil {
		return "", fmt.Errorf("create task cgroup %s: %w", taskCgroupPath, err)
	}
	log.Infof("Set up task cgroup at %s", taskCgroupPath)

	if err := prometheus.Register(newCgroupMemCollector(executorCgroup, taskCgroup)); err != nil {
		return "", fmt.Errorf("register cgroup mem collector: %w", err)
	}

	// Enable the same controllers for the child cgroups that were enabled
	// for the starting cgroup.
	if err := cgroup.DelegateControllers(filepath.Join(cgroup.RootPath, startingCgroup)); err != nil {
		return "", fmt.Errorf("inherit subtree control: %w", err)
	}

	taskCgroupRelpath := filepath.Join(startingCgroup, taskCgroupName)
	return taskCgroupRelpath, nil
}

func moveTiniToExecutorCgroup(executorCgroupPath string) error {
	if os.Getpid() == 1 {
		return fmt.Errorf("tini is not pid 1 (executor is running as root process in namespace)")
	}
	if os.Getppid() != 1 {
		return fmt.Errorf("executor is not running directly under pid 1")
	}
	// Sanity check that we're actually running under tini. If the init binary
	// is named something like `systemd` then this probably means we're not
	// running inside a container.
	b, err := os.ReadFile("/proc/1/comm")
	if err != nil {
		return fmt.Errorf("read /proc/1/comm: %w", err)
	}
	comm := strings.TrimSpace(string(b))
	if comm != "tini" {
		// Not running under tini.
		return fmt.Errorf("pid 1 is not tini (pid 1 is %q)", comm)
	}
	// Move tini into the executor cgroup.
	if err := os.WriteFile(filepath.Join(executorCgroupPath, "cgroup.procs"), []byte("1"), 0); err != nil {
		return fmt.Errorf("add executor to cgroup %s: %w", executorCgroupPath, err)
	}
	log.Infof("Moved pid 1 (in current namespace) into executor cgroup %s", executorCgroupPath)
	return nil
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

type cgroupMemCollector struct {
	desc *prometheus.Desc
	// cgroup v2 names (paths relative to cgroup root)
	cgroups []string
}

func newCgroupMemCollector(cgroups ...string) *cgroupMemCollector {
	return &cgroupMemCollector{
		desc: prometheus.NewDesc(
			"buildbuddy_remote_execution_cgroup_memory_current_bytes",
			"Memory usage from the cgroup2 memory.current file, split by child cgroup",
			[]string{"cgroup"},
			nil,
		),
		cgroups: cgroups,
	}
}

func (c *cgroupMemCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *cgroupMemCollector) Collect(ch chan<- prometheus.Metric) {
	for _, cg := range c.cgroups {
		value, err := cgroup.ReadMemoryCurrent(filepath.Join(cgroup.RootPath, cg))
		if err != nil {
			log.Warningf("Failed to read memory.current for cgroup %q: %s", cg, err)
			continue
		}
		m, err := prometheus.NewConstMetric(
			c.desc,
			prometheus.GaugeValue,
			float64(value),
			filepath.Base(cg), // "cgroup" label value
		)
		if err != nil {
			log.Errorf("Create const metric: %s", err)
			continue
		}
		ch <- m
	}
}

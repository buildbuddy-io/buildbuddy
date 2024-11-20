//go:build linux && !android

package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vbd"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
)

var (
	inheritCgroup = flag.Bool("executor.inherit_cgroup", false, "Ensure that action cgroups inherit from the executor's starting cgroup. This flag will be removed in the future.", flag.Internal)
)

// getActionsCgroupParent returns the parent cgroup under which all action child
// cgroups should be created. The returned cgroup path is relative to the cgroup
// FS root path.
func getActionsCgroupParent() (string, error) {
	if !*inheritCgroup {
		return "", nil
	}
	relpath, err := cgroup.GetCurrent()
	if err != nil {
		if errors.Is(err, cgroup.ErrV1NotSupported) {
			log.Warningf("Note: executor is running under cgroup v1, which has limited support. Some functionality may not work as expected.")
			return "", nil
		}
	}
	// Return the executor cgroup directly, for now.
	// TODO(https://github.com/buildbuddy-io/buildbuddy-internal/issues/4110):
	// restructure cgroups so that all actions are run in a single cgroup and
	// the executor runs as a sibling of that.
	log.Infof("Using current process cgroup %q for action execution", relpath)
	return relpath, nil
}

func setupNetworking(rootContext context.Context) {
	// Clean up net namespaces in case vestiges remain from a previous executor.
	if !networking.PreserveExistingNetNamespaces() {
		if err := networking.DeleteNetNamespaces(rootContext); err != nil {
			log.Debugf("Error cleaning up old net namespaces:  %s", err)
		}
	}
	if err := networking.ConfigureRoutingForIsolation(rootContext); err != nil {
		fmt.Printf("Error configuring secondary network: %s", err)
		os.Exit(1)
	}
}

func cleanupFUSEMounts() {
	if err := vbd.CleanStaleMounts(); err != nil {
		log.Warningf("Failed to cleanup Virtual Block Device mounts from previous runs: %s", err)
	}
}

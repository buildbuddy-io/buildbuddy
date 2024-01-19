//go:build linux && !android

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/podman"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vbd"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/nsutil"
)

func runInNamespaceAsRoot() {
	child, err := nsutil.Unshare(
		// Map our current uid/gid to 0:0 (root) within the new namespace.
		nsutil.MapID(0, 0),
		// Unshare the mount namespace so that we can create overlayfs mounts.
		nsutil.UnshareMount,
	)
	if err != nil {
		log.Fatalf("pseudo-root: unshare failed: %s", err)
	}
	if child != nil {
		// We're the original (parent) process. Wait for the child, then exit.
		nsutil.TerminateAfter(child)
	}
	// We're the pseudo-root child process. Continue execution in main.
}

func setupNetworking(rootContext context.Context) {
	// Clean up net namespaces in case vestiges remain from a previous executor.
	if !networking.PreserveExistingNetNamespaces() {
		if err := networking.DeleteNetNamespaces(rootContext); err != nil {
			log.Debugf("Error cleaning up old net namespaces:  %s", err)
		}
	}
	if err := networking.ConfigurePolicyBasedRoutingForSecondaryNetwork(rootContext); err != nil {
		fmt.Printf("Error configuring secondary network: %s", err)
		os.Exit(1)
	}

	if networking.IsSecondaryNetworkEnabled() {
		if err := podman.ConfigureSecondaryNetwork(rootContext); err != nil {
			fmt.Printf("Error configuring secondary network for podman: %s", err)
			os.Exit(1)
		}
	}
}

func cleanupFUSEMounts() {
	if err := vbd.CleanStaleMounts(); err != nil {
		log.Warningf("Failed to cleanup Virtual Block Device mounts from previous runs: %s", err)
	}
}

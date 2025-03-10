//go:build linux && !android

package runner

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/bare"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/ociruntime"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/podman"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func (p *pool) registerContainerProviders(ctx context.Context, providers map[platform.ContainerType]container.Provider, executor *platform.ExecutorProperties) error {
	if executor.SupportsIsolation(platform.DockerContainerType) {
		dockerProvider, err := docker.NewProvider(p.env, p.hostBuildRoot())
		if err != nil {
			return status.FailedPreconditionErrorf("Failed to initialize docker container provider: %s", err)
		}
		providers[platform.DockerContainerType] = dockerProvider
	}

	if executor.SupportsIsolation(platform.PodmanContainerType) {
		if err := podman.ConfigureIsolation(ctx); err != nil {
			return status.WrapError(err, "configure podman networking")
		}

		podmanProvider, err := podman.NewProvider(p.env, *rootDirectory)
		if err != nil {
			return status.FailedPreconditionErrorf("Failed to initialize podman container provider: %s", err)
		}
		providers[platform.PodmanContainerType] = podmanProvider
	}

	if executor.SupportsIsolation(platform.FirecrackerContainerType) {
		firecrackerProvider, err := firecracker.NewProvider(p.env, *rootDirectory, p.cacheRoot)
		if err != nil {
			return status.FailedPreconditionErrorf("Failed to initialize firecracker container provider: %s", err)
		}
		providers[platform.FirecrackerContainerType] = firecrackerProvider
	}

	if executor.SupportsIsolation(platform.BareContainerType) {
		providers[platform.BareContainerType] = &bare.Provider{}
	}

	if executor.SupportsIsolation(platform.OCIContainerType) {
		ociProvider, err := ociruntime.NewProvider(p.env, p.buildRoot, p.cacheRoot)
		if err != nil {
			return status.FailedPreconditionErrorf("Failed to initialize OCI container provider: %s", err)
		}
		providers[platform.OCIContainerType] = ociProvider
	}

	return nil
}

func (r *taskRunner) prepareVFS(ctx context.Context, layout *container.FileSystemLayout) error {
	if r.PlatformProperties.EnableVFS {
		// Unlike other "container" implementations, for Firecracker VFS is mounted inside the guest VM so we need to
		// pass the layout information to the implementation.
		if fc, ok := r.Container.Delegate.(container.VM); ok {
			fc.SetTaskFileSystemLayout(layout)
		}
	}
	return nil
}

// If a firecracker runner has exceeded a certain % of allocated memory or disk, don't try to recycle
// it, because that may cause failures if it's reused, and we don't want to save
// bad snapshots to the cache.
func (r *taskRunner) hasMaxResourceUtilization(ctx context.Context, usageStats *repb.UsageStats) bool {
	if fc, ok := r.Container.Delegate.(container.VM); ok {
		maxedOutStr := ""
		maxMemory := false
		maxDisk := false

		for _, fsUsage := range usageStats.GetPeakFileSystemUsage() {
			if float64(fsUsage.UsedBytes)/float64(fsUsage.TotalBytes) >= maxRecyclableResourceUtilization {
				maxedOutStr += fmt.Sprintf(" %d/%d B disk used for %s", fsUsage.UsedBytes, fsUsage.TotalBytes, fsUsage.GetSource())
				maxDisk = true
			}
		}

		usedMemoryBytes := usageStats.GetMemoryBytes()
		totalMemoryBytes := fc.VMConfig().GetMemSizeMb() * 1e6
		if usedMemoryBytes >= int64(float64(totalMemoryBytes)*maxRecyclableResourceUtilization) {
			maxedOutStr += fmt.Sprintf("%d/%d B memory used", usedMemoryBytes, totalMemoryBytes)
			maxMemory = true
		}

		if maxedOutStr != "" {
			var groupID string
			u, err := r.env.GetAuthenticator().AuthenticatedUser(ctx)
			if err == nil {
				groupID = u.GetGroupID()
			}

			errStr := fmt.Sprintf("%v runner (group_id=%s) exceeded 90%% of memory or disk usage, not recycling: %s", r.GetIsolationType(), groupID, maxedOutStr)
			debugStr := fc.SnapshotDebugString(ctx)
			var recycledLabel string
			if debugStr == "" {
				errStr += "\nRunner had started clean (not from a snapshot)"
				recycledLabel = "clean"
			} else {
				errStr += fmt.Sprintf("\nSnapshot debug key: %s", fc.SnapshotDebugString(ctx))
				recycledLabel = "recycled"
			}

			if maxDisk {
				metrics.MaxRecyclableResourceUsageEvent.With(prometheus.Labels{
					metrics.GroupID:              groupID,
					metrics.EventName:            "disk",
					metrics.RecycledRunnerStatus: recycledLabel,
				}).Inc()
			}
			if maxMemory {
				metrics.MaxRecyclableResourceUsageEvent.With(prometheus.Labels{
					metrics.GroupID:              groupID,
					metrics.EventName:            "memory",
					metrics.RecycledRunnerStatus: recycledLabel,
				}).Inc()
			}

			log.CtxErrorf(ctx, "%s", errStr)
			return true
		}
	}
	return false
}

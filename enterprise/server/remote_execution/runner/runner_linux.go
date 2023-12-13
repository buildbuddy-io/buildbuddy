//go:build linux && !android

package runner

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/bare"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/podman"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
)

func (p *pool) registerContainerProviders(providers map[platform.ContainerType]container.Provider, executor *platform.ExecutorProperties) error {
	if executor.SupportsIsolation(platform.DockerContainerType) {
		dockerProvider, err := docker.NewProvider(p.env, p.hostBuildRoot())
		if err != nil {
			return status.FailedPreconditionErrorf("Failed to initialize docker container provider: %s", err)
		}
		providers[platform.DockerContainerType] = dockerProvider
	}

	if executor.SupportsIsolation(platform.PodmanContainerType) {
		podmanProvider, err := podman.NewProvider(p.env, *rootDirectory)
		if err != nil {
			return status.FailedPreconditionErrorf("Failed to initialize podman container provider: %s", err)
		}
		providers[platform.PodmanContainerType] = podmanProvider
	}

	if executor.SupportsIsolation(platform.FirecrackerContainerType) {
		p, err := firecracker.NewProvider(p.env, *rootDirectory)
		if err != nil {
			return status.FailedPreconditionErrorf("Failed to initialize firecracker container provider: %s", err)
		}
		providers[platform.FirecrackerContainerType] = p
	}

	if executor.SupportsIsolation(platform.BareContainerType) {
		providers[platform.BareContainerType] = &bare.Provider{}
	}

	return nil
}

func (r *commandRunner) startVFS() error {
	var fs *vfs.VFS
	var vfsServer *vfs_server.Server
	enableVFS := r.PlatformProperties.EnableVFS
	// Firecracker requires mounting the FS inside the guest VM so we can't just swap out the directory in the runner.
	if enableVFS && platform.ContainerType(r.PlatformProperties.WorkloadIsolationType) != platform.FirecrackerContainerType {
		vfsDir := r.Workspace.Path() + "_vfs"
		if err := os.Mkdir(vfsDir, 0755); err != nil {
			return status.UnavailableErrorf("could not create FUSE FS dir: %s", err)
		}

		vfsServer = vfs_server.New(r.p.env, r.Workspace.Path())
		unixSocket := filepath.Join(r.Workspace.Path(), "vfs.sock")

		lis, err := net.Listen("unix", unixSocket)
		if err != nil {
			return err
		}
		if err := vfsServer.Start(lis); err != nil {
			return err
		}

		conn, err := grpc.Dial("unix://"+unixSocket, grpc.WithInsecure())
		if err != nil {
			return err
		}
		vfsClient := vfspb.NewFileSystemClient(conn)
		fs = vfs.New(vfsClient, vfsDir, &vfs.Options{})
		if err := fs.Mount(); err != nil {
			return status.UnavailableErrorf("unable to mount VFS at %q: %s", vfsDir, err)
		}
	}

	r.VFS = fs
	r.VFSServer = vfsServer
	return nil
}

func (r *commandRunner) prepareVFS(ctx context.Context, layout *container.FileSystemLayout) error {
	if r.PlatformProperties.EnableVFS {
		// Unlike other "container" implementations, for Firecracker VFS is mounted inside the guest VM so we need to
		// pass the layout information to the implementation.
		if fc, ok := r.Container.Delegate.(*firecracker.FirecrackerContainer); ok {
			fc.SetTaskFileSystemLayout(layout)
		}
	}

	if r.VFSServer != nil {
		p, err := vfs_server.NewCASLazyFileProvider(r.env, ctx, layout.RemoteInstanceName, layout.DigestFunction, layout.Inputs)
		if err != nil {
			return err
		}
		if err := r.VFSServer.Prepare(p); err != nil {
			return err
		}
	}
	if r.VFS != nil {
		if err := r.VFS.PrepareForTask(ctx, r.task.GetExecutionId()); err != nil {
			return err
		}
	}

	return nil
}

func (r *commandRunner) removeVFS() error {
	var err error
	if r.VFS != nil {
		err = r.VFS.Unmount()
	}
	if r.VFSServer != nil {
		r.VFSServer.Stop()
	}

	return err
}

// If a firecracker runner has exceeded a certain % of allocated memory or disk, don't try to recycle
// it, because that may cause failures if it's reused, and we don't want to save
// bad snapshots to the cache.
func (r *commandRunner) hasMaxResourceUtilization(ctx context.Context, usageStats *repb.UsageStats) bool {
	if fc, ok := r.Container.Delegate.(*firecracker.FirecrackerContainer); ok {
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
			u, err := perms.AuthenticatedUser(ctx, r.env)
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

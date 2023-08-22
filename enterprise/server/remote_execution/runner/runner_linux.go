//go:build linux && !android

package runner

import (
	"context"
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
	vfspb "github.com/buildbuddy-io/buildbuddy/proto/vfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
)

func (p *pool) initContainerProviders() error {
	providers := make(map[platform.ContainerType]container.Provider)
	dockerProvider, err := docker.NewProvider(p.env, p.imageCacheAuth, p.hostBuildRoot())
	if err != nil {
		return status.FailedPreconditionErrorf("Failed to initialize docker container provider: %s", err)
	}
	if dockerProvider != nil {
		providers[platform.DockerContainerType] = dockerProvider
	}
	podmanProvider, err := podman.NewProvider(p.env, p.imageCacheAuth, *rootDirectory)
	if err != nil {
		return status.FailedPreconditionErrorf("Failed to initialize podman container provider: %s", err)
	}
	if podmanProvider != nil {
		providers[platform.PodmanContainerType] = podmanProvider
	}
	providers[platform.FirecrackerContainerType] = firecracker.NewProvider(p.env, p.imageCacheAuth, *rootDirectory)
	providers[platform.BareContainerType] = &bare.Provider{}

	p.containerProviders = providers

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

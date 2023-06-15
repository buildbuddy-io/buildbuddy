//go:build darwin && !ios
// +build darwin,!ios

package firecracker

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vmfspb "github.com/buildbuddy-io/buildbuddy/proto/vmvfs"
)

type FirecrackerContainer struct {
	container.CommandContainer
}

func NewContainer(ctx context.Context, env environment.Env, imageCacheAuth *container.ImageCacheAuthenticator, task *repb.ExecutionTask, opts ContainerOpts) (*FirecrackerContainer, error) {
	return nil, status.UnimplementedError("Firecracker is unsupported on macOS")
}

func (c *FirecrackerContainer) Wait(ctx context.Context) error {
	return status.UnimplementedError("Not yet implemented.")
}

func (c *FirecrackerContainer) SetTaskFileSystemLayout(fsLayout *container.FileSystemLayout) {
}

func (c *FirecrackerContainer) LoadSnapshot(ctx context.Context) error {
	return status.UnimplementedError("Not yet implemented.")
}

func (c *FirecrackerContainer) SaveSnapshot(ctx context.Context) error {
	return status.UnimplementedError("Not yet implemented.")
}

func (c *FirecrackerContainer) SendPrepareFileSystemRequestToGuest(ctx context.Context, req *vmfspb.PrepareRequest) (*vmfspb.PrepareResponse, error) {
	return nil, status.UnimplementedError("Not yet implemented.")
}

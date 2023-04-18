package firecracker

import (
	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	dockerclient "github.com/docker/docker/client"
)

type ContainerOpts struct {
	// The static VM configuration which is applied to the VM once and does not
	// change across snapshots/resumes etc. This is computed from task platform
	// properties and possibly executor-level properties/configuration that is
	// applied to all tasks.
	//
	// This is not required if SavedState is specified, since the configuration
	// is stored along with the state.
	VMConfiguration *fcpb.VMConfiguration

	// Saved state pointing to the snapshot manifest in filecache. When set,
	// the VMConfiguration will be loaded from the snapshot manifest rather than
	// the VMConfiguration field.
	SavedState *rnpb.FirecrackerState

	// The OCI container image. ex "alpine:latest".
	ContainerImage string

	// The "USER[:GROUP]" spec to run commands as (optional).
	User string

	// DockerClient can optionally be specified to pull container images via
	// Docker. This is useful for de-duping in-flight image pull operations and
	// making use of the local Docker cache for images. If not specified, images
	// will be pulled directly by skopeo and no image pull de-duping will be
	// performed.
	DockerClient *dockerclient.Client

	// The action directory with inputs / outputs.
	ActionWorkingDirectory string

	// Optional flags -- these will default to sane values.
	// They are here primarily for debugging and running
	// VMs outside of the normal action-execution framework.

	// ForceVMIdx forces a machine to use a particular vm index,
	// allowing for multiple locally-started VMs to avoid using
	// conflicting network interfaces.
	ForceVMIdx int

	// The root directory to store all files in. This needs to be
	// short, less than 38 characters. If unset, /tmp will be used.
	JailerRoot string
}

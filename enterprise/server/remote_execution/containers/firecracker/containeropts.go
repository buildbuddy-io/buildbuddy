package firecracker

import (
	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	dockerclient "github.com/docker/docker/client"
)

type ContainerOpts struct {
	// The static VM configuration which is applied to the VM once and does not
	// change across snapshots/resumes etc. This is computed from task platform
	// properties and possibly executor-level properties/configuration that is
	// applied to all tasks.
	//
	// This is not required if SnapshotKey is specified, since the configuration
	// is stored in the snapshot.
	VMConfiguration *fcpb.VMConfiguration

	// OverrideSnapshotKey is an optional snapshot key to resume from, which
	// overrides the snapshot key that is normally computed from task platform
	// properties. If it is not found in cache, then an error will be returned
	// when attempting to create the VM.
	OverrideSnapshotKey *fcpb.SnapshotKey

	// ExecutorConfig contains executor-level configuration, such as firecracker
	// and jailer paths / versioning info. This is required.
	ExecutorConfig *ExecutorConfig

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

	// CPUWeightMillis is the CPU weight to assign to this VM, expressed as
	// CPU-millis. This is set to the task size.
	CPUWeightMillis int64

	// Optional flags -- these will default to sane values.
	// They are here primarily for debugging and running
	// VMs outside of the normal action-execution framework.

	// ForceVMIdx forces a machine to use a particular vm index,
	// allowing for multiple locally-started VMs to avoid using
	// conflicting network interfaces.
	ForceVMIdx int
}

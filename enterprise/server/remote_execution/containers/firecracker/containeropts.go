package firecracker

import (
	dockerclient "github.com/docker/docker/client"
)

type ContainerOpts struct {
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

	// The number of CPUs to allocate to this VM.
	NumCPUs int64

	// The amount of RAM, in MB, to allocate to this VM.
	MemSizeMB int64

	// The size of the scratch disk to allocate (for writing files anywhere
	// outside of the workspace directory, such as /tmp or ~/.cache).
	ScratchDiskSizeMB int64

	// Whether or not to enable networking.
	EnableNetworking bool

	// Whether or not to initialize dockerd. Docker must be installed in the
	// VM image in order for this to work.
	InitDockerd bool

	// Optional flags -- these will default to sane values.
	// They are here primarily for debugging and running
	// VMs outside of the normal action-execution framework.

	// DebugMode runs init in debugmode and enables stdin/stdout so
	// that machines can be logged into via the console.
	DebugMode bool

	// ForceVMIdx forces a machine to use a particular vm index,
	// allowing for multiple locally-started VMs to avoid using
	// conflicting network interfaces.
	ForceVMIdx int

	// The root directory to store all files in. This needs to be
	// short, less than 38 characters. If unset, /tmp will be used.
	JailerRoot string

	// Allow starting from snapshot, if one is available. This also
	// means that snapshots can be saved on Create.
	AllowSnapshotStart bool
}

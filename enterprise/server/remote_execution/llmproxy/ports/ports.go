package ports

const (
	// HostVSock is the host port for execution-scoped agent LLM proxy
	// connections forwarded from a Firecracker guest.
	HostVSock = 25412

	// GuestHTTP is the guest-loopback TCP port used by agent SDKs. goinit
	// forwards connections on this port to HostVSock over vsock.
	GuestHTTP = 25418
)

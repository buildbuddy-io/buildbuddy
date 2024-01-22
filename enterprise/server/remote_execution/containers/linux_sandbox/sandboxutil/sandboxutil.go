package sandboxutil

import (
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	sbdpb "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/proto/sandboxd"
)

func SetupMount(m *sbdpb.Mount) error {
	if err := syscall.Mount(m.Source, m.Target, m.Filesystemtype, uintptr(m.Flags), m.Data); err != nil {
		return status.WrapErrorf(err, "mount source=%s,target=%s,data=%s", m.Source, m.Target, m.Data)
	}
	return nil
}

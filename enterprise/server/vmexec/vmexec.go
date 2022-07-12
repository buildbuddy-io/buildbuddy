package vmexec

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"sync"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
	gstatus "google.golang.org/grpc/status"
)

const (
	// NOTE: These must match the values in enterprise/server/cmd/goinit/main.go

	// workspaceDevice is the path to the hot-swappable workspace block device.
	workspaceDevice = "/dev/vdc"

	// workspaceMountPath is the path where the hot-swappable workspace block
	// device is mounted.
	workspaceMountPath = "/workspace"
)

type execServer struct {
	reapMutex *sync.RWMutex
}

func NewServer(reapMutex *sync.RWMutex) (*execServer, error) {
	return &execServer{
		reapMutex: reapMutex,
	}, nil
}

func clearARPCache() error {
	handle, err := netlink.NewHandle(syscall.NETLINK_ROUTE)
	if err != nil {
		return err
	}
	links, err := netlink.LinkList()
	if err != nil {
		return err
	}
	for _, link := range links {
		attrs := link.Attrs()
		if attrs == nil {
			continue
		}
		neigbors, err := handle.NeighList(attrs.Index, netlink.FAMILY_V4)
		if err != nil {
			return err
		}
		v6neigbors, err := handle.NeighList(attrs.Index, netlink.FAMILY_V6)
		if err != nil {
			return err
		}
		neigbors = append(neigbors, v6neigbors...)
		for _, neigh := range neigbors {
			if err := handle.NeighDel(&neigh); err != nil {
				log.Errorf("Error deleting neighbor: %s", err)
			}
		}
	}
	return nil
}

func (x *execServer) Initialize(ctx context.Context, req *vmxpb.InitializeRequest) (*vmxpb.InitializeResponse, error) {
	if req.GetClearArpCache() {
		if err := clearARPCache(); err != nil {
			return nil, err
		}
		log.Debugf("Cleared ARP cache")
	}
	if req.GetUnixTimestampNanoseconds() > 1 {
		tv := syscall.NsecToTimeval(req.GetUnixTimestampNanoseconds())
		if err := syscall.Settimeofday(&tv); err != nil {
			return nil, err
		}
		log.Debugf("Set time of day to %d", req.GetUnixTimestampNanoseconds())
	}
	return &vmxpb.InitializeResponse{}, nil
}

func (x *execServer) UnmountWorkspace(ctx context.Context, req *vmxpb.UnmountWorkspaceRequest) (*vmxpb.UnmountWorkspaceResponse, error) {
	if err := syscall.Unmount(workspaceMountPath, 0); err != nil {
		return nil, status.InternalErrorf("unmount failed: %s", err)
	}
	return &vmxpb.UnmountWorkspaceResponse{}, nil
}

func (x *execServer) MountWorkspace(ctx context.Context, req *vmxpb.MountWorkspaceRequest) (*vmxpb.MountWorkspaceResponse, error) {
	if err := syscall.Mount(workspaceDevice, workspaceMountPath, "ext4", syscall.MS_RELATIME, ""); err != nil {
		return nil, err
	}
	return &vmxpb.MountWorkspaceResponse{}, nil
}

func (x *execServer) Exec(ctx context.Context, req *vmxpb.ExecRequest) (*vmxpb.ExecResponse, error) {
	if len(req.GetArguments()) < 1 {
		return nil, status.InvalidArgumentError("Arguments not specified")
	}
	if req.Timeout != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout.AsDuration())
		defer cancel()
	}

	cmd := exec.Command(req.GetArguments()[0], req.GetArguments()[1:]...)
	if req.GetWorkingDirectory() != "" {
		cmd.Dir = req.GetWorkingDirectory()
	}

	// TODO(tylerw): use syncfs or something better here.
	defer unix.Sync()

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	for _, envVar := range req.GetEnvironmentVariables() {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}

	x.reapMutex.RLock()
	defer x.reapMutex.RUnlock()

	log.Debugf("Running command in VM: %q", cmd.String())
	_, err := commandutil.RunWithProcessTreeCleanup(ctx, cmd, nil /*=statsListener*/)
	exitCode, err := commandutil.ExitCode(ctx, cmd, err)
	rsp := &vmxpb.ExecResponse{}
	rsp.ExitCode = int32(exitCode)
	rsp.Status = gstatus.Convert(err).Proto()
	rsp.Stdout = stdoutBuf.Bytes()
	rsp.Stderr = stderrBuf.Bytes()
	return rsp, nil
}

func (x *execServer) ExecStreamed(stream vmxpb.Exec_ExecStreamedServer) error {
	return status.UnimplementedError("not implemented")
}

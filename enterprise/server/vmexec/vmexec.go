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

func (x *execServer) Exec(ctx context.Context, req *vmxpb.ExecRequest) (*vmxpb.ExecResponse, error) {
	if len(req.GetArguments()) < 1 {
		return nil, status.InvalidArgumentError("Arguments not specified")
	}
	cmd := exec.CommandContext(ctx, req.GetArguments()[0], req.GetArguments()[1:]...)
	if req.GetWorkingDirectory() != "" {
		cmd.Dir = req.GetWorkingDirectory()
	}

	// TODO(tylerw): implement this.
	if req.GetStdinVsockPort() != 0 {
		return nil, status.UnimplementedError("Vsock stdin not implemented")
	}
	if req.GetStdoutVsockPort() != 0 {
		return nil, status.UnimplementedError("Vsock stdout not implemented")
	}
	if req.GetStderrVsockPort() != 0 {
		return nil, status.UnimplementedError("Vsock stderr not implemented")
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
	err := cmd.Run()
	exitCode, err := commandutil.ExitCode(ctx, cmd, err)
	if err != nil {
		return nil, err
	}

	rsp := &vmxpb.ExecResponse{}
	rsp.ExitCode = int32(exitCode)
	rsp.Stdout = stdoutBuf.Bytes()
	rsp.Stderr = stderrBuf.Bytes()
	return rsp, nil
}

package vmexec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

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

func unmountWorkspace() error {
	if err := syscall.Unmount("/workspace", 0); err != nil {
		return err
	}
	unix.Sync()
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
	if req.GetUnmountWorkspace() {
		if err := unmountWorkspace(); err != nil {
			return nil, status.InternalErrorf("unmount failed: %s", err)
		}
	}
	return &vmxpb.InitializeResponse{}, nil
}

func (x *execServer) SyncWorkspace(stream vmxpb.Exec_SyncWorkspaceServer) error {
	const wsPath = "/workspace"
	entries, err := os.ReadDir(wsPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(wsPath, entry.Name())); err != nil {
			return err
		}
	}
	for {
		req, err := stream.Recv()
		eof := err == io.EOF
		if err != nil && !eof {
			return nil
		}
		for _, dirPath := range req.GetDirectories() {
			if err := os.Mkdir(filepath.Join(wsPath, dirPath), 0755); err != nil {
				return err
			}
		}
		for _, chunk := range req.GetChunks() {
			var f *os.File
			var err error
			if chunk.Offset == 0 {
				mode := os.FileMode(0666)
				if chunk.Executable {
					mode |= 0111
				}
				f, err = os.OpenFile(filepath.Join(wsPath, chunk.Path), os.O_CREATE|os.O_WRONLY|os.O_APPEND, mode)
				if err != nil {
					return err
				}
			} else {
				f, err = os.OpenFile(filepath.Join(wsPath, chunk.Path), os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					return err
				}
			}
			if _, err := f.Write(chunk.Content); err != nil {
				return err
			}
		}
		if eof {
			if err := stream.SendAndClose(&vmxpb.SyncWorkspaceResponse{}); err != nil {
				return err
			}
			return nil
		}
	}
}

func (x *execServer) RemountWorkspace(ctx context.Context, req *vmxpb.RemountWorkspaceRequest) (*vmxpb.RemountWorkspaceResponse, error) {
	log.Infof("execServer: Handling /RemountWorkspace")
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Mount("/dev/vdc", "/workspace", "ext4", syscall.MS_RELATIME, ""); err != nil {
		return nil, err
	}
	return &vmxpb.RemountWorkspaceResponse{}, nil
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

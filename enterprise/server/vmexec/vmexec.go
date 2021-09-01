package vmexec

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/casfs"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/dirtools"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/mdlayher/vsock"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"

	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	// The port on which the host proxy is listening.
	byteStreamProxyPort = 9292
)

type execServer struct {
	reapMutex *sync.RWMutex
	cfs       *casfs.CASFS
	bsClient  bspb.ByteStreamClient
}

func NewServer(reapMutex *sync.RWMutex) (*execServer, error) {
	if err := os.Mkdir("/casfs", 0755); err != nil {
		return nil, err
	}
	cfs := casfs.New("/workspace", "/casfs/", &casfs.Options{})
	if err := cfs.Mount(); err != nil {
		return nil, status.InternalErrorf("Could not mount CASFS: %s", err)
	}

	vsockDialer := func(ctx context.Context, s string) (net.Conn, error) {
		conn, err := vsock.Dial(vsock.Host, byteStreamProxyPort)
		return conn, err
	}
	conn, err := grpc.Dial("vsock", grpc.WithContextDialer(vsockDialer), grpc.WithInsecure())
	if err != nil {
		return nil, status.InternalErrorf("Could not dial host: %s", err)
	}
	bsClient := bspb.NewByteStreamClient(conn)

	return &execServer{
		reapMutex: reapMutex,
		cfs:       cfs,
		bsClient:  bsClient,
	}, nil
}

func (x *execServer) Exec(ctx context.Context, req *vmxpb.ExecRequest) (*vmxpb.ExecResponse, error) {
	if len(req.GetArguments()) < 1 {
		return nil, status.InvalidArgumentError("Arguments not specified")
	}
	cmd := exec.CommandContext(ctx, req.GetArguments()[0], req.GetArguments()[1:]...)
	if req.GetWorkingDirectory() != "" {
		cmd.Dir = req.GetWorkingDirectory()
	}

	if req.CasfsConfiguration != nil {
		layout := req.CasfsConfiguration.GetFileSystemLayout()
		ff := dirtools.NewBatchFileFetcher(context.Background(), layout.GetRemoteInstanceName(), nil /* =fileCache */, x.bsClient, nil /* casClient= */)
		if err := x.cfs.PrepareForTask(ctx, ff, "fc" /* =taskID */, &container.FileSystemLayout{Inputs: layout.GetInputs()}); err != nil {
			return nil, err
		}
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

	rsp := &vmxpb.ExecResponse{}
		return rsp, nil
	}

	log.Debugf("Running command in VM: %q", cmd.String())
	err := cmd.Run()
	exitCode, err := commandutil.ExitCode(ctx, cmd, err)
	if err != nil {
		return nil, err
	}

	rsp.ExitCode = int32(exitCode)
	rsp.Stdout = stdoutBuf.Bytes()
	rsp.Stderr = stderrBuf.Bytes()
	return rsp, nil
}

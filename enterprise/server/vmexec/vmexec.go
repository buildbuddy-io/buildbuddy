package vmexec

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
)

type execServer struct{}

func NewServer() *execServer {
	return &execServer{}
}

func (*execServer) Exec(ctx context.Context, req *vmxpb.ExecRequest) (*vmxpb.ExecResponse, error) {
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

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	for _, envVar := range req.GetEnvironmentVariables() {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}

	rsp := &vmxpb.ExecResponse{}
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

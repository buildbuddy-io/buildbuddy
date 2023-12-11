package vmexec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/elastic/gosigar"
	"github.com/tklauser/go-sysconf"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
	gstatus "google.golang.org/grpc/status"
)

func init() {
	clktck, err := sysconf.Sysconf(sysconf.SC_CLK_TCK)
	if err != nil {
		// Treat this as a fatal error because we could potentially report
		// inaccurate CPU usage otherwise.
		log.Fatalf("sysconf(SC_CLK_TCK): %s", err)
	}
	ticksPerSec = clktck
}

const (
	// NOTE: These must match the values in enterprise/server/cmd/goinit/main.go

	// workspaceMountPath is the path where the hot-swappable workspace block
	// device is mounted.
	workspaceMountPath = "/workspace"

	// Stats polling starts off high-frequency to improve the chances of
	// collecting some stats for short-running tasks, then slowly backs off to
	// avoid adding excessive overhead for longer-running tasks.
	initialStatsPollInterval = 10 * time.Millisecond
	maxStatsPollInterval     = 250 * time.Millisecond
	statsPollBackoff         = 1.1
)

var (
	// Number of CPU ticks per second - used for CPU usage stats.
	ticksPerSec int64
)

type execServer struct {
	// workspaceDevice is the path to the hot-swappable workspace block device.
	workspaceDevice string
}

func NewServer(workspaceDevice string) (*execServer, error) {
	return &execServer{workspaceDevice}, nil
}

func clearARPCache() error {
	handle, err := netlink.NewHandle(syscall.NETLINK_ROUTE)
	if err != nil {
		return err
	}
	defer handle.Close()
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

func (x *execServer) Sync(ctx context.Context, req *vmxpb.SyncRequest) (*vmxpb.SyncResponse, error) {
	unix.Sync()
	return &vmxpb.SyncResponse{}, nil
}

func (x *execServer) UnmountWorkspace(ctx context.Context, req *vmxpb.UnmountWorkspaceRequest) (*vmxpb.UnmountWorkspaceResponse, error) {
	if err := syscall.Unmount(workspaceMountPath, 0); err != nil {
		return nil, status.InternalErrorf("unmount failed: %s", err)
	}
	return &vmxpb.UnmountWorkspaceResponse{}, nil
}

func (x *execServer) MountWorkspace(ctx context.Context, req *vmxpb.MountWorkspaceRequest) (*vmxpb.MountWorkspaceResponse, error) {
	if err := syscall.Mount(x.workspaceDevice, workspaceMountPath, "ext4", syscall.MS_NOATIME, ""); err != nil {
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

type message struct {
	Response *vmxpb.ExecStreamedResponse
	Err      error
}

func (x *execServer) ExecStreamed(stream vmxpb.Exec_ExecStreamedServer) error {
	ctx := stream.Context()

	msgs := make(chan *message, 128)

	go func() {
		var cmd *command
		var cmdFinished chan struct{}
		defer func() {
			if cmdFinished != nil {
				// If a command is running, then it may still be streaming
				// messages. Wait for all messages to be streamed before we
				// close the channel.
				<-cmdFinished
			}
			close(msgs)
		}()
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				// If the client hasn't yet started a command, they shouldn't have
				// closed the stream yet. Report an error.
				if cmd == nil {
					msgs <- &message{Err: status.InvalidArgumentError("stream was closed without receiving exec start request")}
					return
				}
				// Otherwise if a command was started and we have an open stdin
				// pipe, the client closing their end of the stream means we
				// should close the stdin pipe.
				if cmd.stdin != nil {
					if err := cmd.stdin.Close(); err != nil {
						msgs <- &message{Err: status.InternalErrorf("failed to close stdin pipe: %s", err)}
						return
					}
				}
				// Done receiving messages; return.
				return
			}
			if err != nil {
				msgs <- &message{Err: err}
				return
			}
			if msg.Start != nil {
				if cmd != nil {
					msgs <- &message{Err: status.InvalidArgumentError("received multiple exec start requests")}
					return
				}
				cmd, err = newCommand(msg.Start)
				if err != nil {
					msgs <- &message{Err: err}
					return
				}
				cmdFinished = make(chan struct{})
				go func() {
					defer close(cmdFinished)
					res, err := cmd.Run(ctx, msgs)
					if err != nil {
						msgs <- &message{Err: err}
						return
					}
					msgs <- &message{Response: res}
				}()
			}
			if len(msg.Stdin) > 0 {
				if cmd == nil {
					msgs <- &message{Err: status.InvalidArgumentError("received stdin before exec start request")}
					return
				}
				if cmd.stdin == nil {
					msgs <- &message{Err: status.InvalidArgumentError("received stdin without specifying open_stdin in exec start request")}
					return
				}
				if _, err := cmd.stdin.Write(msg.Stdin); err != nil {
					msgs <- &message{Err: status.InternalErrorf("failed to write stdin: %s", err)}
					return
				}
			}
		}
	}()
	for msg := range msgs {
		if msg.Err != nil {
			return msg.Err
		}
		if err := stream.Send(msg.Response); err != nil {
			return status.InternalErrorf("failed to send response: %s", err)
		}
	}
	return nil
}

type command struct {
	cmd *exec.Cmd

	stdin        io.WriteCloser
	stdoutWriter *io.PipeWriter
	stdoutReader *io.PipeReader
	stderrWriter *io.PipeWriter
	stderrReader *io.PipeReader
}

func newCommand(start *vmxpb.ExecRequest) (*command, error) {
	if len(start.GetArguments()) == 0 {
		return nil, status.InvalidArgumentError("arguments not specified")
	}
	cmd := exec.Command(start.GetArguments()[0], start.GetArguments()[1:]...)
	if start.GetWorkingDirectory() != "" {
		cmd.Dir = start.GetWorkingDirectory()
	}
	for _, envVar := range start.GetEnvironmentVariables() {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", envVar.GetName(), envVar.GetValue()))
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if start.GetUser() != "" {
		// Need to make the top-level workspace dir writable by non-root users,
		// since we created it with 0755 perms. Subdirs have the proper
		// permissions though, so we don't need to recurse.
		if err := os.Chmod(start.GetWorkingDirectory(), 0777); err != nil {
			return nil, status.InternalErrorf("failed to update workspace dir perms")
		}

		err := commandutil.SetCredential(cmd, start.GetUser())
		if err != nil {
			return nil, err
		}
	}

	stdoutReader, stdoutWriter := io.Pipe()
	cmd.Stdout = stdoutWriter
	stderrReader, stderrWriter := io.Pipe()
	cmd.Stderr = stderrWriter
	var stdin io.WriteCloser
	if start.GetOpenStdin() {
		stdinPipe, err := cmd.StdinPipe()
		if err != nil {
			return nil, status.InternalErrorf("failed to open stdin: %s", err)
		}
		stdin = stdinPipe
	}
	return &command{
		cmd:          cmd,
		stdin:        stdin,
		stdoutReader: stdoutReader,
		stdoutWriter: stdoutWriter,
		stderrReader: stderrReader,
		stderrWriter: stderrWriter,
	}, nil
}

func (c *command) Run(ctx context.Context, msgs chan *message) (*vmxpb.ExecStreamedResponse, error) {
	// TODO(tylerw): use syncfs or something better here.
	defer unix.Sync()

	log.Debugf("Running command in VM: %q", c.cmd.String())
	stdoutErrCh := make(chan error, 1)
	go func() {
		_, err := io.Copy(&stdoutWriter{msgs}, c.stdoutReader)
		stdoutErrCh <- err
	}()
	stderrErrCh := make(chan error, 1)
	go func() {
		_, err := io.Copy(&stderrWriter{msgs}, c.stderrReader)
		stderrErrCh <- err
	}()

	// Start a goroutine to monitor CPU, memory, and disk usage while the
	// command is running.
	var peakFSU []*repb.UsageStats_FileSystemUsage
	var peakMem int64
	commandDone := make(chan struct{})
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		delay := initialStatsPollInterval
		for done := false; !done; {
			select {
			case <-commandDone:
				// Collect stats one more time, then terminate the loop.
				done = true
			case <-time.After(delay):
			}
			delay = min(maxStatsPollInterval, time.Duration(float64(delay)*statsPollBackoff))

			// Collect disk usage.
			stats := &repb.UsageStats{}
			if fsu := getFileSystemUsage(); fsu != nil {
				peakFSU = updatePeakFileSystemUsage(peakFSU, fsu)
			}
			stats.PeakFileSystemUsage = peakFSU
			// Collect memory usage.
			mem := &gosigar.Mem{}
			if err := mem.Get(); err == nil {
				stats.MemoryBytes = int64(mem.ActualUsed)
				if stats.MemoryBytes > peakMem {
					peakMem = stats.MemoryBytes
				}
				stats.PeakMemoryBytes = peakMem
			}
			// Collect CPU usage.
			cpu := &gosigar.Cpu{}
			if err := cpu.Get(); err == nil {
				// Note: gosigar's cpu.Total() returns an undesired value here -
				// it includes "idle" and "wait" time which is not time spent
				// doing actual work.
				ticks := int64(cpu.User + cpu.Nice + cpu.Sys + cpu.Irq + cpu.SoftIrq)
				const nanosPerSec = 1e9
				nanosPerTick := nanosPerSec / ticksPerSec
				stats.CpuNanos = ticks * nanosPerTick
			}
			msgs <- &message{Response: &vmxpb.ExecStreamedResponse{UsageStats: stats}}
		}
	}()
	// Using a nil statsListener since we'd rather report stats from the whole
	// VM (which includes e.g. docker-in-firecracker containers) -- not just the
	// process being run.
	_, err := commandutil.RunWithProcessTreeCleanup(ctx, c.cmd, nil /*=statsListener*/)

	close(commandDone)
	<-statsDone

	exitCode, err := commandutil.ExitCode(ctx, c.cmd, err)
	rsp := &vmxpb.ExecResponse{
		ExitCode: int32(exitCode),
		Status:   gstatus.Convert(err).Proto(),
	}
	c.stdoutWriter.Close()
	if err := <-stdoutErrCh; err != nil {
		return nil, status.InternalErrorf("failed to copy stdout: %s", err)
	}
	c.stderrWriter.Close()
	if err := <-stderrErrCh; err != nil {
		return nil, status.InternalErrorf("failed to copy stderr: %s", err)
	}
	return &vmxpb.ExecStreamedResponse{Response: rsp}, nil
}

type stdoutWriter struct{ msgs chan *message }

func (w *stdoutWriter) Write(b []byte) (int, error) {
	w.msgs <- &message{Response: &vmxpb.ExecStreamedResponse{Stdout: b}}
	return len(b), nil
}

type stderrWriter struct{ msgs chan *message }

func (w *stderrWriter) Write(b []byte) (int, error) {
	w.msgs <- &message{Response: &vmxpb.ExecStreamedResponse{Stderr: b}}
	return len(b), nil
}

func getFileSystemUsage() []*repb.UsageStats_FileSystemUsage {
	fsl := &gosigar.FileSystemList{}
	if err := fsl.Get(); err != nil {
		log.Errorf("Failed to get filesystem usage: %s", err)
		return nil
	}
	out := make([]*repb.UsageStats_FileSystemUsage, 0, len(fsl.List))
	for _, fs := range fsl.List {
		// Only consider the root FS and the workspace FS.
		// Otherwise the list is really messy, containing things like
		// tmpfs, udev, cgroup, etc. which aren't very interesting.
		if !(fs.DirName == "/" || fs.DirName == "/workspace") {
			continue
		}

		fsu := &gosigar.FileSystemUsage{}
		if err := fsu.Get(fs.DirName); err != nil {
			log.Errorf("Failed to get filesystem usage for %s: %s", fs.DevName, err)
			continue
		}
		out = append(out, &repb.UsageStats_FileSystemUsage{
			Source:     fs.DevName,
			Target:     fs.DirName,
			Fstype:     fs.SysTypeName,
			UsedBytes:  int64(fsu.Used),
			TotalBytes: int64(fsu.Total),
		})
	}
	return out
}

func updatePeakFileSystemUsage(peak, current []*repb.UsageStats_FileSystemUsage) []*repb.UsageStats_FileSystemUsage {
	// Keep track of which indexes in the `current` list that we have merged
	// into the `peak` list.
	observed := map[int]bool{}

	for _, p := range peak {
		var cur *repb.UsageStats_FileSystemUsage
		for i, c := range current {
			if p.Target == c.Target {
				cur = c
				observed[i] = true
				break
			}
		}
		if cur == nil {
			// The FS disappeared somehow.
			// Keep it in the `peak` list with its last observed value.
			continue
		}
		if cur.UsedBytes > p.UsedBytes {
			p.UsedBytes = cur.UsedBytes
		}
	}
	for i, c := range current {
		// If we see an FS that we haven't previously observed in the `peak`
		// list then append it to the end.
		if !observed[i] {
			peak = append(peak, c)
		}
	}
	// Sort to ensure deterministic ordering.
	sort.Slice(peak, func(i, j int) bool {
		return peak[i].Target < peak[j].Target
	})

	return peak
}

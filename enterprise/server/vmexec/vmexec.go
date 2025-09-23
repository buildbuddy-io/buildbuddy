package vmexec

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/elastic/gosigar"
	"github.com/miekg/dns"
	"github.com/tklauser/go-sysconf"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
	hlpb "google.golang.org/grpc/health/grpc_health_v1"
	gstatus "google.golang.org/grpc/status"
)

const (
	dockerdInitTimeout       = 30 * time.Second
	dockerdDefaultSocketPath = "/var/run/docker.sock"

	vmDNSInitTimeout = 5 * time.Second
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

func Run(ctx context.Context, port uint32, workspaceDevice string, initDockerd bool, enableDockerdTCP bool, dnsOverrides []*networking.DNSOverride) error {
	listener, err := vsock.NewGuestListener(ctx, port)
	if err != nil {
		return err
	}
	log.Infof("Starting vm exec listener on vsock port: %d", port)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(grpc_server.MaxRecvMsgSizeBytes()))

	vmService, err := NewServer(workspaceDevice)
	if err != nil {
		return err
	}
	vmxpb.RegisterExecServer(server, vmService)
	hc := healthcheck.NewHealthChecker("vmexec")
	// For now, don't register any explicit health checks; if we can ping the
	// health check service at all (within a short timeframe) then assume all is
	// well.
	hlpb.RegisterHealthServer(server, hc)

	// If applicable, wait for dockerd to start before accepting commands, so
	// that commands depending on dockerd do not need to explicitly wait for it.
	if initDockerd {
		if err := waitForDockerd(ctx, enableDockerdTCP); err != nil {
			return err
		}
	}

	// If applicable, wait for the local DNS server to initialize before accepting
	// commands on the vmExec server, to guarantee DNS requests are handled
	// correctly.
	if len(dnsOverrides) > 0 {
		if err := waitForVMDNS(ctx, dnsOverrides); err != nil {
			return err
		}
	}

	return server.Serve(listener)
}

func waitForDockerd(ctx context.Context, enableDockerdTCP bool) error {
	ctx, cancel := context.WithTimeout(ctx, dockerdInitTimeout)
	defer cancel()
	r := retry.New(ctx, &retry.Options{
		InitialBackoff: 10 * time.Microsecond,
		MaxBackoff:     100 * time.Millisecond,
		Multiplier:     1.5,
		MaxRetries:     math.MaxInt, // retry until context deadline
	})
	for r.Next() {
		args := []string{}
		if enableDockerdTCP {
			args = append(args, "--host=tcp://127.0.0.1:2375")
		}
		args = append(args, "ps")
		err := exec.CommandContext(ctx, "docker", args...).Run()
		if err == nil {
			log.Infof("dockerd is ready")
			return nil
		}
	}
	return status.DeadlineExceededErrorf("docker init timed out after %s", dockerdInitTimeout)
}

// Wait for the local DNS server to accept requests.
func waitForVMDNS(ctx context.Context, dnsOverrides []*networking.DNSOverride) error {
	if len(dnsOverrides) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, vmDNSInitTimeout)
	defer cancel()

	// Check that you can query the local DNS server for an overwritten hostname.
	c := &dns.Client{
		Timeout: 200 * time.Millisecond,
	}
	override := dnsOverrides[0]
	m := new(dns.Msg)
	m.SetQuestion(override.HostnameToOverride, dns.TypeA)

	r := retry.New(ctx, &retry.Options{
		InitialBackoff: 10 * time.Microsecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     1.5,
		MaxRetries:     math.MaxInt, // retry until context deadline
	})
	for r.Next() {
		r, _, err := c.Exchange(m, "127.0.0.1:53")
		if err == nil && r.Rcode == dns.RcodeSuccess {
			log.Info("local DNS server is ready")
			return nil
		}
	}
	return status.DeadlineExceededErrorf("polling local DNS server timed out after %s", vmDNSInitTimeout)
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

func isWorkspaceMounted() (bool, error) {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return false, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		target := fields[1]
		if target == workspaceMountPath {
			return true, nil
		}
	}
	return false, nil
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
		log.Errorf("Failed to unmount workspace: %s", err)
		return nil, status.InternalErrorf("unmount failed: %s", err)
	}

	// Try to open the workspace device in exclusive mode. If this fails, the
	// workspace device is still busy even after unmounting. This can happen
	// e.g. if a docker container is running that tries to mount a workspace
	// file as a volume.
	busy := false
	f, err := os.OpenFile(x.workspaceDevice, os.O_RDONLY|os.O_EXCL, 0)
	if err != nil {
		// TODO(bduffany): make this a hard failure once we notify affected
		// users that this is happening.
		busy = true
		log.Warningf("Workspace device is still busy after unmounting. VM will not be recycled.")
	} else {
		f.Close()
	}

	log.Infof("Unmounted workspace device %s", x.workspaceDevice)
	return &vmxpb.UnmountWorkspaceResponse{Busy: busy}, nil
}

func (x *execServer) MountWorkspace(ctx context.Context, req *vmxpb.MountWorkspaceRequest) (*vmxpb.MountWorkspaceResponse, error) {
	// Make sure the workspace drive is not already mounted.
	mounted, err := isWorkspaceMounted()
	if err != nil {
		log.Errorf("Failed to check if workspace is mounted: %s", err)
		return nil, status.InternalErrorf("failed to check whether workspace is mounted: %s", err)
	}
	if mounted {
		log.Errorf("Workspace is already mounted")
		return nil, status.InternalErrorf("workspace is already mounted at %s", workspaceMountPath)
	}

	if err := syscall.Mount(x.workspaceDevice, workspaceMountPath, "ext4", syscall.MS_NOATIME, ""); err != nil {
		log.Errorf("Failed to mount workspace: %s", err)
		return nil, err
	}
	log.Infof("Mounted workspace %s to %s", x.workspaceDevice, workspaceMountPath)
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

	log.Infof("Running command in VM: %q", cmd.String())
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
			} else if msg.Signal != 0 {
				if cmd == nil {
					msgs <- &message{Err: status.FailedPreconditionError("received signal before exec start request")}
					return
				}
				if err := cmd.signal(syscall.Signal(msg.Signal)); err != nil {
					msgs <- &message{Err: status.WrapError(err, "failed to forward signal")}
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

func resolveExecutable(workDir string, executable string) (string, error) {
	if strings.HasPrefix(executable, "/") {
		// Absolute path
		return executable, nil
	} else if strings.Contains(executable, "/") {
		// Workdir-relative path
		return filepath.Join(workDir, executable), nil
	} else {
		// $PATH lookup
		return exec.LookPath(executable)
	}
}

func logExecutableInfo(workDir string, executable string) {
	path, err := resolveExecutable(workDir, executable)
	if err != nil {
		log.Warningf("Failed to resolve executable %s: %s", executable, err)
		return
	}
	stat, err := os.Stat(path)
	if err != nil {
		log.Warningf("Failed to stat executable %s: %s", path, err)
		return
	}
	digestStr := ""
	d, err := digest.ComputeForFile(path, repb.DigestFunction_BLAKE3)
	if err != nil {
		log.Warningf("Failed to compute digest for executable %s: %s", path, err)
	} else {
		digestStr = digest.String(d)
	}
	log.Infof("Executable info: path=%q digest_blake3=%q mode=%q", path, digestStr, stat.Mode())
	// Print up to 1K bytes from the executable - these can be compared with the
	// expected contents to diagnose corruption.
	buf := make([]byte, min(1024, stat.Size()))
	f, err := os.Open(path)
	if err != nil {
		log.Warningf("Failed to open executable: %s", err)
		return
	}
	defer f.Close()
	n, err := io.ReadFull(f, buf)
	if err != nil {
		log.Warningf("Failed to read executable: %s", err)
		return
	}
	log.Infof("Executable first %d bytes: %q", n, buf[:n])
}

func (c *command) logErrorDiagnostics(err error) {
	log.Warningf("Command terminated abnormally: %s", err)
	if strings.Contains(err.Error(), "exec format error") && len(c.cmd.Args) > 0 {
		logExecutableInfo(c.cmd.Dir, c.cmd.Args[0])
	}
}

func (c *command) signal(sig syscall.Signal) error {
	if c.cmd == nil || c.cmd.Process == nil {
		return status.FailedPreconditionError("command process not running")
	}
	pid := c.cmd.Process.Pid
	if err := syscall.Kill(-pid, sig); err != nil {
		return err
	}
	return nil
}

func (c *command) Run(ctx context.Context, msgs chan *message) (*vmxpb.ExecStreamedResponse, error) {
	// TODO(tylerw): use syncfs or something better here.
	defer unix.Sync()

	log.Infof("Running command in VM: %q", c.cmd.String())
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
	var baselineCPUNanos int64
	commandDone := make(chan struct{})
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		// Set initial delay to 0 so that we get a baseline measurement
		// immediately.
		delay := time.Duration(0)
		for done := false; !done; {
			select {
			case <-commandDone:
				// Collect stats one more time, then terminate the loop.
				done = true
			case <-time.After(delay):
			}
			var isFirstSample bool
			if delay == 0 {
				isFirstSample = true
				delay = initialStatsPollInterval
			} else {
				isFirstSample = false
				delay = min(maxStatsPollInterval, time.Duration(float64(delay)*statsPollBackoff))
			}

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
				lifetimeCPUNanos := ticks * nanosPerTick
				if isFirstSample {
					baselineCPUNanos = lifetimeCPUNanos
				}
				stats.CpuNanos = lifetimeCPUNanos - baselineCPUNanos
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
	if err != nil {
		c.logErrorDiagnostics(err)
	}
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
	// Clone the slice to avoid modifying the original.
	peak = slices.Clone(peak)

	// Keep track of which indexes in the `current` list that we have merged
	// into the `peak` list.
	observed := map[int]bool{}
	for i, p := range peak {
		var cur *repb.UsageStats_FileSystemUsage
		for j, c := range current {
			if p.Target == c.Target {
				cur = c
				observed[j] = true
				break
			}
		}
		if cur == nil {
			// The FS disappeared somehow.
			// Keep it in the `peak` list with its last observed value.
			continue
		}
		if cur.UsedBytes > p.UsedBytes {
			// Create a modified copy of the message.
			p = p.CloneVT()
			p.UsedBytes = cur.UsedBytes
			peak[i] = p
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

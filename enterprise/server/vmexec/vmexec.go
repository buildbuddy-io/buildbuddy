package vmexec

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
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

	vmDNSInitTimeout                   = 5 * time.Second
	vmExecReadySignalInitialRetryDelay = 2 * time.Millisecond
	vmExecReadySignalMaxRetryDelay     = 1 * time.Second

	// preloadChunkSize is how much of a preloaded input is read per read
	// call. The contents are discarded, so it only bounds the memory each
	// preload goroutine uses and how often cancellation is checked.
	preloadChunkSize = 1 << 20
)

func init() {
	// Record the process start time as early as possible; init() runs before
	// any of main's code, so this is as close to actual process start as we
	// can easily get.
	processStartTime = time.Now()

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

	// RootfsDevice is the merged rootfs block device when rootfs is enabled.
	RootfsDevice = "/dev/vda"

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

	// The time at which this process started, recorded in init().
	processStartTime time.Time

	// What guestCounters and threadActivities returned when the last command
	// finished, for commands that report what the guest did since then.
	lastCounters map[string]int64
	lastThreads  map[int]threadActivity
)

// threadActivity is how often a thread has been switched out, and how much
// CPU time it has used, since it started.
type threadActivity struct {
	comm     string
	switches int64
	cpuTicks int64
}

type execServer struct {
	rootMu             sync.Mutex
	rootMountAttempted bool
	// workspaceDevice is the path to the hot-swappable workspace block device.
	workspaceDevice string

	// metrics holds metrics about this VM, and is returned from every Exec.
	metrics *vmxpb.Metrics
}

func NewServer(workspaceDevice string) (*execServer, error) {
	return &execServer{
		workspaceDevice: workspaceDevice,
		metrics:         &vmxpb.Metrics{},
	}, nil
}

func Run(ctx context.Context, port uint32, workspaceDevice string, initDockerd bool, enableDockerdTCP bool, dnsOverrides []*networking.DNSOverride) error {
	listener, err := vsock.NewGuestListener(ctx, port)
	if err != nil {
		return err
	}
	log.Infof("Starting vm exec listener on vsock port: %d", port)
	// 50MB - matches the default from grpc_server.MaxRecvMsgSizeBytes() at the time of writing,
	// but does not need to be kept in sync (this value should be fine for vmexec's purposes)
	// Inlined here to avoid pulling in the heavy grpc_server package and
	// all of its transitive deps into the goinit binary.
	server := grpc.NewServer(grpc.MaxRecvMsgSize(50_000_000))

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
		start := time.Now()
		if err := waitForDockerd(ctx, enableDockerdTCP); err != nil {
			return err
		}
		vmService.metrics.DockerdWaitDurationUsec = time.Since(start).Microseconds()
	}

	// If applicable, wait for the local DNS server to initialize before accepting
	// commands on the vmExec server, to guarantee DNS requests are handled
	// correctly.
	if len(dnsOverrides) > 0 {
		start := time.Now()
		if err := waitForVMDNS(ctx, dnsOverrides); err != nil {
			return err
		}
		vmService.metrics.VmDnsWaitDurationUsec = time.Since(start).Microseconds()
	}

	vmService.metrics.VmExecInitDurationUsec = time.Since(processStartTime).Microseconds()
	go maintainVMExecReadySignal(ctx)
	return server.Serve(listener)
}

// maintainVMExecReadySignal tells the host that vmexec is ready, then keeps the
// connection open. Firecracker resets vsock connections across snapshot
// restore, so the read below unblocks in a restored guest and causes it to
// reconnect and send a fresh readiness signal.
func maintainVMExecReadySignal(ctx context.Context) {
	r := retry.New(ctx, &retry.Options{
		InitialBackoff: vmExecReadySignalInitialRetryDelay,
		MaxBackoff:     vmExecReadySignalMaxRetryDelay,
		Multiplier:     2,
		MaxRetries:     math.MaxInt,
	})
	for r.Next() {
		conn, err := vsock.DialGuestToHost(vsock.HostVMExecReadyPort)
		if err == nil {
			_, err = io.WriteString(conn, vsock.VMExecReadyMessage)
		}
		if err == nil {
			log.Debugf("Signaled vmexec readiness to host")
			r.Reset()
			// The host deliberately sends no data. This read returns when the
			// connection is reset, including after snapshot restore.
			var buf [1]byte
			for err == nil && ctx.Err() == nil {
				_, err = conn.Read(buf[:])
			}
		}
		if conn != nil {
			_ = conn.Close()
		}
	}
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
	links, err := networking.RetryDump(netlink.LinkList)
	if err != nil {
		return err
	}
	for _, link := range links {
		attrs := link.Attrs()
		if attrs == nil {
			continue
		}
		neigbors, err := networking.RetryDump(func() ([]netlink.Neigh, error) {
			return handle.NeighList(attrs.Index, netlink.FAMILY_V4)
		})
		if err != nil {
			return err
		}
		v6neigbors, err := networking.RetryDump(func() ([]netlink.Neigh, error) {
			return handle.NeighList(attrs.Index, netlink.FAMILY_V6)
		})
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
	if req.GetUnixTimestampNanoseconds() > 1 {
		tv := syscall.NsecToTimeval(req.GetUnixTimestampNanoseconds())
		if err := syscall.Settimeofday(&tv); err != nil {
			return nil, err
		}
		log.Debugf("Set time of day to %d", req.GetUnixTimestampNanoseconds())
	}
	if len(req.GetEntropy()) > 0 {
		// Require sufficient entropy.
		if len(req.GetEntropy()) < 32 {
			return nil, status.InvalidArgumentError("at least 32 bytes of entropy are required")
		}

		if err := reseedRNG(req.GetEntropy()); err != nil {
			return nil, status.WrapError(err, "reseed RNG")
		}
	}
	if req.GetMountContainerRoot() {
		// Without fresh entropy, every VM restored from the snapshot would
		// generate the same boot ID.
		if len(req.GetEntropy()) == 0 {
			return nil, status.InvalidArgumentError("mounting the container root requires entropy")
		}
		x.rootMu.Lock()
		defer x.rootMu.Unlock()
		if x.rootMountAttempted {
			return nil, status.FailedPreconditionError("container root initialization already attempted")
		}
		// The new root would hide a workspace mounted on the old one.
		if mounted, err := isWorkspaceMounted(); err != nil {
			return nil, err
		} else if mounted {
			return nil, status.FailedPreconditionError("workspace must be mounted after the container root")
		}
		if req.GetContainerRootSizeBytes() > 0 {
			if err := waitForBlockDeviceSize(ctx, RootfsDevice, req.GetContainerRootSizeBytes()); err != nil {
				return nil, err
			}
		}
		x.rootMountAttempted = true
		if err := mountContainerRoot(); err != nil {
			return nil, status.WrapError(err, "mount container root")
		}
	}
	if req.GetClearArpCache() {
		if err := clearARPCache(); err != nil {
			return nil, err
		}
		log.Debugf("Cleared ARP cache")
	}
	return &vmxpb.InitializeResponse{ContainerRootMounted: req.GetMountContainerRoot()}, nil
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

	// Firecracker applies a drive update by sending the guest a config change
	// notification, and the virtio_blk driver reads the new capacity
	// asynchronously. Until it does, the kernel rejects reads past the old
	// capacity with EIO, which makes the mount fail. The old capacity is
	// usually 0, because the host attaches an empty file to the workspace
	// drive between actions.
	if req.GetSizeBytes() > 0 {
		if err := waitForBlockDeviceSize(ctx, x.workspaceDevice, req.GetSizeBytes()); err != nil {
			log.Errorf("Failed to wait for workspace device size: %s", err)
			return nil, err
		}
	}
	if err := syscall.Mount(x.workspaceDevice, workspaceMountPath, "ext4", syscall.MS_NOATIME, ""); err != nil {
		log.Errorf("Failed to mount workspace: %s", err)
		return nil, err
	}
	log.Infof("Mounted workspace %s to %s", x.workspaceDevice, workspaceMountPath)
	return &vmxpb.MountWorkspaceResponse{}, nil
}

// waitForBlockDeviceSize polls until the block device reports the given size.
func waitForBlockDeviceSize(ctx context.Context, device string, sizeBytes int64) error {
	// We stop waiting a second before the caller's deadline so that the caller
	// receives our error, which includes the device size, rather than its own
	// deadline error. If our response takes longer than that to arrive, the
	// caller just sees the less detailed error.
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline.Add(-time.Second))
		defer cancel()
	}
	// sysfs reports the size in 512-byte sectors, regardless of the device's
	// logical block size.
	sizePath := filepath.Join("/sys/class/block", filepath.Base(device), "size")
	wantSectors := sizeBytes / 512
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		b, err := os.ReadFile(sizePath)
		if err != nil {
			return status.InternalErrorf("read device size: %s", err)
		}
		sectors, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if err != nil {
			return status.InternalErrorf("parse device size: %s", err)
		}
		if sectors == wantSectors {
			return nil
		}
		select {
		case <-ctx.Done():
			return status.WrapErrorf(status.FromContextError(ctx), "device %s has %d sectors, want %d", device, sectors, wantSectors)
		case <-ticker.C:
		}
	}
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
	rsp.Metrics = x.metrics
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
					if res.GetResponse() != nil {
						res.Response.Metrics = x.metrics
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
	// preload, if set, names the input files to read into the page cache
	// while cmd runs.
	preload *vmxpb.InputPreload

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
		preload:      start.GetPreload(),
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

func (c *command) Run(ctx context.Context, msgs chan *message) (*vmxpb.ExecStreamedResponse, error) {
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
	var idle string
	if c.preload.GetReportIdleActivity() {
		idle = idleActivity()
	}
	idle += pageCensus(c.preload.GetPageCensus())
	// Read the preloaded inputs while the command runs, and stop once it
	// exits, since reading on would only slow down whatever runs next.
	preloadCtx, cancelPreload := context.WithCancel(ctx)
	preloadDone := make(chan *vmxpb.InputPreloadStats, 1)
	go func() {
		preloadDone <- preloadInputs(preloadCtx, c.preload)
	}()
	// Using a nil statsListener since we'd rather report stats from the whole
	// VM (which includes e.g. docker-in-firecracker containers) -- not just the
	// process being run.
	var preloadStats *vmxpb.InputPreloadStats
	if c.preload.GetWait() {
		preloadStats = <-preloadDone
	}
	countersBefore := guestCounters()
	commandStart := time.Now()
	_, err := commandutil.RunWithProcessTreeCleanup(ctx, c.cmd, nil /*=statsListener*/)
	commandDuration := time.Since(commandStart)
	cancelPreload()
	countersAfter := guestCounters()
	// TODO(tylerw): use syncfs or something better here.
	syncStart := time.Now()
	unix.Sync()
	syncDuration := time.Since(syncStart)

	close(commandDone)
	<-statsDone
	if !c.preload.GetWait() {
		preloadStats = <-preloadDone
	}
	var guestStats strings.Builder
	fmt.Fprintf(&guestStats, "command_usec=%d sync_usec=%d preload_usec=%d", commandDuration.Microseconds(), syncDuration.Microseconds(), preloadStats.GetDurationUsec())
	if ps := c.cmd.ProcessState; ps != nil {
		fmt.Fprintf(&guestStats, " child_user_ms=%d child_system_ms=%d", ps.UserTime().Milliseconds(), ps.SystemTime().Milliseconds())
	}
	for _, name := range slices.Sorted(maps.Keys(countersAfter)) {
		fmt.Fprintf(&guestStats, " %s=%d", name, countersAfter[name]-countersBefore[name])
	}
	guestStats.WriteString(idle)
	if c.preload.GetReportIdleActivity() {
		lastCounters, lastThreads = guestCounters(), threadActivities()
	}

	exitCode, err := commandutil.ExitCode(ctx, c.cmd, err)
	if err != nil {
		c.logErrorDiagnostics(err)
	}
	rsp := &vmxpb.ExecResponse{
		ExitCode:     int32(exitCode),
		Status:       gstatus.Convert(err).Proto(),
		PreloadStats: preloadStats,
		GuestStats:   guestStats.String(),
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

// guestCounters reads the guest's cumulative CPU, pressure, paging and disk
// counters, so that a caller can report how much each grew while a command
// ran. Times are in the unit that each name ends with.
func guestCounters() map[string]int64 {
	out := map[string]int64{}
	fields := func(path string) [][]string {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		var lines [][]string
		for line := range strings.SplitSeq(string(b), "\n") {
			lines = append(lines, strings.Fields(line))
		}
		return lines
	}
	num := func(s string) int64 {
		v, _ := strconv.ParseInt(s, 10, 64)
		return v
	}
	for _, f := range fields("/proc/stat") {
		if len(f) >= 9 && f[0] == "cpu" {
			for i, name := range []string{"user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"} {
				out["cpu_"+name+"_ms"] = num(f[i+1]) * 1000 / ticksPerSec
			}
		} else if len(f) == 2 && (f[0] == "ctxt" || f[0] == "processes") {
			out[f[0]] = num(f[1])
		}
	}
	for _, resource := range []string{"cpu", "io", "memory"} {
		for _, f := range fields("/proc/pressure/" + resource) {
			if len(f) == 5 && strings.HasPrefix(f[4], "total=") {
				out["psi_"+resource+"_"+f[0]+"_usec"] = num(strings.TrimPrefix(f[4], "total="))
			}
		}
	}
	vmstat := []string{"pgfault", "pgmajfault", "pgpgin", "pgpgout", "pgscan_kswapd", "pgscan_direct", "pgsteal_kswapd", "pgsteal_direct", "workingset_refault_file", "nr_dirtied", "nr_written"}
	for _, f := range fields("/proc/vmstat") {
		if len(f) == 2 && slices.Contains(vmstat, f[0]) {
			out[f[0]] = num(f[1])
		}
	}
	for _, f := range fields("/proc/diskstats") {
		if len(f) >= 11 && strings.HasPrefix(f[2], "vd") {
			for i, name := range []string{"reads", "", "read_sectors", "read_ms", "writes", "", "write_sectors", "write_ms"} {
				if name != "" {
					out[f[2]+"_"+name] = num(f[i+3])
				}
			}
		}
	}
	return out
}

// threadActivities reads the activity of every thread in the guest, by
// thread ID.
func threadActivities() map[int]threadActivity {
	out := map[int]threadActivity{}
	dirs, _ := filepath.Glob("/proc/[0-9]*/task/[0-9]*")
	for _, dir := range dirs {
		tid, err := strconv.Atoi(filepath.Base(dir))
		if err != nil {
			continue
		}
		var a threadActivity
		b, err := os.ReadFile(filepath.Join(dir, "status"))
		if err != nil {
			continue
		}
		for line := range strings.SplitSeq(string(b), "\n") {
			name, value, _ := strings.Cut(line, ":")
			value = strings.TrimSpace(value)
			switch name {
			case "Name":
				a.comm = strings.Map(func(r rune) rune {
					if strings.ContainsRune(" ,=:", r) {
						return '_'
					}
					return r
				}, value)
			case "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches":
				n, _ := strconv.ParseInt(value, 10, 64)
				a.switches += n
			}
		}
		// The fields after the command name, which may contain spaces,
		// start with the state. User and system time are the 12th and
		// 13th after it.
		if b, err := os.ReadFile(filepath.Join(dir, "stat")); err == nil {
			if i := strings.LastIndexByte(string(b), ')'); i >= 0 {
				if f := strings.Fields(string(b[i+1:])); len(f) > 12 {
					user, _ := strconv.ParseInt(f[11], 10, 64)
					system, _ := strconv.ParseInt(f[12], 10, 64)
					a.cpuTicks = user + system
				}
			}
		}
		out[tid] = a
	}
	return out
}

// idleActivity reports what the guest did since the last command finished,
// which includes the VM's pause and resume. It reports how much each of
// guestCounters' values grew, how many threads ran, and the commands whose
// threads were switched out most often, each as
// name:threads:switches:cpu_ms.
func idleActivity() string {
	if lastCounters == nil {
		return ""
	}
	var b strings.Builder
	counters := guestCounters()
	for _, name := range slices.Sorted(maps.Keys(counters)) {
		fmt.Fprintf(&b, " gap_%s=%d", name, counters[name]-lastCounters[name])
	}
	type commActivity struct{ threads, switches, cpuTicks int64 }
	byComm := map[string]*commActivity{}
	var ran, total int64
	for tid, now := range threadActivities() {
		total++
		before := lastThreads[tid]
		if before.comm != now.comm {
			// Another thread had this ID, or none did.
			before = threadActivity{}
		}
		if now.switches <= before.switches {
			continue
		}
		ran++
		a := byComm[now.comm]
		if a == nil {
			a = &commActivity{}
			byComm[now.comm] = a
		}
		a.threads++
		a.switches += now.switches - before.switches
		a.cpuTicks += now.cpuTicks - before.cpuTicks
	}
	fmt.Fprintf(&b, " gap_threads_ran=%d gap_threads=%d gap_top=", ran, total)
	names := slices.SortedFunc(maps.Keys(byComm), func(x, y string) int {
		return cmp.Or(cmp.Compare(byComm[y].switches, byComm[x].switches), cmp.Compare(x, y))
	})
	for i, name := range names {
		if i == 40 {
			break
		}
		if i > 0 {
			b.WriteString(",")
		}
		a := byComm[name]
		fmt.Fprintf(&b, "%s:%d:%d:%d", name, a.threads, a.switches, a.cpuTicks*1000/ticksPerSec)
	}
	return b.String()
}

// preloadInputs reads the files that preload names into the page cache,
// preload.parallelism at a time, until every file is read or ctx is
// cancelled, and reports what it read. It returns nil when preload names no
// files.
func preloadInputs(ctx context.Context, preload *vmxpb.InputPreload) *vmxpb.InputPreloadStats {
	if len(preload.GetPaths()) == 0 {
		return nil
	}
	start := time.Now()
	paths := make(chan string)
	var filesRead, bytesRead atomic.Int64
	var wg sync.WaitGroup
	for range max(int(preload.GetParallelism()), 1) {
		wg.Go(func() {
			buf := make([]byte, preloadChunkSize)
			for path := range paths {
				n, err := readIntoPageCache(ctx, filepath.Join(preload.GetInputRoot(), path), buf)
				bytesRead.Add(n)
				if err != nil {
					if ctx.Err() == nil {
						log.Warningf("Could not preload input %q: %s", path, err)
					}
					continue
				}
				filesRead.Add(1)
			}
		})
	}
send:
	for _, path := range preload.GetPaths() {
		select {
		case paths <- path:
		case <-ctx.Done():
			break send
		}
	}
	close(paths)
	wg.Wait()
	stats := &vmxpb.InputPreloadStats{
		FilesRead:    filesRead.Load(),
		BytesRead:    bytesRead.Load(),
		DurationUsec: time.Since(start).Microseconds(),
	}
	log.Infof("Preloaded %d of %d input files (%d bytes) in %s", stats.GetFilesRead(), len(preload.GetPaths()), stats.GetBytesRead(), time.Since(start))
	return stats
}

// readIntoPageCache reads the whole file at path through buf, discarding the
// contents so that only the page cache keeps them. It returns how many bytes
// it read, and stops early when ctx is cancelled.
func readIntoPageCache(ctx context.Context, path string, buf []byte) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var total int64
	for ctx.Err() == nil {
		n, err := f.Read(buf)
		total += int64(n)
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
	return total, ctx.Err()
}

type stdoutWriter struct{ msgs chan *message }

func (w *stdoutWriter) Write(b []byte) (int, error) {
	w.msgs <- &message{Response: &vmxpb.ExecStreamedResponse{Stdout: bytes.Clone(b)}}
	return len(b), nil
}

type stderrWriter struct{ msgs chan *message }

func (w *stderrWriter) Write(b []byte) (int, error) {
	w.msgs <- &message{Response: &vmxpb.ExecStreamedResponse{Stderr: bytes.Clone(b)}}
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

// reseedRNG credits seed to the kernel entropy pool and forces a CRNG reseed,
// so VMs restored from the same snapshot don't share RNG state.
func reseedRNG(seed []byte) error {
	f, err := os.OpenFile("/dev/urandom", os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	// struct rand_pool_info { int entropy_count; int buf_size; __u32 buf[]; }
	info := make([]byte, 8+len(seed))
	binary.NativeEndian.PutUint32(info[0:], uint32(len(seed)*8))
	binary.NativeEndian.PutUint32(info[4:], uint32(len(seed)))
	copy(info[8:], seed)
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), unix.RNDADDENTROPY, uintptr(unsafe.Pointer(&info[0]))); errno != 0 {
		return status.InternalErrorf("RNDADDENTROPY: %s", errno)
	}
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), unix.RNDRESEEDCRNG, 0); errno != 0 {
		return status.InternalErrorf("RNDRESEEDCRNG: %s", errno)
	}
	return nil
}

func mountContainerRoot() error {
	const root = "/mnt"
	if err := os.MkdirAll(root, 0755); err != nil {
		return err
	}
	if err := syscall.Mount(RootfsDevice, root, "ext4", syscall.MS_NOATIME, ""); err != nil {
		return err
	}
	if err := ResizeExt4FS(RootfsDevice, root); err != nil {
		return err
	}
	for _, name := range []string{"dev", "proc", "sys", "run"} {
		target := filepath.Join(root, name)
		if err := os.Mkdir(target, 0755); err != nil && !os.IsExist(err) {
			return err
		}
		info, err := os.Lstat(target)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return status.InvalidArgumentErrorf("container /%s must be a directory", name)
		}
		if err := syscall.Mount("/"+name, target, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
			return err
		}
	}
	config := make(map[string][]byte)
	for _, name := range []string{"hostname", "hosts", "resolv.conf"} {
		data, err := os.ReadFile("/etc/" + name)
		if err != nil {
			return err
		}
		config[name] = data
	}
	// Move the root over / rather than chrooting into it: the kernel won't
	// create user namespaces for a chrooted process.
	if err := os.Chdir(root); err != nil {
		return err
	}
	if err := syscall.Mount(".", "/", "", syscall.MS_MOVE, ""); err != nil {
		return err
	}
	if err := syscall.Chroot("."); err != nil {
		return err
	}
	if err := os.Chdir("/"); err != nil {
		return err
	}
	if err := os.MkdirAll(workspaceMountPath, 0755); err != nil {
		return err
	}
	if err := os.MkdirAll("/root", syscall.S_IRWXU); err != nil {
		return err
	}
	if err := os.MkdirAll("/etc", 0755); err != nil {
		return err
	}
	for name, data := range config {
		path := "/etc/" + name
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.WriteFile(path, data, 0644); err != nil {
			return err
		}
	}
	if _, err := os.Lstat("/etc/mtab"); os.IsNotExist(err) {
		if err := os.Symlink("/proc/mounts", "/etc/mtab"); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}

// ResizeExt4FS resizes the ext4 filesystem mounted at the given path to match the underlying
// block device size.
func ResizeExt4FS(devicePath, mountPath string) error {
	blockSizePath := fmt.Sprintf("/sys/class/block/%s/size", filepath.Base(devicePath))
	log.Debugf("os.ReadFile(%q)", blockSizePath)
	sizeBuf, err := os.ReadFile(blockSizePath)
	if err != nil {
		return status.InternalErrorf("read block device size: %s", err)
	}
	// Here, the block size is always 512.
	// So the size in bytes is deviceSizeBlocks*512.
	deviceSizeBlocks, err := strconv.Atoi(strings.TrimSpace(string(sizeBuf)))
	if err != nil {
		return status.InternalErrorf("failed to parse block device size %q", string(sizeBuf))
	}

	log.Debugf("statfs %q", mountPath)
	s := &syscall.Statfs_t{}
	if err := syscall.Statfs(mountPath, s); err != nil {
		return status.InternalErrorf("statfs %s: %s", mountPath, err)
	}
	blocks := int64(deviceSizeBlocks*512) / s.Bsize
	log.Debugf("open %q", mountPath)
	fd, err := syscall.Open(mountPath, syscall.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(fd)
	log.Debugf("IOC_RESIZE_FS(%q) to %v blocks (%v byte)", mountPath, blocks, deviceSizeBlocks*512)
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(fd),
		uintptr(0x40086610), // EXT4_IOC_RESIZE_FS
		uintptr(unsafe.Pointer(&blocks)),
	)
	if errno != 0 {
		return status.InternalErrorf("EXT4_IOC_RESIZE_FS: errno %s", errno)
	}
	return nil
}

// pageCensus classifies the guest pages that the host names, by the step in
// which the guest first touched them, and returns the count of each class as
// " census_<step>_<class>=<count>". The classes are
//
//   - free, for pages that nothing has allocated,
//   - slab, for the kernel's small objects,
//   - pgtable, for page tables,
//   - anon_<owner>, for process memory, where owner is the command of a
//     process that maps the page (vmvfs, vmexec or goinit for the guest's Go
//     servers), or "unmapped",
//   - file, for page cache, or file_<owner> if a process maps the page,
//   - kernel, for the rest, such as kernel stacks and the kernel's own data.
func pageCensus(steps []*vmxpb.PageCensusStep) string {
	if len(steps) == 0 {
		return ""
	}
	// The memory snapshot holds the guest's memory below Firecracker's MMIO
	// gap, followed by its memory from 4 GiB up. /proc/iomem tells where the
	// memory below the gap ends.
	var lowEnd uint64
	iomem, _ := os.ReadFile("/proc/iomem")
	for line := range strings.SplitSeq(string(iomem), "\n") {
		addrs, name, _ := strings.Cut(line, " : ")
		_, hi, _ := strings.Cut(strings.TrimSpace(addrs), "-")
		if end, err := strconv.ParseUint(hi, 16, 64); err == nil && name == "System RAM" && end < 1<<32 {
			lowEnd = max(lowEnd, (end+1<<20)&^(1<<20-1))
		}
	}
	pageFrame := func(offset int64) uint64 {
		addr := uint64(offset)
		if addr >= lowEnd {
			addr += 1<<32 - lowEnd
		}
		return addr / 4096
	}
	owners := map[uint64]string{}
	for _, step := range steps {
		for _, offset := range step.GetOffsets() {
			owners[pageFrame(offset)] = ""
		}
	}
	// Find a process that maps each page, in the page map of each readable
	// mapping of each process.
	procs, _ := os.ReadDir("/proc")
	buf := make([]byte, 8*8192)
	for _, proc := range procs {
		pid := proc.Name()
		if _, err := strconv.Atoi(pid); err != nil {
			continue
		}
		comm, _ := os.ReadFile("/proc/" + pid + "/comm")
		owner := strings.TrimSpace(string(comm))
		if owner == "exe" {
			cmdline, _ := os.ReadFile("/proc/" + pid + "/cmdline")
			switch {
			case bytes.Contains(cmdline, []byte("--vmvfs")):
				owner = "vmvfs"
			case bytes.Contains(cmdline, []byte("--vmexec")):
				owner = "vmexec"
			default:
				owner = "goinit"
			}
		}
		owner = strings.Map(func(r rune) rune {
			if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
				return r
			}
			return '_'
		}, owner)
		mappings, err := os.ReadFile("/proc/" + pid + "/maps")
		if err != nil {
			continue
		}
		pagemap, err := os.Open("/proc/" + pid + "/pagemap")
		if err != nil {
			continue
		}
		for line := range strings.SplitSeq(string(mappings), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 || !strings.HasPrefix(fields[1], "r") {
				continue
			}
			lo, hi, _ := strings.Cut(fields[0], "-")
			start, err1 := strconv.ParseUint(lo, 16, 64)
			end, err2 := strconv.ParseUint(hi, 16, 64)
			if err1 != nil || err2 != nil {
				continue
			}
			for addr := start; addr < end; {
				n, err := pagemap.ReadAt(buf[:min(uint64(len(buf)), (end-addr)/4096*8)], int64(addr/4096*8))
				for i := 0; i+8 <= n; i += 8 {
					entry := binary.LittleEndian.Uint64(buf[i:])
					// Bit 63 is set if the page is in memory, and the low
					// 55 bits are then its page frame number.
					if entry>>63 == 0 {
						continue
					}
					pfn := entry & (1<<55 - 1)
					if cur, ok := owners[pfn]; ok && cur == "" {
						owners[pfn] = owner
					}
				}
				if n < 8 || err != nil {
					break
				}
				addr += uint64(n/8) * 4096
			}
		}
		pagemap.Close()
	}
	flagsFile, err := os.Open("/proc/kpageflags")
	if err != nil {
		return " census_error=1"
	}
	defer flagsFile.Close()
	var b strings.Builder
	for _, step := range steps {
		counts := map[string]int{}
		for _, offset := range step.GetOffsets() {
			pfn := pageFrame(offset)
			var raw [8]byte
			if _, err := flagsFile.ReadAt(raw[:], int64(pfn*8)); err != nil {
				counts["unknown"]++
				continue
			}
			// The bits are the KPF_* flags of the kernel's
			// include/uapi/linux/kernel-page-flags.h.
			flags := binary.LittleEndian.Uint64(raw[:])
			owner := owners[pfn]
			switch {
			case flags&(1<<20) != 0:
				counts["nopage"]++
			case flags&(1<<10) != 0:
				counts["free"]++
			case flags&(1<<7) != 0:
				counts["slab"]++
			case flags&(1<<26) != 0:
				counts["pgtable"]++
			case flags&(1<<12) != 0:
				counts["anon_"+cmp.Or(owner, "unmapped")]++
			case owner != "":
				counts["file_"+owner]++
			case flags&(1<<5|1<<11) != 0:
				counts["file"]++
			default:
				counts["kernel"]++
			}
		}
		for _, class := range slices.Sorted(maps.Keys(counts)) {
			fmt.Fprintf(&b, " census_%s_%s=%d", step.GetName(), class, counts[class])
		}
	}
	return b.String()
}

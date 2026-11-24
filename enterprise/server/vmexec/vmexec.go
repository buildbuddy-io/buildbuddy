package vmexec

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/diskimage"
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
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
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

	printkPath = "/proc/sys/kernel/printk"
	// Only messages more severe than warnings reach a quiet console.
	quietConsoleLogLevel = "4"
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
	// ScratchDevice holds writes to an EROFS rootfs in generic snapshot VMs.
	ScratchDevice = "/dev/vdc"
	// scratchMountPath is where goinit mounts ScratchDevice.
	scratchMountPath = "/scratch"

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
)

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

// StartDockerd writes daemonJSON to /etc/docker/daemon.json and starts dockerd
// in the background.
func StartDockerd(ctx context.Context, daemonJSON []byte, enableTCP bool) error {
	// Make sure we can locate both docker and dockerd.
	for _, bin := range []string{"docker", "dockerd"} {
		if _, err := exec.LookPath(bin); err != nil {
			return status.FailedPreconditionErrorf("init-dockerd enabled, but %q was not found in PATH: %s", bin, err)
		}
	}
	if len(daemonJSON) > 0 {
		if err := os.MkdirAll("/etc/docker", 0755); err != nil {
			return err
		}
		if err := os.WriteFile("/etc/docker/daemon.json", daemonJSON, 0644); err != nil {
			return err
		}
	}

	log.Infof("Starting dockerd")

	args := []string{}
	if enableTCP {
		args = append(args, "--host=unix:///var/run/docker.sock", "--host=tcp://0.0.0.0:2375", "--tls=false")
	}
	cmd := exec.CommandContext(ctx, "dockerd", args...)
	// TODO(bduffany): update arm64 image and remove this check for arm64 as well.
	if runtime.GOARCH != "amd64" {
		// Note: despite the big scary INSECURE env var name, dockerd is completely sandboxed inside a VM, so it's secure for our usage. Once we upgrade our guest kernels to support nf tables, we can remove this.
		cmd.Env = append(os.Environ(), "DOCKER_INSECURE_NO_IPTABLES_RAW=1")
	}
	// TODO(https://github.com/buildbuddy-io/buildbuddy-internal/issues/3306):
	// enable logging by default
	if *log.LogLevel == "debug" {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() {
		if err := cmd.Wait(); err != nil {
			log.Errorf("dockerd exited with error: %s", err)
		} else {
			log.Infof("dockerd exited")
		}
	}()
	return nil
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

func isMounted(target string) (bool, error) {
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
		if fields[1] == target {
			return true, nil
		}
	}
	return false, nil
}

func (x *execServer) Initialize(ctx context.Context, req *vmxpb.InitializeRequest) (*vmxpb.InitializeResponse, error) {
	defer quietConsole()()
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
		if mounted, err := isMounted(workspaceMountPath); err != nil {
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
		if err := mountContainerRoot(req.GetOverlayContainerRoot(), req.GetDockerd() != nil); err != nil {
			return nil, status.WrapError(err, "mount container root")
		}
	}
	// Started before the memory limit so that dockerd runs in the root cgroup
	// and can create subcgroups for containers without conflicting with the task's cgroup.
	if d := req.GetDockerd(); d != nil {
		// The root is mounted at most once, so dockerd starts at most once.
		if !req.GetMountContainerRoot() {
			return nil, status.InvalidArgumentError("starting dockerd requires mounting the container root")
		}
		start := time.Now()
		if err := StartDockerd(context.Background(), d.GetDaemonJson(), d.GetEnableTcp()); err != nil {
			return nil, status.WrapError(err, "start dockerd")
		}
		if err := waitForDockerd(ctx, d.GetEnableTcp()); err != nil {
			return nil, err
		}
		x.metrics.DockerdWaitDurationUsec = time.Since(start).Microseconds()
	}
	if req.GetMemoryLimitBytes() > 0 {
		if err := limitTaskMemory(ctx, req.GetMemoryLimitBytes()); err != nil {
			return nil, status.WrapError(err, "limit task memory")
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

// limitTaskMemory moves this process into a cgroup limited to limitBytes, so
// that commands started afterwards inherit the limit. The kernel never
// OOM-kills init, so a command is killed instead.
func limitTaskMemory(ctx context.Context, limitBytes int64) error {
	dir := filepath.Join(cgroup.RootPath, "task")
	if err := os.Mkdir(dir, 0755); err != nil && !os.IsExist(err) {
		return err
	}
	if err := cgroup.EnableController(dir, "memory"); err != nil {
		return err
	}
	if err := cgroup.Setup(ctx, dir, &scpb.CgroupSettings{MemoryLimitBytes: &limitBytes}, nil /*=blockDevice*/); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte(strconv.Itoa(os.Getpid())), 0)
}

func (x *execServer) Sync(ctx context.Context, req *vmxpb.SyncRequest) (*vmxpb.SyncResponse, error) {
	unix.Sync()
	return &vmxpb.SyncResponse{}, nil
}

func (x *execServer) UnmountWorkspace(ctx context.Context, req *vmxpb.UnmountWorkspaceRequest) (*vmxpb.UnmountWorkspaceResponse, error) {
	defer quietConsole()()
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
	defer quietConsole()()
	// Make sure the workspace drive is not already mounted.
	mounted, err := isMounted(workspaceMountPath)
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

	defer syncFS(cmd.Dir)

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

func (c *command) Run(ctx context.Context, msgs chan *message) (*vmxpb.ExecStreamedResponse, error) {
	defer syncFS(c.cmd.Dir)

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

// detectFormat returns the format of the filesystem on device.
func detectFormat(device string) (diskimage.Format, error) {
	f, err := os.Open(device)
	if err != nil {
		return "", err
	}
	defer f.Close()
	sb := make([]byte, 64)
	if _, err := f.ReadAt(sb, 1024); err != nil {
		return "", err
	}
	if binary.LittleEndian.Uint32(sb) == unix.EROFS_SUPER_MAGIC_V1 {
		return diskimage.Erofs, nil
	}
	if binary.LittleEndian.Uint16(sb[56:]) == unix.EXT4_SUPER_MAGIC {
		return diskimage.Ext4, nil
	}
	return "", status.InvalidArgumentErrorf("unrecognized filesystem on %s", device)
}

// mountOverlayRoot mounts the container image read-only at lower, under an
// overlay at root whose writes go to the scratch disk.
func mountOverlayRoot(format diskimage.Format, root string, initDockerd bool) error {
	if mounted, err := isMounted(scratchMountPath); err != nil {
		return err
	} else if !mounted {
		return status.FailedPreconditionErrorf("%s is not mounted", scratchMountPath)
	}
	const lower = "/container"
	if err := os.MkdirAll(lower, 0755); err != nil {
		return err
	}
	if err := syscall.Mount(RootfsDevice, lower, string(format), syscall.MS_RDONLY, ""); err != nil {
		return err
	}
	upper := filepath.Join(scratchMountPath, "upper")
	work := filepath.Join(scratchMountPath, "work")
	for _, dir := range []string{upper, work} {
		if err := os.Mkdir(dir, 0755); err != nil {
			return err
		}
	}
	// The root never needs to survive a guest crash.
	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s,volatile", lower, upper, work)
	if err := syscall.Mount("overlay", root, "overlay", syscall.MS_NOATIME, opts); err != nil {
		return err
	}
	if !initDockerd {
		return nil
	}
	// Container storage drivers refuse to run on overlayfs, so give them
	// directories on the scratch disk, copying any pre-existing image content
	// first so it is not hidden by the bind mount.
	for _, name := range []string{"docker", "containers", "containerd"} {
		p := root
		symlinkOrInvalid := false
		var targetInfo os.FileInfo
		for _, part := range []string{"var", "lib", name} {
			p = filepath.Join(p, part)
			info, err := os.Lstat(p)
			if err != nil {
				if os.IsNotExist(err) {
					if err := os.Mkdir(p, 0755); err != nil {
						symlinkOrInvalid = true
						break
					}
					targetInfo = nil
					continue
				}
				symlinkOrInvalid = true
				break
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				symlinkOrInvalid = true
				break
			}
			targetInfo = info
		}
		if symlinkOrInvalid {
			continue
		}
		src := filepath.Join(scratchMountPath, name)
		if err := os.MkdirAll(src, 0755); err != nil {
			return err
		}
		if targetInfo != nil {
			// Copy from lower (the raw container image), not the merged
			// overlay view, so overlay2 whiteouts and opaque xattrs are
			// preserved.
			lowerSrc := filepath.Join(lower, "var", "lib", name)
			if err := CopyDirContents(lowerSrc, src, targetInfo); err != nil {
				return err
			}
		}
		if err := syscall.Mount(src, p, "", syscall.MS_BIND, ""); err != nil {
			return err
		}
	}
	return nil
}

type copyFileID struct {
	dev uint64
	ino uint64
}

// CopyDirContents recursively copies the contents of src into dst (which must
// already exist), preserving permissions, ownership, symlinks, hardlinks, and
// special files.
func CopyDirContents(src, dst string, srcInfo os.FileInfo) error {
	return copyDirTree(src, dst, srcInfo, make(map[copyFileID]string))
}

func copyDirTree(src, dst string, srcInfo os.FileInfo, hardlinks map[copyFileID]string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		info, err := os.Lstat(srcPath)
		if err != nil {
			return err
		}
		if err := copyEntry(srcPath, dstPath, info, hardlinks); err != nil {
			return err
		}
	}
	return applyFileMetadata(src, dst, srcInfo)
}

func copyEntry(src, dst string, info os.FileInfo, hardlinks map[copyFileID]string) error {
	mode := info.Mode()
	switch {
	case mode.IsDir():
		if err := os.Mkdir(dst, 0755); err != nil {
			return err
		}
		return copyDirTree(src, dst, info, hardlinks)
	case mode&os.ModeSymlink != 0:
		target, err := os.Readlink(src)
		if err != nil {
			return err
		}
		if err := os.Symlink(target, dst); err != nil {
			return err
		}
		if err := applyOwnership(dst, info); err != nil {
			return err
		}
		return copyXattrs(src, dst)
	case mode&os.ModeSocket != 0:
		return nil
	case mode&os.ModeNamedPipe != 0:
		if err := unix.Mkfifo(dst, 0600); err != nil {
			return err
		}
		return applyFileMetadata(src, dst, info)
	case mode&os.ModeDevice != 0:
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return status.InternalErrorf("missing stat_t for device %s", src)
		}
		if err := unix.Mknod(dst, uint32(st.Mode), int(st.Rdev)); err != nil {
			return err
		}
		return applyFileMetadata(src, dst, info)
	case mode.IsRegular():
		if st, ok := info.Sys().(*syscall.Stat_t); ok && st.Nlink > 1 {
			id := copyFileID{dev: uint64(st.Dev), ino: st.Ino}
			if existing, ok := hardlinks[id]; ok {
				return os.Link(existing, dst)
			}
			hardlinks[id] = dst
		}
		if err := copyRegularFile(src, dst); err != nil {
			return err
		}
		return applyFileMetadata(src, dst, info)
	default:
		return nil
	}
}

func copyRegularFile(src, dst string) (retErr error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if err := out.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}()
	_, err = io.Copy(out, in)
	return err
}

func applyOwnership(dst string, info os.FileInfo) error {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	return os.Lchown(dst, int(st.Uid), int(st.Gid))
}

func applyFileMetadata(src, dst string, info os.FileInfo) error {
	if err := applyOwnership(dst, info); err != nil {
		return err
	}
	if err := copyXattrs(src, dst); err != nil {
		return err
	}
	return os.Chmod(dst, info.Mode()&(os.ModePerm|os.ModeSetuid|os.ModeSetgid|os.ModeSticky))
}

func copyXattrs(src, dst string) error {
	size, err := unix.Llistxattr(src, nil)
	if err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENODATA) {
			return nil
		}
		return err
	}
	if size == 0 {
		return nil
	}
	buf := make([]byte, size)
	size, err = unix.Llistxattr(src, buf)
	if err != nil {
		return err
	}
	var valBuf []byte
	for rawName := range bytes.SplitSeq(buf[:size], []byte{0}) {
		if len(rawName) == 0 {
			continue
		}
		name := string(rawName)
		valSize, err := unix.Lgetxattr(src, name, nil)
		if err != nil {
			if errors.Is(err, unix.ENODATA) {
				continue
			}
			return err
		}
		if cap(valBuf) < valSize {
			valBuf = make([]byte, valSize)
		} else {
			valBuf = valBuf[:valSize]
		}
		if valSize > 0 {
			valSize, err = unix.Lgetxattr(src, name, valBuf)
			if err != nil {
				return err
			}
		}
		if err := unix.Lsetxattr(dst, name, valBuf[:valSize], 0); err != nil {
			return err
		}
	}
	return nil
}

func mountContainerRoot(overlay, initDockerd bool) error {
	const root = "/mnt"
	if err := os.MkdirAll(root, 0755); err != nil {
		return err
	}
	format, err := detectFormat(RootfsDevice)
	if err != nil {
		return err
	}
	switch {
	case overlay:
		if err := mountOverlayRoot(format, root, initDockerd); err != nil {
			return status.WrapError(err, "mount overlay root")
		}
	case format == diskimage.Ext4:
		if err := syscall.Mount(RootfsDevice, root, "ext4", syscall.MS_NOATIME, ""); err != nil {
			return err
		}
		if err := ResizeExt4FS(RootfsDevice, root); err != nil {
			return err
		}
	default:
		return status.InvalidArgumentErrorf("%s root requires overlay_container_root", format)
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

// consoleMu serializes quietConsole callers, so each restores the level it
// found.
var consoleMu sync.Mutex

// quietConsole keeps routine kernel messages, such as mount and resize
// notices, off the serial console until the returned func is called, since
// each console byte exits to the VMM. Errors still reach the console.
func quietConsole() (restore func()) {
	consoleMu.Lock()
	b, err := os.ReadFile(printkPath)
	if err != nil {
		consoleMu.Unlock()
		log.Warningf("Failed to read console log level: %s", err)
		return func() {}
	}
	level := strings.Fields(string(b))[0]
	setConsoleLogLevel(quietConsoleLogLevel)
	return func() {
		setConsoleLogLevel(level)
		consoleMu.Unlock()
	}
}

func setConsoleLogLevel(level string) {
	if err := os.WriteFile(printkPath, []byte(level), 0); err != nil {
		log.Warningf("Failed to set console log level: %s", err)
	}
}

// syncFS flushes the filesystem containing dir, so the host sees the
// command's outputs.
func syncFS(dir string) {
	if dir == "" {
		dir = "/"
	}
	f, err := os.Open(dir)
	if err != nil {
		log.Warningf("Failed to open %s for sync: %s", dir, err)
		return
	}
	defer f.Close()
	if err := unix.Syncfs(int(f.Fd())); err != nil {
		log.Warningf("Failed to sync %s: %s", dir, err)
	}
}

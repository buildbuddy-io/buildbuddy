package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/vmexec"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/vmvfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/rlimit"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jsimonetti/rtnetlink/rtnl"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"

	hlpb "github.com/buildbuddy-io/buildbuddy/proto/health"
	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
)

const (
	commonMountFlags = syscall.MS_NODEV | syscall.MS_NOEXEC | syscall.MS_NOSUID
	cgroupMountFlags = syscall.MS_NODEV | syscall.MS_NOEXEC | syscall.MS_NOSUID | syscall.MS_RELATIME

	dockerdInitTimeout       = 30 * time.Second
	dockerdDefaultSocketPath = "/var/run/docker.sock"

	// EXT4_IOC_RESIZE_FS is the ioctl constant for resizing an ext4 FS.
	// Computed from C: https://gist.github.com/bduffany/ce9b594c2166ea1a4564cba1b5ed652d
	EXT4_IOC_RESIZE_FS = 0x40086610
)

var (
	path             = flag.String("path", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "The path to use when executing cmd")
	vmExecPort       = flag.Uint("vm_exec_port", vsock.VMExecPort, "The vsock port number to listen on for VM Exec service.")
	enableRootfs     = flag.Bool("enable_rootfs", false, "Whether the rootfs disk is enabled instead of separate containerfs + scratchfs disks")
	enableVFS        = flag.Bool("enable_vfs", false, "Whether to run the VFS client.")
	debugMode        = flag.Bool("debug_mode", false, "If true, attempt to set root pw and start getty.")
	enableLogging    = flag.Bool("enable_logging", false, "If true, output logs to the console. By default, only kernel logs are output to the console.")
	setDefaultRoute  = flag.Bool("set_default_route", false, "If true, will set the default eth0 route to 192.168.246.1")
	initDockerd      = flag.Bool("init_dockerd", false, "If true, init dockerd before accepting exec requests. Requires docker to be installed.")
	enableDockerdTCP = flag.Bool("enable_dockerd_tcp", false, "If true, dockerd will listen to for tcp traffic on port 2375.")

	isVMExec = flag.Bool("vmexec", false, "Whether to run as the vmexec server.")
	isVMVFS  = flag.Bool("vmvfs", false, "Whether to run as the vmvfs binary.")
)

var (
	// Device paths.
	rootDevice      = ""
	containerDevice = "/dev/vda"
	scratchDevice   = "/dev/vdb"
	workspaceDevice = "/dev/vdc"
)

// die logs the provided error if it is not nil and then terminates the program.
func die(err error) {
	if err != nil {
		// NOTE: do not change this "die: " prefix. We rely on it to parse the fatal
		// error from the firecracker machine logs and return it back to the user.
		log.Fatalf("die: %s", err)
	}
}

func mkdirp(path string, mode fs.FileMode) error {
	log.Debugf("mkdir %q (mode: %d)", path, mode)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.MkdirAll(path, mode)
	}
	return nil
}

func mount(source, target, fstype string, flags uintptr, options string) error {
	log.Debugf("mount %q => %q (%s) flags: %d, options: %s", source, target, fstype, flags, options)
	return os.NewSyscallError("MOUNT", syscall.Mount(source, target, fstype, flags, options))
}

func chdir(path string) error {
	log.Debugf("chdir %q", path)
	return os.NewSyscallError("CHDIR", syscall.Chdir(path))
}

func chroot(path string) error {
	log.Debugf("chroot %q", path)
	return os.NewSyscallError("CHROOT", syscall.Chroot(path))
}

func reapChildren(ctx context.Context) {
	// Note: it's OK to drop some signals here because we reap child processes
	// in a loop each time we get SIGCHLD.
	c := make(chan os.Signal, 128)
	signal.Notify(c, unix.SIGCHLD)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c:
			var status syscall.WaitStatus
			// Successful SIGCHLD deliveries are not necessarily 1-to-1 with
			// child processes terminating. So each time we get SIGCHLD, don't
			// just reap once - reap all zombie processes in a loop until there
			// is nothing left to reap.
			for {
				if _, err := syscall.Wait4(-1, &status, unix.WNOHANG, nil); err != nil {
					break
				}
			}
		}
	}
}

func configureDefaultRoute(ifaceName, ipAddr string) error {
	// Setup the default route
	iface, err := net.InterfaceByName(ifaceName)
	if err != nil {
		return err
	}
	nlConn, err := rtnl.Dial(nil)
	if err != nil {
		return err
	}
	_, ipNet, err := net.ParseCIDR("0.0.0.0/0") // this is the default route.
	if err != nil {
		return err
	}

	if err := nlConn.RouteAdd(iface, *ipNet, net.ParseIP(ipAddr)); err != nil {
		return err
	}
	return nlConn.Close()
}

func copyFile(src, dest string, mode os.FileMode) error {
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}

	if err := in.Close(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}

	return nil
}

func startDockerd(ctx context.Context) error {
	// Make sure we can locate both docker and dockerd.
	if _, err := exec.LookPath("docker"); err != nil {
		return err
	}
	if _, err := exec.LookPath("dockerd"); err != nil {
		return err
	}

	log.Infof("Starting dockerd")

	args := []string{}
	if *enableDockerdTCP {
		args = append(args, "--host=unix:///var/run/docker.sock", "--host=tcp://0.0.0.0:2375", "--tls=false")
	}

	cmd := exec.CommandContext(ctx, "dockerd", args...)
	// TODO(https://github.com/buildbuddy-io/buildbuddy-internal/issues/3306):
	// enable logging by default
	if *enableLogging {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	return cmd.Start()
}

func waitForDockerd(ctx context.Context) error {
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
		if *enableDockerdTCP {
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

// This is mostly cribbed from github.com/superfly/init-snapshot
// which was very helpful <3!
func main() {
	start := time.Now()
	rootContext := context.Background()

	flag.Parse()

	// setup logging

	// Temporarily disable non-fatal logs unless explicitly enabled.
	// TODO(https://github.com/buildbuddy-io/buildbuddy-internal/issues/3306):
	// figure out why logging causes the VM to hang and re-enable logging.
	*log.LogLevel = "fatal"
	if *enableLogging {
		*log.LogLevel = "debug"
	}

	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1) // in case log.Fatalf does not work.
	}

	if *enableRootfs {
		rootDevice = "/dev/vda"
		workspaceDevice = "/dev/vdb"
		containerDevice = ""
		scratchDevice = ""
	}

	// If we were re-exec'd by the init binary as a child process, run the
	// appropriate handler.
	if *isVMExec {
		die(runVMExecServer(rootContext))
		return
	}
	if *isVMVFS {
		die(vmvfs.Run())
		return
	}

	log.Infof("Starting BuildBuddy init (args: %s)", os.Args)

	die(mkdirp("/dev", 0755))
	die(mount("devtmpfs", "/dev", "devtmpfs", syscall.MS_NOSUID, "mode=0620,gid=5"))

	// The following devices are provided by our firecracker implementation:
	//
	// - /dev/vda: The read-only container disk image generated from the docker/OCI image
	// - /dev/vdb: A read-write "scratch" disk image which is initially empty, and persists
	//   for the lifetime of the container.
	// - /dev/vdc: A read-write workspace disk image, which is replaced by the host when
	//   each action is run.
	//
	// We mount the scratch disk as an overlay on top of the container disk, so
	// that if actions want to write files outside of the workspace directory,
	// they can do so, and the container disk image will remain untouched (and
	// safe for re-use across multiple VMs).
	//
	// We additionally mount the action working directory to /workspace within the
	// chroot.

	// sysfs is needed in the root dir for block device metadata.
	die(mkdirp("/sys", 0555))
	die(mount("sys", "/sys", "sysfs", commonMountFlags, ""))

	if *enableRootfs {
		die(mkdirp("/mnt", 0755))
		die(mount(rootDevice, "/mnt", "ext4", syscall.MS_NOATIME, ""))
		die(resizeExt4FS(rootDevice, "/mnt"))
	} else {
		// If rootfs is not enabled then set up an overlayfs with a readonly
		// containerfs layer and a read/write scratchfs layer.
		die(mkdirp("/container", 0755))
		die(mount(containerDevice, "/container", "ext4", syscall.MS_RDONLY, ""))
		die(mkdirp("/scratch", 0755))
		die(mount(scratchDevice, "/scratch", "ext4", syscall.MS_RELATIME, ""))
		die(mkdirp("/scratch/bbvmroot", 0755))
		die(mkdirp("/scratch/bbvmwork", 0755))
		die(mkdirp("/mnt", 0755))
		die(mount("overlayfs:/scratch/bbvmroot", "/mnt", "overlay", syscall.MS_NOATIME, "lowerdir=/container,upperdir=/scratch/bbvmroot,workdir=/scratch/bbvmwork"))
	}

	die(mkdirp("/mnt/workspace", 0755))
	die(mount(workspaceDevice, "/mnt/workspace", "ext4", syscall.MS_NOATIME, ""))

	die(mkdirp("/mnt/dev", 0755))
	die(mount("/dev", "/mnt/dev", "", syscall.MS_MOVE, ""))

	die(copyFile("/init", "/mnt/init", 0555))

	log.Debugf("switching root!")
	die(chdir("/mnt"))
	die(mount(".", "/", "", syscall.MS_MOVE, ""))
	die(chroot("."))
	die(chdir("/"))

	die(mkdirp("/dev/pts", 0755))
	die(mount("devpts", "/dev/pts", "devpts", syscall.MS_NOEXEC|syscall.MS_NOSUID|syscall.MS_NOATIME, "mode=0620,gid=5,ptmxmode=666"))

	die(mkdirp("/dev/mqueue", 0755))
	die(mount("mqueue", "/dev/mqueue", "mqueue", commonMountFlags, ""))

	die(mkdirp("/dev/shm", 1777))
	die(mount("shm", "/dev/shm", "tmpfs", syscall.MS_NOSUID|syscall.MS_NODEV, ""))

	die(mkdirp("/dev/hugepages", 0755))
	die(mount("hugetlbfs", "/dev/hugepages", "hugetlbfs", syscall.MS_RELATIME, "pagesize=2M"))

	die(mkdirp("/proc", 0555))
	die(mount("proc", "/proc", "proc", commonMountFlags, ""))
	die(mount("binfmt_misc", "/proc/sys/fs/binfmt_misc", "binfmt_misc", commonMountFlags|syscall.MS_RELATIME, ""))

	die(mkdirp("/sys", 0555))
	die(mount("sys", "/sys", "sysfs", commonMountFlags, ""))

	die(mkdirp("/run", 0755))
	die(mount("run", "/run", "tmpfs", syscall.MS_NOSUID|syscall.MS_NODEV, ""))
	die(mkdirp("/run/lock", 1777))

	die(syscall.Symlink("/proc/self/fd", "/dev/fd"))
	die(syscall.Symlink("/proc/self/fd/0", "/dev/stdin"))
	die(syscall.Symlink("/proc/self/fd/1", "/dev/stdout"))
	die(syscall.Symlink("/proc/self/fd/2", "/dev/stderr"))

	die(mkdirp("/root", syscall.S_IRWXU))

	die(mount("tmpfs", "/sys/fs/cgroup", "tmpfs", syscall.MS_NOSUID|syscall.MS_NOEXEC|syscall.MS_NODEV, "mode=755"))
	die(mkdirp("/sys/fs/cgroup/unified", 0555))
	die(mount("cgroup2", "/sys/fs/cgroup/unified", "cgroup2", cgroupMountFlags, "nsdelegate"))

	die(mkdirp("/sys/fs/cgroup/net_cls,net_prio", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/net_cls,net_prio", "cgroup", cgroupMountFlags, "net_cls,net_prio"))

	die(mkdirp("/sys/fs/cgroup/hugetlb", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/hugetlb", "cgroup", cgroupMountFlags, "hugetlb"))

	die(mkdirp("/sys/fs/cgroup/pids", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/pids", "cgroup", cgroupMountFlags, "pids"))

	die(mkdirp("/sys/fs/cgroup/freezer", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/freezer", "cgroup", cgroupMountFlags, "freezer"))

	die(mkdirp("/sys/fs/cgroup/devices", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/devices", "cgroup", cgroupMountFlags, "devices"))

	die(mkdirp("/sys/fs/cgroup/blkio", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/blkio", "cgroup", cgroupMountFlags, "blkio"))

	die(mkdirp("/sys/fs/cgroup/memory", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/memory", "cgroup", cgroupMountFlags, "memory"))

	die(mkdirp("/sys/fs/cgroup/perf_event", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/perf_event", "cgroup", cgroupMountFlags, "perf_event"))

	die(mkdirp("/sys/fs/cgroup/cpuset", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/cpuset", "cgroup", cgroupMountFlags, "cpuset"))

	if err := rlimit.SetOpenFileDescriptorLimit(16384); err != nil {
		log.Errorf("Unable to increase file open descriptor limit: %s", err)
	}

	die(mkdirp("/etc", 0755))
	die(os.WriteFile("/etc/hostname", []byte("localhost\n"), 0755))
	hosts := []string{
		"127.0.0.1	        localhost",
		"::1		        localhost ip6-localhost ip6-loopback",
		"ff02::1		ip6-allnodes",
		"ff02::2		ip6-allrouters",
	}
	die(os.WriteFile("/etc/hosts", []byte(strings.Join(hosts, "\n")), 0755))
	nameServers := []string{
		"nameserver 8.8.8.8",
		"nameserver 8.8.4.4",
		"nameserver 1.1.1.1",
	}
	die(os.WriteFile("/etc/resolv.conf", []byte(strings.Join(nameServers, "\n")), 0755))
	if _, err := os.Stat("/etc/mtab"); err != nil {
		if os.IsNotExist(err) {
			die(syscall.Symlink("/proc/mounts", "/etc/mtab"))
		} else {
			die(err)
		}
	}

	// Increase file watcher limit to 1% of the default 8GB memory.
	// See https://github.com/torvalds/linux/blob/929ed21dfdb6ee94391db51c9eedb63314ef6847/fs/notify/inotify/inotify_user.c#L838-L844
	if err := os.WriteFile("/proc/sys/fs/inotify/max_user_watches", []byte("65536"), 0); err != nil {
		die(fmt.Errorf("failed to set fs.inotify.max_user_watches: %s", err))
	}

	if *setDefaultRoute {
		die(configureDefaultRoute("eth0", "192.168.241.1"))
	}

	die(os.Setenv("PATH", *path))

	// Done configuring the FS and env.
	// Now initialize child processes.

	go reapChildren(rootContext)

	eg, ctx := errgroup.WithContext(rootContext)
	if *debugMode {
		log.Warningf("Running init in debug mode; this is not secure!")
		eg.Go(func() error {
			c := exec.CommandContext(rootContext, "chpasswd")
			c.Stdin = bytes.NewBuffer([]byte("root:root"))
			if _, err := c.CombinedOutput(); err != nil {
				log.Errorf("Error setting root pw: %s", err)
			}
			for {
				c2 := exec.CommandContext(ctx, "getty", "-L", "ttyS0", "115200", "vt100")
				if err := c2.Run(); err != nil {
					return err
				}
			}
		})
	}

	if *initDockerd {
		die(startDockerd(ctx))
	}
	eg.Go(func() error {
		// Run the vmexec server as a child process so that when we call wait()
		// to reap direct zombie children, we aren't stealing the WaitStatus
		// from the vmexec server (since only the parent process can wait() for
		// a pid). We could alternatively use a mutex to avoid reaping while
		// vmexec is running a command, but that causes problems for Bazel,
		// which explicitly waits for stale server processes to be reaped.
		cmd := exec.CommandContext(ctx, os.Args[0], append(os.Args[1:], "--vmexec")...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	})
	eg.Go(func() error {
		if !*enableVFS {
			return nil
		}
		cmd := exec.CommandContext(ctx, os.Args[0], append(os.Args[1:], "--vmvfs")...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	})

	log.Printf("Finished init in %s", time.Since(start))
	if err := eg.Wait(); err != nil {
		log.Errorf("Init errgroup finished with err: %s", err)
	}

	// Halt the system explicitly to prevent a kernel panic.
	syscall.Reboot(syscall.LINUX_REBOOT_CMD_RESTART)
}

func runVMExecServer(ctx context.Context) error {
	listener, err := vsock.NewGuestListener(ctx, uint32(*vmExecPort))
	if err != nil {
		return err
	}
	log.Infof("Starting vm exec listener on vsock port: %d", *vmExecPort)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(grpc_server.MaxRecvMsgSizeBytes()))

	vmService, err := vmexec.NewServer(workspaceDevice)
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
	if *initDockerd {
		if err := waitForDockerd(ctx); err != nil {
			return err
		}
	}

	return server.Serve(listener)
}

// Resizes the ext4 filesystem mounted at the given path to match the underlying
// block device size.
func resizeExt4FS(devicePath, mountPath string) error {
	sizeBuf, err := os.ReadFile(fmt.Sprintf("/sys/class/block/%s/size", filepath.Base(devicePath)))
	if err != nil {
		return status.InternalErrorf("read block device size: %s", err)
	}
	// Here, the block size is always 512.
	// So the size in bytes is deviceSizeBlocks*512.
	deviceSizeBlocks, err := strconv.Atoi(strings.TrimSpace(string(sizeBuf)))
	if err != nil {
		return status.InternalErrorf("failed to parse block device size %q", string(sizeBuf))
	}

	s := &syscall.Statfs_t{}
	if err := syscall.Statfs(mountPath, s); err != nil {
		return status.InternalErrorf("statfs %s: %s", mountPath, err)
	}
	blocks := int64(deviceSizeBlocks*512) / s.Bsize
	fd, err := syscall.Open(mountPath, syscall.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(fd)
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		uintptr(fd),
		uintptr(EXT4_IOC_RESIZE_FS),
		uintptr(unsafe.Pointer(&blocks)),
	)
	if errno != 0 {
		return status.InternalErrorf("EXT4_IOC_RESIZE_FS: errno %s", errno)
	}
	return nil
}

package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/vmexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rlimit"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"

	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
)

const (
	logLevel = "debug"

	commonMountFlags = syscall.MS_NODEV | syscall.MS_NOEXEC | syscall.MS_NOSUID
	cgroupMountFlags = syscall.MS_NODEV | syscall.MS_NOEXEC | syscall.MS_NOSUID | syscall.MS_RELATIME
)

var (
	cmd  = flag.String("cmd", "/sbin/getty -L ttyS0 115200 vt100", "The command to execute.")
	path = flag.String("path", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "The path to use when executing cmd")
	port = flag.Uint("port", vsock.DefaultPort, "The vsock port number to listen on")
)

// die logs the provided error if it is not nil and then terminates the program.
func die(err error) {
	if err != nil {
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

func reapChildren(ctx context.Context, reapMutex *sync.RWMutex) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, unix.SIGCHLD)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c:
			reapMutex.Lock()
			var status syscall.WaitStatus
			syscall.Wait4(-1, &status, unix.WNOHANG, nil)
			reapMutex.Unlock()
		}
	}
}

// This is mostly cribbed from github.com/superfly/init-snapshot
// which was very helpful <3!
func main() {
	rootContext := context.Background()

	// setup logging
	opts := log.Opts{
		Level: logLevel,
	}
	if err := log.Configure(opts); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1) // in case log.Fatalf does not work.
	}

	flag.Parse()
	log.Infof("Starting BuildBuddy init (args: %s)", os.Args)

	// Quick note about devices: This script is passed to the kernel via
	// initrd, which is nice because it's small / read-only. 2 additional
	// devices are attached to the VM, a containerfs (RO) which is generated
	// from the container image, and a workspacefs (RW) which is mounted,
	// with the containerfs using overlayfs. That way all writes done inside
	// the container are written to the workspacefs and the containerfs is
	// untouched (and safe for re-use across multiple VMs).
	die(mkdirp("/dev", 0755))
	die(mount("devtmpfs", "/dev", "devtmpfs", syscall.MS_NOSUID, "mode=0620,gid=5,ptmxmode=666"))

	die(mkdirp("/container", 0755))
	die(mount("/dev/vda", "/container", "ext4", syscall.MS_RDONLY, ""))

	die(mkdirp("/overlay", 0755))
	die(mount("/dev/vdb", "/overlay", "ext4", syscall.MS_RELATIME, ""))

	die(mkdirp("/overlay/root", 0755))
	die(mkdirp("/overlay/work", 0755))

	die(mkdirp("/mnt", 0755))
	die(mount("overlayfs:/overlay/root", "/mnt", "overlay", syscall.MS_NOATIME, "lowerdir=/container,upperdir=/overlay/root,workdir=/overlay/work"))

	die(mkdirp("/mnt/dev", 0755))
	die(mount("/dev", "/mnt/dev", "", syscall.MS_MOVE, ""))

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

	if err := rlimit.MaxRLimit(); err != nil {
		log.Errorf("Unable to increase rlimit: %s", err)
	}

	die(mkdirp("/etc", 0755))
	die(os.WriteFile("/etc/hostname", []byte("localhost\n"), 0755))
	hosts := []string{
		"127.0.0.1	localhost",
		"::1		localhost ip6-localhost ip6-loopback",
		"ff02::1		ip6-allnodes",
		"ff02::2		ip6-allrouters",
	}
	die(os.WriteFile("/etc/hosts", []byte(strings.Join(hosts, "\n")), 0755))

	// TODO(tylerw): setup networking

	reapMutex := sync.RWMutex{}
	go reapChildren(rootContext, &reapMutex)

	eg, ctx := errgroup.WithContext(rootContext)
	if *cmd != "" {
		eg.Go(func() error {
			cmdParts := strings.Split(*cmd, " ")
			c := exec.CommandContext(ctx, cmdParts[0], cmdParts[1:]...)
			c.Env = append(c.Env, fmt.Sprintf("PATH=%s", *path))
			if err := c.Run(); err != nil {
				log.Errorf("Configured command failed with err: %s", err)
				return err
			}
			log.Debugf("cmd exited!")
			return nil
		})
	} else {
		log.Infof("Starting vm exec listener on vsock port: %d", *port)
		eg.Go(func() error {
			listener, err := vsock.NewGuestListener(ctx, uint32(*port))
			if err != nil {
				return err
			}
			server := grpc.NewServer()
			vmService := vmexec.NewServer(&reapMutex)
			vmxpb.RegisterExecServer(server, vmService)
			return server.Serve(listener)
		})
	}

	if err := eg.Wait(); err != nil {
		log.Errorf("Init errgroup finished with err: %s", err)
	}

	// Halt the system explicitly to prevent a kernel panic.
	syscall.Reboot(syscall.LINUX_REBOOT_CMD_RESTART)
}

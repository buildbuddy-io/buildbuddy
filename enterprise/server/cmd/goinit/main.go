package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/vmexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rlimit"
	"golang.org/x/sync/errgroup"
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
	for {
		select {
		case <-ctx.Done():
			break
		default:
			// borrowed from https://github.com/driusan/dainit/blob/master/zombie.go

			// If there are multiple zombies, Wait4 will reap one, and then block until the next child changes state.
			// We call it with NOHANG a few times to clear up any backlog, and then make a blocking call until our
			// next child dies.
			var status syscall.WaitStatus

			// If there are more than 10 zombies, we likely have other problems.
			for i := 0; i < 10; i++ {
				// We don't really care what the pid was that got reaped, or if there's nothing to wait for
				syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
			}

			// This blocks, so that we're not spending all of our time reaping processes..
			syscall.Wait4(-1, &status, 0, nil)
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

	die(os.MkdirAll("/dev", 0755))
	die(mount("devtmpfs", "/dev", "devtmpfs", syscall.MS_NOSUID, "mode=0620,gid=5,ptmxmode=666"))

	die(os.MkdirAll("/newroot", 0755))
	die(mount("/dev/vdb", "/newroot", "ext4", syscall.MS_RELATIME, ""))

	die(os.MkdirAll("/newroot/dev", 0755))
	die(mount("/dev", "/newroot/dev", "", syscall.MS_MOVE, ""))

	log.Debugf("switching root!")

	die(chdir("/newroot"))
	die(mount(".", "/", "", syscall.MS_MOVE, ""))
	die(chroot("."))
	die(chdir("/"))

	die(os.MkdirAll("/workspace", 0755))
	die(mount("/dev/vdc", "/workspace", "ext4", syscall.MS_RELATIME, ""))

	die(os.MkdirAll("/dev/pts", 0755))
	die(mount("devpts", "/dev/pts", "devpts", syscall.MS_NOEXEC|syscall.MS_NOSUID|syscall.MS_NOATIME, "mode=0620,gid=5,ptmxmode=666"))

	die(os.MkdirAll("/dev/mqueue", 0755))
	die(mount("mqueue", "/dev/mqueue", "mqueue", commonMountFlags, ""))

	die(os.MkdirAll("/dev/shm", 1777))
	die(mount("shm", "/dev/shm", "tmpfs", syscall.MS_NOSUID|syscall.MS_NODEV, ""))

	die(os.MkdirAll("/dev/hugepages", 0755))
	die(mount("hugetlbfs", "/dev/hugepages", "hugetlbfs", syscall.MS_RELATIME, "pagesize=2M"))

	die(os.MkdirAll("/proc", 0555))
	die(mount("proc", "/proc", "proc", commonMountFlags, ""))
	die(mount("binfmt_misc", "/proc/sys/fs/binfmt_misc", "binfmt_misc", commonMountFlags|syscall.MS_RELATIME, ""))

	die(os.MkdirAll("/sys", 0555))
	die(mount("sys", "/sys", "sysfs", commonMountFlags, ""))

	die(os.MkdirAll("/run", 0755))
	die(mount("run", "/run", "tmpfs", syscall.MS_NOSUID|syscall.MS_NODEV, ""))
	die(os.MkdirAll("/run/lock", 1777))

	die(syscall.Symlink("/proc/self/fd", "/dev/fd"))
	die(syscall.Symlink("/proc/self/fd/0", "/dev/stdin"))
	die(syscall.Symlink("/proc/self/fd/1", "/dev/stdout"))
	die(syscall.Symlink("/proc/self/fd/2", "/dev/stderr"))

	die(os.MkdirAll("/root", syscall.S_IRWXU))

	die(mount("tmpfs", "/sys/fs/cgroup", "tmpfs", syscall.MS_NOSUID|syscall.MS_NOEXEC|syscall.MS_NODEV, "mode=755"))
	die(os.MkdirAll("/sys/fs/cgroup/unified", 0555))
	die(mount("cgroup2", "/sys/fs/cgroup/unified", "cgroup2", cgroupMountFlags, "nsdelegate"))

	die(os.MkdirAll("/sys/fs/cgroup/net_cls,net_prio", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/net_cls,net_prio", "cgroup", cgroupMountFlags, "net_cls,net_prio"))

	die(os.MkdirAll("/sys/fs/cgroup/hugetlb", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/hugetlb", "cgroup", cgroupMountFlags, "hugetlb"))

	die(os.MkdirAll("/sys/fs/cgroup/pids", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/pids", "cgroup", cgroupMountFlags, "pids"))

	die(os.MkdirAll("/sys/fs/cgroup/freezer", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/freezer", "cgroup", cgroupMountFlags, "freezer"))

	die(os.MkdirAll("/sys/fs/cgroup/devices", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/devices", "cgroup", cgroupMountFlags, "devices"))

	die(os.MkdirAll("/sys/fs/cgroup/blkio", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/blkio", "cgroup", cgroupMountFlags, "blkio"))

	die(os.MkdirAll("/sys/fs/cgroup/memory", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/memory", "cgroup", cgroupMountFlags, "memory"))

	die(os.MkdirAll("/sys/fs/cgroup/perf_event", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/perf_event", "cgroup", cgroupMountFlags, "perf_event"))

	die(os.MkdirAll("/sys/fs/cgroup/cpuset", 0555))
	die(mount("cgroup", "/sys/fs/cgroup/cpuset", "cgroup", cgroupMountFlags, "cpuset"))

	if err := rlimit.MaxRLimit(); err != nil {
		log.Errorf("Unable to increase rlimit: %s", err)
	}

	die(os.MkdirAll("/etc", 0755))
	die(os.WriteFile("/etc/hostname", []byte("localhost\n"), 0755))

	// TODO(tylerw): setup networking

	go reapChildren(rootContext) // start outside of eg so it does not block shutdown.

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
	}
	eg.Go(func() error {
		listener, err := vsock.NewGuestListener(ctx, uint32(*port))
		if err != nil {
			return err
		}
		server := grpc.NewServer()
		vmService := vmexec.NewServer()
		vmxpb.RegisterExecServer(server, vmService)
		return server.Serve(listener)
	})

	eg.Wait() // ignore errors.

	// Halt the system explicitly to prevent a kernel panic.
	syscall.Reboot(syscall.LINUX_REBOOT_CMD_RESTART)
}

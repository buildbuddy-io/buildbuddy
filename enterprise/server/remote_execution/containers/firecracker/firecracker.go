//go:build linux && !android
// +build linux,!android

package firecracker

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/armon/circbuf"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/blockio"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/nbd/nbdserver"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/uffd"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vmexec_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/firecracker-microvm/firecracker-go-sdk/client/operations"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	containerutil "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/container"
	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
	vmfspb "github.com/buildbuddy-io/buildbuddy/proto/vmvfs"
	dockerclient "github.com/docker/docker/client"
	fcclient "github.com/firecracker-microvm/firecracker-go-sdk"
	fcmodels "github.com/firecracker-microvm/firecracker-go-sdk/client/models"
)

var firecrackerMountWorkspaceFile = flag.Bool("executor.firecracker_mount_workspace_file", false, "Enables mounting workspace filesystem to improve performance of copying action outputs.")
var firecrackerCgroupVersion = flag.String("executor.firecracker_cgroup_version", "", "Specifies the cgroup version for firecracker to use.")
var debugStreamVMLogs = flag.Bool("executor.firecracker_debug_stream_vm_logs", false, "Stream firecracker VM logs to the terminal.")
var debugTerminal = flag.Bool("executor.firecracker_debug_terminal", false, "Run an interactive terminal in the Firecracker VM connected to the executor's controlling terminal. For debugging only.")
var enableNBD = flag.Bool("executor.firecracker_enable_nbd", false, "Enables network block devices for firecracker VMs.")
var enableUFFD = flag.Bool("executor.firecracker_enable_uffd", false, "Enables userfaultfd for firecracker VMs.")
var dieOnFirecrackerFailure = flag.Bool("executor.die_on_firecracker_failure", false, "Makes the host executor process die if any command orchestrating or running Firecracker fails. Useful for capturing failures preemptively. WARNING: using this option MAY leave the host machine in an unhealthy state on Firecracker failure; some post-hoc cleanup may be necessary.")

const (
	// How long to wait for the VMM to listen on the firecracker socket.
	firecrackerSocketWaitTimeout = 3 * time.Second

	// How long to wait when dialing the vmexec server inside the VM.
	vSocketDialTimeout = 10 * time.Second

	// How long to wait for the jailer directory to be created.
	jailerDirectoryCreationTimeout = 1 * time.Second

	// The firecracker socket path (will be relative to the chroot).
	firecrackerSocketPath = "/run/fc.sock"

	// The vSock path (also relative to the chroot).
	firecrackerVSockPath = "/run/v.sock"

	// UFFD socket path relative to the chroot.
	uffdSockName = "uffd.sock"

	// The names to use when creating snapshots (relative to chroot).
	vmStateSnapshotName = "vmstate.snap"
	fullMemSnapshotName = "full-mem.snap"
	diffMemSnapshotName = "diff-mem.snap"
	// Directory storing the memory file chunks.
	memoryChunkDirName = "memory"

	fullSnapshotType = "Full"
	diffSnapshotType = "Diff"

	mergeDiffSnapshotConcurrency = 4
	// Firecracker writes changed blocks in 4Kb blocks.
	mergeDiffSnapshotBlockSize = 4096

	// Size of machine log tail to retain in memory so that we can parse logs for
	// errors.
	vmLogTailBufSize = 1024 * 12 // 12 KB
	// Log prefix used by goinit when logging fatal errors.
	fatalInitLogPrefix = "die: "

	// The workspacefs image name and drive ID.
	workspaceFSName  = "workspacefs.ext4"
	workspaceDriveID = "workspacefs"
	// workspaceDiskSlackSpaceMB is the amount of additional storage allocated to
	// the workspace disk for writing action outputs, test tempfiles, etc.
	// TODO(bduffany): Consider making this configurable
	workspaceDiskSlackSpaceMB = 2_000 // 2 GB

	// The scratchfs image name and drive ID.
	scratchFSName  = "scratchfs.ext4"
	scratchDriveID = "scratchfs"
	// minScratchDiskSizeBytes is the minimum size needed for the scratch disk.
	// This is needed because the init binary needs some space to copy files around.
	minScratchDiskSizeBytes = 64e6

	// Chunk size to use when creating COW images from files.
	cowChunkSizeBytes = 512 * 1024

	// The containerfs drive ID.
	containerFSName  = "containerfs.ext4"
	containerDriveID = "containerfs"

	// The networking deets for host and vm interfaces.
	// All VMs are configured with the same IP and tap device via boot args,
	// but because they run inside of a network namespace, they do not
	// conflict. More details here:
	// https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/network-for-clones.md
	tapDeviceName = "vmtap0"
	tapDeviceMac  = "7a:a8:fa:dc:76:b7"
	tapIP         = "192.168.241.1"
	tapAddr       = tapIP + "/29"

	vmIP    = "192.168.241.2"
	vmAddr  = vmIP + "/29"
	vmIface = "eth0"

	// https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/html/networking_guide/sec-configuring_ip_networking_from_the_kernel_command_line
	// ip<client-IP-number>:[<server-id>]:<gateway-IP-number>:<netmask>:<client-hostname>:<interface>:{dhcp|dhcp6|auto6|on|any|none|off}
	machineIPBootArgs = "ip=" + vmIP + ":::255.255.255.48::" + vmIface + ":off"

	// This is pretty arbitrary limit -- when vmIdx gets this big it will
	// roll over to 0, causing new VMs to start re-using old local IPs. But
	// net namespaces are deleted upon VM removal, so this should not cause
	// any issue. If more than this many VMs were active on a single host at
	// once -- it would cause an error.
	maxVMSPerHost = 1000

	// The path in the guest where VFS is mounted.
	guestVFSMountDir = "/vfs"

	// How long to allow for the VM to be finalized (paused, outputs copied, etc.)
	finalizationTimeout = 10 * time.Second
)

var (
	locateBinariesOnceMap sync.Map
	locateBinariesError   error
	masqueradingOnce      sync.Once

	// kernel + initrd
	kernelImagePath string
	initrdImagePath string

	// firecracker + jailer
	firecrackerBinPath string
	jailerBinPath      string

	vmIdx   int
	vmIdxMu sync.Mutex

	fatalErrPattern = regexp.MustCompile(`\b` + fatalInitLogPrefix + `(.*)`)
)

func openFile(ctx context.Context, env environment.Env, fileName string) (io.ReadCloser, error) {
	// If the file exists on the filesystem, use that.
	if path, err := exec.LookPath(fileName); err == nil {
		log.CtxDebugf(ctx, "Located %q at %s", fileName, path)
		return os.Open(path)
	}

	// Otherwise try to find it in the bundle or runfiles.
	return env.GetFileResolver().Open(fileName)
}

// Returns the cgroup version available on this machine, which is returned from
//
//	stat -fc %T /sys/fs/cgroup/
//
// and should be either 'cgroup2fs' (version 2) or 'tmpfs' (version 1)
//
// More info here: https://kubernetes.io/docs/concepts/architecture/cgroups/
// TODO(iain): preemptively get this in a provider a la Provider in podman.go.
func getCgroupVersion() (string, error) {
	if *firecrackerCgroupVersion != "" {
		return *firecrackerCgroupVersion, nil
	}
	b, err := exec.Command("stat", "-fc", "%T", "/sys/fs/cgroup/").Output()
	if err != nil {
		return "", status.UnavailableErrorf("Error determining system cgroup version: %v", err)
	}
	v := strings.TrimSpace(string(b))
	if v == "cgroup2fs" {
		return "2", nil
	} else if v == "tmpfs" {
		return "1", nil
	} else {
		return "", status.InternalErrorf("No cgroup version found (system reported %s)", v)
	}
}

// putFileIntoDir finds "fileName" on the local filesystem, in runfiles, or
// in the bundle. It then puts that file into destdir (via hardlink or copying)
// and returns a path to the file in the new location. Files are written in
// a content-addressable-storage-based location, so when files are updated they
// will be put into new paths.
func putFileIntoDir(ctx context.Context, env environment.Env, fileName, destDir string, mode fs.FileMode) (string, error) {
	f, err := openFile(ctx, env, fileName)
	if err != nil {
		return "", err
	}
	// If fileReader is still nil, the file was not found, so return an error.
	if f == nil {
		return "", status.NotFoundErrorf("File %q not found on fs, in runfiles or in bundle.", fileName)
	}
	defer f.Close()

	// Compute the file hash to determine the new location where it should be written.
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	fileHash := fmt.Sprintf("%x", h.Sum(nil))
	fileHome := filepath.Join(destDir, "executor", fileHash)
	if err := disk.EnsureDirectoryExists(fileHome); err != nil {
		return "", err
	}
	casPath := filepath.Join(fileHome, filepath.Base(fileName))
	if exists, err := disk.FileExists(ctx, casPath); err == nil && exists {
		log.CtxDebugf(ctx, "Found existing %q in path: %q", fileName, casPath)
		return casPath, nil
	}
	// Write the file to the new location if it does not exist there already.
	f, err = openFile(ctx, env, fileName)
	if err != nil {
		return "", err
	}
	defer f.Close()
	writer, err := disk.FileWriter(ctx, casPath)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(writer, f); err != nil {
		return "", err
	}
	if err := writer.Commit(); err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	log.CtxDebugf(ctx, "Put %q into new path: %q", fileName, casPath)
	return casPath, nil
}

func getLogrusLogger() *logrus.Entry {
	logrusLogger := logrus.New()
	logrusLogger.SetLevel(logrus.ErrorLevel)
	if *debugStreamVMLogs {
		logrusLogger.SetLevel(logrus.TraceLevel)
	}
	return logrus.NewEntry(logrusLogger)
}

func checkIfFilesExist(targetDir string, files ...string) bool {
	for _, f := range files {
		fileName := filepath.Base(f)
		newPath := filepath.Join(targetDir, fileName)
		if _, err := os.Stat(newPath); err != nil {
			return false
		}
	}
	return true
}

// FirecrackerContainer executes commands inside of a firecracker VM.
type FirecrackerContainer struct {
	id          string // a random GUID, unique per-run of firecracker
	vmIdx       int    // the index of this vm on the host machine
	loader      snaploader.Loader
	snapshotKey *fcpb.SnapshotKey

	vmConfig         *fcpb.VMConfiguration
	containerImage   string // the OCI container image. ex "alpine:latest"
	actionWorkingDir string // the action directory with inputs / outputs
	containerFSPath  string // the path to the container ext4 image
	tempDir          string // path for writing disk images before the chroot is created
	user             string // user to execute all commands as

	rmOnce *sync.Once
	rmErr  error

	// Whether networking has been set up (and needs to be cleaned up).
	isNetworkSetup bool

	// Whether the VM was recycled.
	recycled bool

	// dockerClient is used to optimize image pulls by reusing image layers from
	// the Docker cache as well as deduping multiple requests for the same image.
	dockerClient *dockerclient.Client

	// when VFS is enabled, this contains the layout for the next execution
	fsLayout  *container.FileSystemLayout
	vfsServer *vfs_server.Server

	// When NBD is enabled, this is the running NBD server that serves the VM
	// disks.
	nbdServer      *nbdserver.Server
	scratchStore   *blockio.COWStore
	workspaceStore *blockio.COWStore

	memoryStore *blockio.COWStore
	uffdHandler *uffd.Handler

	jailerRoot         string            // the root dir the jailer will work in
	machine            *fcclient.Machine // the firecracker machine object.
	vmLog              *VMLog
	env                environment.Env
	imageCacheAuth     *container.ImageCacheAuthenticator
	allowSnapshotStart bool
	mountWorkspaceFile bool

	// If a container is resumed from a snapshot, the jailer
	// is started first using an external command and then the snapshot
	// is loaded. This slightly breaks the firecracker SDK's "Wait()"
	// method, so in that case we wait for the external jailer command
	// to finish, rather than calling "Wait()" on the sdk machine object.
	externalJailerCmd *exec.Cmd

	cleanupVethPair func(context.Context) error
	// cancelVmCtx cancels the Machine context, stopping the VMM if it hasn't
	// already been stopped manually.
	cancelVmCtx context.CancelFunc
}

func NewContainer(ctx context.Context, env environment.Env, imageCacheAuth *container.ImageCacheAuthenticator, task *repb.ExecutionTask, opts ContainerOpts) (*FirecrackerContainer, error) {
	if opts.VMConfiguration == nil {
		return nil, status.InvalidArgumentError("missing VMConfiguration")
	}

	vmLog, err := NewVMLog(vmLogTailBufSize)
	if err != nil {
		return nil, err
	}

	// WARNING: because of the limitation on the length of unix sock file
	// paths (103), this directory path needs to be short. Specifically, a
	// full sock path will look like:
	// /tmp/firecracker/217d4de0-4b28-401b-891b-18e087718ad1/root/run/fc.sock
	// everything after "/tmp" is 65 characters, so 38 are left for the
	// jailerRoot.
	if opts.JailerRoot == "" {
		opts.JailerRoot = "/tmp"
	}
	if len(opts.JailerRoot) > 38 {
		return nil, status.InvalidArgumentErrorf("JailerRoot must be < 38 characters. Was %q (%d).", opts.JailerRoot, len(opts.JailerRoot))
	}
	if err := disk.EnsureDirectoryExists(opts.JailerRoot); err != nil {
		return nil, err
	}

	// Ensure our kernel and initrd exist on the same filesystem where we'll
	// be jailing containers. This allows us to hardlink these files rather
	// than copying them around over and over again.
	if err := copyStaticFiles(context.Background(), env, opts.JailerRoot); err != nil {
		return nil, err
	}
	loader, err := snaploader.New(env)
	if err != nil {
		return nil, err
	}

	c := &FirecrackerContainer{
		vmConfig:           opts.VMConfiguration,
		jailerRoot:         opts.JailerRoot,
		dockerClient:       opts.DockerClient,
		containerImage:     opts.ContainerImage,
		user:               opts.User,
		actionWorkingDir:   opts.ActionWorkingDirectory,
		env:                env,
		loader:             loader,
		vmLog:              vmLog,
		imageCacheAuth:     imageCacheAuth,
		mountWorkspaceFile: *firecrackerMountWorkspaceFile,
		cancelVmCtx:        func() {},
	}

	if opts.ForceVMIdx != 0 {
		c.vmIdx = opts.ForceVMIdx
	}

	if opts.SavedState == nil {
		c.vmConfig = proto.Clone(c.vmConfig).(*fcpb.VMConfiguration)
		c.vmConfig.DebugMode = *debugTerminal

		if err := c.newID(ctx); err != nil {
			return nil, err
		}
		cd, err := digest.ComputeForMessage(c.vmConfig, repb.DigestFunction_SHA256)
		if err != nil {
			return nil, err
		}
		c.snapshotKey, err = snaploader.NewKey(task, cd.GetHash(), c.id)
		if err != nil {
			return nil, err
		}
	} else {
		c.snapshotKey = opts.SavedState.GetSnapshotKey()

		// TODO(bduffany): add version info to snapshots. For example, if a
		// breaking change is made to the vmexec API, the executor should not
		// attempt to connect to snapshots that were created before the change.
	}

	return c, nil
}

// mergeDiffSnapshot reads from diffSnapshotPath and writes all non-zero blocks
// into the baseSnapshotPath file or the baseSnapshotStore if non-nil.
func mergeDiffSnapshot(ctx context.Context, baseSnapshotPath string, baseSnapshotStore *blockio.COWStore, diffSnapshotPath string, concurrency int, bufSize int) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	var out io.WriterAt
	if baseSnapshotStore == nil {
		f, err := os.OpenFile(baseSnapshotPath, os.O_WRONLY, 0644)
		if err != nil {
			return status.UnavailableErrorf("Could not open base snapshot file %q: %s", baseSnapshotPath, err)
		}
		defer f.Close()
		out = f
	} else {
		out = baseSnapshotStore
	}

	in, err := os.Open(diffSnapshotPath)
	if err != nil {
		return status.UnavailableErrorf("Could not open diff snapshot file %q: %s", diffSnapshotPath, err)
	}
	defer in.Close()

	inInfo, err := in.Stat()
	if err != nil {
		return status.UnavailableErrorf("Could not stat diff snapshot file %q: %s", diffSnapshotPath, err)
	}

	eg, ctx := errgroup.WithContext(ctx)

	perThreadBytes := int64(math.Ceil(float64(inInfo.Size()) / float64(concurrency)))
	perThreadBytes = ((perThreadBytes + int64(bufSize)) / int64(bufSize)) * int64(bufSize)
	for i := 0; i < concurrency; i++ {
		i := i
		offset := perThreadBytes * int64(i)
		regionEnd := perThreadBytes * int64(i+1)
		if regionEnd > inInfo.Size() {
			regionEnd = inInfo.Size()
		}
		eg.Go(func() error {
			gin, err := os.Open(diffSnapshotPath)
			if err != nil {
				return status.UnavailableErrorf("Could not open diff snapshot file %q: %s", diffSnapshotPath, err)
			}
			defer gin.Close()
			buf := make([]byte, bufSize)
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					break
				}
				// 3 is the Linux constant for the SEEK_DATA option to lseek.
				newOffset, err := syscall.Seek(int(gin.Fd()), offset, 3)
				if err != nil {
					// ENXIO is expected when the offset is within a hole at the end of
					// the file.
					if err == syscall.ENXIO {
						break
					}
					return err
				}
				offset = newOffset
				if offset >= regionEnd {
					break
				}
				n, err := gin.ReadAt(buf, offset)
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				if _, err := out.WriteAt(buf[:n], offset); err != nil {
					return err
				}
				offset += int64(n)
			}
			return nil
		})
	}

	return eg.Wait()
}

// State returns the container state to be persisted to disk so that this
// container can be reconstructed from the state on disk after an executor
// restart.
func (c *FirecrackerContainer) State(ctx context.Context) (*rnpb.ContainerState, error) {
	state := &rnpb.ContainerState{
		IsolationType: string(platform.FirecrackerContainerType),
		FirecrackerState: &rnpb.FirecrackerState{
			VmConfiguration: c.vmConfig,
			SnapshotKey:     c.snapshotKey,
		},
	}
	return state, nil
}

func (c *FirecrackerContainer) unpackBaseSnapshot(ctx context.Context) (string, error) {
	baseDir := filepath.Join(c.getChroot(), "base")
	if err := disk.EnsureDirectoryExists(baseDir); err != nil {
		return "", err
	}
	snap, err := c.loader.GetSnapshot(ctx, c.snapshotKey)
	if err != nil {
		return "", err
	}
	if _, err := c.loader.UnpackSnapshot(ctx, snap, baseDir); err != nil {
		return "", err
	}
	// The base snapshot is no longer useful since we're merging on top
	// of it and replacing the paused VM snapshot with the new merged
	// snapshot. Delete it to prevent unnecessary filecache evictions.
	if err := c.loader.DeleteSnapshot(ctx, snap); err != nil {
		log.Warningf("Failed to delete snapshot: %s", err)
	}

	return baseDir, nil
}

func (c *FirecrackerContainer) pauseVM(ctx context.Context) error {
	if c.machine == nil {
		return status.InternalError("failed to pause VM: machine is not started")
	}

	if err := c.machine.PauseVM(ctx); err != nil {
		log.CtxErrorf(ctx, "Error pausing VM: %s", err)
		return err
	}
	// Now that we've paused the VM, it's a good time to Sync the NBD backing
	// files. This is particularly important when the files are backed with an
	// mmap. The File backing the mmap may differ from the in-memory contents
	// until we explicitly call msync.
	if c.workspaceStore != nil {
		if err := c.workspaceStore.Sync(); err != nil {
			return status.WrapError(err, "failed to sync workspace device store")
		}
	}
	if c.scratchStore != nil {
		if err := c.scratchStore.Sync(); err != nil {
			return status.WrapError(err, "failed to sync scratchfs device store")
		}
	}
	return nil
}

// SaveSnapshot pauses the VM and takes a snapshot, saving the snapshot to
// cache.
//
// If the VM is resumed after this point, the VM state and memory snapshots
// immediately become invalid and another call to SaveSnapshot is required. This
// is because we don't save a copy of the disk when taking a snapshot, and
// instead reuse the same disk image across snapshots (for performance reasons).
// Resuming the VM may change the disk state, causing the original VM state to
// become invalid (e.g., the kernel's page cache may no longer be in sync with
// the disk, which can cause all sorts of issues).
func (c *FirecrackerContainer) SaveSnapshot(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "SaveSnapshot took %s", time.Since(start))
	}()

	snapshotType := fullSnapshotType
	memSnapshotFile := fullMemSnapshotName
	baseMemSnapshotPath := ""

	if *enableUFFD {
		if c.memoryStore != nil {
			snapshotType = diffSnapshotType
			memSnapshotFile = diffMemSnapshotName
		}
	} else {
		// If a snapshot already exists, get a reference to the memory snapshot so
		// that we can perform a diff snapshot and merge the modified pages on top
		// of the existing memory snapshot.
		baseSnapshotUnpackDir, err := c.unpackBaseSnapshot(ctx)
		if err != nil {
			// Ignore Unavailable errors since it just means there's no base
			// snapshot yet (which will happen on the first task executed), or the
			// snapshot was evicted from cache. When this happens, just fall back to
			// taking a full snapshot.
			if !status.IsUnavailableError(err) {
				return err
			}
		} else {
			// Once we've merged the base snapshot and linked it to filecache, we
			// don't need the unpack dir anymore and can unlink.
			defer os.RemoveAll(baseSnapshotUnpackDir)
			baseMemSnapshotPath = filepath.Join(baseSnapshotUnpackDir, fullMemSnapshotName)
			snapshotType = diffSnapshotType
			memSnapshotFile = diffMemSnapshotName
		}
	}

	if err := c.pauseVM(ctx); err != nil {
		return err
	}

	memSnapshotPath := filepath.Join(c.getChroot(), memSnapshotFile)
	vmStateSnapshotPath := filepath.Join(c.getChroot(), vmStateSnapshotName)

	// If an older snapshot is present -- nuke it since we're writing a new one.
	if err := disk.RemoveIfExists(memSnapshotPath); err != nil {
		return status.WrapError(err, "failed to remove existing memory snapshot")
	}
	if err := disk.RemoveIfExists(vmStateSnapshotPath); err != nil {
		return status.WrapError(err, "failed to remove existing VM state snapshot")
	}

	machineStart := time.Now()
	snapshotTypeOpt := func(params *operations.CreateSnapshotParams) {
		params.Body.SnapshotType = snapshotType
	}
	if err := c.machine.CreateSnapshot(ctx, memSnapshotFile, vmStateSnapshotName, snapshotTypeOpt); err != nil {
		log.CtxErrorf(ctx, "Error creating snapshot: %s", err)
		return err
	}

	log.CtxDebugf(ctx, "VMM CreateSnapshot %s took %s", snapshotType, time.Since(machineStart))

	if snapshotType == diffSnapshotType {
		mergeStart := time.Now()
		if err := mergeDiffSnapshot(ctx, baseMemSnapshotPath, c.memoryStore, memSnapshotPath, mergeDiffSnapshotConcurrency, mergeDiffSnapshotBlockSize); err != nil {
			return status.UnknownErrorf("merge diff snapshot failed: %s", err)
		}
		log.CtxDebugf(ctx, "VMM merge diff snapshot took %s", time.Since(mergeStart))
		// Use the merged memory snapshot.
		memSnapshotPath = baseMemSnapshotPath
	}

	// If we're creating a snapshot for the first time, create a COWStore from
	// the initial full snapshot. (If we have a diff snapshot, then we already
	// updated the memoryStore in mergeDiffSnapshot above).
	if *enableUFFD && c.memoryStore == nil {
		memChunkDir := filepath.Join(c.getChroot(), memoryChunkDirName)
		memoryStore, err := c.convertToCOW(ctx, memSnapshotPath, memChunkDir)
		if err != nil {
			return status.WrapError(err, "convert memory snapshot to COWStore")
		}
		c.memoryStore = memoryStore
	}

	opts := &snaploader.CacheSnapshotOptions{
		VMConfiguration:     c.vmConfig,
		VMStateSnapshotPath: vmStateSnapshotPath,
		KernelImagePath:     kernelImagePath,
		InitrdImagePath:     initrdImagePath,
		ContainerFSPath:     filepath.Join(c.getChroot(), containerFSName),
		ChunkedFiles:        map[string]*blockio.COWStore{},
	}
	if *enableNBD {
		opts.ChunkedFiles[scratchDriveID] = c.scratchStore
		opts.ChunkedFiles[workspaceDriveID] = c.workspaceStore
	} else {
		opts.ScratchFSPath = c.scratchFSPath()
		opts.WorkspaceFSPath = c.workspaceFSPath()
	}
	if *enableUFFD {
		opts.ChunkedFiles[memoryChunkDirName] = c.memoryStore
	} else {
		opts.MemSnapshotPath = memSnapshotPath
	}

	snaploaderStart := time.Now()
	if _, err := c.loader.CacheSnapshot(ctx, c.snapshotKey, opts); err != nil {
		return status.WrapError(err, "add snapshot to cache")
	}
	log.CtxDebugf(ctx, "snaploader.CacheSnapshot took %s", time.Since(snaploaderStart))
	return nil
}

// LoadSnapshot loads a VM snapshot from the given snapshot digest and resumes
// the VM.
func (c *FirecrackerContainer) LoadSnapshot(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "LoadSnapshot took %s", time.Since(start))
	}()

	c.rmOnce = &sync.Once{}
	c.rmErr = nil

	if err := c.newID(ctx); err != nil {
		return err
	}

	var netNS string
	if c.vmConfig.EnableNetworking {
		netNS = networking.NetNamespacePath(c.id)
	}

	cgroupVersion, err := getCgroupVersion()
	if err != nil {
		return err
	}

	// We start firecracker with this reduced config because we will load a
	// snapshot that is already configured.
	cfg := fcclient.Config{
		SocketPath:        firecrackerSocketPath,
		NetNS:             netNS,
		Seccomp:           fcclient.SeccompConfig{Enabled: true},
		DisableValidation: true,
		JailerCfg: &fcclient.JailerConfig{
			JailerBinary:   jailerBinPath,
			ChrootBaseDir:  c.jailerRoot,
			ID:             c.id,
			UID:            fcclient.Int(unix.Geteuid()),
			GID:            fcclient.Int(unix.Getegid()),
			NumaNode:       fcclient.Int(0), // TODO(tylerw): randomize this?
			ExecFile:       firecrackerBinPath,
			ChrootStrategy: fcclient.NewNaiveChrootStrategy(""),
			Stdout:         c.vmLogWriter(),
			Stderr:         c.vmLogWriter(),
			CgroupVersion:  cgroupVersion,
		},
		Snapshot: fcclient.SnapshotConfig{
			EnableDiffSnapshots: true,
			ResumeVM:            true,
		},
		ForwardSignals: make([]os.Signal, 0),
	}

	if err := c.setupNetworking(ctx); err != nil {
		return err
	}

	if err := c.setupVFSServer(ctx); err != nil {
		return err
	}

	vmCtx, cancel := context.WithCancel(context.Background())
	c.cancelVmCtx = cancel
	var snapOpt fcclient.Opt
	if *enableUFFD {
		uffdType := fcclient.MemoryBackendType(fcmodels.MemoryBackendBackendTypeUffdPrivileged)
		snapOpt = fcclient.WithSnapshot(uffdSockName, vmStateSnapshotName, uffdType)
	} else {
		snapOpt = fcclient.WithSnapshot(fullMemSnapshotName, vmStateSnapshotName)
	}
	machineOpts := []fcclient.Opt{
		fcclient.WithLogger(getLogrusLogger()),
		snapOpt,
	}

	log.CtxDebugf(ctx, "fullMemSnapshotName: %s, vmStateSnapshotName %s", fullMemSnapshotName, vmStateSnapshotName)

	machine, err := fcclient.NewMachine(vmCtx, cfg, machineOpts...)
	if err != nil {
		return status.InternalErrorf("Failed creating machine: %s", err)
	}
	log.CtxDebugf(ctx, "Command: %v", reflect.Indirect(reflect.Indirect(reflect.ValueOf(machine)).FieldByName("cmd")).FieldByName("Args"))

	snap, err := c.loader.GetSnapshot(ctx, c.snapshotKey)
	if err != nil {
		return status.WrapError(err, "failed to get snapshot")
	}
	unpacked, err := c.loader.UnpackSnapshot(ctx, snap, c.getChroot())
	if err != nil {
		return err
	}
	if len(unpacked.ChunkedFiles) > 0 && !(*enableNBD || *enableUFFD) {
		return status.InternalError("blockio support is disabled but snapshot contains chunked files")
	}
	for name, cow := range unpacked.ChunkedFiles {
		switch name {
		case scratchDriveID:
			c.scratchStore = cow
		case workspaceDriveID:
			c.workspaceStore = cow
		case memoryChunkDirName:
			c.memoryStore = cow
		default:
			return status.InternalErrorf("snapshot contains unsupported chunked artifact %q", name)
		}
	}

	if err := c.setupNBDServer(ctx); err != nil {
		return status.WrapError(err, "failed to init nbd server")
	}

	if err := c.setupUFFDHandler(ctx); err != nil {
		return err
	}

	err = (func() error {
		_, span := tracing.StartSpan(ctx)
		defer span.End()
		span.SetName("StartMachine")
		return machine.Start(vmCtx)
	})()
	if err != nil {
		return status.InternalErrorf("Failed starting machine: %s", err)
	}
	c.machine = machine

	if err := c.machine.ResumeVM(ctx); err != nil {
		return status.InternalErrorf("error resuming VM: %s", err)
	}

	conn, err := c.dialVMExecServer(ctx)
	if err != nil {
		return status.InternalErrorf("Failed to dial firecracker VM exec port: %s", err)
	}
	defer conn.Close()

	execClient := vmxpb.NewExecClient(conn)
	_, err = execClient.Initialize(ctx, &vmxpb.InitializeRequest{
		UnixTimestampNanoseconds: time.Now().UnixNano(),
		ClearArpCache:            true,
	})
	if err != nil {
		return status.WrapError(err, "Failed to initialize firecracker VM exec client")
	}

	return nil
}

// initScratchImage creates the empty scratch ext4 disk for the VM.
func (c *FirecrackerContainer) initScratchImage(ctx context.Context, path string) error {
	scratchDiskSizeBytes := ext4.MinDiskImageSizeBytes + minScratchDiskSizeBytes + c.vmConfig.ScratchDiskSizeMb*1e6
	if err := ext4.MakeEmptyImage(ctx, path, scratchDiskSizeBytes); err != nil {
		return err
	}
	if !*enableNBD {
		return nil
	}
	chunkDir := filepath.Join(filepath.Dir(path), scratchDriveID)
	cow, err := c.convertToCOW(ctx, path, chunkDir)
	if err != nil {
		return err
	}
	c.scratchStore = cow
	return nil
}

// createWorkspaceImage creates a new ext4 image from the action working dir.
func (c *FirecrackerContainer) createWorkspaceImage(ctx context.Context, workspaceDir, ext4ImagePath string) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// The existing workspace disk may still be mounted in the VM, so we unlink
	// it then create a new file rather than overwriting the existing file
	// to avoid corruption.
	if err := os.RemoveAll(ext4ImagePath); err != nil {
		return status.WrapError(err, "failed to delete existing workspace disk image")
	}
	if workspaceDir == "" {
		if err := ext4.MakeEmptyImage(ctx, ext4ImagePath, ext4.MinDiskImageSizeBytes); err != nil {
			return err
		}
	} else {
		workspaceSizeBytes, err := disk.DirSize(workspaceDir)
		if err != nil {
			return err
		}
		workspaceDiskSizeBytes := ext4.MinDiskImageSizeBytes + workspaceSizeBytes + workspaceDiskSlackSpaceMB*1e6
		if err := ext4.DirectoryToImage(ctx, workspaceDir, ext4ImagePath, workspaceDiskSizeBytes); err != nil {
			return status.WrapError(err, "failed to convert workspace dir to ext4 image")
		}
	}
	if !*enableNBD {
		return nil
	}
	// If there's already a workspace store created, then we're swapping in a
	// new one; close the old one to prevent further writes to disk.
	if c.workspaceStore != nil {
		c.workspaceStore.Close()
		c.workspaceStore = nil
	}
	// Remove existing workspace chunk dir if it exists.
	chunkDir := filepath.Join(filepath.Dir(ext4ImagePath), workspaceDriveID)
	if err := os.RemoveAll(chunkDir); err != nil {
		return err
	}
	cow, err := c.convertToCOW(ctx, ext4ImagePath, chunkDir)
	if err != nil {
		return err
	}
	c.workspaceStore = cow
	return nil
}

func (c *FirecrackerContainer) convertToCOW(ctx context.Context, filePath, chunkDir string) (*blockio.COWStore, error) {
	start := time.Now()
	if err := os.Mkdir(chunkDir, 0755); err != nil {
		return nil, status.WrapError(err, "make chunk dir")
	}
	cow, err := blockio.ConvertFileToCOW(filePath, cowChunkSizeBytes, chunkDir)
	if err != nil {
		return nil, status.WrapError(err, "convert file to COW")
	}
	// Original non-chunked file is no longer needed.
	if err := os.RemoveAll(filePath); err != nil {
		cow.Close()
		return nil, err
	}
	size, _ := cow.SizeBytes()
	log.CtxInfof(ctx, "COWStore conversion for %q (%d MB) completed in %s", filepath.Base(chunkDir), size/1e6, time.Since(start))
	return cow, nil
}

// hotSwapWorkspace unmounts the workspace drive from a running firecracker
// container, updates the workspace block device to an ext4 image pointed to
// by chrootRelativeImagePath, and re-mounts the drive.
func (c *FirecrackerContainer) hotSwapWorkspace(ctx context.Context, execClient vmxpb.ExecClient) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	if _, err := execClient.UnmountWorkspace(ctx, &vmxpb.UnmountWorkspaceRequest{}); err != nil {
		return status.WrapError(err, "failed to unmount workspace")
	}

	if err := c.createWorkspaceImage(ctx, c.actionWorkingDir, c.workspaceFSPath()); err != nil {
		return status.WrapError(err, "failed to create workspace image")
	}

	if *enableNBD {
		wd, err := nbdserver.NewExt4Device(c.workspaceStore, workspaceDriveID)
		if err != nil {
			return status.WrapError(err, "failed to create new workspace NBD")
		}
		c.nbdServer.SetDevice(wd.Metadata.GetName(), wd)
	} else {
		chrootRelativeImagePath := filepath.Base(c.workspaceFSPath())
		if err := c.machine.UpdateGuestDrive(ctx, workspaceDriveID, chrootRelativeImagePath); err != nil {
			return status.InternalErrorf("error updating workspace drive attached to snapshot: %s", err)
		}
	}

	if _, err := execClient.MountWorkspace(ctx, &vmxpb.MountWorkspaceRequest{}); err != nil {
		return status.WrapError(err, "failed to remount workspace after update")
	}
	return nil
}

func nonCmdExit(ctx context.Context, err error) *interfaces.CommandResult {
	if *dieOnFirecrackerFailure {
		log.CtxFatalf(ctx, "dying on firecracker error: %s", err)
	} else {
		log.CtxErrorf(ctx, "nonCmdExit returning error: %s", err)
	}
	return &interfaces.CommandResult{
		Error:    err,
		ExitCode: -2,
	}
}

func (c *FirecrackerContainer) startedFromSnapshot() bool {
	return c.externalJailerCmd != nil
}

func (c *FirecrackerContainer) newID(ctx context.Context) error {
	vmIdxMu.Lock()
	defer vmIdxMu.Unlock()
	u, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	vmIdx += 1
	log.CtxDebugf(ctx, "Container id changing from %q (%d) to %q (%d)", c.id, c.vmIdx, u.String(), vmIdx)
	c.id = u.String()
	c.vmIdx = vmIdx

	if vmIdx > maxVMSPerHost {
		vmIdx = 0
	}

	return nil
}

// scratchFSPath returns the path to the scratch image in the chroot.
func (c *FirecrackerContainer) scratchFSPath() string {
	return filepath.Join(c.getChroot(), scratchFSName)
}

// workspaceFSPath returns the path to the workspace image in the chroot.
func (c *FirecrackerContainer) workspaceFSPath() string {
	return filepath.Join(c.getChroot(), workspaceFSName)
}

func (c *FirecrackerContainer) getChroot() string {
	// This path matches the path the jailer will use when jailing
	// firecracker. Because we need to copy some (snapshot) files into
	// this directory before starting firecracker, we need to compute
	// the same path.
	return filepath.Join(c.jailerRoot, "firecracker", c.id, "root")
}

// getConfig returns the firecracker config for the current container and given
// filesystem image paths. The image paths are not expected to be in the chroot;
// they will be hardlinked to the chroot when starting the machine (see
// NaiveChrootStrategy).
func (c *FirecrackerContainer) getConfig(ctx context.Context, containerFS, scratchFS, workspaceFS string) (*fcclient.Config, error) {
	bootArgs := "ro console=ttyS0 noapic reboot=k panic=1 pci=off nomodules=1 random.trust_cpu=on i8042.noaux=1 tsc=reliable ipv6.disable=1"
	var netNS string

	if c.vmConfig.EnableNetworking {
		bootArgs += " " + machineIPBootArgs
		netNS = networking.NetNamespacePath(c.id)
	}

	// Pass some flags to the init script.
	if c.vmConfig.DebugMode {
		bootArgs = "-debug_mode " + bootArgs
	}
	if c.vmConfig.EnableNetworking {
		bootArgs = "-set_default_route " + bootArgs
	}
	if c.vmConfig.InitDockerd {
		bootArgs = "-init_dockerd " + bootArgs
	}
	if *enableNBD {
		bootArgs = "-enable_nbd " + bootArgs
	}
	cgroupVersion, err := getCgroupVersion()
	if err != nil {
		return nil, err
	}
	cfg := &fcclient.Config{
		VMID:            c.id,
		SocketPath:      firecrackerSocketPath,
		KernelImagePath: kernelImagePath,
		InitrdPath:      initrdImagePath,
		KernelArgs:      bootArgs,
		ForwardSignals:  make([]os.Signal, 0),
		NetNS:           netNS,
		Seccomp:         fcclient.SeccompConfig{Enabled: true},
		// Note: ordering in this list determines the device lettering
		// (/dev/vda, /dev/vdb, /dev/vdc, ...)
		Drives: []fcmodels.Drive{
			{
				DriveID:      fcclient.String(containerDriveID),
				PathOnHost:   &containerFS,
				IsRootDevice: fcclient.Bool(false),
				IsReadOnly:   fcclient.Bool(true),
			},
		},
		VsockDevices: []fcclient.VsockDevice{
			{Path: firecrackerVSockPath},
		},
		JailerCfg: &fcclient.JailerConfig{
			JailerBinary:   jailerBinPath,
			ChrootBaseDir:  c.jailerRoot,
			ID:             c.id,
			UID:            fcclient.Int(unix.Geteuid()),
			GID:            fcclient.Int(unix.Getegid()),
			NumaNode:       fcclient.Int(0), // TODO(tylerw): randomize this?
			ExecFile:       firecrackerBinPath,
			ChrootStrategy: fcclient.NewNaiveChrootStrategy(kernelImagePath),
			Stdout:         c.vmLogWriter(),
			Stderr:         c.vmLogWriter(),
			CgroupVersion:  cgroupVersion,
		},
		MachineCfg: fcmodels.MachineConfiguration{
			VcpuCount:       fcclient.Int64(c.vmConfig.NumCpus),
			MemSizeMib:      fcclient.Int64(c.vmConfig.MemSizeMb),
			Smt:             fcclient.Bool(false),
			TrackDirtyPages: true,
		},
	}
	if !*enableNBD {
		cfg.Drives = append(cfg.Drives, []fcmodels.Drive{
			{
				DriveID:      fcclient.String(scratchDriveID),
				PathOnHost:   &scratchFS,
				IsRootDevice: fcclient.Bool(false),
				IsReadOnly:   fcclient.Bool(false),
			},
			{
				DriveID:      fcclient.String(workspaceDriveID),
				PathOnHost:   &workspaceFS,
				IsRootDevice: fcclient.Bool(false),
				IsReadOnly:   fcclient.Bool(false),
			},
		}...)
	}

	if c.vmConfig.EnableNetworking {
		cfg.NetworkInterfaces = []fcclient.NetworkInterface{
			{
				StaticConfiguration: &fcclient.StaticNetworkConfiguration{
					HostDevName: tapDeviceName,
					MacAddress:  tapDeviceMac,
				},
			},
		}
	}
	cfg.JailerCfg.Stdout = c.vmLogWriter()
	cfg.JailerCfg.Stderr = c.vmLogWriter()
	if *debugTerminal {
		cfg.JailerCfg.Stdin = os.Stdin
	}
	return cfg, nil
}

func (c *FirecrackerContainer) vmLogWriter() io.Writer {
	if *debugStreamVMLogs || *debugTerminal {
		return io.MultiWriter(c.vmLog, os.Stderr)
	}
	return c.vmLog
}

func copyStaticFiles(ctx context.Context, env environment.Env, workingDir string) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	locateBinariesOnce, _ := locateBinariesOnceMap.LoadOrStore(workingDir, &sync.Once{})
	locateBinariesOnce.(*sync.Once).Do(func() {
		initrdImagePath, locateBinariesError = putFileIntoDir(ctx, env, "enterprise/vmsupport/bin/initrd.cpio", workingDir, 0755)
		if locateBinariesError != nil {
			return
		}
		kernelImagePath, locateBinariesError = putFileIntoDir(ctx, env, "enterprise/vmsupport/bin/vmlinux", workingDir, 0755)
		if locateBinariesError != nil {
			return
		}
		firecrackerBinPath, locateBinariesError = exec.LookPath("firecracker")
		if locateBinariesError != nil {
			return
		}
		jailerBinPath, locateBinariesError = exec.LookPath("jailer")
	})
	return locateBinariesError
}

type loopMount struct {
	loopControlFD *os.File
	imageFD       *os.File
	loopDevIdx    int
	loopFD        *os.File
	mountDir      string
}

func (m *loopMount) Unmount() {
	if m.mountDir != "" {
		if err := syscall.Unmount(m.mountDir, 0); err != nil {
			alert.UnexpectedEvent("firecracker_could_not_unmount_loop_device", err.Error())
		}
		m.mountDir = ""
	}
	if m.loopDevIdx >= 0 && m.loopControlFD != nil {
		err := unix.IoctlSetInt(int(m.loopControlFD.Fd()), unix.LOOP_CTL_REMOVE, m.loopDevIdx)
		if err != nil {
			alert.UnexpectedEvent("firecracker_could_not_release_loop_device", err.Error())
		}
		m.loopDevIdx = -1
	}
	if m.loopFD != nil {
		m.loopFD.Close()
		m.loopFD = nil
	}
	if m.imageFD != nil {
		m.imageFD.Close()
		m.imageFD = nil
	}
	if m.loopControlFD != nil {
		m.loopControlFD.Close()
		m.loopControlFD = nil
	}
}

func mountExt4ImageUsingLoopDevice(imagePath string, mountTarget string) (lm *loopMount, retErr error) {
	loopControlFD, err := os.Open("/dev/loop-control")
	if err != nil {
		return nil, err
	}
	defer loopControlFD.Close()

	m := &loopMount{loopDevIdx: -1}
	defer func() {
		if retErr != nil {
			m.Unmount()
		}
	}()

	imageFD, err := os.Open(imagePath)
	if err != nil {
		return nil, err
	}
	m.imageFD = imageFD

	loopDevIdx, err := unix.IoctlRetInt(int(loopControlFD.Fd()), unix.LOOP_CTL_GET_FREE)
	if err != nil {
		return nil, status.UnknownErrorf("could not allocate loop device: %s", err)
	}
	m.loopDevIdx = loopDevIdx

	loopDevicePath := fmt.Sprintf("/dev/loop%d", loopDevIdx)
	loopFD, err := os.OpenFile(loopDevicePath, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	m.loopFD = loopFD

	if err := unix.IoctlSetInt(int(loopFD.Fd()), unix.LOOP_SET_FD, int(imageFD.Fd())); err != nil {
		return nil, status.UnknownErrorf("could not set loop device FD: %s", err)
	}

	if err := syscall.Mount(loopDevicePath, mountTarget, "ext4", unix.MS_RDONLY, "norecovery"); err != nil {
		return nil, err
	}
	m.mountDir = mountTarget
	return m, nil
}

// copyOutputsToWorkspace copies output files from the workspace filesystem
// image to the local filesystem workdir. It will not overwrite existing files
// and it will skip copying rootfs-overlay files. Callers should ensure that
// data has already been synced to the workspace filesystem and the VM has
// been paused before calling this.
func (c *FirecrackerContainer) copyOutputsToWorkspace(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	if *enableNBD {
		// Reassemble the workspace chunks into a single file.
		// TODO(bduffany): figure out how to avoid doing this work, e.g. by
		// mounting the workspace disk as a local NBD on the host.
		// For now, this approach is acceptable because firecracker workspaces
		// are typically small or empty (e.g. workflows run in $HOME rather than
		// the workspace dir).
		if err := c.workspaceStore.WriteFile(c.workspaceFSPath()); err != nil {
			return err
		}
	}

	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "copyOutputsToWorkspace took %s", time.Since(start))
	}()
	if exists, err := disk.FileExists(ctx, c.workspaceFSPath()); err != nil || !exists {
		return status.FailedPreconditionErrorf("workspacefs path %q not found", c.workspaceFSPath())
	}
	if exists, err := disk.FileExists(ctx, c.actionWorkingDir); err != nil || !exists {
		return status.FailedPreconditionErrorf("actionWorkingDir path %q not found", c.actionWorkingDir)
	}

	wsDir, err := os.MkdirTemp(c.jailerRoot, "workspacefs-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(wsDir) // clean up

	if c.mountWorkspaceFile {
		m, err := mountExt4ImageUsingLoopDevice(c.workspaceFSPath(), wsDir)
		if err != nil {
			log.CtxWarningf(ctx, "could not mount ext4 image: %s", err)
			return err
		}
		defer m.Unmount()
	} else {
		if err := ext4.ImageToDirectory(ctx, c.workspaceFSPath(), wsDir); err != nil {
			return err
		}
	}

	walkErr := fs.WalkDir(os.DirFS(wsDir), ".", func(path string, d fs.DirEntry, err error) error {
		// Skip filesystem layerfs write-layer files.
		if strings.HasPrefix(path, "bbvmroot/") || strings.HasPrefix(path, "bbvmwork/") {
			return nil
		}
		targetLocation := filepath.Join(c.actionWorkingDir, path)
		alreadyExists, err := disk.FileExists(ctx, targetLocation)
		if err != nil {
			return err
		}
		if !alreadyExists {
			if d.IsDir() {
				return disk.EnsureDirectoryExists(filepath.Join(c.actionWorkingDir, path))
			}
			return os.Rename(filepath.Join(wsDir, path), targetLocation)
		}
		return nil
	})
	return walkErr
}

func (c *FirecrackerContainer) setupNetworking(ctx context.Context) error {
	if !c.vmConfig.EnableNetworking {
		return nil
	}
	c.isNetworkSetup = true
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// Setup masquerading on the host if it isn't already.
	var masqueradingErr error
	masqueradingOnce.Do(func() {
		masqueradingErr = networking.EnableMasquerading(ctx)
	})
	if masqueradingErr != nil {
		return masqueradingErr
	}

	if err := networking.CreateNetNamespace(ctx, c.id); err != nil {
		return err
	}
	if err := networking.CreateTapInNamespace(ctx, c.id, tapDeviceName); err != nil {
		return err
	}
	if err := networking.ConfigureTapInNamespace(ctx, c.id, tapDeviceName, tapAddr); err != nil {
		return err
	}
	if err := networking.BringUpTapInNamespace(ctx, c.id, tapDeviceName); err != nil {
		return err
	}
	cleanupVethPair, err := networking.SetupVethPair(ctx, c.id, vmIP, c.vmIdx)
	if err != nil {
		return err
	}
	c.cleanupVethPair = cleanupVethPair
	return nil
}

func (c *FirecrackerContainer) setupUFFDHandler(ctx context.Context) error {
	if c.memoryStore == nil {
		// No memory file to serve over UFFD; do nothing.
		return nil
	}
	if c.uffdHandler != nil {
		return status.InternalErrorf("uffd handler is already running")
	}
	h, err := uffd.NewHandler()
	if err != nil {
		return status.WrapError(err, "create uffd handler")
	}
	c.uffdHandler = h
	sockAbsPath := filepath.Join(c.getChroot(), uffdSockName)
	if err := h.Start(ctx, sockAbsPath, c.memoryStore); err != nil {
		return status.WrapError(err, "start uffd handler")
	}
	return nil
}

func (c *FirecrackerContainer) setupNBDServer(ctx context.Context) error {
	if !*enableNBD {
		return nil
	}
	if c.nbdServer != nil {
		return nil
	}
	if c.workspaceStore == nil {
		return status.FailedPreconditionError("workspaceStore is nil")
	}
	if c.scratchStore == nil {
		return status.FailedPreconditionError("scratchStore is nil")
	}
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	vsockServerPath := vsock.HostListenSocketPath(filepath.Join(c.getChroot(), firecrackerVSockPath), vsock.HostBlockDeviceServerPort)
	if err := os.MkdirAll(filepath.Dir(vsockServerPath), 0755); err != nil {
		return err
	}
	wd, err := nbdserver.NewExt4Device(c.workspaceStore, workspaceDriveID)
	if err != nil {
		return err
	}
	sd, err := nbdserver.NewExt4Device(c.scratchStore, scratchDriveID)
	if err != nil {
		return err
	}

	c.nbdServer, err = nbdserver.New(ctx, c.env, sd, wd)
	if err != nil {
		return err
	}
	lis, err := net.Listen("unix", vsockServerPath)
	if err != nil {
		return err
	}
	if err := c.nbdServer.Start(lis); err != nil {
		return status.InternalErrorf("Could not start VFS server: %s", err)
	}
	return nil
}

func (c *FirecrackerContainer) setupVFSServer(ctx context.Context) error {
	if c.vfsServer != nil {
		return nil
	}
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	vsockServerPath := vsock.HostListenSocketPath(filepath.Join(c.getChroot(), firecrackerVSockPath), vsock.HostVFSServerPort)
	if err := os.MkdirAll(filepath.Dir(vsockServerPath), 0755); err != nil {
		return err
	}
	c.vfsServer = vfs_server.New(c.env, c.actionWorkingDir)
	lis, err := net.Listen("unix", vsockServerPath)
	if err != nil {
		return err
	}
	if err := c.vfsServer.Start(lis); err != nil {
		return status.InternalErrorf("Could not start VFS server: %s", err)
	}
	return nil
}

func (c *FirecrackerContainer) cleanupNetworking(ctx context.Context) error {
	if !c.isNetworkSetup {
		return nil
	}
	c.isNetworkSetup = false

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// These cleanup functions should not depend on each other, so try cleaning
	// up everything and return the last error if there is one.
	var lastErr error
	if c.cleanupVethPair != nil {
		if err := c.cleanupVethPair(ctx); err != nil {
			lastErr = err
		}
	}
	if err := networking.RemoveNetNamespace(ctx, c.id); err != nil {
		lastErr = err
	}
	if err := networking.DeleteRoute(ctx, c.vmIdx); err != nil {
		lastErr = err
	}
	if err := networking.DeleteRuleIfSecondaryNetworkEnabled(ctx, c.vmIdx); err != nil {
		lastErr = err
	}
	return lastErr
}

func (c *FirecrackerContainer) SetTaskFileSystemLayout(fsLayout *container.FileSystemLayout) {
	c.fsLayout = fsLayout
}

// Run the given command within the container and remove the container after
// it is done executing.
//
// It is approximately the same as calling PullImageIfNecessary, Create,
// Exec, then Remove.
func (c *FirecrackerContainer) Run(ctx context.Context, command *repb.Command, actionWorkingDir string, creds container.PullCredentials) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.CtxInfof(ctx, "Run took %s", time.Since(start))
	}()

	// If a snapshot was already loaded, then c.machine will be set, so
	// there's no need to Create the machine.
	if c.machine == nil {
		log.CtxInfof(ctx, "Pulling image %q", c.containerImage)
		if err := container.PullImageIfNecessary(ctx, c.env, c.imageCacheAuth, c, creds, c.containerImage); err != nil {
			return nonCmdExit(ctx, err)
		}

		log.CtxInfof(ctx, "Creating VM.")
		if err := c.Create(ctx, actionWorkingDir); err != nil {
			return nonCmdExit(ctx, err)
		}
	}

	// TODO(bduffany): Remove in the background so that we don't unnecessarily
	// block the client on container removal. Need a way to explicitly wait for
	// the removal result so that we can ensure the removal is complete during
	// executor shutdown and test cleanup.
	defer func() {
		log.CtxInfof(ctx, "Removing VM.")
		ctx, cancel := background.ExtendContextForFinalization(ctx, finalizationTimeout)
		defer cancel()
		if err := c.Remove(ctx); err != nil {
			log.CtxErrorf(ctx, "Failed to remove firecracker VM: %s", err)
		}
	}()

	log.CtxInfof(ctx, "Executing command.")
	cmdResult := c.Exec(ctx, command, &container.Stdio{})
	return cmdResult
}

// Create creates a new VM and starts a top-level process inside it listening
// for commands to execute.
func (c *FirecrackerContainer) Create(ctx context.Context, actionWorkingDir string) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "Create took %s", time.Since(start))
	}()

	c.rmOnce = &sync.Once{}
	c.rmErr = nil

	c.actionWorkingDir = actionWorkingDir

	var err error
	c.tempDir, err = os.MkdirTemp(c.jailerRoot, "fc-container-*")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(c.getChroot(), 0755); err != nil {
		return status.InternalErrorf("failed to create chroot dir: %s", err)
	}

	scratchFSPath := c.scratchFSPath()
	workspaceFSPath := c.workspaceFSPath()

	// When mounting the workspace image directly as a block device (rather than
	// as an NBD), the firecracker go SDK expects the disk images to be outside
	// the chroot, and will move them to the chroot for us. So we place them in
	// a temp dir so that the SDK doesn't complain that the chroot paths already
	// exist when it tries to create them.
	if !*enableNBD {
		scratchFSPath = filepath.Join(c.tempDir, scratchFSName)
		workspaceFSPath = filepath.Join(c.tempDir, workspaceFSName)
	}
	if err := c.initScratchImage(ctx, scratchFSPath); err != nil {
		return status.WrapError(err, "create initial scratch image")
	}
	// Create an empty workspace image initially; the real workspace will be
	// hot-swapped just before running each command in order to ensure that the
	// workspace contents are up to date.
	if err := c.createWorkspaceImage(ctx, "" /*=workspaceDir*/, workspaceFSPath); err != nil {
		return err
	}
	log.CtxDebugf(ctx, "Scratch and workspace disk images written to %q", c.tempDir)
	log.CtxDebugf(ctx, "Using container image at %q", c.containerFSPath)
	log.CtxDebugf(ctx, "getChroot() is %q", c.getChroot())
	fcCfg, err := c.getConfig(ctx, c.containerFSPath, scratchFSPath, workspaceFSPath)
	if err != nil {
		return err
	}

	if err := c.setupNetworking(ctx); err != nil {
		return err
	}

	if err := c.setupVFSServer(ctx); err != nil {
		return err
	}

	if err := c.setupNBDServer(ctx); err != nil {
		return status.WrapError(err, "failed to init nbd server")
	}

	vmCtx, cancel := context.WithCancel(context.Background())
	c.cancelVmCtx = cancel

	machineOpts := []fcclient.Opt{
		fcclient.WithLogger(getLogrusLogger()),
	}

	m, err := fcclient.NewMachine(vmCtx, *fcCfg, machineOpts...)
	if err != nil {
		return status.InternalErrorf("Failed creating machine: %s", err)
	}
	log.CtxDebugf(ctx, "Command: %v", reflect.Indirect(reflect.Indirect(reflect.ValueOf(m)).FieldByName("cmd")).FieldByName("Args"))

	err = (func() error {
		_, span := tracing.StartSpan(ctx)
		defer span.End()
		span.SetName("StartMachine")
		return m.Start(vmCtx)
	})()
	if err != nil {
		return status.InternalErrorf("Failed starting machine: %s", err)
	}
	c.machine = m
	return nil
}

func (c *FirecrackerContainer) SendExecRequestToGuest(ctx context.Context, cmd *repb.Command, workDir string, stdio *container.Stdio) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// TODO(bduffany): Reuse connection from Unpause(), if applicable
	conn, err := c.dialVMExecServer(ctx)
	if err != nil {
		return commandutil.ErrorResult(status.InternalErrorf("Firecracker exec failed: failed to dial VM exec port: %s", err))
	}
	defer conn.Close()

	client := vmxpb.NewExecClient(conn)

	defer container.Metrics.Unregister(c)
	statsListener := func(stats *repb.UsageStats) {
		container.Metrics.Observe(c, stats)
	}
	log.CtxDebug(ctx, "Starting Execute stream.")
	return vmexec_client.Execute(ctx, client, cmd, workDir, c.user, statsListener, stdio)
}

func (c *FirecrackerContainer) dialVMExecServer(ctx context.Context) (*grpc.ClientConn, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, vSocketDialTimeout)
	defer cancel()

	vsockPath := filepath.Join(c.getChroot(), firecrackerVSockPath)
	conn, err := vsock.SimpleGRPCDial(ctx, vsockPath, vsock.VMExecPort)
	if err != nil {
		if err == context.DeadlineExceeded {
			// If we never connected to the VM exec port, it's likely because of a
			// fatal error in the init binary. Search for the logged error and return
			// it if found.
			if err := c.parseFatalInitError(); err != nil {
				return nil, err
			}
			// Intentionally not returning DeadlineExceededError here since it is not
			// a Bazel-retryable error, but this particular timeout should be retryable.
		}
		return nil, status.InternalErrorf("Failed to connect to firecracker VM exec server: %s", err)
	}
	return conn, nil
}

func (c *FirecrackerContainer) SendPrepareFileSystemRequestToGuest(ctx context.Context, req *vmfspb.PrepareRequest) (*vmfspb.PrepareResponse, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	p, err := vfs_server.NewCASLazyFileProvider(c.env, ctx, c.fsLayout.RemoteInstanceName, c.fsLayout.DigestFunction, c.fsLayout.Inputs)
	if err != nil {
		return nil, err
	}
	if err := c.vfsServer.Prepare(p); err != nil {
		return nil, err
	}

	dialCtx, cancel := context.WithTimeout(ctx, vSocketDialTimeout)
	defer cancel()

	vsockPath := filepath.Join(c.getChroot(), firecrackerVSockPath)
	conn, err := vsock.SimpleGRPCDial(dialCtx, vsockPath, vsock.VMVFSPort)
	if err != nil {
		return nil, err
	}
	client := vmfspb.NewFileSystemClient(conn)
	rsp, err := client.Prepare(ctx, req)
	if err != nil {
		return nil, err
	}
	return rsp, err
}

// Exec runs a command inside a container, with the same working dir set when
// creating the container.
// If stdin is non-nil, the contents of stdin reader will be piped to the stdin of
// the executed process.
// If stdout is non-nil, the stdout of the executed process will be written to the
// stdout writer.
func (c *FirecrackerContainer) Exec(ctx context.Context, cmd *repb.Command, stdio *container.Stdio) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "Exec took %s", time.Since(start))
	}()

	result := &interfaces.CommandResult{ExitCode: commandutil.NoExitCode}

	if c.fsLayout == nil {
		if err := c.syncWorkspace(ctx); err != nil {
			result.Error = status.WrapError(err, "failed to sync workspace")
			return result
		}
	} else {
		req := &vmfspb.PrepareRequest{}
		_, err := c.SendPrepareFileSystemRequestToGuest(ctx, req)
		if err != nil {
			result.Error = err
			return result
		}
	}

	workDir := "/workspace/"
	if c.fsLayout != nil {
		workDir = guestVFSMountDir
	}

	defer func() {
		// TODO(bduffany): Figure out a good way to surface this in the command result.
		if err := c.parseOOMError(); err != nil {
			log.CtxWarningf(ctx, "OOM error occurred during task execution: %s", err)
		}
		if err := c.parseSegFault(result); err != nil {
			log.CtxWarningf(ctx, "Segfault occurred during task execution (recycled=%v) : %s", c.recycled, err)
		}
	}()

	execDone := make(chan struct{})
	go func() {
		t := time.NewTimer(1 * time.Hour)
		defer t.Stop()
		select {
		case <-execDone:
			return
		case <-t.C:
			log.CtxWarningf(ctx, "execution possibly stuck. vm log:\n%s", string(c.vmLog.Tail()))
			return
		}
	}()

	result = c.SendExecRequestToGuest(ctx, cmd, workDir, stdio)
	close(execDone)

	ctx, cancel := background.ExtendContextForFinalization(ctx, finalizationTimeout)
	defer cancel()

	// If FUSE is enabled then outputs are already in the workspace.
	if c.fsLayout == nil {
		// Command was successful, let's unpack the files back to our
		// workspace directory now.
		if err := c.pauseVM(ctx); err != nil {
			result.Error = status.InternalErrorf("error pausing VM: %s", err)
			return result
		}

		copyOutputsErr := c.copyOutputsToWorkspace(ctx)
		if err := c.machine.ResumeVM(ctx); err != nil {
			result.Error = status.InternalErrorf("error resuming VM: %s", err)
			return result
		}

		if copyOutputsErr != nil {
			result.Error = status.WrapError(copyOutputsErr, "failed to copy action outputs from VM workspace")
			return result
		}
	}

	return result
}

func (c *FirecrackerContainer) IsImageCached(ctx context.Context) (bool, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	diskImagePath, err := containerutil.CachedDiskImagePath(ctx, c.jailerRoot, c.containerImage)
	if err != nil {
		return false, err
	}
	if diskImagePath != "" {
		c.containerFSPath = diskImagePath
	}
	return diskImagePath != "", nil
}

// PullImage pulls the container image from the remote. It always
// re-authenticates the request, but may serve the image from a local cache
// in order to avoid re-downloading the image.
func (c *FirecrackerContainer) PullImage(ctx context.Context, creds container.PullCredentials) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "PullImage took %s", time.Since(start))
	}()
	if c.containerFSPath != "" {
		return nil
	}
	containerFSPath, err := containerutil.CreateDiskImage(ctx, c.dockerClient, c.jailerRoot, c.containerImage, creds)
	if err != nil {
		return err
	}
	c.containerFSPath = containerFSPath

	// TODO(tylerw): support loading a VM from snapshot instead for speed.
	return nil
}

// Remove kills any processes currently running inside the container and
// removes any resources associated with the container itself.
func (c *FirecrackerContainer) Remove(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "Remove took %s", time.Since(start))
	}()

	if c.rmOnce == nil {
		// Container was probably loaded from persisted state and never
		// unpaused; ignore this type of error.
		return nil
	}
	c.rmOnce.Do(func() {
		c.rmErr = c.remove(ctx)
	})
	return c.rmErr
}

func (c *FirecrackerContainer) remove(ctx context.Context) error {
	// Make sure we don't get stuck for too long trying to remove.
	ctx, cancel := context.WithTimeout(ctx, finalizationTimeout)
	defer cancel()

	defer c.cancelVmCtx()

	var lastErr error

	if c.machine != nil {
		// Note: we don't attempt any kind of clean shutdown here, because at this
		// point either (a) the VM should be paused and we should have already taken
		// a snapshot, or (b) we are just calling Remove() to force a cleanup
		// regardless of VM state (e.g. executor shutdown).

		if err := c.machine.StopVMM(); err != nil {
			log.CtxErrorf(ctx, "Error stopping VMM: %s", err)
			lastErr = err
		}
		c.machine = nil
	}
	if err := c.cleanupNetworking(ctx); err != nil {
		log.CtxErrorf(ctx, "Error cleaning up networking: %s", err)
		lastErr = err
	}
	if c.tempDir != "" {
		if err := os.RemoveAll(c.tempDir); err != nil {
			log.CtxErrorf(ctx, "Error removing workspace fs: %s", err)
			lastErr = err
		}
	}
	if err := os.RemoveAll(filepath.Dir(c.getChroot())); err != nil {
		log.CtxErrorf(ctx, "Error removing chroot: %s", err)
		lastErr = err
	}
	if c.vfsServer != nil {
		c.vfsServer.Stop()
		c.vfsServer = nil
	}
	if c.nbdServer != nil {
		c.nbdServer.Stop()
		c.nbdServer = nil
	}
	if c.workspaceStore != nil {
		c.workspaceStore.Close()
		c.workspaceStore = nil
	}
	if c.scratchStore != nil {
		c.scratchStore.Close()
		c.scratchStore = nil
	}
	if c.uffdHandler != nil {
		c.uffdHandler.Stop()
		c.uffdHandler = nil
	}
	if c.memoryStore != nil {
		c.memoryStore.Close()
		c.memoryStore = nil
	}
	return lastErr
}

// Pause freezes a container so that it no longer consumes CPU resources.
func (c *FirecrackerContainer) Pause(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "Pause took %s", time.Since(start))
	}()
	if err := c.SaveSnapshot(ctx); err != nil {
		log.CtxErrorf(ctx, "Error saving snapshot: %s", err)
		return err
	}

	if err := c.Remove(ctx); err != nil {
		log.CtxErrorf(ctx, "Error cleaning up after pause: %s", err)
		return err
	}
	return nil
}

// Unpause un-freezes a container so that it can be used to execute commands.
func (c *FirecrackerContainer) Unpause(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "Unpause took %s", time.Since(start))
	}()

	c.recycled = true

	// Don't hot-swap the workspace into the VM since we haven't yet downloaded inputs.
	return c.LoadSnapshot(ctx)
}

// syncWorkspace creates a new disk image from the given working directory
// and hot-swaps the currently mounted workspace drive in the guest.
//
// This is intended to be called just before Exec, so that the inputs to
// the executed action will be made available to the VM.
func (c *FirecrackerContainer) syncWorkspace(ctx context.Context) error {
	// TODO(bduffany): reuse the connection created in Unpause(), if applicable
	conn, err := c.dialVMExecServer(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	execClient := vmxpb.NewExecClient(conn)

	return c.hotSwapWorkspace(ctx, execClient)
}

// Wait waits until the underlying VM exits. It returns an error if one is
// encountered while waiting.
func (c *FirecrackerContainer) Wait(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	if c.externalJailerCmd != nil {
		return c.externalJailerCmd.Wait()
	}
	return c.machine.Wait(ctx)
}

func (c *FirecrackerContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
	// Note: We return a non-nil value here since paused Firecracker containers
	// only exist on disk and legitimately consume 0 memory / CPU resources when
	// paused.
	return &repb.UsageStats{}, nil
}

// parseFatalInitError looks for a fatal error logged by the init binary, and
// returns an InternalError with the fatal error message if one is found;
// otherwise it returns nil.
func (c *FirecrackerContainer) parseFatalInitError() error {
	tail := string(c.vmLog.Tail())
	if !strings.Contains(tail, fatalInitLogPrefix) {
		return nil
	}
	// Logs contain "\r\n"; convert these to universal line endings.
	tail = strings.ReplaceAll(tail, "\r\n", "\n")
	lines := strings.Split(tail, "\n")
	for _, line := range lines {
		if m := fatalErrPattern.FindStringSubmatch(line); len(m) >= 1 {
			return status.InternalErrorf("Firecracker VM crashed: %s", m[1])
		}
	}
	return nil
}

// parseOOMError looks for oom-kill entries in the kernel logs and returns an
// error if found.
func (c *FirecrackerContainer) parseOOMError() error {
	tail := string(c.vmLog.Tail())
	if !strings.Contains(tail, "oom-kill:") {
		return nil
	}
	// Logs contain "\r\n"; convert these to universal line endings.
	tail = strings.ReplaceAll(tail, "\r\n", "\n")
	lines := strings.Split(tail, "\n")
	oomLines := ""
	for _, line := range lines {
		if strings.Contains(line, "oom-kill:") || strings.Contains(line, "Out of memory: Killed process") {
			oomLines += line + "\n"
		}
	}
	return status.ResourceExhaustedErrorf("some processes ran out of memory, and were killed:\n%s", oomLines)
}

// parseSegFault looks for segfaults in the kernel logs and returns an error if found.
func (c *FirecrackerContainer) parseSegFault(cmdResult *interfaces.CommandResult) error {
	if !strings.Contains(string(cmdResult.Stderr), "SIGSEGV") {
		return nil
	}
	tail := string(c.vmLog.Tail())
	// Logs contain "\r\n"; convert these to universal line endings.
	tail = strings.ReplaceAll(tail, "\r\n", "\n")
	return status.InternalErrorf("process hit a segfault:\n%s", tail)
}

// VMLog retains the tail of the VM log.
type VMLog struct {
	buf *circbuf.Buffer
	mu  sync.RWMutex
}

func NewVMLog(size int64) (*VMLog, error) {
	buf, err := circbuf.NewBuffer(size)
	if err != nil {
		return nil, err
	}
	return &VMLog{buf: buf}, nil
}

func (log *VMLog) Write(p []byte) (int, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.buf.Write(p)
}

// Tail returns the tail of the log.
func (log *VMLog) Tail() []byte {
	log.mu.RLock()
	defer log.mu.RUnlock()
	b := log.buf.Bytes()
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

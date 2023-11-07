package firecracker

import (
	"context"
	"crypto/sha256"
	"errors"
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

	_ "embed"

	"github.com/armon/circbuf"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaputil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/uffd"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vbd"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vmexec_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ociconv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/firecracker-microvm/firecracker-go-sdk/client/operations"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	vmsupport_bundle "github.com/buildbuddy-io/buildbuddy/enterprise/vmsupport"
	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	hlpb "github.com/buildbuddy-io/buildbuddy/proto/health"
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
var enableVBD = flag.Bool("executor.firecracker_enable_vbd", false, "Enables the FUSE-based virtual block device interface for block devices.")
var EnableRootfs = flag.Bool("executor.firecracker_enable_merged_rootfs", false, "Merges the containerfs and scratchfs into a single rootfs, removing the need to use overlayfs for the guest's root filesystem. Requires NBD to also be enabled.")
var enableUFFD = flag.Bool("executor.firecracker_enable_uffd", false, "Enables userfaultfd for firecracker VMs.")
var dieOnFirecrackerFailure = flag.Bool("executor.die_on_firecracker_failure", false, "Makes the host executor process die if any command orchestrating or running Firecracker fails. Useful for capturing failures preemptively. WARNING: using this option MAY leave the host machine in an unhealthy state on Firecracker failure; some post-hoc cleanup may be necessary.")
var workspaceDiskSlackSpaceMB = flag.Int64("executor.firecracker_workspace_disk_slack_space_mb", 2_000, "Extra space to allocate to firecracker workspace disks, in megabytes. ** Experimental **")
var healthCheckInterval = flag.Duration("executor.firecracker_health_check_interval", 10*time.Second, "How often to run VM health checks while tasks are executing.")
var healthCheckTimeout = flag.Duration("executor.firecracker_health_check_timeout", 30*time.Second, "Timeout for VM health check requests.")

//go:embed guest_api_hash.sha256
var GuestAPIHash string

const (
	// goinitVersion determines the version of the go init binary that this
	// executor supports. This version needs to be bumped when making
	// incompatible changes to the goinit binary. This includes but is not
	// limited to:
	//
	// - Adding new platform prop based features that depend on goinit support,
	//   such as dockerd init options.
	// - Adding new features to the vmexec.Exec service.
	//
	// We manually maintain this version instead of using a hash of the goinit
	// binary because we expect the hash to be unstable, likely changing with
	// each executor release.
	//
	// NOTE: this is part of the snapshot cache key, so bumping this version
	// will make existing cached snapshots unusable.
	GuestAPIVersion = "2"

	// How long to wait for the VMM to listen on the firecracker socket.
	firecrackerSocketWaitTimeout = 3 * time.Second

	// How long to wait when dialing the vmexec server inside the VM.
	vSocketDialTimeout = 30 * time.Second

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

	// VBD mount path suffix, which is appended to the drive ID.
	// Example:
	//	"{chrootPath}/workspacefs.vbd/file"
	vbdMountDirSuffix = ".vbd"

	// The workspacefs image name and drive ID.
	workspaceFSName  = "workspacefs.ext4"
	workspaceDriveID = "workspacefs"

	// The scratchfs image name and drive ID.
	scratchFSName  = "scratchfs.ext4"
	scratchDriveID = "scratchfs"
	// The rootfs image name and drive ID (merged containerfs + scratchfs).
	rootFSName  = "rootfs.ext4"
	rootDriveID = "rootfs"
	// minScratchDiskSizeBytes is the minimum size needed for the scratch disk.
	// This is needed because the init binary needs some space to copy files around.
	minScratchDiskSizeBytes = 64e6

	// Chunk size to use when creating COW images from files.
	cowChunkSizeInPages = 1000

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
	masqueradingOnce sync.Once
	masqueradingErr  error

	vmIdx   int
	vmIdxMu sync.Mutex

	fatalErrPattern = regexp.MustCompile(`\b` + fatalInitLogPrefix + `(.*)`)
)

func init() {
	// Configure firecracker request timeout (default: 500ms).
	//
	// We're increasing it from the default here since we do some remote reads
	// during ResumeVM to handle page faults with UFFD.
	//
	// This 30s timeout was determined as the time it takes to load a bb repo
	// snapshot in dev with a cold filecache (10s), times 3 to account for tail
	// latency + larger VMs that could take longer to resume.
	os.Setenv("FIRECRACKER_GO_SDK_REQUEST_TIMEOUT_MILLISECONDS", fmt.Sprint(30_000))
}

func openFile(ctx context.Context, fsys fs.FS, fileName string) (io.ReadCloser, error) {
	// If the file exists on the filesystem, use that.
	if path, err := exec.LookPath(fileName); err == nil {
		log.CtxDebugf(ctx, "Located %q at %s", fileName, path)
		return os.Open(path)
	}

	// Otherwise try to find it in the provided fs
	return fsys.Open(fileName)
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
func putFileIntoDir(ctx context.Context, fsys fs.FS, fileName, destDir string, mode fs.FileMode) (string, error) {
	f, err := openFile(ctx, fsys, fileName)
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
	f, err = openFile(ctx, fsys, fileName)
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

// ExecutorConfig contains configuration that is computed once at executor
// startup and applies to all VMs created by the executor.
type ExecutorConfig struct {
	JailerRoot string

	InitrdImagePath       string
	KernelImagePath       string
	FirecrackerBinaryPath string
	JailerBinaryPath      string

	KernelVersion      string
	FirecrackerVersion string
	GuestAPIVersion    string
}

// GetExecutorConfig computes the ExecutorConfig for this executor instance.
//
// WARNING: The given buildRootDir will be used as the jailer root dir. Because
// of the limitation on the length of unix sock file paths (103), this directory
// path needs to be short. Specifically, a full sock path will look like:
// /tmp/firecracker/217d4de0-4b28-401b-891b-18e087718ad1/root/run/fc.sock
// everything after "/tmp" is 65 characters, so 38 are left for the jailerRoot.
func GetExecutorConfig(ctx context.Context, buildRootDir string) (*ExecutorConfig, error) {
	bundle := vmsupport_bundle.Get()
	initrdPath, err := putFileIntoDir(ctx, bundle, "enterprise/vmsupport/bin/initrd.cpio", buildRootDir, 0755)
	if err != nil {
		return nil, err
	}
	kernelPath, err := putFileIntoDir(ctx, bundle, "enterprise/vmsupport/bin/vmlinux", buildRootDir, 0755)
	if err != nil {
		return nil, err
	}
	// TODO: when running as root, these should come from the bundle instead of
	// $PATH, since we don't need to rely on the user having configured special
	// perms on these binaries.
	firecrackerPath, err := exec.LookPath("firecracker")
	if err != nil {
		return nil, err
	}
	jailerPath, err := exec.LookPath("jailer")
	if err != nil {
		return nil, err
	}
	kernelDigest, err := digest.ComputeForFile(kernelPath, repb.DigestFunction_SHA256)
	if err != nil {
		return nil, err
	}
	firecrackerDigest, err := digest.ComputeForFile(firecrackerPath, repb.DigestFunction_SHA256)
	if err != nil {
		return nil, err
	}
	return &ExecutorConfig{
		// For now just use the build root dir as the jailer root dir, since
		// these are guaranteed to be on the same FS.
		JailerRoot:            buildRootDir,
		InitrdImagePath:       initrdPath,
		KernelImagePath:       kernelPath,
		FirecrackerBinaryPath: firecrackerPath,
		JailerBinaryPath:      jailerPath,
		KernelVersion:         kernelDigest.GetHash(),
		FirecrackerVersion:    firecrackerDigest.GetHash(),
		GuestAPIVersion:       GuestAPIVersion,
	}, nil
}

type Provider struct {
	env            environment.Env
	dockerClient   *dockerclient.Client
	executorConfig *ExecutorConfig
}

func NewProvider(env environment.Env, hostBuildRoot string) (*Provider, error) {
	// Best effort trying to initialize the docker client. If it fails, we'll
	// simply fall back to use skopeo to download and cache container images.
	client, err := docker.NewClient()
	if err != nil {
		client = nil
	}

	executorConfig, err := GetExecutorConfig(env.GetServerContext(), hostBuildRoot)
	if err != nil {
		return nil, err
	}

	return &Provider{
		env:            env,
		dockerClient:   client,
		executorConfig: executorConfig,
	}, nil
}

func (p *Provider) New(ctx context.Context, props *platform.Properties, task *repb.ScheduledTask, state *rnpb.RunnerState, workingDir string) (container.CommandContainer, error) {
	var vmConfig *fcpb.VMConfiguration
	savedState := state.GetContainerState().GetFirecrackerState()
	if savedState == nil {
		sizeEstimate := task.GetSchedulingMetadata().GetTaskSize()
		vmConfig = &fcpb.VMConfiguration{
			NumCpus:           int64(math.Max(1.0, float64(sizeEstimate.GetEstimatedMilliCpu())/1000)),
			MemSizeMb:         int64(math.Max(1.0, float64(sizeEstimate.GetEstimatedMemoryBytes())/1e6)),
			ScratchDiskSizeMb: int64(float64(sizeEstimate.GetEstimatedFreeDiskBytes()) / 1e6),
			EnableNetworking:  true,
			InitDockerd:       props.InitDockerd,
			EnableDockerdTcp:  props.EnableDockerdTCP,
		}
	} else {
		vmConfig = state.GetContainerState().GetFirecrackerState().GetVmConfiguration()
	}
	opts := ContainerOpts{
		VMConfiguration:        vmConfig,
		SavedState:             savedState,
		ContainerImage:         props.ContainerImage,
		User:                   props.DockerUser,
		DockerClient:           p.dockerClient,
		ActionWorkingDirectory: workingDir,
		ExecutorConfig:         p.executorConfig,
	}
	c, err := NewContainer(ctx, p.env, task.GetExecutionTask(), opts)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// FirecrackerContainer executes commands inside of a firecracker VM.
type FirecrackerContainer struct {
	id     string // a random GUID, unique per-run of firecracker
	vmIdx  int    // the index of this vm on the host machine
	loader *snaploader.FileCacheLoader

	vmConfig         *fcpb.VMConfiguration
	containerImage   string // the OCI container image. ex "alpine:latest"
	actionWorkingDir string // the action directory with inputs / outputs
	pulled           bool   // whether the container ext4 image has been pulled
	user             string // user to execute all commands as

	rmOnce *sync.Once
	rmErr  error

	// Whether networking has been set up (and needs to be cleaned up).
	isNetworkSetup bool

	// Whether the VM was recycled.
	recycled bool

	// The following snapshot-related fields are initialized in NewContainer
	// based on the incoming task. If there is a new task, you may want to call
	// NewContainer again rather than directly unpausing a pre-existing container,
	// to make sure these fields are updated and the best snapshot match is used
	snapshotKey             *fcpb.SnapshotKey
	createFromSnapshot      bool
	supportsRemoteSnapshots bool

	// When the VM was initialized (i.e. created or unpaused) for the command
	// it is currently executing
	//
	// This can be used to understand the total time it takes to execute a task,
	// including VM startup time
	currentTaskInitTimeUsec int64

	executorConfig *ExecutorConfig
	// dockerClient is used to optimize image pulls by reusing image layers from
	// the Docker cache as well as deduping multiple requests for the same image.
	dockerClient *dockerclient.Client

	// when VFS is enabled, this contains the layout for the next execution
	fsLayout  *container.FileSystemLayout
	vfsServer *vfs_server.Server

	scratchStore   *copy_on_write.COWStore
	scratchVBD     *vbd.FS
	rootStore      *copy_on_write.COWStore
	rootVBD        *vbd.FS
	workspaceStore *copy_on_write.COWStore
	workspaceVBD   *vbd.FS

	uffdHandler *uffd.Handler
	memoryStore *copy_on_write.COWStore

	jailerRoot         string            // the root dir the jailer will work in
	machine            *fcclient.Machine // the firecracker machine object.
	vmLog              *VMLog
	env                environment.Env
	mountWorkspaceFile bool

	cleanupVethPair func(context.Context) error
	vmCtx           context.Context
	// cancelVmCtx cancels the Machine context, stopping the VMM if it hasn't
	// already been stopped manually.
	cancelVmCtx context.CancelCauseFunc
}

func NewContainer(ctx context.Context, env environment.Env, task *repb.ExecutionTask, opts ContainerOpts) (*FirecrackerContainer, error) {
	if *snaputil.EnableLocalSnapshotSharing && !(*enableVBD && *enableUFFD) {
		return nil, status.FailedPreconditionError("executor configuration error: local snapshot sharing requires VBD and UFFD to be enabled")
	}
	if *EnableRootfs && !*enableVBD {
		return nil, status.FailedPreconditionError("executor configuration error: merged rootfs requires VBD to be enabled")
	}

	if opts.VMConfiguration == nil {
		return nil, status.InvalidArgumentError("missing VMConfiguration")
	}

	vmLog, err := NewVMLog(vmLogTailBufSize)
	if err != nil {
		return nil, err
	}
	if opts.ExecutorConfig == nil {
		return nil, status.InvalidArgumentError("missing opts.ExecutorConfig")
	}
	if len(opts.ExecutorConfig.JailerRoot) > 38 {
		return nil, status.InvalidArgumentErrorf("build root dir %q length %d exceeds 38 character limit", opts.ExecutorConfig.JailerRoot, len(opts.ExecutorConfig.JailerRoot))
	}
	if err := disk.EnsureDirectoryExists(opts.ExecutorConfig.JailerRoot); err != nil {
		return nil, err
	}

	loader, err := snaploader.New(env)
	if err != nil {
		return nil, err
	}

	c := &FirecrackerContainer{
		vmConfig:           proto.Clone(opts.VMConfiguration).(*fcpb.VMConfiguration),
		executorConfig:     opts.ExecutorConfig,
		jailerRoot:         opts.ExecutorConfig.JailerRoot,
		dockerClient:       opts.DockerClient,
		containerImage:     opts.ContainerImage,
		user:               opts.User,
		actionWorkingDir:   opts.ActionWorkingDirectory,
		env:                env,
		loader:             loader,
		vmLog:              vmLog,
		mountWorkspaceFile: *firecrackerMountWorkspaceFile,
		cancelVmCtx:        func(err error) {},
	}

	c.vmConfig.KernelVersion = c.executorConfig.KernelVersion
	c.vmConfig.FirecrackerVersion = c.executorConfig.FirecrackerVersion
	c.vmConfig.GuestApiVersion = c.executorConfig.GuestAPIVersion

	if opts.ForceVMIdx != 0 {
		c.vmIdx = opts.ForceVMIdx
	}

	isWorkflow := platform.FindValue(task.GetCommand().GetPlatform(), platform.WorkflowIDPropertyName) != ""
	c.supportsRemoteSnapshots = isWorkflow && *snaputil.EnableRemoteSnapshotSharing

	if opts.SavedState == nil {
		c.vmConfig.DebugMode = *debugTerminal

		if err := c.newID(ctx); err != nil {
			return nil, err
		}
		cd, err := digest.ComputeForMessage(c.vmConfig, repb.DigestFunction_SHA256)
		if err != nil {
			return nil, err
		}

		// TODO(Maggie): Once local snapshot sharing is stable, remove runner ID
		// from the snapshot key
		runnerID := c.id
		if *snaputil.EnableLocalSnapshotSharing {
			runnerID = ""
		}
		c.snapshotKey, err = snaploader.NewKey(task, cd.GetHash(), runnerID)
		if err != nil {
			return nil, err
		}
		// If recycling is enabled and a snapshot exists, then when calling
		// Create(), load the snapshot instead of creating a new VM.

		recyclingEnabled := platform.IsTrue(platform.FindValue(task.GetCommand().GetPlatform(), platform.RecycleRunnerPropertyName))
		if recyclingEnabled && *snaputil.EnableLocalSnapshotSharing {
			_, err := loader.GetSnapshot(ctx, c.snapshotKey, c.supportsRemoteSnapshots)
			c.createFromSnapshot = (err == nil)
		}
	} else {
		c.snapshotKey = opts.SavedState.GetSnapshotKey()

		// TODO(bduffany): add version info to snapshots. For example, if a
		// breaking change is made to the vmexec API, the executor should not
		// attempt to connect to snapshots that were created before the change.
	}

	return c, nil
}

// MergeDiffSnapshot reads from diffSnapshotPath and writes all non-zero blocks
// into the baseSnapshotPath file or the baseSnapshotStore if non-nil.
func MergeDiffSnapshot(ctx context.Context, baseSnapshotPath string, baseSnapshotStore *copy_on_write.COWStore, diffSnapshotPath string, concurrency int, bufSize int) error {
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
	perThreadBytes = alignToMultiple(perThreadBytes, int64(bufSize))
	if *enableUFFD {
		// Ensure goroutines don't cross chunk boundaries and cause race conditions when writing
		perThreadBytes = alignToMultiple(perThreadBytes, cowChunkSizeBytes())
	}
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
					return context.Cause(ctx)
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
				if err != nil && err != io.EOF {
					return err
				}
				if _, err := out.WriteAt(buf[:n], offset); err != nil {
					return err
				}
				offset += int64(n)
				if err == io.EOF {
					break
				}
			}
			return nil
		})
	}

	return eg.Wait()
}

// alignToMultiple aligns the value n to a multiple of `multiple`
// It will round up if necessary
func alignToMultiple(n int64, multiple int64) int64 {
	remainder := n % multiple

	// If remainder is zero, n is already aligned
	if remainder == 0 {
		return n
	}
	// Otherwise, adjust n to the next multiple of size
	return n + multiple - remainder
}

func (c *FirecrackerContainer) SnapshotKey() *fcpb.SnapshotKey {
	return proto.Clone(c.snapshotKey).(*fcpb.SnapshotKey)
}

// State returns the container state to be persisted to disk so that this
// container can be reconstructed from the state on disk after an executor
// restart.
func (c *FirecrackerContainer) State(ctx context.Context) (*rnpb.ContainerState, error) {
	if *snaputil.EnableLocalSnapshotSharing {
		// When local snapshot sharing is enabled, don't bother explicitly
		// persisting container state across reboots. Instead we can
		// deterministically match tasks to cached snapshots just based on the
		// task's snapshot key.
		return nil, status.UnimplementedError("not implemented")
	}

	state := &rnpb.ContainerState{
		IsolationType: string(platform.FirecrackerContainerType),
		FirecrackerState: &rnpb.FirecrackerState{
			VmConfiguration: c.vmConfig,
			SnapshotKey:     c.snapshotKey,
		},
	}
	return state, nil
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

func (c *FirecrackerContainer) saveSnapshot(ctx context.Context, snapshotDetails *snapshotDetails) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "SaveSnapshot took %s", time.Since(start))
	}()

	baseMemSnapshotPath := filepath.Join(c.getChroot(), fullMemSnapshotName)
	memSnapshotPath := filepath.Join(c.getChroot(), snapshotDetails.memSnapshotName)

	if snapshotDetails.snapshotType == diffSnapshotType {
		mergeStart := time.Now()
		if err := MergeDiffSnapshot(ctx, baseMemSnapshotPath, c.memoryStore, memSnapshotPath, mergeDiffSnapshotConcurrency, mergeDiffSnapshotBlockSize); err != nil {
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
		VMStateSnapshotPath: filepath.Join(c.getChroot(), snapshotDetails.vmStateSnapshotName),
		KernelImagePath:     c.executorConfig.KernelImagePath,
		InitrdImagePath:     c.executorConfig.InitrdImagePath,
		ChunkedFiles:        map[string]*copy_on_write.COWStore{},
		Recycled:            c.recycled,
		Remote:              c.supportsRemoteSnapshots,
	}
	if *enableVBD {
		if c.rootStore != nil {
			opts.ChunkedFiles[rootDriveID] = c.rootStore
		} else {
			opts.ContainerFSPath = filepath.Join(c.getChroot(), containerFSName)
			opts.ChunkedFiles[scratchDriveID] = c.scratchStore
		}
		opts.ChunkedFiles[workspaceDriveID] = c.workspaceStore
	} else {
		opts.ContainerFSPath = filepath.Join(c.getChroot(), containerFSName)
		opts.ScratchFSPath = filepath.Join(c.getChroot(), scratchFSName)
		opts.WorkspaceFSPath = filepath.Join(c.getChroot(), workspaceFSName)
	}
	if *enableUFFD {
		opts.ChunkedFiles[memoryChunkDirName] = c.memoryStore
	} else {
		opts.MemSnapshotPath = memSnapshotPath
	}

	snaploaderStart := time.Now()
	if err := c.loader.CacheSnapshot(ctx, c.snapshotKey, opts); err != nil {
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

	vmCtx, cancelVmCtx := context.WithCancelCause(background.ToBackground(ctx))
	c.vmCtx = vmCtx
	c.cancelVmCtx = cancelVmCtx

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
			JailerBinary:   c.executorConfig.JailerBinaryPath,
			ChrootBaseDir:  c.jailerRoot,
			ID:             c.id,
			UID:            fcclient.Int(unix.Geteuid()),
			GID:            fcclient.Int(unix.Getegid()),
			NumaNode:       fcclient.Int(0), // TODO(tylerw): randomize this?
			ExecFile:       c.executorConfig.FirecrackerBinaryPath,
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

	snap, err := c.loader.GetSnapshot(ctx, c.snapshotKey, c.supportsRemoteSnapshots)
	if err != nil {
		return status.WrapError(err, "failed to get snapshot")
	}

	// Use vmCtx for COWs since IO may be done outside of the task ctx.
	unpacked, err := c.loader.UnpackSnapshot(vmCtx, snap, c.getChroot())
	if err != nil {
		return status.WrapError(err, "failed to unpack snapshot")
	}
	if len(unpacked.ChunkedFiles) > 0 && !(*enableVBD || *enableUFFD) {
		return status.InternalError("copy_on_write support is disabled but snapshot contains chunked files")
	}
	for name, cow := range unpacked.ChunkedFiles {
		switch name {
		case rootDriveID:
			c.rootStore = cow
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

	if err := c.setupVBDMounts(ctx); err != nil {
		return status.WrapError(err, "failed to init virtual block devices")
	}

	if err := c.setupUFFDHandler(ctx); err != nil {
		return err
	}

	ctx, cancel := c.monitorVMContext(ctx)
	defer cancel()

	err = (func() error {
		_, span := tracing.StartSpan(ctx)
		defer span.End()
		span.SetName("StartMachine")
		// Note: using vmCtx here, which outlives the ctx above and is not
		// cancelled until calling Remove().
		return machine.Start(vmCtx)
	})()
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return status.InternalErrorf("failed to start machine: %s", err)
	}
	c.machine = machine

	if err := c.machine.ResumeVM(ctx); err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return status.InternalErrorf("error resuming VM: %s", err)
	}

	conn, err := c.dialVMExecServer(ctx)
	if err != nil {
		return err
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
	if !*enableVBD {
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

// initRootfsStore creates the initial rootfs chunk layout in the chroot.
func (c *FirecrackerContainer) initRootfsStore(ctx context.Context) error {
	if err := os.MkdirAll(c.getChroot(), 0755); err != nil {
		return status.InternalErrorf("failed to create rootfs chunk dir: %s", err)
	}
	containerExt4Path := filepath.Join(c.getChroot(), containerFSName)
	cf, err := snaploader.UnpackContainerImage(c.vmCtx, c.loader, c.containerImage, containerExt4Path, c.getChroot(), cowChunkSizeBytes())
	if err != nil {
		return status.WrapError(err, "unpack container image")
	}
	if err := c.resizeRootfs(ctx, cf); err != nil {
		cf.Close()
		return err
	}
	c.rootStore = cf
	return nil
}

// Resizes the rootfs store to match the disk size requested for the action.
// Note, this operation doesn't mutate the original containerfs, and is also
// cheap (just updates size metadata). Also note that this does not update the
// ext4 superblock to be aware of the increased block device size - instead, the
// guest does that by issuing an EXT4_IOC_RESIZE_FS syscall.
func (c *FirecrackerContainer) resizeRootfs(ctx context.Context, cf *copy_on_write.COWStore) error {
	curSize, err := cf.SizeBytes()
	if err != nil {
		return status.WrapError(err, "get container image size")
	}
	newSize := curSize + minScratchDiskSizeBytes + c.vmConfig.ScratchDiskSizeMb*1e6
	if _, err := cf.Resize(newSize); err != nil {
		return status.WrapError(err, "resize root fs")
	}
	log.Debugf("Resized rootfs from %d => %d bytes", curSize, newSize)
	return nil
}

// createWorkspaceImage creates a new ext4 image from the action working dir.
func (c *FirecrackerContainer) createWorkspaceImage(ctx context.Context, workspaceDir, ext4ImagePath string) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	if c.workspaceStore != nil {
		return status.InternalError("workspace image is already created")
	}

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
		workspaceDiskSizeBytes := ext4.MinDiskImageSizeBytes + workspaceSizeBytes + *workspaceDiskSlackSpaceMB*1e6
		if err := ext4.DirectoryToImage(ctx, workspaceDir, ext4ImagePath, workspaceDiskSizeBytes); err != nil {
			return status.WrapError(err, "failed to convert workspace dir to ext4 image")
		}
	}
	if !*enableVBD {
		return nil
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

func (c *FirecrackerContainer) convertToCOW(ctx context.Context, filePath, chunkDir string) (*copy_on_write.COWStore, error) {
	start := time.Now()
	if err := os.Mkdir(chunkDir, 0755); err != nil {
		return nil, status.WrapError(err, "make chunk dir")
	}
	// Use vmCtx for the COW since IO may be done outside of the task ctx.
	cow, err := copy_on_write.ConvertFileToCOW(c.vmCtx, c.env, filePath, cowChunkSizeBytes(), chunkDir, c.snapshotKey.InstanceName, c.supportsRemoteSnapshots)
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

func cowChunkSizeBytes() int64 {
	return int64(os.Getpagesize() * cowChunkSizeInPages)
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

	if *enableVBD {
		// Stub out the workspace drive with an empty file so that firecracker
		// releases any open file handles on the VBD. We can't unmount the FUSE
		// FS while there are open file handles.
		//
		// TODO(bduffany): have the guest download files from the host to avoid
		// this awkwardness.
		const emptyFileName = "empty.ext4"
		if err := os.WriteFile(filepath.Join(c.getChroot(), emptyFileName), nil, 0644); err != nil {
			return status.InternalErrorf("failed to create empty workspace file")
		}
		if err := c.machine.UpdateGuestDrive(ctx, workspaceDriveID, emptyFileName); err != nil {
			return status.InternalErrorf("failed to stub out workspace drive: %s", err)
		}
		if c.workspaceVBD != nil {
			if err := c.workspaceVBD.Unmount(); err != nil {
				return status.WrapError(err, "unmount workspace vbd")
			}
			c.workspaceVBD = nil
		}
		if c.workspaceStore != nil {
			c.workspaceStore.Close()
			c.workspaceStore = nil
		}
	}

	workspaceExt4Path := filepath.Join(c.getChroot(), workspaceFSName)
	if err := c.createWorkspaceImage(ctx, c.actionWorkingDir, workspaceExt4Path); err != nil {
		return status.WrapError(err, "failed to create workspace image")
	}

	if *enableVBD {
		d, err := vbd.New(c.workspaceStore)
		if err != nil {
			return err
		}
		mountPath := filepath.Join(c.getChroot(), workspaceDriveID+vbdMountDirSuffix)
		if err := d.Mount(mountPath); err != nil {
			return status.WrapError(err, "mount workspace VBD")
		}
		c.workspaceVBD = d
	}

	chrootRelativeImagePath := workspaceFSName
	if *enableVBD {
		chrootRelativeImagePath = filepath.Join(workspaceDriveID+vbdMountDirSuffix, vbd.FileName)
	}
	if err := c.machine.UpdateGuestDrive(ctx, workspaceDriveID, chrootRelativeImagePath); err != nil {
		return status.InternalErrorf("error updating workspace drive attached to snapshot: %s", err)
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
func (c *FirecrackerContainer) getConfig(ctx context.Context, rootFS, containerFS, scratchFS, workspaceFS string) (*fcclient.Config, error) {
	bootArgs := "ro console=ttyS0 noapic reboot=k panic=1 pci=off nomodules=1 random.trust_cpu=on i8042.noaux i8042.nomux i8042.nopnp i8042.dumbkbd tsc=reliable ipv6.disable=1"
	var netNS string

	if c.vmConfig.EnableNetworking {
		bootArgs += " " + machineIPBootArgs
		netNS = networking.NetNamespacePath(c.id)
	}

	// Pass some flags to the init script.
	//
	// !!! WARNING !!!
	//
	// When adding new flags, you'll probably want to bump goinitVersion,
	// otherwise the flags will not actually be used until all of the older
	// snapshots expire from cache (which can take an indefinite amount of
	// time).
	if c.vmConfig.DebugMode {
		bootArgs = "-debug_mode " + bootArgs
	}
	if c.vmConfig.EnableNetworking {
		bootArgs = "-set_default_route " + bootArgs
	}
	if c.vmConfig.InitDockerd {
		bootArgs = "-init_dockerd " + bootArgs
	}
	if c.vmConfig.EnableDockerdTcp {
		bootArgs = "-enable_dockerd_tcp " + bootArgs
	}
	if *EnableRootfs {
		bootArgs = "-enable_rootfs " + bootArgs
	}
	cgroupVersion, err := getCgroupVersion()
	if err != nil {
		return nil, err
	}
	cfg := &fcclient.Config{
		VMID:            c.id,
		SocketPath:      firecrackerSocketPath,
		KernelImagePath: c.executorConfig.KernelImagePath,
		InitrdPath:      c.executorConfig.InitrdImagePath,
		KernelArgs:      bootArgs,
		ForwardSignals:  make([]os.Signal, 0),
		NetNS:           netNS,
		Seccomp:         fcclient.SeccompConfig{Enabled: true},
		// Note: ordering in this list determines the device lettering
		// (/dev/vda, /dev/vdb, /dev/vdc, ...)
		Drives: []fcmodels.Drive{},
		VsockDevices: []fcclient.VsockDevice{
			{Path: firecrackerVSockPath},
		},
		JailerCfg: &fcclient.JailerConfig{
			JailerBinary:   c.executorConfig.JailerBinaryPath,
			ChrootBaseDir:  c.jailerRoot,
			ID:             c.id,
			UID:            fcclient.Int(unix.Geteuid()),
			GID:            fcclient.Int(unix.Getegid()),
			NumaNode:       fcclient.Int(0), // TODO(tylerw): randomize this?
			ExecFile:       c.executorConfig.FirecrackerBinaryPath,
			ChrootStrategy: fcclient.NewNaiveChrootStrategy(c.executorConfig.KernelImagePath),
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
	if *EnableRootfs {
		cfg.Drives = append(cfg.Drives, fcmodels.Drive{
			DriveID:      fcclient.String(rootDriveID),
			PathOnHost:   &rootFS,
			IsRootDevice: fcclient.Bool(false),
			IsReadOnly:   fcclient.Bool(false),
		})
	} else {
		cfg.Drives = append(cfg.Drives, fcmodels.Drive{
			DriveID:      fcclient.String(containerDriveID),
			PathOnHost:   &containerFS,
			IsRootDevice: fcclient.Bool(false),
			IsReadOnly:   fcclient.Bool(true),
		}, fcmodels.Drive{
			DriveID:      fcclient.String(scratchDriveID),
			PathOnHost:   &scratchFS,
			IsRootDevice: fcclient.Bool(false),
			IsReadOnly:   fcclient.Bool(false),
		})
	}
	// Workspace drive will be /dev/vdb if merged rootfs is enabled, /dev/vdc
	// otherwise.
	cfg.Drives = append(cfg.Drives, []fcmodels.Drive{
		{
			DriveID:      fcclient.String(workspaceDriveID),
			PathOnHost:   &workspaceFS,
			IsRootDevice: fcclient.Bool(false),
			IsReadOnly:   fcclient.Bool(false),
		},
	}...)

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

	workspaceExt4Path := filepath.Join(c.getChroot(), workspaceFSName)

	if *enableVBD {
		// Reassemble the workspace chunks into a single file.
		// TODO(bduffany): figure out how to avoid doing this work, e.g. by
		// mounting the workspace disk as a local NBD on the host.
		// For now, this approach is acceptable because firecracker workspaces
		// are typically small or empty (e.g. workflows run in $HOME rather than
		// the workspace dir).
		if err := c.workspaceStore.WriteFile(workspaceExt4Path); err != nil {
			return err
		}
	}

	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "copyOutputsToWorkspace took %s", time.Since(start))
	}()
	if exists, err := disk.FileExists(ctx, workspaceExt4Path); err != nil || !exists {
		return status.FailedPreconditionErrorf("workspacefs path %q not found", workspaceExt4Path)
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
		m, err := mountExt4ImageUsingLoopDevice(workspaceExt4Path, wsDir)
		if err != nil {
			log.CtxWarningf(ctx, "could not mount ext4 image: %s", err)
			return err
		}
		defer m.Unmount()
	} else {
		if err := ext4.ImageToDirectory(ctx, workspaceExt4Path, wsDir); err != nil {
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
	masqueradingOnce.Do(func() {
		masqueradingErr = networking.EnableMasquerading(ctx)
	})
	if masqueradingErr != nil {
		return masqueradingErr
	}

	if err := networking.CreateNetNamespace(ctx, c.id); err != nil {
		if strings.Contains(err.Error(), "File exists") {
			// Don't fail if we failed to cleanup the networking on a previous run
			log.Warningf("Networking cleanup failure. Net namespace already exists: %s", err)
		} else {
			return err
		}
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
	sockAbsPath := filepath.Join(c.getChroot(), uffdSockName)
	if err := h.Start(ctx, sockAbsPath, c.memoryStore); err != nil {
		return status.WrapError(err, "start uffd handler")
	}
	go func() {
		// If we fail to handle a uffd request, terminate the VM.
		if err := h.Wait(); err != nil {
			c.cancelVmCtx(err)
		}
	}()
	c.uffdHandler = h
	return nil
}

func (c *FirecrackerContainer) setupVBDMounts(ctx context.Context) error {
	if !*enableVBD {
		return nil
	}

	ctx, span := tracing.StartSpan(ctx) // nolint:SA4006
	defer span.End()
	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "Set up VBD mounts in %s", time.Since(start))
	}()

	setup := func(driveID string, store *copy_on_write.COWStore) (*vbd.FS, error) {
		if store == nil {
			return nil, nil
		}
		d, err := vbd.New(store)
		if err != nil {
			return nil, err
		}
		mountPath := filepath.Join(c.getChroot(), driveID+vbdMountDirSuffix)
		if err := d.Mount(mountPath); err != nil {
			return nil, err
		}
		log.CtxDebugf(ctx, "Mounted %s VBD FUSE filesystem to %s", driveID, mountPath)
		return d, nil
	}

	if d, err := setup(rootDriveID, c.rootStore); err != nil {
		return err
	} else {
		c.rootVBD = d
	}
	if d, err := setup(scratchDriveID, c.scratchStore); err != nil {
		return err
	} else {
		c.scratchVBD = d
	}
	if d, err := setup(workspaceDriveID, c.workspaceStore); err != nil {
		return err
	} else {
		c.workspaceVBD = d
	}
	return nil
}

func (c *FirecrackerContainer) setupVFSServer(ctx context.Context) error {
	if c.vfsServer != nil {
		return nil
	}
	ctx, span := tracing.StartSpan(ctx) // nolint:SA4006
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

	// Even if the context was canceled, extend the life of the context for
	// cleanup
	ctx, cancel := background.ExtendContextForFinalization(ctx, time.Second*1)
	defer cancel()

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// These cleanup functions should not depend on each other, so try cleaning
	// up everything and return the last error if there is one.
	var lastErr error
	if c.cleanupVethPair != nil {
		if err := c.cleanupVethPair(ctx); err != nil {
			log.Warningf("Networking cleanup failure. CleanupVethPair for vm id %s failed with: %s", c.id, err)
			lastErr = err
		}
	}
	if err := networking.RemoveNetNamespace(ctx, c.id); err != nil {
		log.Warningf("Networking cleanup failure. RemoveNetNamespace for vm id %s failed with: %s", c.id, err)
		lastErr = err
	}
	if err := networking.DeleteRoute(ctx, c.vmIdx); err != nil {
		if !strings.Contains(err.Error(), "No such process") {
			log.Warningf("Networking cleanup failure. DeleteRoute for vm idx %d failed with: %s", c.vmIdx, err)
			lastErr = err
		}
	}
	if err := networking.DeleteRuleIfSecondaryNetworkEnabled(ctx, c.vmIdx); err != nil {
		log.Warningf("Networking cleanup failure. DeleteRuleIfSecondaryNetworkEnabled for vm idx %d failed with: %s", c.vmIdx, err)
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
func (c *FirecrackerContainer) Run(ctx context.Context, command *repb.Command, actionWorkingDir string, creds oci.Credentials) *interfaces.CommandResult {
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
		if err := container.PullImageIfNecessary(ctx, c.env, c, creds, c.containerImage); err != nil {
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

	cmdResult := c.Exec(ctx, command, &commandutil.Stdio{})
	return cmdResult
}

// Create creates a new VM and starts a top-level process inside it listening
// for commands to execute.
func (c *FirecrackerContainer) Create(ctx context.Context, actionWorkingDir string) error {
	c.actionWorkingDir = actionWorkingDir

	if c.createFromSnapshot {
		log.Debugf("Create: will unpause snapshot")
		return c.Unpause(ctx)
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	err := c.create(ctx)

	createTime := time.Since(start)
	log.CtxDebugf(ctx, "Create took %s", createTime)

	success := err == nil
	c.observeStageDuration(ctx, "init", createTime.Microseconds(), success)
	return err
}

func (c *FirecrackerContainer) create(ctx context.Context) error {
	c.currentTaskInitTimeUsec = time.Now().UnixMicro()
	c.rmOnce = &sync.Once{}
	c.rmErr = nil

	vmCtx, cancel := context.WithCancelCause(background.ToBackground(ctx))
	c.vmCtx = vmCtx
	c.cancelVmCtx = cancel

	if err := os.MkdirAll(c.getChroot(), 0755); err != nil {
		return status.InternalErrorf("failed to create chroot dir: %s", err)
	}
	log.Debugf("Created chroot dir %s", c.getChroot())

	containerFSPath := filepath.Join(c.getChroot(), containerFSName)
	rootFSPath := filepath.Join(c.getChroot(), rootFSName)
	scratchFSPath := filepath.Join(c.getChroot(), scratchFSName)
	workspaceFSPath := filepath.Join(c.getChroot(), workspaceFSName)

	// Hardlink the ext4 image to the chroot at containerFSPath.
	imageExt4Path, err := ociconv.CachedDiskImagePath(ctx, c.jailerRoot, c.containerImage)
	if err != nil {
		return status.UnavailableErrorf("disk image is unavailable: %s", err)
	}
	if err := os.Link(imageExt4Path, containerFSPath); err != nil {
		return err
	}

	if *EnableRootfs {
		if err := c.initRootfsStore(ctx); err != nil {
			return status.WrapError(err, "create root image")
		}
	} else {
		if err := c.initScratchImage(ctx, scratchFSPath); err != nil {
			return status.WrapError(err, "create initial scratch image")
		}
	}
	// Create an empty workspace image initially; the real workspace will be
	// hot-swapped just before running each command in order to ensure that the
	// workspace contents are up to date.
	if err := c.createWorkspaceImage(ctx, "" /*=workspaceDir*/, workspaceFSPath); err != nil {
		return err
	}

	if *enableVBD {
		rootFSPath = filepath.Join(c.getChroot(), rootDriveID+vbdMountDirSuffix, vbd.FileName)
		scratchFSPath = filepath.Join(c.getChroot(), scratchDriveID+vbdMountDirSuffix, vbd.FileName)
		workspaceFSPath = filepath.Join(c.getChroot(), workspaceDriveID+vbdMountDirSuffix, vbd.FileName)
	}
	fcCfg, err := c.getConfig(ctx, rootFSPath, containerFSPath, scratchFSPath, workspaceFSPath)
	if err != nil {
		return err
	}

	if err := c.setupNetworking(ctx); err != nil {
		return err
	}

	if err := c.setupVFSServer(ctx); err != nil {
		return err
	}

	if err := c.setupVBDMounts(ctx); err != nil {
		return status.WrapError(err, "failed to init VBD mounts")
	}

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

func (c *FirecrackerContainer) SendExecRequestToGuest(ctx context.Context, cmd *repb.Command, workDir string, stdio *commandutil.Stdio) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// TODO(bduffany): Reuse connection from Unpause(), if applicable
	conn, err := c.dialVMExecServer(ctx)
	if err != nil {
		return commandutil.ErrorResult(status.InternalErrorf("Firecracker exec failed: failed to dial VM exec port: %s", err))
	}
	defer conn.Close()

	client := vmxpb.NewExecClient(conn)
	health := hlpb.NewHealthClient(conn)

	defer container.Metrics.Unregister(c)
	var lastObservedStatsMutex sync.Mutex
	var lastObservedStats *repb.UsageStats
	statsListener := func(stats *repb.UsageStats) {
		container.Metrics.Observe(c, stats)
		lastObservedStatsMutex.Lock()
		lastObservedStats = stats
		lastObservedStatsMutex.Unlock()
	}

	resultCh := make(chan *interfaces.CommandResult, 1)
	healthCheckErrCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		log.CtxDebug(ctx, "Starting Execute stream.")
		res := vmexec_client.Execute(ctx, client, cmd, workDir, c.user, statsListener, stdio)
		resultCh <- res
	}()
	go func() {
		for {
			select {
			case <-time.After(*healthCheckInterval):
			case <-ctx.Done():
				// Task completed; stop health checking.
				return
			}
			ctx, cancel := context.WithTimeout(ctx, *healthCheckTimeout)
			_, err := health.Check(ctx, &hlpb.HealthCheckRequest{Service: "vmexec"})
			cancel()
			if err != nil {
				healthCheckErrCh <- err
				return
			}
		}
	}()
	select {
	case res := <-resultCh:
		return res
	case err := <-healthCheckErrCh:
		res := commandutil.ErrorResult(status.UnavailableErrorf("VM health check failed (possibly crashed?): %s", err))
		lastObservedStatsMutex.Lock()
		res.UsageStats = lastObservedStats
		lastObservedStatsMutex.Unlock()
		return res
	}
}

func (c *FirecrackerContainer) dialVMExecServer(ctx context.Context) (*grpc.ClientConn, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, vSocketDialTimeout)
	defer cancel()

	vsockPath := filepath.Join(c.getChroot(), firecrackerVSockPath)
	conn, err := vsock.SimpleGRPCDial(ctx, vsockPath, vsock.VMExecPort)
	if err != nil {
		if err := context.Cause(ctx); err != nil {
			// If the context was cancelled for any reason (timed out or VM
			// crashed), check the VM logs which might have more relevant crash
			// info, otherwise return the context error.
			if err := c.parseFatalInitError(); err != nil {
				return nil, err
			}
			// Intentionally not returning DeadlineExceededError here since it
			// is not a Bazel-retryable error, but this particular timeout
			// should be retryable.
			return nil, status.InternalErrorf("failed to connect to VM: %s", err)
		}
		return nil, status.InternalErrorf("failed to connect to VM: %s", err)
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

// monitorVMContext returns a context that is cancelled if the VM exits. The
// returned cancel func should be called after any VM requests are completed, to
// clean up the goroutine that monitors the VM context. Otherwise, the goroutine
// will stay running until the VM exits, which may not be desirable in some
// situations.
func (c *FirecrackerContainer) monitorVMContext(ctx context.Context) (context.Context, context.CancelFunc) {
	vmCtx := c.vmCtx
	ctx, cancel := context.WithCancelCause(ctx)
	go func() {
		select {
		case <-ctx.Done():
		case <-vmCtx.Done():
			cancel(context.Cause(vmCtx))
		}
	}()
	return ctx, func() { cancel(nil) }
}

// Exec runs a command inside a container, with the same working dir set when
// creating the container.
// If stdin is non-nil, the contents of stdin reader will be piped to the stdin of
// the executed process.
// If stdout is non-nil, the stdout of the executed process will be written to the
// stdout writer.
func (c *FirecrackerContainer) Exec(ctx context.Context, cmd *repb.Command, stdio *commandutil.Stdio) *interfaces.CommandResult {
	log.CtxInfof(ctx, "Executing command.")

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()

	// Ensure that ctx gets cancelled if the VM crashes.
	ctx, cancel := c.monitorVMContext(ctx)
	defer cancel()

	result := &interfaces.CommandResult{ExitCode: commandutil.NoExitCode}
	defer func() {
		execDuration := time.Since(start)
		log.CtxDebugf(ctx, "Exec took %s", execDuration)

		timeSinceContainerInit := time.Since(time.UnixMicro(c.currentTaskInitTimeUsec))
		execSuccess := result.Error == nil
		c.observeStageDuration(ctx, "task_lifecycle", timeSinceContainerInit.Microseconds(), execSuccess)
		c.observeStageDuration(ctx, "exec", execDuration.Microseconds(), execSuccess)
	}()

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

	ctx, cancel = background.ExtendContextForFinalization(ctx, finalizationTimeout)
	defer cancel()

	// If FUSE is enabled then outputs are already in the workspace.
	if c.fsLayout == nil {
		// Command was successful, let's unpack the files back to our
		// workspace directory now.
		if err := c.pauseVM(ctx); err != nil {
			result.Error = status.InternalErrorf("error pausing VM: %s", err)
			return result
		}

		if err := c.copyOutputsToWorkspace(ctx); err != nil {
			result.Error = status.WrapError(err, "failed to copy action outputs from VM workspace")
			return result
		}

		if err := c.machine.ResumeVM(ctx); err != nil {
			result.Error = status.InternalErrorf("error resuming VM after copying workspace outputs: %s", err)
			return result
		}
	}

	return result
}

func (c *FirecrackerContainer) IsImageCached(ctx context.Context) (bool, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	diskImagePath, err := ociconv.CachedDiskImagePath(ctx, c.jailerRoot, c.containerImage)
	if err != nil {
		return false, err
	}
	return diskImagePath != "", nil
}

// PullImage pulls the container image from the remote. It always
// re-authenticates the request, but may serve the image from a local cache
// in order to avoid re-downloading the image.
func (c *FirecrackerContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	if c.pulled {
		return nil
	}
	c.pulled = true

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.CtxDebugf(ctx, "PullImage took %s", time.Since(start))
	}()

	_, err := ociconv.CreateDiskImage(ctx, c.dockerClient, c.jailerRoot, c.containerImage, creds)
	if err != nil {
		return err
	}

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

	defer c.cancelVmCtx(fmt.Errorf("VM removed"))

	var lastErr error

	// Note: we don't attempt any kind of clean shutdown here, because at this
	// point either (a) the VM should be paused and we should have already taken
	// a snapshot, or (b) we are just calling Remove() to force a cleanup
	// regardless of VM state (e.g. executor shutdown).
	if err := c.stopMachine(ctx); err != nil {
		log.CtxErrorf(ctx, "Error stopping machine: %s", err)
		lastErr = err
	}
	if err := c.cleanupNetworking(ctx); err != nil {
		log.CtxErrorf(ctx, "Error cleaning up networking: %s\n%s", err, string(c.vmLog.Tail()))
		lastErr = err
	}

	if c.vfsServer != nil {
		c.vfsServer.Stop()
		c.vfsServer = nil
	}

	if c.workspaceVBD != nil {
		if err := c.workspaceVBD.Unmount(); err != nil {
			log.CtxErrorf(ctx, "Failed to unmount workspace VBD: %s", err)
			lastErr = err
		}
		c.workspaceVBD = nil
	}
	if c.workspaceStore != nil {
		c.workspaceStore.Close()
		c.workspaceStore = nil
	}
	if c.scratchVBD != nil {
		if err := c.scratchVBD.Unmount(); err != nil {
			log.CtxErrorf(ctx, "Failed to unmount scratch VBD: %s", err)
			lastErr = err
		}
		c.scratchVBD = nil
	}
	if c.scratchStore != nil {
		c.scratchStore.Close()
		c.scratchStore = nil
	}
	if c.rootVBD != nil {
		if err := c.rootVBD.Unmount(); err != nil {
			log.CtxErrorf(ctx, "Failed to unmount root VBD: %s", err)
			lastErr = err
		}
		c.rootVBD = nil
	}
	if c.rootStore != nil {
		c.rootStore.Close()
		c.rootStore = nil
	}

	if c.uffdHandler != nil {
		if err := c.uffdHandler.Stop(); err != nil {
			log.CtxErrorf(ctx, "Error stopping uffd handler: %s", err)
			lastErr = err
		}
		c.uffdHandler = nil
	}
	if c.memoryStore != nil {
		c.memoryStore.Close()
		c.memoryStore = nil
	}
	if err := os.RemoveAll(filepath.Dir(c.getChroot())); err != nil {
		log.CtxErrorf(ctx, "Error removing chroot: %s", err)
		lastErr = err
	}
	return lastErr
}

func (c *FirecrackerContainer) stopMachine(ctx context.Context) error {
	if c.machine == nil {
		return nil
	}
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	if err := c.machine.StopVMM(); err != nil {
		return status.WrapError(err, "kill firecracker")
	}
	// StopVMM just sends SIGTERM to firecracker; wait for the firecracker
	// process to exit. SIGTERM error is expected, so ignore it.
	if err := c.machine.Wait(ctx); err != nil && !isExitErrorSIGTERM(err) {
		return status.WrapError(err, "wait for firecracker to exit")
	}
	c.machine = nil
	return nil
}

// Pause freezes the container so that it no longer consumes CPU resources.
// It also takes a snapshot and saves it to the cache
//
// If the VM is resumed after this point, the VM state and memory snapshots
// immediately become invalid and another call to SaveSnapshot is required. This
// is because we don't save a copy of the disk when taking a snapshot, and
// instead reuse the same disk image across snapshots (for performance reasons).
// Resuming the VM may change the disk state, causing the original VM state to
// become invalid (e.g., the kernel's page cache may no longer be in sync with
// the disk, which can cause all sorts of issues).
func (c *FirecrackerContainer) Pause(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	err := c.pause(ctx)

	pauseTime := time.Since(start)
	log.CtxDebugf(ctx, "Pause took %s", pauseTime)

	success := err == nil
	c.observeStageDuration(ctx, "pause", pauseTime.Microseconds(), success)
	return err
}

func (c *FirecrackerContainer) pause(ctx context.Context) error {
	ctx, cancel := c.monitorVMContext(ctx)
	defer cancel()

	snapDetails, err := c.snapshotDetails(ctx)
	if err != nil {
		return err
	}

	if err = c.pauseVM(ctx); err != nil {
		return err
	}

	// If an older snapshot is present -- nuke it since we're writing a new one.
	if err = c.cleanupOldSnapshots(snapDetails); err != nil {
		return err
	}

	if err = c.createSnapshot(ctx, snapDetails); err != nil {
		return err
	}

	// Stop the VM, UFFD page fault handler, and NBD server to ensure nothing is
	// modifying the snapshot files as we save them
	if err := c.stopMachine(ctx); err != nil {
		return err
	}
	if c.uffdHandler != nil {
		if err := c.uffdHandler.Stop(); err != nil {
			log.CtxErrorf(ctx, "Error stopping uffd handler: %s", err)
			return err
		}
		c.uffdHandler = nil
	}

	if err = c.saveSnapshot(ctx, snapDetails); err != nil {
		return err
	}

	// Finish cleaning up VM resources
	if err = c.Remove(ctx); err != nil {
		return err
	}

	return nil
}

type snapshotDetails struct {
	snapshotType        string
	memSnapshotName     string
	vmStateSnapshotName string
}

func (c *FirecrackerContainer) snapshotDetails(ctx context.Context) (*snapshotDetails, error) {
	if c.recycled {
		return &snapshotDetails{
			snapshotType:        diffSnapshotType,
			memSnapshotName:     diffMemSnapshotName,
			vmStateSnapshotName: vmStateSnapshotName,
		}, nil
	}
	return &snapshotDetails{
		snapshotType:        fullSnapshotType,
		memSnapshotName:     fullMemSnapshotName,
		vmStateSnapshotName: vmStateSnapshotName,
	}, nil
}

func (c *FirecrackerContainer) createSnapshot(ctx context.Context, snapshotDetails *snapshotDetails) error {
	machineStart := time.Now()
	snapshotTypeOpt := func(params *operations.CreateSnapshotParams) {
		params.Body.SnapshotType = snapshotDetails.snapshotType
	}
	if err := c.machine.CreateSnapshot(ctx, snapshotDetails.memSnapshotName, snapshotDetails.vmStateSnapshotName, snapshotTypeOpt); err != nil {
		log.CtxErrorf(ctx, "Error creating snapshot: %s", err)
		return err
	}

	log.CtxDebugf(ctx, "VMM CreateSnapshot %s took %s", snapshotDetails.snapshotType, time.Since(machineStart))
	return nil
}

func (c *FirecrackerContainer) cleanupOldSnapshots(snapshotDetails *snapshotDetails) error {
	memSnapshotPath := filepath.Join(c.getChroot(), snapshotDetails.memSnapshotName)
	vmStateSnapshotPath := filepath.Join(c.getChroot(), snapshotDetails.vmStateSnapshotName)

	if err := disk.RemoveIfExists(memSnapshotPath); err != nil {
		return status.WrapError(err, "failed to remove existing memory snapshot")
	}
	if err := disk.RemoveIfExists(vmStateSnapshotPath); err != nil {
		return status.WrapError(err, "failed to remove existing VM state snapshot")
	}
	return nil
}

// Unpause un-freezes a container so that it can be used to execute commands.
func (c *FirecrackerContainer) Unpause(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	err := c.unpause(ctx)

	unpauseTime := time.Since(start)
	log.CtxDebugf(ctx, "Unpause took %s", unpauseTime)

	success := err == nil
	c.observeStageDuration(ctx, "init", unpauseTime.Microseconds(), success)
	return err
}

func (c *FirecrackerContainer) unpause(ctx context.Context) error {
	c.recycled = true
	c.currentTaskInitTimeUsec = time.Now().UnixMicro()

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
	if c.machine == nil {
		return nil
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

func (c *FirecrackerContainer) observeStageDuration(ctx context.Context, taskStage string, durationUsec int64, success bool) {
	taskStatus := "success"
	if !success {
		taskStatus = "failure"
	}

	recycleStatus := "clean"
	if c.recycled {
		recycleStatus = "recycled"
	}

	var groupID string
	u, err := perms.AuthenticatedUser(ctx, c.env)
	if err == nil {
		groupID = u.GetGroupID()
	}

	snapshotSharingStatus := "disabled"
	if *snaputil.EnableLocalSnapshotSharing {
		snapshotSharingStatus = "local_sharing_enabled"
	}

	metrics.FirecrackerStageDurationUsec.With(prometheus.Labels{
		metrics.Stage:                    taskStage,
		metrics.StatusHumanReadableLabel: taskStatus,
		metrics.RecycledRunnerStatus:     recycleStatus,
		metrics.GroupID:                  groupID,
		metrics.SnapshotSharingStatus:    snapshotSharingStatus,
	}).Observe(float64(durationUsec))
}

func isExitErrorSIGTERM(err error) bool {
	err = errors.Unwrap(err)
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		return false
	}
	ws, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus)
	if !ok {
		return false
	}
	return ws.Signal() == syscall.SIGTERM
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

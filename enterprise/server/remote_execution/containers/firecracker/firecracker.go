package firecracker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "embed"

	"github.com/armon/circbuf"
	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/block_io"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaputil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/uffd"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vbd"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vmexec_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cpuset"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ociconv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/firecracker-microvm/firecracker-go-sdk/client/operations"
	"github.com/klauspost/cpuid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"

	vmsupport_bundle "github.com/buildbuddy-io/buildbuddy/enterprise/vmsupport"
	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
	vmfspb "github.com/buildbuddy-io/buildbuddy/proto/vmvfs"
	fcclient "github.com/firecracker-microvm/firecracker-go-sdk"
	fcmodels "github.com/firecracker-microvm/firecracker-go-sdk/client/models"
	hlpb "google.golang.org/grpc/health/grpc_health_v1"
)

var (
	firecrackerCgroupVersion              = flag.String("executor.firecracker_cgroup_version", "", "Specifies the cgroup version for firecracker to use.")
	debugStreamVMLogs                     = flag.Bool("executor.firecracker_debug_stream_vm_logs", false, "Stream firecracker VM logs to the terminal.")
	debugTerminal                         = flag.Bool("executor.firecracker_debug_terminal", false, "Run an interactive terminal in the Firecracker VM connected to the executor's controlling terminal. For debugging only.")
	dieOnFirecrackerFailure               = flag.Bool("executor.die_on_firecracker_failure", false, "Makes the host executor process die if any command orchestrating or running Firecracker fails. Useful for capturing failures preemptively. WARNING: using this option MAY leave the host machine in an unhealthy state on Firecracker failure; some post-hoc cleanup may be necessary.")
	workspaceDiskSlackSpaceMB             = flag.Int64("executor.firecracker_workspace_disk_slack_space_mb", 2_000, "Extra space to allocate to firecracker workspace disks, in megabytes. ** Experimental **")
	healthCheckInterval                   = flag.Duration("executor.firecracker_health_check_interval", 10*time.Second, "How often to run VM health checks while tasks are executing.")
	healthCheckTimeout                    = flag.Duration("executor.firecracker_health_check_timeout", 30*time.Second, "Timeout for VM health check requests.")
	overprovisionCPUs                     = flag.Int("executor.firecracker_overprovision_cpus", 3, "Number of CPUs to overprovision for VMs. This allows VMs to more effectively utilize CPU resources on the host machine. Set to -1 to allow all VMs to use max CPU.")
	initOnAllocAndFree                    = flag.Bool("executor.firecracker_init_on_alloc_and_free", false, "Set init_on_alloc=1 and init_on_free=1 in firecracker vms")
	firecrackerVMDockerMirrors            = flag.Slice("executor.firecracker_vm_docker_mirrors", []string{}, "Registry mirror hosts (and ports) for public Docker images. Only used if InitDockerd is set to true.")
	firecrackerVMDockerInsecureRegistries = flag.Slice("executor.firecracker_vm_docker_insecure_registries", []string{}, "Tell Docker to communicate over HTTP with these URLs. Only used if InitDockerd is set to true.")

	forceRemoteSnapshotting = flag.Bool("debug_force_remote_snapshots", false, "When remote snapshotting is enabled, force remote snapshotting even for tasks which otherwise wouldn't support it.")
	disableWorkspaceSync    = flag.Bool("debug_disable_firecracker_workspace_sync", false, "Do not sync the action workspace to the guest, instead using the existing workspace from the VM snapshot.")
	debugDisableCgroup      = flag.Bool("debug_disable_cgroup", false, "Disable firecracker cgroup setup.")
)

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
	GuestAPIVersion = "14"

	// How long to wait when dialing the vmexec server inside the VM.
	vSocketDialTimeout = 60 * time.Second

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
	memoryChunkDirName = snaputil.MemoryFileName

	fullSnapshotType = "Full"
	diffSnapshotType = "Diff"

	mergeDiffSnapshotConcurrency = 4
	// Firecracker writes changed blocks in 4Kb blocks.
	mergeDiffSnapshotBlockSize = 4096

	// Size of machine log tail to retain in memory so that we can parse logs for
	// errors.
	vmLogTailBufSize = 1024 * 12 // 12 KB
	// File name of the VM logs in CommandResult.AuxiliaryLogs
	vmLogTailFileName = "vm_log_tail.txt"
	// Log prefix used by goinit when logging fatal errors.
	fatalInitLogPrefix = "die: "

	// VBD mount path suffix, which is appended to the drive ID.
	// Example:
	//	"{chrootPath}/workspacefs.vbd/file"
	vbdMountDirSuffix = ".vbd"

	// Name of the empty file used as a placeholder for the workspace drive.
	emptyFileName = "empty.bin"

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

	// Timeout when mounting/unmounting the workspace within the guest.
	mountTimeout = 1 * time.Minute

	// How long to allow for the VM to be finalized (paused, outputs copied, etc.)
	finalizationTimeout = 10 * time.Second

	// Firecracker does not allow VMs over a certain size.
	// See MAX_SUPPORTED_VCPUS in firecracker repo.
	firecrackerMaxCPU = 32

	// Special file that actions can create in the workspace directory to
	// invalidate the snapshot the action was run in. This can be written
	// if the action detects that the snapshot was corrupted upon startup.
	invalidateSnapshotMarkerFile = ".BUILDBUDDY_INVALIDATE_SNAPSHOT"
)

var (
	vmIdx   int
	vmIdxMu sync.Mutex

	fatalErrPattern             = regexp.MustCompile(`\b` + fatalInitLogPrefix + `(.*)`)
	slowInterruptWarningPattern = regexp.MustCompile(`hrtimer: interrupt took \d+ ns`)
)

func init() {
	// Configure firecracker request timeout (default: 500ms).
	//
	// We're increasing it from the default here since we do some remote reads
	// during ResumeVM to handle page faults with UFFD.
	os.Setenv("FIRECRACKER_GO_SDK_REQUEST_TIMEOUT_MILLISECONDS", fmt.Sprint(60_000))
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
	CacheRoot  string

	InitrdImagePath       string
	KernelImagePath       string
	FirecrackerBinaryPath string
	JailerBinaryPath      string

	KernelVersion      string
	FirecrackerVersion string
	GuestAPIVersion    string
}

var (
	// set by x_defs in BUILD file
	initrdRunfilePath  string
	vmlinuxRunfilePath string
)

// GetExecutorConfig computes the ExecutorConfig for this executor instance.
//
// WARNING: The given buildRootDir will be used as the jailer root dir. Because
// of the limitation on the length of unix sock file paths (103), this directory
// path needs to be short. Specifically, a full sock path will look like:
// /tmp/firecracker/217d4de0-4b28-401b-891b-18e087718ad1/root/run/fc.sock
// everything after "/tmp" is 65 characters, so 38 are left for the jailerRoot.
func GetExecutorConfig(ctx context.Context, buildRootDir, cacheRootDir string) (*ExecutorConfig, error) {
	bundle := vmsupport_bundle.Get()
	initrdRunfileLocation, err := runfiles.Rlocation(initrdRunfilePath)
	if err != nil {
		return nil, err
	}
	initrdPath, err := putFileIntoDir(ctx, bundle, initrdRunfileLocation, buildRootDir, 0755)
	if err != nil {
		return nil, err
	}
	vmlinuxRunfileLocation, err := runfiles.Rlocation(vmlinuxRunfilePath)
	if err != nil {
		return nil, err
	}
	kernelPath, err := putFileIntoDir(ctx, bundle, vmlinuxRunfileLocation, buildRootDir, 0755)
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
		CacheRoot:             cacheRootDir,
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
	executorConfig *ExecutorConfig
}

func NewProvider(env environment.Env, buildRoot, cacheRoot string) (*Provider, error) {
	executorConfig, err := GetExecutorConfig(env.GetServerContext(), buildRoot, cacheRoot)
	if err != nil {
		return nil, err
	}

	// Enable masquerading on the host once on startup.
	if err := networking.EnableMasquerading(env.GetServerContext()); err != nil {
		return nil, status.WrapError(err, "enable masquerading")
	}

	return &Provider{
		env:            env,
		executorConfig: executorConfig,
	}, nil
}

func (p *Provider) New(ctx context.Context, args *container.Init) (container.CommandContainer, error) {
	var vmConfig *fcpb.VMConfiguration
	sizeEstimate := args.Task.GetSchedulingMetadata().GetTaskSize()
	numCPUs := int64(max(1.0, float64(sizeEstimate.GetEstimatedMilliCpu())/1000))
	op := *overprovisionCPUs
	if op == -1 {
		numCPUs = int64(runtime.NumCPU())
	} else {
		numCPUs = min(numCPUs+int64(op), int64(runtime.NumCPU()))
	}
	if numCPUs > firecrackerMaxCPU {
		numCPUs = firecrackerMaxCPU
	}
	vmConfig = &fcpb.VMConfiguration{
		NumCpus:           numCPUs,
		MemSizeMb:         int64(math.Max(1.0, float64(sizeEstimate.GetEstimatedMemoryBytes())/1e6)),
		ScratchDiskSizeMb: int64(float64(sizeEstimate.GetEstimatedFreeDiskBytes()) / 1e6),
		EnableLogging:     platform.IsTrue(platform.FindEffectiveValue(args.Task.GetExecutionTask(), "debug-enable-vm-logs")),
		EnableNetworking:  true,
		InitDockerd:       args.Props.InitDockerd,
		EnableDockerdTcp:  args.Props.EnableDockerdTCP,
		HostCpuid:         getCPUID(),
	}
	vmConfig.BootArgs = getBootArgs(vmConfig)
	opts := ContainerOpts{
		VMConfiguration:        vmConfig,
		ContainerImage:         args.Props.ContainerImage,
		User:                   args.Props.DockerUser,
		ActionWorkingDirectory: args.WorkDir,
		CgroupParent:           args.CgroupParent,
		CgroupSettings:         args.Task.GetSchedulingMetadata().GetCgroupSettings(),
		BlockDevice:            args.BlockDevice,
		ExecutorConfig:         p.executorConfig,
		CPUWeightMillis:        sizeEstimate.GetEstimatedMilliCpu(),
		OverrideSnapshotKey:    args.Props.OverrideSnapshotKey,
	}
	c, err := NewContainer(ctx, p.env, args.Task.GetExecutionTask(), opts)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// FirecrackerContainer executes commands inside of a firecracker VM.
type FirecrackerContainer struct {
	id         string // a random GUID, unique per-run of firecracker
	snapshotID string // a random GUID, unique per-run of firecracker
	vmIdx      int    // the index of this vm on the host machine
	loader     *snaploader.FileCacheLoader

	vmConfig         *fcpb.VMConfiguration
	containerImage   string // the OCI container image. ex "alpine:latest"
	actionWorkingDir string // the action directory with inputs / outputs
	pulled           bool   // whether the container ext4 image has been pulled
	user             string // user to execute all commands as

	rmOnce *sync.Once
	rmErr  error

	network *networking.VMNetwork

	// Whether the VM was recycled.
	recycled bool

	// The following snapshot-related fields are initialized in NewContainer
	// based on the incoming task. If there is a new task, you may want to call
	// NewContainer again rather than directly unpausing a pre-existing container,
	// to make sure these fields are updated and the best snapshot match is used
	snapshotKeySet          *fcpb.SnapshotKeySet
	createFromSnapshot      bool
	supportsRemoteSnapshots bool

	// If set, the snapshot used to load the VM
	snapshot *snaploader.Snapshot

	// The current task assigned to the VM.
	task *repb.ExecutionTask
	// When the VM was initialized (i.e. created or unpaused) for the command
	// it is currently executing
	//
	// This can be used to understand the total time it takes to execute a task,
	// including VM startup time
	currentTaskInitTime time.Time

	executorConfig *ExecutorConfig

	// when VFS is enabled, this contains the layout for the next execution
	fsLayout  *container.FileSystemLayout
	vfsServer *vfs_server.Server

	scratchStore *copy_on_write.COWStore
	scratchVBD   *vbd.FS
	rootStore    *copy_on_write.COWStore
	rootVBD      *vbd.FS

	uffdHandler *uffd.Handler
	memoryStore *copy_on_write.COWStore

	jailerRoot      string               // the root dir the jailer will work in
	cpuWeightMillis int64                // milliCPU for cgroup CPU weight
	cgroupParent    string               // parent cgroup path (root-relative)
	cgroupSettings  *scpb.CgroupSettings // jailer cgroup settings
	blockDevice     *block_io.Device     // block device for cgroup IO settings
	machine         *fcclient.Machine    // the firecracker machine object.
	vmLog           *VMLog
	env             environment.Env

	vmCtx context.Context
	// cancelVmCtx cancels the Machine context, stopping the VMM if it hasn't
	// already been stopped manually.
	cancelVmCtx context.CancelCauseFunc

	// releaseCPUs returns any CPUs that were leased for running the VM with.
	releaseCPUs func()

	vmExec struct {
		conn         *grpc.ClientConn
		err          error
		singleflight *singleflight.Group[string, *grpc.ClientConn]
	}
}

var _ container.VM = (*FirecrackerContainer)(nil)

func NewContainer(ctx context.Context, env environment.Env, task *repb.ExecutionTask, opts ContainerOpts) (*FirecrackerContainer, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	if opts.VMConfiguration == nil {
		return nil, status.InvalidArgumentError("missing VMConfiguration")
	}
	if opts.VMConfiguration.InitDockerd && !opts.VMConfiguration.EnableNetworking {
		return nil, status.FailedPreconditionError("InitDockerd set to true but EnableNetworking set to false. EnableNetworking must also be set to true to pass dockerd configuration over MMDS.")
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
		vmConfig:         opts.VMConfiguration.CloneVT(),
		executorConfig:   opts.ExecutorConfig,
		jailerRoot:       opts.ExecutorConfig.JailerRoot,
		containerImage:   opts.ContainerImage,
		user:             opts.User,
		actionWorkingDir: opts.ActionWorkingDirectory,
		cpuWeightMillis:  opts.CPUWeightMillis,
		cgroupParent:     opts.CgroupParent,
		cgroupSettings:   &scpb.CgroupSettings{},
		blockDevice:      opts.BlockDevice,
		env:              env,
		task:             task,
		loader:           loader,
		vmLog:            vmLog,
		cancelVmCtx:      func(err error) {},
	}
	c.vmExec.singleflight = &singleflight.Group[string, *grpc.ClientConn]{}

	if opts.CgroupSettings != nil {
		c.cgroupSettings = opts.CgroupSettings
	}

	c.vmConfig.KernelVersion = c.executorConfig.KernelVersion
	c.vmConfig.FirecrackerVersion = c.executorConfig.FirecrackerVersion
	c.vmConfig.GuestApiVersion = c.executorConfig.GuestAPIVersion

	if opts.ForceVMIdx != 0 {
		c.vmIdx = opts.ForceVMIdx
	}

	c.supportsRemoteSnapshots = *snaputil.EnableRemoteSnapshotSharing && (platform.IsCICommand(task.GetCommand(), platform.GetProto(task.GetAction(), task.GetCommand())) || *forceRemoteSnapshotting)

	if opts.OverrideSnapshotKey == nil {
		c.vmConfig.DebugMode = *debugTerminal

		if err := c.newID(ctx); err != nil {
			return nil, err
		}
		cd, err := digest.ComputeForMessage(c.vmConfig, repb.DigestFunction_SHA256)
		if err != nil {
			return nil, err
		}

		runnerID := c.id
		if snaputil.IsChunkedSnapshotSharingEnabled() {
			runnerID = ""
		}
		c.snapshotKeySet, err = loader.SnapshotKeySet(ctx, task, cd.GetHash(), runnerID)
		if err != nil {
			return nil, err
		}
		// If recycling is enabled and a snapshot exists, then when calling
		// Create(), load the snapshot instead of creating a new VM.

		recyclingEnabled := platform.IsTrue(platform.FindValue(platform.GetProto(task.GetAction(), task.GetCommand()), platform.RecycleRunnerPropertyName))
		if recyclingEnabled && snaputil.IsChunkedSnapshotSharingEnabled() {
			snap, err := loader.GetSnapshot(ctx, c.snapshotKeySet, c.supportsRemoteSnapshots)
			c.createFromSnapshot = (err == nil)
			label := ""
			if err != nil {
				label = metrics.MissStatusLabel
				log.CtxInfof(ctx, "Failed to get VM snapshot for keyset %s: %s", snaploader.KeysetDebugString(ctx, c.env, c.SnapshotKeySet(), c.supportsRemoteSnapshots), err)
			} else {
				label = metrics.HitStatusLabel
				log.CtxInfof(ctx, "Found snapshot for key %s", snaploader.KeyDebugString(ctx, c.env, snap.GetKey(), c.supportsRemoteSnapshots))
			}
			metrics.RecycleRunnerRequests.With(prometheus.Labels{
				metrics.RecycleRunnerRequestStatusLabel: label,
			}).Inc()
		}
	} else {
		if !snaputil.IsChunkedSnapshotSharingEnabled() {
			return nil, status.InvalidArgumentError("chunked snapshot sharing must be enabled to provide an override snapshot key")
		}
		c.snapshotKeySet = &fcpb.SnapshotKeySet{BranchKey: opts.OverrideSnapshotKey, WriteKey: opts.OverrideSnapshotKey}
		c.createFromSnapshot = true

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
	var storeChunkSizeBytes int64
	if baseSnapshotStore == nil {
		f, err := os.OpenFile(baseSnapshotPath, os.O_WRONLY, 0644)
		if err != nil {
			return status.UnavailableErrorf("Could not open base snapshot file %q: %s", baseSnapshotPath, err)
		}
		defer f.Close()
		out = f
	} else {
		out = baseSnapshotStore
		storeChunkSizeBytes = baseSnapshotStore.ChunkSizeBytes()
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
	if baseSnapshotStore != nil {
		// Ensure goroutines don't cross chunk boundaries and cause race conditions when writing
		perThreadBytes = alignToMultiple(perThreadBytes, storeChunkSizeBytes)
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
					// continue with for loop
				}
				// 3 is the Linux constant for the SEEK_DATA option to lseek.
				newOffset, err := syscall.Seek(int(gin.Fd()), offset, 3)
				if err != nil {
					// ENXIO is expected when the offset is within a hole at the end of
					// the file.
					if err == syscall.ENXIO {
						if baseSnapshotStore != nil {
							if err := baseSnapshotStore.UnmapChunk(offset); err != nil {
								return err
							}
						}
						break
					}
					return err
				}

				if baseSnapshotStore != nil {
					ogChunkStartOffset := baseSnapshotStore.ChunkStartOffset(offset)
					newChunkStartOffset := baseSnapshotStore.ChunkStartOffset(newOffset)

					// If we've seeked to a new chunk, unmap the previous chunk to save memory
					// usage on the executor
					if newChunkStartOffset != ogChunkStartOffset {
						if err := baseSnapshotStore.UnmapChunk(offset); err != nil {
							return err
						}
					}
				}

				offset = newOffset
				if offset >= regionEnd {
					break
				}
				n, readErr := gin.ReadAt(buf, offset)
				if readErr != nil && readErr != io.EOF {
					return readErr
				}
				endOfDiffSnapshot := readErr == io.EOF

				if _, err := out.WriteAt(buf[:n], offset); err != nil {
					return err
				}

				if baseSnapshotStore != nil {
					// If we've finished processing a chunk, unmap it to save memory
					// usage on the executor
					currentChunkStartOffset := baseSnapshotStore.ChunkStartOffset(offset)
					newChunkStartOffset := baseSnapshotStore.ChunkStartOffset(offset + int64(n))

					nextOffsetInNextChunk := newChunkStartOffset != currentChunkStartOffset
					if nextOffsetInNextChunk || endOfDiffSnapshot {
						if err := baseSnapshotStore.UnmapChunk(offset); err != nil {
							return err
						}
					}
				}

				offset += int64(n)
				if endOfDiffSnapshot {
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

func (c *FirecrackerContainer) SnapshotKeySet() *fcpb.SnapshotKeySet {
	return c.snapshotKeySet.CloneVT()
}

func (c *FirecrackerContainer) SnapshotID() string {
	return c.snapshotID
}

func (c *FirecrackerContainer) pauseVM(ctx context.Context) error {
	if c.machine == nil {
		return status.InternalError("failed to pause VM: machine is not started")
	}
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	if err := c.machine.PauseVM(ctx); err != nil {
		log.CtxErrorf(ctx, "Error pausing VM: %s", err)
		return err
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

	if c.snapshotKeySet.GetWriteKey() == nil {
		return status.InvalidArgumentErrorf("write key required to save snapshot")
	}

	snapshotSharingEnabled := snaputil.IsChunkedSnapshotSharingEnabled()

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
	if snapshotSharingEnabled && c.memoryStore == nil {
		memChunkDir := filepath.Join(c.getChroot(), memoryChunkDirName)
		memoryStore, err := c.convertToCOW(ctx, memSnapshotPath, memChunkDir)
		if err != nil {
			return status.WrapError(err, "convert memory snapshot to COWStore")
		}
		c.memoryStore = memoryStore
	}

	vmd := c.getVMMetadata().CloneVT()
	vmd.LastExecutedTask = c.getVMTask()
	opts := &snaploader.CacheSnapshotOptions{
		VMMetadata:          vmd,
		VMConfiguration:     c.vmConfig,
		VMStateSnapshotPath: filepath.Join(c.getChroot(), snapshotDetails.vmStateSnapshotName),
		KernelImagePath:     c.executorConfig.KernelImagePath,
		InitrdImagePath:     c.executorConfig.InitrdImagePath,
		ChunkedFiles:        map[string]*copy_on_write.COWStore{},
		Recycled:            c.recycled,
		Remote:              c.supportsRemoteSnapshots,
	}
	if snapshotSharingEnabled {
		if c.rootStore != nil {
			opts.ChunkedFiles[rootDriveID] = c.rootStore
		} else {
			opts.ContainerFSPath = filepath.Join(c.getChroot(), containerFSName)
			opts.ChunkedFiles[scratchDriveID] = c.scratchStore
		}
		opts.ChunkedFiles[memoryChunkDirName] = c.memoryStore
	} else {
		opts.ContainerFSPath = filepath.Join(c.getChroot(), containerFSName)
		opts.ScratchFSPath = filepath.Join(c.getChroot(), scratchFSName)
		opts.MemSnapshotPath = memSnapshotPath
	}

	snaploaderStart := time.Now()
	if err := c.loader.CacheSnapshot(ctx, c.snapshotKeySet.GetWriteKey(), opts); err != nil {
		return status.WrapError(err, "add snapshot to cache")
	}
	log.CtxDebugf(ctx, "snaploader.CacheSnapshot took %s", time.Since(snaploaderStart))

	return nil
}

func (c *FirecrackerContainer) getVMMetadata() *fcpb.VMMetadata {
	if c.snapshot == nil || c.snapshot.GetVMMetadata() == nil {
		return &fcpb.VMMetadata{
			VmId:        c.id,
			SnapshotId:  c.snapshotID,
			SnapshotKey: c.SnapshotKeySet().BranchKey,
		}
	}
	return c.snapshot.GetVMMetadata()
}

func (c *FirecrackerContainer) getVMTask() *fcpb.VMMetadata_VMTask {
	d, _ := digest.Compute(strings.NewReader(c.task.GetExecutionId()), c.task.GetExecuteRequest().GetDigestFunction())
	return &fcpb.VMMetadata_VMTask{
		InvocationId:          c.task.GetInvocationId(),
		ExecutionId:           c.task.GetExecutionId(),
		ActionDigest:          c.task.GetExecuteRequest().GetActionDigest(),
		ExecuteResponseDigest: d,
		SnapshotId:            c.snapshotID, // Unique ID pertaining to this execution run
	}
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

	vmCtx, cancelVmCtx := context.WithCancelCause(context.WithoutCancel(ctx))
	c.vmCtx = vmCtx
	c.cancelVmCtx = cancelVmCtx

	if err := c.newID(ctx); err != nil {
		return err
	}

	if err := c.setupCgroup(ctx); err != nil {
		return status.UnavailableErrorf("setup cgroup: %s", err)
	}

	if err := c.setupNetworking(ctx); err != nil {
		return err
	}

	var netnsPath string
	if c.network != nil {
		netnsPath = c.network.NamespacePath()
	}

	// We start firecracker with this reduced config because we will load a
	// snapshot that is already configured.
	jailerCfg, err := c.getJailerConfig(ctx, "" /*=kernelImagePath*/)
	if err != nil {
		return status.WrapError(err, "get jailer config")
	}
	cfg := fcclient.Config{
		SocketPath:        firecrackerSocketPath,
		NetNS:             netnsPath,
		Seccomp:           fcclient.SeccompConfig{Enabled: true},
		DisableValidation: true,
		JailerCfg:         jailerCfg,
		Snapshot: fcclient.SnapshotConfig{
			EnableDiffSnapshots: true,
			ResumeVM:            true,
		},
		ForwardSignals: make([]os.Signal, 0),
	}

	if err := c.setupVFSServer(ctx); err != nil {
		return err
	}

	snapshotSharingEnabled := snaputil.IsChunkedSnapshotSharingEnabled()
	var snapOpt fcclient.Opt
	if snapshotSharingEnabled {
		uffdType := fcclient.WithMemoryBackend(fcmodels.MemoryBackendBackendTypeUffd, uffdSockName)
		snapOpt = fcclient.WithSnapshot("" /*=memFilePath*/, vmStateSnapshotName, uffdType)
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
		return status.UnavailableErrorf("Failed creating machine: %s", err)
	}
	log.CtxDebugf(ctx, "Command: %v", reflect.Indirect(reflect.Indirect(reflect.ValueOf(machine)).FieldByName("cmd")).FieldByName("Args"))

	snap, err := c.loader.GetSnapshot(ctx, c.snapshotKeySet, c.supportsRemoteSnapshots)
	if err != nil {
		return status.WrapError(err, "failed to get snapshot")
	}

	// Set unique per-run identifier on the vm metadata so this exact snapshot
	// run can be identified
	if snap.GetVMMetadata() == nil {
		md := &fcpb.VMMetadata{
			VmId:        c.id,
			SnapshotKey: c.SnapshotKeySet().BranchKey,
		}
		snap.SetVMMetadata(md)
	}
	snap.GetVMMetadata().SnapshotId = c.snapshotID
	c.snapshot = snap

	if err := os.MkdirAll(c.getChroot(), 0777); err != nil {
		return status.UnavailableErrorf("make chroot dir: %s", err)
	}

	// Write the workspace drive placeholder file. Firecracker will expect this
	// file to exist since we stubbed out the workspace drive with this
	// placeholder file just before saving the snapshot, so the snapshot will
	// include a reference to this file.
	if err := os.WriteFile(filepath.Join(c.getChroot(), emptyFileName), nil, 0644); err != nil {
		return status.UnavailableErrorf("write empty file: %s", err)
	}

	// Use vmCtx for COWs since IO may be done outside of the task ctx.
	unpacked, err := c.loader.UnpackSnapshot(vmCtx, snap, c.getChroot())
	if err != nil {
		return status.WrapError(err, "failed to unpack snapshot")
	}
	if len(unpacked.ChunkedFiles) > 0 && !snapshotSharingEnabled {
		return status.InternalError("copy_on_write support is disabled but snapshot contains chunked files")
	}
	for name, cow := range unpacked.ChunkedFiles {
		switch name {
		case rootDriveID:
			c.rootStore = cow
		case scratchDriveID:
			c.scratchStore = cow
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
		return status.UnavailableErrorf("failed to start machine: %s", err)
	}
	c.machine = machine

	if err := c.machine.ResumeVM(ctx); err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return status.UnavailableErrorf("error resuming VM: %s", err)
	}
	c.createFromSnapshot = true

	return nil
}

// initScratchImage creates the empty scratch ext4 disk for the VM.
func (c *FirecrackerContainer) initScratchImage(ctx context.Context, path string) error {
	scratchDiskSizeBytes := ext4.MinDiskImageSizeBytes + minScratchDiskSizeBytes + c.vmConfig.ScratchDiskSizeMb*1e6
	scratchDiskSizeBytes = alignToMultiple(scratchDiskSizeBytes, int64(os.Getpagesize()))
	if err := ext4.MakeEmptyImage(ctx, path, scratchDiskSizeBytes); err != nil {
		return err
	}
	if !snaputil.IsChunkedSnapshotSharingEnabled() {
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
	cowChunkDir := filepath.Join(c.getChroot(), rootFSName)
	if err := os.MkdirAll(cowChunkDir, 0755); err != nil {
		return status.UnavailableErrorf("failed to create rootfs chunk dir: %s", err)
	}
	containerExt4Path := filepath.Join(c.getChroot(), containerFSName)
	cf, err := snaploader.UnpackContainerImage(c.vmCtx, c.loader, c.snapshotKeySet.GetBranchKey().GetInstanceName(), c.containerImage, containerExt4Path, cowChunkDir, cowChunkSizeBytes(), c.supportsRemoteSnapshots)
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
	newSize = alignToMultiple(newSize, int64(os.Getpagesize()))
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

	// The existing workspace disk may still be mounted in the VM, so we unlink
	// it then create a new file rather than overwriting the existing file
	// to avoid corruption.
	if err := os.RemoveAll(ext4ImagePath); err != nil {
		return status.WrapError(err, "failed to delete existing workspace disk image")
	}
	workspaceSizeBytes, err := ext4.DiskSizeBytes(ctx, workspaceDir)
	if err != nil {
		return err
	}
	workspaceDiskSizeBytes := ext4.MinDiskImageSizeBytes + workspaceSizeBytes + *workspaceDiskSlackSpaceMB*1e6
	workspaceDiskSizeBytes = alignToMultiple(workspaceDiskSizeBytes, int64(os.Getpagesize()))
	if err := ext4.DirectoryToImage(ctx, workspaceDir, ext4ImagePath, workspaceDiskSizeBytes); err != nil {
		return status.WrapError(err, "failed to convert workspace dir to ext4 image")
	}
	return nil
}

func (c *FirecrackerContainer) convertToCOW(ctx context.Context, filePath, chunkDir string) (*copy_on_write.COWStore, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	start := time.Now()
	if err := os.Mkdir(chunkDir, 0755); err != nil {
		return nil, status.WrapError(err, "make chunk dir")
	}
	// Use vmCtx for the COW since IO may be done outside of the task ctx.
	cow, err := copy_on_write.ConvertFileToCOW(c.vmCtx, c.env, filePath, cowChunkSizeBytes(), chunkDir, c.snapshotKeySet.GetBranchKey().GetInstanceName(), c.supportsRemoteSnapshots)
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

// updateWorkspaceDriveToEmptyFile updates the workspace device to point to an
// empty file, which is located at /empty.bin relative to the chroot dir.
//
// This lets us avoid saving the workspace contents as part of the VM snapshot,
// which would be wasteful - the workspace contents aren't needed for the next
// action to be executed in the VM, because we create a clean workspace for each
// action.
//
// Note that the guest should not mount this file, since it is not a valid ext4
// filesystem.
func (c *FirecrackerContainer) updateWorkspaceDriveToEmptyFile(ctx context.Context) error {
	chrootRelativePath := emptyFileName
	fullPath := filepath.Join(c.getChroot(), emptyFileName)
	if err := os.WriteFile(fullPath, nil, 0644); err != nil {
		return status.UnavailableErrorf("write file: %s", err)
	}
	if err := c.machine.UpdateGuestDrive(ctx, workspaceDriveID, chrootRelativePath); err != nil {
		return status.UnavailableErrorf("update guest drive: %s", err)
	}
	return nil
}

// createAndAttachWorkspace creates a new disk image from the given working
// directory, updates the workspace drive attached to the VM, and mounts the
// workspace contents in the guest.
//
// The workspace drive must not be mounted in the guest when calling this
// function.
//
// This is intended to be called just before Exec, so that the inputs to the
// executed action will be made available to the VM.
func (c *FirecrackerContainer) createAndAttachWorkspace(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	if *disableWorkspaceSync {
		return nil
	}

	workspaceExt4Path := filepath.Join(c.getChroot(), workspaceFSName)
	if err := c.createWorkspaceImage(ctx, c.actionWorkingDir, workspaceExt4Path); err != nil {
		return status.WrapError(err, "failed to create workspace image")
	}

	conn, err := c.vmExecConn(ctx)
	if err != nil {
		return err
	}
	execClient := vmxpb.NewExecClient(conn)

	// UpdateGuestDrive can only be called after the VM boots (https://github.com/firecracker-microvm/firecracker/blob/c862760999f15d27034098a53a4d5bee3fba829d/src/firecracker/swagger/firecracker.yaml#L256-L260)
	// The only way to tell if the VM booted seems to be monitoring its stdout.
	// Instead we'll just wait for the vm exec server to start above, which
	// happens after boot.
	chrootRelativeImagePath := workspaceFSName
	if err := c.machine.UpdateGuestDrive(ctx, workspaceDriveID, chrootRelativeImagePath); err != nil {
		return status.UnavailableErrorf("error updating workspace drive attached to snapshot: %s", err)
	}

	if err := c.mountWorkspace(ctx, execClient); err != nil {
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
	uuid1 := uuid.New()
	uuid2 := uuid.New()

	vmIdx += 1
	log.CtxDebugf(ctx, "Container id changing from %q (%d) to %q (%d)", c.id, c.vmIdx, uuid1, vmIdx)
	c.id = uuid1
	c.snapshotID = uuid2
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

func getBootArgs(vmConfig *fcpb.VMConfiguration) string {
	kernelArgs := []string{
		"ro",
		"console=ttyS0",
		"reboot=k",
		"panic=1",
		"pci=off",
		"nomodules=1",
		"random.trust_cpu=on",
		"i8042.noaux",
		"i8042.nomux",
		"i8042.nopnp",
		"i8042.dumbkbd",
		"tsc=reliable",
		"ipv6.disable=1",
	}
	if vmConfig.EnableNetworking {
		kernelArgs = append(kernelArgs, machineIPBootArgs)
	}

	if *initOnAllocAndFree {
		kernelArgs = append(kernelArgs, "init_on_alloc=1", "init_on_free=1")
	}

	var initArgs []string
	if vmConfig.DebugMode {
		initArgs = append(initArgs, "-debug_mode")
	}
	if vmConfig.EnableLogging {
		initArgs = append(initArgs, "-enable_logging")
	}
	if vmConfig.EnableNetworking {
		initArgs = append(initArgs, "-set_default_route")
	}
	if vmConfig.InitDockerd {
		initArgs = append(initArgs, "-init_dockerd")
	}
	if vmConfig.EnableDockerdTcp {
		initArgs = append(initArgs, "-enable_dockerd_tcp")
	}
	if snaputil.IsChunkedSnapshotSharingEnabled() {
		initArgs = append(initArgs, "-enable_rootfs")
	}
	if platform.VFSEnabled() {
		initArgs = append(initArgs, "-enable_vfs")
	}
	return strings.Join(append(initArgs, kernelArgs...), " ")
}

// getConfig returns the firecracker config for the current container and given
// filesystem image paths. The image paths are not expected to be in the chroot;
// they will be hardlinked to the chroot when starting the machine (see
// NaiveChrootStrategy).
func (c *FirecrackerContainer) getConfig(ctx context.Context, rootFS, containerFS, scratchFS, workspaceFS string) (*fcclient.Config, error) {
	var netnsPath string
	if c.network != nil {
		netnsPath = c.network.NamespacePath()
	}
	bootArgs := getBootArgs(c.vmConfig)
	jailerCfg, err := c.getJailerConfig(ctx, c.executorConfig.KernelImagePath)
	if err != nil {
		return nil, status.WrapError(err, "get jailer config")
	}
	cfg := &fcclient.Config{
		VMID:            c.id,
		SocketPath:      firecrackerSocketPath,
		KernelImagePath: c.executorConfig.KernelImagePath,
		InitrdPath:      c.executorConfig.InitrdImagePath,
		KernelArgs:      bootArgs,
		ForwardSignals:  make([]os.Signal, 0),
		NetNS:           netnsPath,
		Seccomp:         fcclient.SeccompConfig{Enabled: true},
		// Note: ordering in this list determines the device lettering
		// (/dev/vda, /dev/vdb, /dev/vdc, ...)
		Drives: []fcmodels.Drive{},
		VsockDevices: []fcclient.VsockDevice{
			{Path: firecrackerVSockPath},
		},
		JailerCfg: jailerCfg,
		MachineCfg: fcmodels.MachineConfiguration{
			VcpuCount:       fcclient.Int64(c.vmConfig.NumCpus),
			MemSizeMib:      fcclient.Int64(c.vmConfig.MemSizeMb),
			Smt:             fcclient.Bool(false),
			TrackDirtyPages: fcclient.Bool(true),
		},
	}
	if snaputil.IsChunkedSnapshotSharingEnabled() {
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
				// We only use MMDS for the dockerd config,
				// which is only needed if dockerd is enabled.
				AllowMMDS: c.vmConfig.InitDockerd,
			},
		}
	}
	if *debugTerminal {
		cfg.JailerCfg.Stdin = os.Stdin
	}
	return cfg, nil
}

func (c *FirecrackerContainer) cgroupPath() string {
	return filepath.Join("/sys/fs/cgroup", c.cgroupParent, c.id)
}

func (c *FirecrackerContainer) getJailerConfig(ctx context.Context, kernelImagePath string) (*fcclient.JailerConfig, error) {
	cgroupVersion, err := getCgroupVersion()
	if err != nil {
		return nil, status.WrapError(err, "get cgroup version")
	}

	numaNode := 0
	if c.cgroupSettings.NumaNode != nil {
		numaNode = int(c.cgroupSettings.GetNumaNode())
	}
	return &fcclient.JailerConfig{
		JailerBinary:   c.executorConfig.JailerBinaryPath,
		ChrootBaseDir:  c.jailerRoot,
		ID:             c.id,
		UID:            fcclient.Int(unix.Geteuid()),
		GID:            fcclient.Int(unix.Getegid()),
		NumaNode:       fcclient.Int(numaNode),
		ExecFile:       c.executorConfig.FirecrackerBinaryPath,
		ChrootStrategy: fcclient.NewNaiveChrootStrategy(kernelImagePath),
		Stdout:         c.vmLogWriter(),
		Stderr:         c.vmLogWriter(),
		CgroupVersion:  cgroupVersion,
		// We normally set cpuset.cpus in cgroup.Setup(), but the go
		// SDK clobbers our setting when applying the NUMA node setting.
		// Override this manually for now.
		CgroupArgs: []string{
			fmt.Sprintf("cpuset.cpus=%s", cpuset.Format(c.cgroupSettings.GetCpusetCpus())),
			fmt.Sprintf("cpuset.mems=%d", numaNode),
		},
		// The jailer computes the full cgroup path by appending three path
		// components:
		// 1. The cgroup FS root: "/sys/fs/cgroup"
		// 2. This ParentCgroup setting
		// 3. The ID setting
		ParentCgroup: fcclient.String(c.cgroupParent),
	}, nil
}

func (c *FirecrackerContainer) vmLogWriter() io.Writer {
	if *debugStreamVMLogs || *debugTerminal {
		return io.MultiWriter(c.vmLog, os.Stderr)
	}
	return c.vmLog
}

// copyOutputsToWorkspace copies output files from the workspace filesystem
// image to the local filesystem workdir. It does not overwrite existing files.
//
// Callers should ensure that the workspace block device is not mounted in the
// guest before calling this. If the workspace is still mounted, then some of
// the blocks may not be synchronized with the kernel's in-memory state,
// resulting in the file potentially containing invalid contents.
func (c *FirecrackerContainer) copyOutputsToWorkspace(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	workspaceExt4Path := filepath.Join(c.getChroot(), workspaceFSName)

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

	outputPaths := workspacePathsToExtract(c.task)
	if err := ext4.ImageToDirectory(ctx, workspaceExt4Path, wsDir, outputPaths); err != nil {
		return err
	}

	walkErr := fs.WalkDir(os.DirFS(wsDir), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
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
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	network, err := networking.CreateVMNetwork(ctx, tapDeviceName, tapAddr, vmIP)
	if err != nil {
		return status.UnavailableErrorf("create VM network: %s", err)
	}
	c.network = network

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
	if !snaputil.IsChunkedSnapshotSharingEnabled() {
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
		if err := d.Mount(c.vmCtx, mountPath); err != nil {
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
	return nil
}

func (c *FirecrackerContainer) setupVFSServer(ctx context.Context) error {
	if c.fsLayout == nil {
		return nil
	}
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
	if c.network == nil {
		return nil
	}
	network := c.network
	c.network = nil

	// Even if the context was canceled, extend the life of the context for
	// cleanup
	ctx, cancel := background.ExtendContextForFinalization(ctx, time.Second*1)
	defer cancel()

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	return network.Cleanup(ctx)
}

func (c *FirecrackerContainer) SetTaskFileSystemLayout(fsLayout *container.FileSystemLayout) {
	c.fsLayout = fsLayout
}

func (c *FirecrackerContainer) IsolationType() string {
	return "firecracker"
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

	cmdResult := c.Exec(ctx, command, &interfaces.Stdio{})
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

	c.observeStageDuration("create", createTime)
	return err
}

func withMetadata(metadata interface{}) fcclient.Opt {
	return func(m *fcclient.Machine) {
		// Set metadata during init, before the VM instance is created,
		// since goinit expects metadata to be available on startup.
		m.Handlers.FcInit = m.Handlers.FcInit.AppendAfter(fcclient.ConfigMmdsHandlerName, fcclient.NewSetMetadataHandler(metadata))
	}
}

func (c *FirecrackerContainer) create(ctx context.Context) error {
	c.currentTaskInitTime = time.Now()
	c.rmOnce = &sync.Once{}
	c.rmErr = nil

	vmCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	c.vmCtx = vmCtx
	c.cancelVmCtx = cancel

	if err := os.MkdirAll(c.getChroot(), 0755); err != nil {
		return status.InternalErrorf("failed to create chroot dir: %s", err)
	}
	log.CtxInfof(ctx, "Created chroot dir %q", c.getChroot())

	containerFSPath := filepath.Join(c.getChroot(), containerFSName)
	rootFSPath := filepath.Join(c.getChroot(), rootFSName)
	scratchFSPath := filepath.Join(c.getChroot(), scratchFSName)
	workspacePlaceholderPath := filepath.Join(c.getChroot(), emptyFileName)

	// Hardlink the ext4 image to the chroot at containerFSPath.
	imageExt4Path, err := ociconv.CachedDiskImagePath(ctx, c.executorConfig.CacheRoot, c.containerImage)
	if err != nil {
		return status.UnavailableErrorf("disk image is unavailable: %s", err)
	}
	if err := os.Link(imageExt4Path, containerFSPath); err != nil {
		return err
	}

	if snaputil.IsChunkedSnapshotSharingEnabled() {
		rootFSPath = filepath.Join(c.getChroot(), rootDriveID+vbdMountDirSuffix, vbd.FileName)
		scratchFSPath = filepath.Join(c.getChroot(), scratchDriveID+vbdMountDirSuffix, vbd.FileName)
		if err := c.initRootfsStore(ctx); err != nil {
			return status.WrapError(err, "create root image")
		}
	} else {
		if err := c.initScratchImage(ctx, scratchFSPath); err != nil {
			return status.WrapError(err, "create initial scratch image")
		}
	}

	// Create the workspace drive placeholder contents.
	if err := os.WriteFile(workspacePlaceholderPath, nil, 0644); err != nil {
		return status.UnavailableErrorf("write workspace placeholder file: %s", err)
	}

	if err := c.setupCgroup(ctx); err != nil {
		return status.UnavailableErrorf("setup cgroup: %s", err)
	}

	if err := c.setupNetworking(ctx); err != nil {
		return err
	}

	fcCfg, err := c.getConfig(ctx, rootFSPath, containerFSPath, scratchFSPath, workspacePlaceholderPath)
	if err != nil {
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
	if c.vmConfig.InitDockerd {
		dockerDaemonConfig, err := getDockerDaemonConfig()
		if err != nil {
			return status.InternalErrorf("Could not write Docker daemon config: %s", err)
		}
		metadata := map[string]string{
			"dockerd_daemon_json": string(dockerDaemonConfig),
		}
		machineOpts = append(machineOpts, withMetadata(metadata))
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

func getDockerDaemonConfig() ([]byte, error) {
	config := map[string][]string{}
	if len(*firecrackerVMDockerMirrors) > 0 {
		config["registry-mirrors"] = *firecrackerVMDockerMirrors
	}
	if len(*firecrackerVMDockerInsecureRegistries) > 0 {
		config["insecure-registries"] = *firecrackerVMDockerInsecureRegistries
	}
	configJSON, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, err
	}
	return configJSON, nil
}

func (c *FirecrackerContainer) sendExecRequestToGuest(ctx context.Context, conn *grpc.ClientConn, cmd *repb.Command, workDir string, stdio *interfaces.Stdio) (_ *interfaces.CommandResult, healthy bool) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	execDone := make(chan struct{})
	defer close(execDone)
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

	client := vmxpb.NewExecClient(conn)
	health := hlpb.NewHealthClient(conn)

	var statsMu sync.Mutex
	var lastGuestStats *repb.UsageStats

	resultCh := make(chan *interfaces.CommandResult, 1)
	healthCheckErrCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		log.CtxDebug(ctx, "Starting Execute stream.")
		statsListener := func(stats *repb.UsageStats) {
			statsMu.Lock()
			defer statsMu.Unlock()
			lastGuestStats = stats
		}
		res := vmexec_client.Execute(ctx, client, cmd, workDir, c.user, statsListener, stdio)
		resultCh <- res
	}()
	// While we're executing the task in the VM, also track cgroup stats on the
	// host. Some stats such as disk capacity are better tracked in the guest;
	// other stats such as CPU pressure stalling are better tracked on the
	// host.
	hostCgroupStats := &container.UsageStats{}
	cancelCgroupPoll := hostCgroupStats.TrackExecution(ctx, func(ctx context.Context) (*repb.UsageStats, error) {
		return cgroup.Stats(ctx, c.cgroupPath(), nil /*=blockDevice*/)
	})

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
		cancelCgroupPoll()
		res.UsageStats = combineHostAndGuestStats(hostCgroupStats.TaskStats(), res.UsageStats)
		return res, true
	case err := <-healthCheckErrCh:
		cancelCgroupPoll()
		res := commandutil.ErrorResult(status.UnavailableErrorf("VM health check failed (possibly crashed?): %s", err))
		statsMu.Lock()
		res.UsageStats = combineHostAndGuestStats(hostCgroupStats.TaskStats(), lastGuestStats)
		statsMu.Unlock()
		return res, false
	}
}
func (c *FirecrackerContainer) vmExecConn(ctx context.Context) (*grpc.ClientConn, error) {
	conn, _, err := c.vmExec.singleflight.Do(ctx, "",
		func(ctx context.Context) (*grpc.ClientConn, error) {
			if c.vmExec.conn == nil && c.vmExec.err == nil {
				c.vmExec.conn, c.vmExec.err = c.dialVMExecServer(ctx)
			}
			return c.vmExec.conn, c.vmExec.err
		})
	return conn, err
}

func (c *FirecrackerContainer) dialVMExecServer(ctx context.Context) (*grpc.ClientConn, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		metrics.FirecrackerExecDialDurationUsec.With(prometheus.Labels{
			metrics.CreatedFromSnapshot: strconv.FormatBool(c.createFromSnapshot),
		}).Observe(float64(time.Since(start).Microseconds()))
	}()

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
			return nil, status.UnavailableErrorf("failed to connect to VM: %s", err)
		}
		return nil, status.UnavailableErrorf("failed to connect to VM: %s", err)
	}
	return conn, nil
}

func (c *FirecrackerContainer) SendPrepareFileSystemRequestToGuest(ctx context.Context, req *vmfspb.PrepareRequest) (*vmfspb.PrepareResponse, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	if err := c.vfsServer.Prepare(ctx, c.fsLayout); err != nil {
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
func (c *FirecrackerContainer) Exec(ctx context.Context, cmd *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	log.CtxInfof(ctx, "Executing command.")

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()

	// Ensure that ctx gets cancelled if the VM crashes.
	ctx, cancel := c.monitorVMContext(ctx)
	defer cancel()

	result := &interfaces.CommandResult{ExitCode: commandutil.NoExitCode}
	defer func() {
		// Attach VM metadata to the result
		result.VMMetadata = c.getVMMetadata()

		// Attach VM logs to the result
		if result.AuxiliaryLogs == nil {
			result.AuxiliaryLogs = map[string][]byte{}
		}
		result.AuxiliaryLogs[vmLogTailFileName] = c.vmLog.Tail()

		execDuration := time.Since(start)
		log.CtxDebugf(ctx, "Exec took %s", execDuration)

		timeSinceContainerInit := time.Since(c.currentTaskInitTime)
		c.observeStageDuration("task_lifecycle", timeSinceContainerInit)
		c.observeStageDuration("exec", execDuration)
	}()

	if c.fsLayout == nil {
		if err := c.createAndAttachWorkspace(ctx); err != nil {
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
	if c.createFromSnapshot {
		err := c.resetGuestState(ctx, result)
		if err != nil {
			result.Error = status.WrapError(err, "Failed to initialize firecracker VM exec client")
			return result
		}
	}

	guestWorkspaceMountDir := "/workspace/"
	if c.fsLayout != nil {
		guestWorkspaceMountDir = guestVFSMountDir
	}

	defer func() {
		// Once the execution is complete, look for certain interesting errors
		// in the VM logs and log them as warnings.
		logTail := string(c.vmLog.Tail())
		// Logs end with "\r\n"; convert these to universal line endings.
		logTail = strings.ReplaceAll(logTail, "\r\n", "\n")

		if result.Error != nil {
			if !status.IsDeadlineExceededError(result.Error) {
				log.CtxWarningf(ctx, "Execution error occurred. VM logs: %s", string(c.vmLog.Tail()))
			}
		} else if err := c.parseOOMError(logTail); err != nil {
			// TODO(bduffany): maybe fail the whole command if we see an OOM
			// in the kernel logs, and the command failed?
			log.CtxWarningf(ctx, "OOM error occurred during task execution: %s", err)
		}
		if err := c.parseSegFault(logTail, result); err != nil {
			log.CtxWarningf(ctx, "Segfault occurred during task execution (recycled=%v) : %s", c.recycled, err)
		}
		// Slow hrtimer interrupts can happen during periods of high contention
		// and may help explain action failures - surface these in the executor
		// logs.
		if warning := slowInterruptWarningPattern.FindString(logTail); warning != "" {
			log.CtxWarningf(ctx, "Slow interrupt warning reported in kernel logs: %q", warning)
		}
	}()

	// Emit metrics to track time spent preparing VM to execute a command
	if c.memoryStore != nil {
		c.memoryStore.EmitUsageMetrics("init")
	}
	if c.uffdHandler != nil {
		c.uffdHandler.EmitSummaryMetrics("init")
	}
	if c.rootStore != nil {
		c.rootStore.EmitUsageMetrics("init")
	}

	conn, err := c.vmExecConn(ctx)
	if err != nil {
		return commandutil.ErrorResult(status.InternalErrorf("Firecracker exec failed: failed to dial VM exec port: %s", err))
	}

	result, vmHealthy := c.sendExecRequestToGuest(ctx, conn, cmd, guestWorkspaceMountDir, stdio)

	ctx, cancel = background.ExtendContextForFinalization(ctx, finalizationTimeout)
	defer cancel()

	// If FUSE is enabled then outputs are already in the workspace.
	if c.fsLayout == nil && !*disableWorkspaceSync {
		// Unmount the workspace and pause the VM before syncing to ensure that
		// the guest's FS cache is flushed to the backing file on the host and
		// that no concurrent operations can occur while we're reading the
		// workspace disk.
		//
		// TODO(bduffany): after notifying users of these warnings below,
		// make it a hard failure if we fail to unmount or if the workspace disk
		// is still busy after unmounting.
		client := vmxpb.NewExecClient(conn)

		// If the healthcheck failed then the vmexec server has probably crashed
		// and unmounting will most likely not work - skip unmounting, but still
		// do a best-effort attempt to copy outputs from the image.
		if vmHealthy {
			if rsp, err := c.unmountWorkspace(ctx, client); err != nil {
				log.CtxWarningf(ctx, "Failed to unmount workspace - not recycling VM")
				result.DoNotRecycle = true
			} else {
				if rsp.GetBusy() {
					// Do not recycle the VM if the workspace device is still busy after
					// unmounting.
					result.DoNotRecycle = true
					log.CtxWarningf(ctx, "Workspace device is still busy after unmounting - not recycling VM")
				}
			}
		}
		if err := c.pauseVM(ctx); err != nil {
			result.Error = status.UnavailableErrorf("error pausing VM: %s", err)
			return result
		}
		if err := c.copyOutputsToWorkspace(ctx); err != nil {
			result.Error = status.WrapError(err, "failed to copy action outputs from VM workspace")
			return result
		}
		if err := c.machine.ResumeVM(ctx); err != nil {
			result.Error = status.UnavailableErrorf("error resuming VM after copying workspace outputs: %s", err)
			return result
		}
	}

	return result
}

func (c *FirecrackerContainer) resetGuestState(ctx context.Context, result *interfaces.CommandResult) error {
	conn, err := c.vmExecConn(ctx)
	if err != nil {
		return err
	}

	execClient := vmxpb.NewExecClient(conn)
	_, err = execClient.Initialize(ctx, &vmxpb.InitializeRequest{
		UnixTimestampNanoseconds: time.Now().UnixNano(),
		ClearArpCache:            true,
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *FirecrackerContainer) Signal(ctx context.Context, sig syscall.Signal) error {
	// TODO: forward the signal as a message on any currently running vmexec
	// stream.
	return status.UnimplementedError("not implemented")
}

func (c *FirecrackerContainer) IsImageCached(ctx context.Context) (bool, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	diskImagePath, err := ociconv.CachedDiskImagePath(ctx, c.executorConfig.CacheRoot, c.containerImage)
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

	// If we're creating from a snapshot, we don't need to pull the base image
	// since the rootfs image contains our full desired disk contents.
	if c.createFromSnapshot {
		return nil
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		c.observeStageDuration("pull_image", time.Since(start))
		log.CtxDebugf(ctx, "PullImage took %s", time.Since(start))
	}()

	_, err := ociconv.CreateDiskImage(ctx, c.executorConfig.CacheRoot, c.containerImage, creds)
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
		c.observeStageDuration("remove", time.Since(start))
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

	if err := c.unmountAllVBDs(ctx, true /*logErrors*/); err != nil {
		// Don't log the err - unmountAllVBDs logs it internally.
		lastErr = err
	}
	if c.scratchStore != nil {
		c.scratchStore.Close()
		c.scratchStore = nil
	}
	if c.rootStore != nil {
		c.rootStore.Close()
		c.rootStore = nil
	}

	if c.uffdHandler != nil {
		if err := c.stopUffdHandler(ctx); err != nil {
			lastErr = err
		}
	}
	if c.memoryStore != nil {
		c.closeMemoryStore(ctx)
	}

	exists, err := disk.FileExists(ctx, filepath.Join(c.actionWorkingDir, invalidateSnapshotMarkerFile))
	if err != nil {
		log.CtxWarningf(ctx, "Failed to check existence of %s: %s", invalidateSnapshotMarkerFile, err)
	} else if exists {
		log.CtxInfof(ctx, "Action created %s file in workspace root; invalidating snapshot for key %v", invalidateSnapshotMarkerFile, c.SnapshotKeySet().GetBranchKey())
		_, err = snaploader.NewSnapshotService(c.env).InvalidateSnapshot(ctx, c.SnapshotKeySet().GetBranchKey())
		if err != nil {
			log.CtxWarningf(ctx, "Failed to invalidate snapshot despite existence of %s: %s", invalidateSnapshotMarkerFile, err)
		}
	}

	if err := os.RemoveAll(filepath.Dir(c.getChroot())); err != nil {
		log.CtxErrorf(ctx, "Error removing chroot %q: %s", c.getChroot(), err)
		lastErr = err
	} else {
		log.CtxInfof(ctx, "Removed chroot %q", c.getChroot())
	}

	if c.releaseCPUs != nil {
		c.releaseCPUs()
	}
	return lastErr
}

func (c *FirecrackerContainer) closeMemoryStore(ctx context.Context) {
	_, span := tracing.StartSpan(ctx)
	defer span.End()
	c.memoryStore.Close()
	c.memoryStore = nil
}

func (c *FirecrackerContainer) stopUffdHandler(ctx context.Context) error {
	_, span := tracing.StartSpan(ctx)
	defer span.End()
	if err := c.uffdHandler.Stop(); err != nil {
		log.CtxErrorf(ctx, "Error stopping uffd handler: %s", err)
		return err
	}
	c.uffdHandler = nil
	return nil
}

// Unmounts any mounted VBD filesystems.
// If this func returns a nil error, then the VBD filesystems were successfully
// unmounted and the backing COWStores can no longer be accessed using
// VBD file handles.
func (c *FirecrackerContainer) unmountAllVBDs(ctx context.Context, logErrors bool) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	var lastErr error
	if c.scratchVBD != nil {
		if err := c.scratchVBD.Unmount(ctx); err != nil {
			if logErrors {
				log.CtxErrorf(ctx, "Failed to unmount scratch VBD: %s", err)
			}
			lastErr = err
		}
		c.scratchVBD = nil
	}
	if c.rootVBD != nil {
		if err := c.rootVBD.Unmount(ctx); err != nil {
			if logErrors {
				log.CtxErrorf(ctx, "Failed to unmount root VBD: %s", err)
			}
			lastErr = err
		}
		c.rootVBD = nil
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

	// Once the VM exits, delete the cgroup.
	// Jailer docs say that this cleanup must be handled by us:
	// https://github.com/firecracker-microvm/firecracker/blob/main/docs/jailer.md#observations
	if err := os.Remove(c.cgroupPath()); err != nil && !os.IsNotExist(err) {
		log.CtxWarningf(ctx, "Failed to remove jailer cgroup: %s", err)
	}

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

	if c.releaseCPUs != nil {
		c.releaseCPUs()
	}
	c.vmExec.conn.Close()
	c.vmExec.singleflight.Forget("")
	c.vmExec.conn, c.vmExec.err = nil, nil

	pauseTime := time.Since(start)
	log.CtxDebugf(ctx, "Pause took %s", pauseTime)

	c.observeStageDuration("pause", pauseTime)
	return err
}

func (c *FirecrackerContainer) pause(ctx context.Context) error {
	ctx, cancel := c.monitorVMContext(ctx)
	defer cancel()

	log.CtxInfof(ctx, "Pausing VM")

	snapDetails, err := c.snapshotDetails(ctx)
	if err != nil {
		return err
	}

	// Before taking a snapshot, set the workspace drive to point to an empty
	// file, so that we don't have to persist the workspace contents.
	if err := c.updateWorkspaceDriveToEmptyFile(ctx); err != nil {
		return status.WrapError(err, "update workspace drive to empty file")
	}

	if err = c.pauseVM(ctx); err != nil {
		return err
	}

	// If an older snapshot is present -- nuke it since we're writing a new one.
	if err = c.cleanupOldSnapshots(ctx, snapDetails); err != nil {
		return err
	}

	if err = c.createSnapshot(ctx, snapDetails); err != nil {
		return err
	}

	// Stop the VM, UFFD page fault handler, and VBD servers to ensure nothing
	// is modifying the snapshot files as we save them
	if err := c.stopMachine(ctx); err != nil {
		return err
	}
	if c.uffdHandler != nil {
		if err := c.uffdHandler.Stop(); err != nil {
			return status.WrapError(err, "stop uffd handler")
		}
		c.uffdHandler = nil
	}

	// Note: If the unmount fails, we will retry in `c.Remove`.
	// Don't log errors here because it may succeed the second try, especially
	// as we are extending the context for that cleanup.
	if err := c.unmountAllVBDs(ctx, false /*logErrors*/); err != nil {
		return status.WrapError(err, "unmount vbds")
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

func pointer[T any](val T) *T {
	return &val
}

func toInt32s(in []int) []int32 {
	out := make([]int32, len(in))
	for i, l := range in {
		out[i] = int32(l)
	}
	return out
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
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	machineStart := time.Now()
	snapshotTypeOpt := func(params *operations.CreateSnapshotParams) {
		params.Body.SnapshotType = snapshotDetails.snapshotType
	}
	if err := c.machine.CreateSnapshot(ctx, snapshotDetails.memSnapshotName, snapshotDetails.vmStateSnapshotName, snapshotTypeOpt); err != nil {
		log.CtxErrorf(ctx, "Error creating %s snapshot after %v: %s", snapshotDetails.snapshotType, time.Since(machineStart), err)
		return err
	}

	log.CtxDebugf(ctx, "VMM CreateSnapshot %s took %s", snapshotDetails.snapshotType, time.Since(machineStart))
	return nil
}

func (c *FirecrackerContainer) cleanupOldSnapshots(ctx context.Context, snapshotDetails *snapshotDetails) error {
	_, span := tracing.StartSpan(ctx)
	defer span.End()
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

func (c *FirecrackerContainer) setupCgroup(ctx context.Context) error {
	// Lease CPUs for task execution, and set cleanup function.
	leaseID := uuid.New()
	numaNode, leasedCPUs, cleanupFunc := c.env.GetCPULeaser().Acquire(c.vmConfig.NumCpus*1000, leaseID, cpuset.WithNoOverhead())
	log.CtxInfof(ctx, "Lease %s granted %+v cpus on numa node: %d", leaseID, leasedCPUs, numaNode)
	c.releaseCPUs = cleanupFunc
	c.cgroupSettings.CpusetCpus = toInt32s(leasedCPUs)
	c.cgroupSettings.NumaNode = pointer(int32(numaNode))

	if *debugDisableCgroup {
		return nil
	}
	if err := os.MkdirAll(c.cgroupPath(), 0755); err != nil {
		return status.UnavailableErrorf("create cgroup: %s", err)
	}
	if err := cgroup.Setup(ctx, c.cgroupPath(), c.cgroupSettings, c.blockDevice); err != nil {
		return status.UnavailableErrorf("setup cgroup: %s", err)
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
	c.observeStageDuration("unpause", unpauseTime)
	return err
}

func (c *FirecrackerContainer) unpause(ctx context.Context) error {
	c.recycled = true
	c.currentTaskInitTime = time.Now()

	log.CtxInfof(ctx, "Unpausing VM")

	// Don't hot-swap the workspace into the VM since we haven't yet downloaded inputs.
	return c.LoadSnapshot(ctx)
}

func (c *FirecrackerContainer) mountWorkspace(ctx context.Context, client vmxpb.ExecClient) error {
	ctx, cancel := context.WithTimeout(ctx, mountTimeout)
	defer cancel()
	_, err := client.MountWorkspace(ctx, &vmxpb.MountWorkspaceRequest{})
	return err
}

func (c *FirecrackerContainer) unmountWorkspace(ctx context.Context, client vmxpb.ExecClient) (*vmxpb.UnmountWorkspaceResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, mountTimeout)
	defer cancel()
	return client.UnmountWorkspace(ctx, &vmxpb.UnmountWorkspaceRequest{})
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
			return status.UnavailableErrorf("Firecracker VM crashed: %s", m[1])
		}
	}
	return nil
}

// parseOOMError looks for oom-kill entries in the kernel logs and returns an
// error if found.
func (c *FirecrackerContainer) parseOOMError(logTail string) error {
	if !strings.Contains(logTail, "oom-kill:") {
		return nil
	}
	lines := strings.Split(logTail, "\n")
	oomLines := ""
	for _, line := range lines {
		if strings.Contains(line, "oom-kill:") || strings.Contains(line, "Out of memory: Killed process") {
			oomLines += line + "\n"
		}
	}
	return status.ResourceExhaustedErrorf("some processes ran out of memory, and were killed:\n%s", oomLines)
}

// parseSegFault looks for segfaults in the kernel logs and returns an error if found.
func (c *FirecrackerContainer) parseSegFault(logTail string, cmdResult *interfaces.CommandResult) error {
	if !strings.Contains(string(cmdResult.Stderr), "SIGSEGV") {
		return nil
	}
	return status.UnavailableErrorf("process hit a segfault:\n%s", logTail)
}

func (c *FirecrackerContainer) observeStageDuration(taskStage string, d time.Duration) {
	metrics.FirecrackerStageDurationUsec.With(prometheus.Labels{
		metrics.Stage: taskStage,
	}).Observe(float64(d.Microseconds()))
}

func (c *FirecrackerContainer) SnapshotDebugString(ctx context.Context) string {
	if c.snapshot == nil {
		return ""
	}
	return snaploader.KeyDebugString(ctx, c.env, c.snapshot.GetKey(), c.supportsRemoteSnapshots)
}

func (c *FirecrackerContainer) VMConfig() *fcpb.VMConfiguration {
	return c.vmConfig
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

func getCPUID() *fcpb.CPUID {
	return &fcpb.CPUID{
		VendorId: int64(cpuid.CPU.VendorID),
		Family:   int64(cpuid.CPU.Family),
		Model:    int64(cpuid.CPU.Model),
	}
}

func getRandomNUMANode() (int, error) {
	b, err := os.ReadFile("/sys/devices/system/node/online")
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return 0, fmt.Errorf("unexpected empty file")
	}

	// Parse file contents
	// Example: "0-1,3" is parsed as []int{0, 1, 3}
	var nodes []int
	nodeRanges := strings.Split(s, ",")
	for _, r := range nodeRanges {
		startStr, endStr, _ := strings.Cut(r, "-")
		start, err := strconv.Atoi(startStr)
		if err != nil {
			return 0, fmt.Errorf("malformed file contents")
		}
		end := start
		if endStr != "" {
			n, err := strconv.Atoi(endStr)
			if err != nil {
				return 0, fmt.Errorf("malformed file contents")
			}
			if n < start {
				return 0, fmt.Errorf("malformed file contents")
			}
			end = n
		}
		for node := start; node <= end; node++ {
			nodes = append(nodes, node)
		}
	}

	return nodes[rand.IntN(len(nodes))], nil
}

func combineHostAndGuestStats(host, guest *repb.UsageStats) *repb.UsageStats {
	stats := host.CloneVT()
	// The guest exports some disk usage stats which we can't easily track on
	// the host without introspection into the ext4 metadata blocks - just
	// continue to get these from the guest for now.
	stats.PeakFileSystemUsage = guest.GetPeakFileSystemUsage()
	// Host memory usage stats might be confusing to the user, because the
	// firecracker process might hold some extra memory that isn't visible to
	// the guest. Use guest stats for memory usage too, for now.
	stats.MemoryBytes = guest.GetMemoryBytes()
	stats.PeakMemoryBytes = guest.GetPeakMemoryBytes()
	return stats
}

// Returns the paths relative to the workspace root that should be copied back
// to the action workspace directory after execution has completed.
//
// For performance reasons, we only extract the action's declared outputs,
// unless the action is running with preserve-workspace=true.
func workspacePathsToExtract(task *repb.ExecutionTask) []string {
	if platform.IsTrue(platform.FindEffectiveValue(task, platform.PreserveWorkspacePropertyName)) {
		return []string{"/"}
	}

	// Special files
	// TODO: declare this list as a constant somewhere?
	paths := []string{
		".BUILDBUDDY_DO_NOT_RECYCLE",
		".BUILDBUDDY_INVALIDATE_SNAPSHOT",
	}

	// Declared paths
	paths = append(paths, task.GetCommand().GetOutputDirectories()...)
	paths = append(paths, task.GetCommand().GetOutputFiles()...)
	paths = append(paths, task.GetCommand().GetOutputPaths()...)

	return paths
}

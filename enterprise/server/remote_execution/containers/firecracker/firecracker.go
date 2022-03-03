//go:build linux && !android
// +build linux,!android

package firecracker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/armon/circbuf"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/firecracker-microvm/firecracker-go-sdk/client/operations"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"

	containerutil "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/container"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
	vmfspb "github.com/buildbuddy-io/buildbuddy/proto/vmvfs"
	dockerclient "github.com/docker/docker/client"
	fcclient "github.com/firecracker-microvm/firecracker-go-sdk"
	fcmodels "github.com/firecracker-microvm/firecracker-go-sdk/client/models"
)

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

	// The names to use when creating snapshots (relative to chroot).
	vmStateSnapshotName = "vmstate.snap"
	fullMemSnapshotName = "full-mem.snap"
	diffMemSnapshotName = "diff-mem.snap"

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

	// The scratchfs image name and drive ID.
	scratchFSName  = "scratchfs.ext4"
	scratchDriveID = "scratchfs"
	// TODO(bduffany): Make this configurable
	scratchDiskSizeBytes = 2e9

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
)

var (
	locateBinariesOnce  sync.Once
	locateBinariesError error
	masqueradingOnce    sync.Once

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
		log.Debugf("Located %q at %s", fileName, path)
		return os.Open(path)
	}

	// Otherwise try to find it in the bundle or runfiles.
	return env.GetFileResolver().Open(fileName)
}

// putFileIntoDir finds "fileName" on the local filesystem, in runfiles, or
// in the bundle. It then puts that file into destdir (via hardlink or copying)
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
		log.Debugf("Found existing %q in path: %q", fileName, casPath)
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
	if err := writer.Close(); err != nil {
		return "", err
	}
	log.Debugf("Put %q into new path: %q", fileName, casPath)
	return casPath, nil
}

// waitUntilExists waits, up to maxWait, for localPath to exist. If the provided
// context is cancelled, this method returns immediately.
func waitUntilExists(ctx context.Context, maxWait time.Duration, localPath string) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()

	start := time.Now()
	defer func() {
		log.Debugf("Waited %s for %q to be created.", time.Since(start), localPath)
	}()

	ticker := time.NewTicker(1 * time.Millisecond)
	defer func() {
		cancel()
		ticker.Stop()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := os.Stat(localPath); err != nil {
				continue
			}
			return
		}
	}
}

func getLogrusLogger(debugMode bool) *logrus.Entry {
	logrusLogger := logrus.New()
	logrusLogger.SetLevel(logrus.ErrorLevel)
	if debugMode {
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

// Container invariants which cannot be changed across snapshot/resume cycles.
// Things like the container used to create the image, the numCPUs / RAM, etc.
// Importantly, the files attached in the actionWorkingDir, which are attached
// to the VM, can change. This string will be hashed into the snapshot ID, so
// changing this algorithm will invalidate all existing cached snapshots. Be
// careful!
type Constants struct {
	NumCPUs          int64
	MemSizeMB        int64
	DiskSlackSpaceMB int64
	EnableNetworking bool
	InitDockerd      bool
	DebugMode        bool
}

// FirecrackerContainer executes commands inside of a firecracker VM.
type FirecrackerContainer struct {
	id    string // a random GUID, unique per-run of firecracker
	vmIdx int    // the index of this vm on the host machine

	constants           Constants
	containerImage      string // the OCI container image. ex "alpine:latest"
	actionWorkingDir    string // the action directory with inputs / outputs
	workspaceFSPath     string // the path to the workspace ext4 image
	workspaceGeneration int    // the number of times the workspace has been re-mounted into the guest VM
	scratchFSPath       string // the path fo the scratch ext4 image
	containerFSPath     string // the path to the container ext4 image

	rmOnce *sync.Once
	rmErr  error

	// dockerClient is used to optimize image pulls by reusing image layers from
	// the Docker cache as well as deduping multiple requests for the same image.
	dockerClient *dockerclient.Client

	// when VFS is enabled, this contains the layout for the next execution
	fsLayout  *container.FileSystemLayout
	vfsServer *vfs_server.Server

	jailerRoot           string            // the root dir the jailer will work in
	machine              *fcclient.Machine // the firecracker machine object.
	vmLog                *VMLog
	env                  environment.Env
	imageCacheAuth       *container.ImageCacheAuthenticator
	pausedSnapshotDigest *repb.Digest
	allowSnapshotStart   bool
	mountWorkspaceFile   bool

	// If a container is resumed from a snapshot, the jailer
	// is started first using an external command and then the snapshot
	// is loaded. This slightly breaks the firecracker SDK's "Wait()"
	// method, so in that case we wait for the external jailer command
	// to finish, rather than calling "Wait()" on the sdk machine object.
	externalJailerCmd *exec.Cmd

	cleanupVethPair func(context.Context) error
}

// ConfigurationHash returns a digest that can be used to look up or save a
// cached snapshot for this container configuration.
func (c *FirecrackerContainer) ConfigurationHash() *repb.Digest {
	params := []string{
		fmt.Sprintf("cpus=%d", c.constants.NumCPUs),
		fmt.Sprintf("mb=%d", c.constants.MemSizeMB),
		fmt.Sprintf("net=%t", c.constants.EnableNetworking),
		fmt.Sprintf("dockerd=%t", c.constants.InitDockerd),
		fmt.Sprintf("debug=%t", c.constants.DebugMode),
		fmt.Sprintf("container=%s", c.containerImage),
	}
	return &repb.Digest{
		Hash:      hash.String(strings.Join(params, "&")),
		SizeBytes: int64(102),
	}
}

func NewContainer(env environment.Env, imageCacheAuth *container.ImageCacheAuthenticator, opts ContainerOpts) (*FirecrackerContainer, error) {
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

	c := &FirecrackerContainer{
		constants: Constants{
			NumCPUs:          opts.NumCPUs,
			MemSizeMB:        opts.MemSizeMB,
			DiskSlackSpaceMB: opts.DiskSlackSpaceMB,
			EnableNetworking: opts.EnableNetworking,
			InitDockerd:      opts.InitDockerd,
			DebugMode:        opts.DebugMode,
		},
		jailerRoot:         opts.JailerRoot,
		dockerClient:       opts.DockerClient,
		containerImage:     opts.ContainerImage,
		actionWorkingDir:   opts.ActionWorkingDirectory,
		env:                env,
		vmLog:              vmLog,
		imageCacheAuth:     imageCacheAuth,
		allowSnapshotStart: opts.AllowSnapshotStart,
		mountWorkspaceFile: env.GetConfigurator().GetExecutorConfig().FirecrackerMountWorkspaceFile,
	}

	if err := c.newID(); err != nil {
		return nil, err
	}
	if opts.ForceVMIdx != 0 {
		c.vmIdx = opts.ForceVMIdx
	}
	return c, nil
}

// mergeDiffSnapshot reads from diffSnapshotPath and writes all non-zero blocks into the baseSnapshotPath file.
func mergeDiffSnapshot(ctx context.Context, baseSnapshotPath string, diffSnapshotPath string, concurrency int, bufSize int) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	out, err := os.OpenFile(baseSnapshotPath, os.O_WRONLY, 0644)
	if err != nil {
		return status.UnavailableErrorf("Could not open base snapshot file %q: %s", baseSnapshotPath, err)
	}
	defer out.Close()

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

func (c *FirecrackerContainer) SaveSnapshot(ctx context.Context, instanceName string, d *repb.Digest, baseSnapshotDigest *repb.Digest) (*repb.Digest, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.Debugf("SaveSnapshot took %s", time.Since(start))
	}()

	// If a snapshot already exists, get a reference to the memory snapshot so that we can perform a diff snapshot and
	// merge the modified pages on top of the existing memory snapshot.
	baseMemSnapshotPath := ""
	if baseSnapshotDigest != nil {
		loader, err := snaploader.New(ctx, c.env, c.jailerRoot, instanceName, baseSnapshotDigest)
		if err != nil {
			return nil, err
		}
		baseDir := filepath.Join(c.getChroot(), "base")
		if err := disk.EnsureDirectoryExists(baseDir); err != nil {
			return nil, err
		}
		if err := loader.UnpackSnapshot(baseDir); err != nil {
			return nil, err
		}
		baseMemSnapshotPath = filepath.Join(baseDir, fullMemSnapshotName)
	}

	if err := c.machine.PauseVM(ctx); err != nil {
		log.Errorf("Error pausing VM: %s", err)
		return nil, err
	}

	snapshotType := diffSnapshotType
	memSnapshotFile := fullMemSnapshotName
	if baseSnapshotDigest != nil {
		memSnapshotFile = diffMemSnapshotName
	}
	memSnapshotPath := filepath.Join(c.getChroot(), memSnapshotFile)
	vmStateSnapshotPath := filepath.Join(c.getChroot(), vmStateSnapshotName)

	// If an older snapshot is present -- nuke it since we're writing a new one.
	disk.DeleteLocalFileIfExists(memSnapshotPath)
	disk.DeleteLocalFileIfExists(vmStateSnapshotPath)

	machineStart := time.Now()
	snapshotTypeOpt := func(params *operations.CreateSnapshotParams) {
		params.Body.SnapshotType = snapshotType
	}
	if err := c.machine.CreateSnapshot(ctx, memSnapshotFile, vmStateSnapshotName, snapshotTypeOpt); err != nil {
		log.Errorf("Error creating snapshot: %s", err)
		return nil, err
	}

	log.Debugf("VMM CreateSnapshot %s took %s", snapshotType, time.Since(machineStart))

	if baseMemSnapshotPath != "" {
		mergeStart := time.Now()
		if err := mergeDiffSnapshot(ctx, baseMemSnapshotPath, memSnapshotPath, mergeDiffSnapshotConcurrency, mergeDiffSnapshotBlockSize); err != nil {
			return nil, status.UnknownErrorf("merge diff snapshot failed: %s", err)
		}
		log.Debugf("VMM merge diff snapshot took %s", time.Since(mergeStart))
		// Use the merged memory snapshot.
		memSnapshotPath = baseMemSnapshotPath
	}

	configJson, err := json.Marshal(c.constants)
	if err != nil {
		return nil, err
	}
	opts := &snaploader.LoadSnapshotOptions{
		ConfigurationData:   configJson,
		MemSnapshotPath:     memSnapshotPath,
		VMStateSnapshotPath: vmStateSnapshotPath,
		KernelImagePath:     kernelImagePath,
		InitrdImagePath:     initrdImagePath,
		ContainerFSPath:     filepath.Join(c.getChroot(), containerFSName),
		ScratchFSPath:       filepath.Join(c.getChroot(), scratchFSName),
		WorkspaceFSPath:     c.workspaceFSPath,
		ForceSnapshotDigest: d,
	}

	snaploaderStart := time.Now()
	snapshotDigest, err := snaploader.CacheSnapshot(ctx, c.env, instanceName, c.jailerRoot, opts)
	if err != nil {
		return nil, err
	}
	log.Debugf("snaploader.CacheSnapshot took %s", time.Since(snaploaderStart))

	resumeStart := time.Now()
	if err := c.machine.ResumeVM(ctx); err != nil {
		return nil, err
	}
	log.Debugf("VMM ResumeVM took %s", time.Since(resumeStart))

	return snapshotDigest, nil
}

// LoadSnapshot loads a VM snapshot from the given snapshot digest and resumes
// the VM. If workspaceDirOverride is set, it will also hot-swap the workspace
// drive; otherwise, the workspace will be loaded as-is from the snapshot.
func (c *FirecrackerContainer) LoadSnapshot(ctx context.Context, workspaceDirOverride string, instanceName string, snapshotDigest *repb.Digest) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.Debugf("LoadSnapshot %s took %s", snapshotDigest.GetHash(), time.Since(start))
	}()

	c.rmOnce = &sync.Once{}
	c.rmErr = nil

	if err := c.newID(); err != nil {
		return err
	}

	// We start firecracker with this reduced config because we will load a
	// snapshot that is already configured.
	cfg := fcclient.Config{
		SocketPath:        firecrackerSocketPath,
		DisableValidation: true,
		JailerCfg: &fcclient.JailerConfig{
			JailerBinary:   jailerBinPath,
			ChrootBaseDir:  c.jailerRoot,
			ID:             c.id,
			UID:            fcclient.Int(unix.Geteuid()),
			GID:            fcclient.Int(unix.Getegid()),
			NumaNode:       fcclient.Int(0),
			ExecFile:       firecrackerBinPath,
			ChrootStrategy: fcclient.NewNaiveChrootStrategy(""),
		},
		ForwardSignals: make([]os.Signal, 0),
	}

	loader, err := snaploader.New(ctx, c.env, c.jailerRoot, instanceName, snapshotDigest)
	if err != nil {
		return err
	}

	configurationData, err := loader.GetConfigurationData()
	if err != nil {
		return err
	}
	if err := json.Unmarshal(configurationData, &c.constants); err != nil {
		return err
	}

	if err := c.setupNetworking(ctx); err != nil {
		return err
	}

	vmCtx := context.Background()
	cmd := c.getJailerCommand(vmCtx)
	machineOpts := []fcclient.Opt{
		fcclient.WithLogger(getLogrusLogger(c.constants.DebugMode)),
		fcclient.WithProcessRunner(cmd),
	}

	// Start Firecracker
	err = cmd.Start()
	if err != nil {
		return status.InternalErrorf("Failed starting firecracker binary: %s", err)
	}

	c.externalJailerCmd = cmd

	// Wait for the jailer directory to be created. We have to do this because we
	// are starting the command ourselves and loading a snapshot, rather than
	// going through the normal flow and letting the library start the cmd.
	waitUntilExists(ctx, jailerDirectoryCreationTimeout, c.getChroot())

	// Wait until jailer directory exists because the host vsock socket is created in that directory.
	if err := c.setupVFSServer(ctx); err != nil {
		return err
	}

	if err := loader.UnpackSnapshot(c.getChroot()); err != nil {
		return err
	}

	machine, err := fcclient.NewMachine(vmCtx, cfg, machineOpts...)
	if err != nil {
		return status.InternalErrorf("Failed creating machine: %s", err)
	}
	c.machine = machine

	socketWaitStart := time.Now()
	errCh := make(chan error)
	if err := c.machine.WaitForSocket(firecrackerSocketWaitTimeout, errCh); err != nil {
		return status.InternalErrorf("timeout waiting for firecracker socket: %s", err)
	}
	log.Debugf("waitforsocket took %s", time.Since(socketWaitStart))
	enableDiffSnapshotsOpt := func(params *operations.LoadSnapshotParams) {
		params.Body.EnableDiffSnapshots = true
	}
	if err := c.machine.LoadSnapshot(ctx, fullMemSnapshotName, vmStateSnapshotName, enableDiffSnapshotsOpt); err != nil {
		return status.InternalErrorf("error loading snapshot: %s", err)
	}
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

	if workspaceDirOverride != "" {
		// If the snapshot is being loaded with a different workspaceFS
		// then handle that now.
		wsImgRelPath, err := c.createWorkspaceImage(ctx, workspaceDirOverride)
		if err != nil {
			return err
		}
		if err := c.hotSwapWorkspace(ctx, execClient, wsImgRelPath); err != nil {
			return err
		}
	}

	return nil
}

// createWorkspaceImage creates a new ext4 image from the action working dir
// and returns the chroot-relative path to the created image.
func (c *FirecrackerContainer) createWorkspaceImage(ctx context.Context, workspacePath string) (string, error) {
	c.workspaceGeneration++
	relativePath := fmt.Sprintf("%s.gen_%d.ext4", workspaceFSName, c.workspaceGeneration)
	hostPath := filepath.Join(c.getChroot(), relativePath)
	sizeBytes, err := disk.DirSize(workspacePath)
	if err != nil {
		return "", err
	}
	if err := ext4.DirectoryToImage(ctx, workspacePath, hostPath, sizeBytes+c.constants.DiskSlackSpaceMB*1e6); err != nil {
		return "", err
	}
	c.workspaceFSPath = hostPath
	return relativePath, nil
}

// hotSwapWorkspace unmounts the workspace drive from a running firecracker
// container, updates the workspace block device to an ext4 image pointed to
// by chrootRelativeImagePath, and re-mounts the drive.
func (c *FirecrackerContainer) hotSwapWorkspace(ctx context.Context, execClient vmxpb.ExecClient, chrootRelativeImagePath string) error {
	log.Infof("Hot-swapping guest workspace drive")
	if _, err := execClient.UnmountWorkspace(ctx, &vmxpb.UnmountWorkspaceRequest{}); err != nil {
		return status.WrapError(err, "Failed to unmount workspace")
	}
	if err := c.machine.UpdateGuestDrive(ctx, workspaceDriveID, chrootRelativeImagePath); err != nil {
		return status.InternalErrorf("error updating workspace drive attached to snapshot: %s", err)
	}
	if _, err := execClient.MountWorkspace(ctx, &vmxpb.MountWorkspaceRequest{}); err != nil {
		return status.WrapError(err, "Failed to remount workspace after update")
	}
	return nil
}

func nonCmdExit(err error) *interfaces.CommandResult {
	log.Errorf("nonCmdExit returning error: %s", err)
	return &interfaces.CommandResult{
		Error:    err,
		ExitCode: -2,
	}
}

func (c *FirecrackerContainer) startedFromSnapshot() bool {
	return c.externalJailerCmd != nil
}

func (c *FirecrackerContainer) newID() error {
	vmIdxMu.Lock()
	defer vmIdxMu.Unlock()
	u, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	vmIdx += 1
	log.Debugf("Container id changing from %q (%d) to %q (%d)", c.id, c.vmIdx, u.String(), vmIdx)
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

func (c *FirecrackerContainer) getJailerCommand(ctx context.Context) *exec.Cmd {
	builder := fcclient.NewJailerCommandBuilder().
		WithBin(jailerBinPath).
		WithChrootBaseDir(c.jailerRoot).
		WithID(c.id).
		WithUID(unix.Geteuid()).
		WithGID(unix.Getegid()).
		WithNumaNode(0). // TODO(tylerw): randomize this?
		WithExecFile(firecrackerBinPath).
		WithFirecrackerArgs("--api-sock", firecrackerSocketPath)

	if c.constants.EnableNetworking {
		builder = builder.WithNetNS("/var/run/netns/" + c.id)
	}
	var stdout io.Writer = c.vmLog
	var stderr io.Writer = c.vmLog
	if c.constants.DebugMode {
		stdout = io.MultiWriter(stdout, os.Stdout)
		stderr = io.MultiWriter(stderr, os.Stderr)
		builder = builder.WithStdin(os.Stdin)
	}
	builder = builder.WithStdout(stdout).WithStderr(stderr)
	return builder.Build(ctx)
}

func (c *FirecrackerContainer) getConfig(ctx context.Context, containerFS, scratchFS, workspaceFS string) (*fcclient.Config, error) {
	bootArgs := "ro console=ttyS0 noapic reboot=k panic=1 pci=off nomodules=1 random.trust_cpu=on i8042.noaux=1 tsc=reliable ipv6.disable=1"
	if c.constants.EnableNetworking {
		bootArgs += " " + machineIPBootArgs
	}

	// End the kernel args, before passing some more args to init.
	if !c.constants.DebugMode {
		bootArgs += " quiet"
	}

	// Pass some flags to the init script.
	if c.constants.DebugMode {
		bootArgs = "-debug_mode " + bootArgs
	}
	if c.constants.EnableNetworking {
		bootArgs = "-set_default_route " + bootArgs
	}
	if c.constants.InitDockerd {
		bootArgs = "-init_dockerd " + bootArgs
	}
	cfg := &fcclient.Config{
		VMID:            c.id,
		SocketPath:      firecrackerSocketPath,
		KernelImagePath: kernelImagePath,
		InitrdPath:      initrdImagePath,
		KernelArgs:      bootArgs,
		ForwardSignals:  make([]os.Signal, 0),
		// Note: ordering in this list determines the device lettering
		// (/dev/vda, /dev/vdb, /dev/vdc, ...)
		Drives: []fcmodels.Drive{
			fcmodels.Drive{
				DriveID:      fcclient.String(containerDriveID),
				PathOnHost:   &containerFS,
				IsRootDevice: fcclient.Bool(false),
				IsReadOnly:   fcclient.Bool(true),
			},
			fcmodels.Drive{
				DriveID:      fcclient.String(scratchDriveID),
				PathOnHost:   &scratchFS,
				IsRootDevice: fcclient.Bool(false),
				IsReadOnly:   fcclient.Bool(false),
			},
			fcmodels.Drive{
				DriveID:      fcclient.String(workspaceDriveID),
				PathOnHost:   &workspaceFS,
				IsRootDevice: fcclient.Bool(false),
				IsReadOnly:   fcclient.Bool(false),
			},
		},
		VsockDevices: []fcclient.VsockDevice{
			fcclient.VsockDevice{
				Path: firecrackerVSockPath,
			},
		},
		JailerCfg: &fcclient.JailerConfig{
			JailerBinary:   jailerBinPath,
			ChrootBaseDir:  c.jailerRoot,
			ID:             c.id,
			UID:            fcclient.Int(unix.Geteuid()),
			GID:            fcclient.Int(unix.Getegid()),
			NumaNode:       fcclient.Int(0),
			ExecFile:       firecrackerBinPath,
			ChrootStrategy: fcclient.NewNaiveChrootStrategy(kernelImagePath),
		},
		MachineCfg: fcmodels.MachineConfiguration{
			VcpuCount:       fcclient.Int64(c.constants.NumCPUs),
			MemSizeMib:      fcclient.Int64(c.constants.MemSizeMB),
			HtEnabled:       fcclient.Bool(false),
			TrackDirtyPages: true,
		},
	}

	if c.constants.EnableNetworking {
		cfg.NetworkInterfaces = []fcclient.NetworkInterface{
			{
				StaticConfiguration: &fcclient.StaticNetworkConfiguration{
					HostDevName: tapDeviceName,
					MacAddress:  tapDeviceMac,
				},
			},
		}
	}
	cfg.JailerCfg.Stdout = c.vmLog
	cfg.JailerCfg.Stderr = c.vmLog
	if c.constants.DebugMode {
		cfg.JailerCfg.Stdout = io.MultiWriter(cfg.JailerCfg.Stdout, os.Stdout)
		cfg.JailerCfg.Stderr = io.MultiWriter(cfg.JailerCfg.Stderr, os.Stderr)
		cfg.JailerCfg.Stdin = os.Stdin
	}
	return cfg, nil
}

func copyStaticFiles(ctx context.Context, env environment.Env, workingDir string) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	locateBinariesOnce.Do(func() {
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

	start := time.Now()
	defer func() {
		log.Debugf("copyOutputsToWorkspace took %s", time.Since(start))
	}()
	if exists, err := disk.FileExists(ctx, c.workspaceFSPath); err != nil || !exists {
		return status.FailedPreconditionErrorf("workspacefs path %q not found", c.workspaceFSPath)
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
		m, err := mountExt4ImageUsingLoopDevice(c.workspaceFSPath, wsDir)
		if err != nil {
			log.Warningf("could not mount ext4 image: %s", err)
			return err
		}
		defer m.Unmount()
	} else {
		if err := ext4.ImageToDirectory(ctx, c.workspaceFSPath, wsDir); err != nil {
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
	if !c.constants.EnableNetworking {
		return nil
	}
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
	if !c.constants.EnableNetworking {
		return nil
	}
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
		log.Debugf("Run took %s", time.Since(start))
	}()

	snapDigest := c.ConfigurationHash()

	// See if we can lookup a cached snapshot to run from; if not, it's not
	// a huge deal, we can start a new VM and create one.
	if c.allowSnapshotStart {
		if err := c.LoadSnapshot(ctx, actionWorkingDir, "" /*=instanceName*/, snapDigest); err == nil {
			log.Debugf("Started from snapshot %s/%d!", snapDigest.GetHash(), snapDigest.GetSizeBytes())
		}
	}

	// If a snapshot was already loaded, then c.machine will be set, so
	// there's no need to Create the machine.
	if c.machine == nil {
		log.Debugf("Pulling image %q", c.containerImage)
		if err := container.PullImageIfNecessary(ctx, c.env, c.imageCacheAuth, c, creds, c.containerImage); err != nil {
			return nonCmdExit(err)
		}

		if err := c.Create(ctx, actionWorkingDir); err != nil {
			return nonCmdExit(err)
		}

		if c.allowSnapshotStart && !c.startedFromSnapshot() {
			// save the workspaceFSPath in a local variable and null it out
			// before saving the snapshot so it's not uploaded.
			wsPath := c.workspaceFSPath
			c.workspaceFSPath = ""
			if _, err := c.SaveSnapshot(ctx, "" /*=instanceName*/, snapDigest, nil /*=baseSnapshotDigest*/); err != nil {
				return nonCmdExit(err)
			}
			c.workspaceFSPath = wsPath
			log.Debugf("Saved snapshot %s/%d for next run", snapDigest.GetHash(), snapDigest.GetSizeBytes())
		}
	}

	defer c.Remove(ctx)

	cmdResult := c.Exec(ctx, command, nil /*=stdin*/, nil /*=stdout*/)
	return cmdResult
}

// Create creates a new VM and starts a top-level process inside it listening
// for commands to execute.
func (c *FirecrackerContainer) Create(ctx context.Context, actionWorkingDir string) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.Debugf("Create took %s", time.Since(start))
	}()

	c.rmOnce = &sync.Once{}
	c.rmErr = nil

	c.actionWorkingDir = actionWorkingDir
	workspaceSizeBytes, err := disk.DirSize(c.actionWorkingDir)
	if err != nil {
		return err
	}

	containerHome, err := os.MkdirTemp(c.jailerRoot, "fc-container-*")
	if err != nil {
		return err
	}
	scratchPath := filepath.Join(containerHome, scratchFSName)
	if err := ext4.MakeEmptyImage(ctx, scratchPath, scratchDiskSizeBytes); err != nil {
		return err
	}
	c.scratchFSPath = scratchPath
	wsPath := filepath.Join(containerHome, workspaceFSName)
	if err := ext4.DirectoryToImage(ctx, c.actionWorkingDir, wsPath, workspaceSizeBytes+(c.constants.DiskSlackSpaceMB*1e6)); err != nil {
		return err
	}
	c.workspaceFSPath = wsPath

	log.Debugf("c.containerFSPath: %q", c.containerFSPath)
	log.Debugf("c.scratchFSPath: %q", c.scratchFSPath)
	log.Debugf("c.workspaceFSPath: %q", c.workspaceFSPath)
	log.Debugf("getChroot() is %q", c.getChroot())
	fcCfg, err := c.getConfig(ctx, c.containerFSPath, c.scratchFSPath, c.workspaceFSPath)
	if err != nil {
		return err
	}

	if err := c.setupNetworking(ctx); err != nil {
		return err
	}

	if err := c.setupVFSServer(ctx); err != nil {
		return err
	}

	vmCtx := context.Background()

	machineOpts := []fcclient.Opt{
		fcclient.WithLogger(getLogrusLogger(c.constants.DebugMode)),
		fcclient.WithProcessRunner(c.getJailerCommand(vmCtx)),
	}

	m, err := fcclient.NewMachine(vmCtx, *fcCfg, machineOpts...)
	if err != nil {
		return status.InternalErrorf("Failed creating machine: %s", err)
	}
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

func (c *FirecrackerContainer) SendExecRequestToGuest(ctx context.Context, req *vmxpb.ExecRequest) (*vmxpb.ExecResponse, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// TODO(bduffany): Reuse connection from Unpause(), if applicable
	conn, err := c.dialVMExecServer(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Firecracker exec failed: failed to dial VM exec port: %s", err)
	}
	defer conn.Close()

	client := vmxpb.NewExecClient(conn)
	return c.vmExec(ctx, client, req)
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
		}
		return nil, status.InternalErrorf("Failed to connect to firecracker VM exec server: %s", err)
	}
	return conn, nil
}

func (c *FirecrackerContainer) vmExec(ctx context.Context, client vmxpb.ExecClient, req *vmxpb.ExecRequest) (*vmxpb.ExecResponse, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	rsp, err := client.Exec(ctx, req)
	if err != nil {
		return nil, status.WrapError(err, "Firecracker exec failed")
	}
	return rsp, nil
}

func (c *FirecrackerContainer) SendPrepareFileSystemRequestToGuest(ctx context.Context, req *vmfspb.PrepareRequest) (*vmfspb.PrepareResponse, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	p, err := vfs_server.NewCASLazyFileProvider(c.env, ctx, c.fsLayout.RemoteInstanceName, c.fsLayout.Inputs)
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
func (c *FirecrackerContainer) Exec(ctx context.Context, cmd *repb.Command, stdin io.Reader, stdout io.Writer) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.Debugf("Exec took %s", time.Since(start))
	}()

	result := &interfaces.CommandResult{
		CommandDebugString: fmt.Sprintf("(firecracker) %s", cmd.GetArguments()),
		ExitCode:           commandutil.NoExitCode,
	}

	if c.fsLayout != nil {
		req := &vmfspb.PrepareRequest{}
		_, err := c.SendPrepareFileSystemRequestToGuest(ctx, req)
		if err != nil {
			result.Error = err
			return result
		}
	}

	execRequest := &vmxpb.ExecRequest{
		Arguments:        cmd.GetArguments(),
		WorkingDirectory: "/workspace/",
	}
	if c.fsLayout != nil {
		execRequest.WorkingDirectory = guestVFSMountDir
	}
	for _, ev := range cmd.GetEnvironmentVariables() {
		execRequest.EnvironmentVariables = append(execRequest.EnvironmentVariables, &vmxpb.ExecRequest_EnvironmentVariable{
			Name: ev.GetName(), Value: ev.GetValue(),
		})
	}

	rsp, err := c.SendExecRequestToGuest(ctx, execRequest)
	if err != nil {
		result.Error = err
		return result
	}

	// If FUSE is enabled then outputs are already in the workspace.
	if c.fsLayout == nil {
		// Command was successful, let's unpack the files back to our
		// workspace directory now.
		if err := c.machine.PauseVM(ctx); err != nil {
			result.Error = status.InternalErrorf("error pausing VM: %s", err)
			return result
		}

		copyOutputsErr := c.copyOutputsToWorkspace(ctx)
		if err := c.machine.ResumeVM(ctx); err != nil {
			result.Error = status.InternalErrorf("error resuming VM: %s", err)
			return result
		}

		if copyOutputsErr != nil {
			result.Error = copyOutputsErr
			return result
		}
	}

	result.ExitCode = int(rsp.GetExitCode())
	result.Stdout = rsp.GetStdout()
	result.Stderr = rsp.GetStderr()
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
		log.Debugf("PullImage took %s", time.Since(start))
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
		log.Debugf("Remove took %s", time.Since(start))
	}()

	if c.rmOnce == nil {
		return status.FailedPreconditionError("Attempted to remove a container that is not created")
	}
	c.rmOnce.Do(func() {
		c.rmErr = c.remove(ctx)
	})
	return c.rmErr
}

func (c *FirecrackerContainer) remove(ctx context.Context) error {
	var lastErr error

	if err := c.machine.Shutdown(ctx); err != nil {
		log.Errorf("Error shutting down machine: %s", err)
		lastErr = err
	}
	if err := c.machine.StopVMM(); err != nil {
		log.Errorf("Error stopping VM: %s", err)
		lastErr = err
	}
	if err := c.cleanupNetworking(ctx); err != nil {
		log.Errorf("Error cleaning up networking: %s", err)
		lastErr = err
	}
	if err := os.RemoveAll(filepath.Dir(c.workspaceFSPath)); err != nil {
		log.Errorf("Error removing workspace fs: %s", err)
		lastErr = err
	}
	if err := os.RemoveAll(filepath.Dir(c.getChroot())); err != nil {
		log.Errorf("Error removing chroot: %s", err)
		lastErr = err
	}
	if c.vfsServer != nil {
		c.vfsServer.Stop()
		c.vfsServer = nil
	}
	return lastErr
}

// Pause freezes a container so that it no longer consumes CPU resources.
func (c *FirecrackerContainer) Pause(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	start := time.Now()
	defer func() {
		log.Debugf("Pause took %s", time.Since(start))
	}()
	snapshotDigest, err := c.SaveSnapshot(ctx, "" /*=instanceName*/, nil /*=digest*/, c.pausedSnapshotDigest)
	if err != nil {
		log.Errorf("Error saving snapshot: %s", err)
		return err
	}
	c.pausedSnapshotDigest = snapshotDigest

	if err := c.Remove(ctx); err != nil {
		log.Errorf("Error cleaning up after pause: %s", err)
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
		log.Debugf("Unpause took %s", time.Since(start))
	}()

	// Don't hot-swap the workspace into the VM since we haven't yet downloaded inputs.
	return c.LoadSnapshot(ctx, "" /*=workspaceOverride*/, "" /*=instanceName*/, c.pausedSnapshotDigest)
}

// SyncWorkspace creates a new disk image from the given working directory
// and hot-swaps the currently mounted workspace drive in the guest.
//
// This is intended to be called just before Exec, so that the inputs to
// the executed action will be made available to the VM.
func (c *FirecrackerContainer) SyncWorkspace(ctx context.Context) error {
	// TODO(bduffany): reuse the connection created in Unpause(), if applicable
	conn, err := c.dialVMExecServer(ctx)
	if err != nil {
		return err
	}
	execClient := vmxpb.NewExecClient(conn)

	chrootRelativeImagePath, err := c.createWorkspaceImage(ctx, c.actionWorkingDir)
	if err != nil {
		return err
	}

	return c.hotSwapWorkspace(ctx, execClient, chrootRelativeImagePath)
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

func (c *FirecrackerContainer) Stats(ctx context.Context) (*container.Stats, error) {
	return &container.Stats{}, nil
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
	return log.buf.Bytes()
}

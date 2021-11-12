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
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vfs_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"

	containerutil "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/container"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
	vmfspb "github.com/buildbuddy-io/buildbuddy/proto/vmvfs"
	fcclient "github.com/firecracker-microvm/firecracker-go-sdk"
	fcmodels "github.com/firecracker-microvm/firecracker-go-sdk/client/models"
)

const (
	// How long to wait for the VMM to listen on the firecracker socket.
	firecrackerSocketWaitTimeout = 3 * time.Second

	// How long to wait when dialing the vmexec server inside the VM.
	vSocketDialTimeout = 3 * time.Second

	// How long to wait for the jailer directory to be created.
	jailerDirectoryCreationTimeout = 1 * time.Second

	// The firecracker socket path (will be relative to the chroot).
	firecrackerSocketPath = "/run/fc.sock"

	// The vSock path (also relative to the chroot).
	firecrackerVSockPath = "/run/v.sock"

	// The names to use when creating a full snapshot (relative to chroot).
	fullDiskSnapshotName = "full-disk.snap"
	fullMemSnapshotName  = "full-mem.snap"

	// The workspacefs image name and drive ID.
	workspaceFSName  = "workspacefs.ext4"
	workspaceDriveID = "workspacefs"

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
	if exists, err := disk.FileExists(casPath); err == nil && exists {
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

// Container invariants which cannot bechanged across snapshot/resume cycles.
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
	DebugMode        bool
}

// FirecrackerContainer executes commands inside of a firecracker VM.
type FirecrackerContainer struct {
	id    string // a random GUID, unique per-run of firecracker
	vmIdx int    // the index of this vm on the host machine

	constants        Constants
	containerImage   string // the OCI container image. ex "alpine:latest"
	actionWorkingDir string // the action directory with inputs / outputs
	workspaceFSPath  string // the path to the workspace ext4 image
	containerFSPath  string // the path to the container ext4 image

	// when VFS is enabled, this contains the layout for the next execution
	fsLayout  *container.FileSystemLayout
	vfsServer *vfs_server.Server

	jailerRoot           string            // the root dir the jailer will work in
	machine              *fcclient.Machine // the firecracker machine object.
	env                  environment.Env
	imageCacheAuth       *container.ImageCacheAuthenticator
	pausedSnapshotDigest *repb.Digest
	allowSnapshotStart   bool

	// If a container is resumed from a snapshot, the jailer
	// is started first using an external command and then the snapshot
	// is loaded. This slightly breaks the firecracker SDK's "Wait()"
	// method, so in that case we wait for the external jailer command
	// to finish, rather than calling "Wait()" on the sdk machine object.
	externalJailerCmd *exec.Cmd
}

// ConfigurationHash returns a digest that can be used to look up or save a
// cached snapshot for this container configuration.
func (c *FirecrackerContainer) ConfigurationHash() *repb.Digest {
	params := []string{
		fmt.Sprintf("cpus=%d", c.constants.NumCPUs),
		fmt.Sprintf("mb=%d", c.constants.MemSizeMB),
		fmt.Sprintf("net=%t", c.constants.EnableNetworking),
		fmt.Sprintf("debug=%t", c.constants.DebugMode),
		fmt.Sprintf("container=%s", c.containerImage),
	}
	return &repb.Digest{
		Hash:      hash.String(strings.Join(params, "&")),
		SizeBytes: int64(102),
	}
}

func NewContainer(env environment.Env, imageCacheAuth *container.ImageCacheAuthenticator, opts ContainerOpts) (*FirecrackerContainer, error) {
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
			DebugMode:        opts.DebugMode,
		},
		jailerRoot:         opts.JailerRoot,
		containerImage:     opts.ContainerImage,
		actionWorkingDir:   opts.ActionWorkingDirectory,
		env:                env,
		imageCacheAuth:     imageCacheAuth,
		allowSnapshotStart: opts.AllowSnapshotStart,
	}

	if err := c.newID(); err != nil {
		return nil, err
	}
	if opts.ForceVMIdx != 0 {
		c.vmIdx = opts.ForceVMIdx
	}
	return c, nil
}

func (c *FirecrackerContainer) SaveSnapshot(ctx context.Context, instanceName string, d *repb.Digest) (*repb.Digest, error) {
	start := time.Now()
	defer func() {
		log.Debugf("SaveSnapshot took %s", time.Since(start))
	}()

	if err := c.machine.PauseVM(ctx); err != nil {
		log.Errorf("Error pausing VM: %s", err)
		return nil, err
	}
	memSnapshotPath := filepath.Join(c.getChroot(), fullMemSnapshotName)
	diskSnapshotPath := filepath.Join(c.getChroot(), fullDiskSnapshotName)

	// If an older snapshot is present -- nuke it since we're writing a new one.
	disk.DeleteLocalFileIfExists(memSnapshotPath)
	disk.DeleteLocalFileIfExists(diskSnapshotPath)

	machineStart := time.Now()
	if err := c.machine.CreateSnapshot(ctx, fullMemSnapshotName, fullDiskSnapshotName); err != nil {
		log.Errorf("Error creating snapshot: %s", err)
		return nil, err
	}
	log.Debugf("VMM CreateSnapshot took %s", time.Since(machineStart))

	configJson, err := json.Marshal(c.constants)
	if err != nil {
		return nil, err
	}
	opts := &snaploader.LoadSnapshotOptions{
		ConfigurationData:   configJson,
		MemSnapshotPath:     memSnapshotPath,
		DiskSnapshotPath:    diskSnapshotPath,
		KernelImagePath:     kernelImagePath,
		InitrdImagePath:     initrdImagePath,
		ContainerFSPath:     filepath.Join(c.getChroot(), containerFSName),
		WorkspaceFSPath:     filepath.Join(c.getChroot(), workspaceFSName),
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

func (c *FirecrackerContainer) LoadSnapshot(ctx context.Context, workspaceDirOverride, instanceName string, snapshotDigest *repb.Digest) error {
	start := time.Now()
	defer func() {
		log.Debugf("LoadSnapshot %s took %s", snapshotDigest.GetHash(), time.Since(start))
	}()

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
	if err := c.setupVFSServer(); err != nil {
		return err
	}

	if err := loader.UnpackSnapshot(c.getChroot()); err != nil {
		return err
	}

	workspaceFileInChroot := filepath.Join(c.getChroot(), workspaceFSName)
	if workspaceDirOverride != "" {
		// Put the filesystem in place
		workspaceSizeBytes, err := disk.DirSize(workspaceDirOverride)
		if err != nil {
			return err
		}
		if err := ext4.DirectoryToImage(ctx, workspaceDirOverride, workspaceFileInChroot, workspaceSizeBytes+(c.constants.DiskSlackSpaceMB*1e6)); err != nil {
			return err
		}
	}
	c.workspaceFSPath = workspaceFileInChroot

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
	if err := c.machine.LoadSnapshot(ctx, fullMemSnapshotName, fullDiskSnapshotName); err != nil {
		return status.InternalErrorf("error loading snapshot: %s", err)
	}
	if workspaceDirOverride != "" {
		// If the snapshot is being loaded with a different workspaceFS
		// then handle that now.
		if err := c.machine.UpdateGuestDrive(ctx, workspaceDriveID, workspaceFSName); err != nil {
			return status.InternalErrorf("error updating workspace drive attached to snapshot: %s", err)
		}
	}
	if err := c.machine.ResumeVM(ctx); err != nil {
		return status.InternalErrorf("error resuming VM: %s", err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, vSocketDialTimeout)
	defer cancel()

	vsockPath := filepath.Join(c.getChroot(), firecrackerVSockPath)
	conn, err := vsock.SimpleGRPCDial(dialCtx, vsockPath, vsock.VMExecPort)
	if err != nil {
		return err
	}
	defer conn.Close()

	execClient := vmxpb.NewExecClient(conn)
	_, err = execClient.Initialize(ctx, &vmxpb.InitializeRequest{
		UnixTimestampNanoseconds: time.Now().UnixNano(),
		ClearArpCache:            true,
	})
	return err
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
	if c.constants.DebugMode {
		builder = builder.WithStdin(os.Stdin).WithStdout(os.Stdout).WithStderr(os.Stderr)
	}
	return builder.Build(ctx)
}

func (c *FirecrackerContainer) getConfig(ctx context.Context, containerFS, workspaceFS string) (*fcclient.Config, error) {
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
		bootArgs = "--debug_mode " + bootArgs
	}
	if c.constants.EnableNetworking {
		bootArgs = "--set_default_route " + bootArgs
	}
	cfg := &fcclient.Config{
		VMID:            c.id,
		SocketPath:      firecrackerSocketPath,
		KernelImagePath: kernelImagePath,
		InitrdPath:      initrdImagePath,
		KernelArgs:      bootArgs,
		ForwardSignals:  make([]os.Signal, 0),
		Drives: []fcmodels.Drive{
			fcmodels.Drive{
				DriveID:      fcclient.String(containerDriveID),
				PathOnHost:   &containerFS,
				IsRootDevice: fcclient.Bool(false),
				IsReadOnly:   fcclient.Bool(true),
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
			VcpuCount:  fcclient.Int64(c.constants.NumCPUs),
			MemSizeMib: fcclient.Int64(c.constants.MemSizeMB),
			HtEnabled:  fcclient.Bool(false),
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
	if c.constants.DebugMode {
		cfg.JailerCfg.Stdout = os.Stdout
		cfg.JailerCfg.Stderr = os.Stderr
		cfg.JailerCfg.Stdin = os.Stdin
	}
	return cfg, nil
}

func copyStaticFiles(ctx context.Context, env environment.Env, workingDir string) error {
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

// copyOutputsToWorkspace copies output files from the workspace filesystem
// image to the local filesystem workdir. It will not overwrite existing files
// and it will skip copying rootfs-overlay files. Callers should ensure that
// data has already been synced to the workspace filesystem and the VM has
// been paused before calling this.
func (c *FirecrackerContainer) copyOutputsToWorkspace(ctx context.Context) error {
	start := time.Now()
	defer func() {
		log.Debugf("copyOutputsToWorkspace took %s", time.Since(start))
	}()
	if exists, err := disk.FileExists(c.workspaceFSPath); err != nil || !exists {
		return status.FailedPreconditionErrorf("workspacefs path %q not found", c.workspaceFSPath)
	}
	if exists, err := disk.FileExists(c.actionWorkingDir); err != nil || !exists {
		return status.FailedPreconditionErrorf("actionWorkingDir path %q not found", c.actionWorkingDir)
	}

	unpackDir, err := os.MkdirTemp(c.jailerRoot, "unpacked-workspacefs-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(unpackDir) // clean up

	if err := ext4.ImageToDirectory(ctx, c.workspaceFSPath, unpackDir); err != nil {
		return err
	}
	walkErr := fs.WalkDir(os.DirFS(unpackDir), ".", func(path string, d fs.DirEntry, err error) error {
		// Skip filesystem layerfs write-layer files.
		if strings.HasPrefix(path, "bbvmroot/") || strings.HasPrefix(path, "bbvmwork/") {
			return nil
		}
		targetLocation := filepath.Join(c.actionWorkingDir, path)
		alreadyExists, err := disk.FileExists(targetLocation)
		if err != nil {
			return err
		}
		if !alreadyExists {
			if d.IsDir() {
				return disk.EnsureDirectoryExists(filepath.Join(c.actionWorkingDir, path))
			}
			return os.Rename(filepath.Join(unpackDir, path), targetLocation)
		}
		return nil
	})
	return walkErr
}

func (c *FirecrackerContainer) setupNetworking(ctx context.Context) error {
	if !c.constants.EnableNetworking {
		return nil
	}

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
	if err := networking.SetupVethPair(ctx, c.id, vmIP, c.vmIdx); err != nil {
		return err
	}
	return nil
}

func (c *FirecrackerContainer) setupVFSServer() error {
	if c.vfsServer != nil {
		return nil
	}

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
	if err := networking.RemoveNetNamespace(ctx, c.id); err != nil {
		return err
	}
	return networking.DeleteRoute(ctx, c.vmIdx)
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
			if _, err := c.SaveSnapshot(ctx, "" /*=instanceName*/, snapDigest); err != nil {
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
	start := time.Now()
	defer func() {
		log.Debugf("Create took %s", time.Since(start))
	}()
	c.actionWorkingDir = actionWorkingDir
	workspaceSizeBytes, err := disk.DirSize(c.actionWorkingDir)
	if err != nil {
		return err
	}

	containerHome, err := os.MkdirTemp(c.jailerRoot, "fc-container-*")
	if err != nil {
		return err
	}
	wsPath := filepath.Join(containerHome, workspaceFSName)
	if err := ext4.DirectoryToImage(ctx, c.actionWorkingDir, wsPath, workspaceSizeBytes+(c.constants.DiskSlackSpaceMB*1e6)); err != nil {
		return err
	}
	c.workspaceFSPath = wsPath

	log.Debugf("c.containerFSPath: %q", c.containerFSPath)
	log.Debugf("c.workspaceFSPath: %q", c.workspaceFSPath)
	log.Debugf("getChroot() is %q", c.getChroot())
	fcCfg, err := c.getConfig(ctx, c.containerFSPath, c.workspaceFSPath)
	if err != nil {
		return err
	}

	if err := c.setupNetworking(ctx); err != nil {
		return err
	}

	if err := c.setupVFSServer(); err != nil {
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
	if err := m.Start(vmCtx); err != nil {
		return status.InternalErrorf("Failed starting machine: %s", err)
	}
	c.machine = m
	return nil
}

func (c FirecrackerContainer) SendExecRequestToGuest(ctx context.Context, req *vmxpb.ExecRequest) (*vmxpb.ExecResponse, error) {
	dialCtx, cancel := context.WithTimeout(ctx, vSocketDialTimeout)
	defer cancel()

	vsockPath := filepath.Join(c.getChroot(), firecrackerVSockPath)
	conn, err := vsock.SimpleGRPCDial(dialCtx, vsockPath, vsock.VMExecPort)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	execClient := vmxpb.NewExecClient(conn)
	rsp, err := execClient.Exec(ctx, req)
	if err != nil {
		return nil, err
	}
	return rsp, err
}

func (c *FirecrackerContainer) SendPrepareFileSystemRequestToGuest(ctx context.Context, req *vmfspb.PrepareRequest) (*vmfspb.PrepareResponse, error) {
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
			result.Error = err
			return result
		}
	}

	result.ExitCode = int(rsp.GetExitCode())
	result.Stdout = rsp.GetStdout()
	result.Stderr = rsp.GetStderr()
	return result
}

func (c *FirecrackerContainer) IsImageCached(ctx context.Context) (bool, error) {
	diskImagePath, err := containerutil.CachedDiskImagePath(c.jailerRoot, c.containerImage)
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
	start := time.Now()
	defer func() {
		log.Debugf("PullImage took %s", time.Since(start))
	}()
	if c.containerFSPath != "" {
		return nil
	}
	containerFSPath, err := containerutil.CreateDiskImage(ctx, c.jailerRoot, c.containerImage, creds)
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
	start := time.Now()
	defer func() {
		log.Debugf("Remove took %s", time.Since(start))
	}()

	if err := c.machine.Shutdown(ctx); err != nil {
		log.Errorf("Error shutting down machine: %s", err)
	}
	if err := c.machine.StopVMM(); err != nil {
		log.Errorf("Error stopping VM: %s", err)
	}
	if err := c.cleanupNetworking(ctx); err != nil {
		log.Errorf("Error cleaning up networking: %s", err)
	}
	if err := os.RemoveAll(filepath.Dir(c.workspaceFSPath)); err != nil {
		log.Errorf("Error removing workspace fs: %s", err)
	}
	if err := os.RemoveAll(filepath.Dir(c.getChroot())); err != nil {
		log.Errorf("Error removing chroot: %s", err)
	}
	if c.vfsServer != nil {
		c.vfsServer.Stop()
		c.vfsServer = nil
	}
	return nil
}

// Pause freezes a container so that it no longer consumes CPU resources.
func (c *FirecrackerContainer) Pause(ctx context.Context) error {
	start := time.Now()
	defer func() {
		log.Debugf("Pause took %s", time.Since(start))
	}()
	snapshotDigest, err := c.SaveSnapshot(ctx, "" /*=instanceName*/, nil /*=digest*/)
	if err != nil {
		log.Errorf("Error saving snapshot: %s", err)
		return err
	}
	c.pausedSnapshotDigest = snapshotDigest

	if err := c.Remove(ctx); err != nil {
		log.Errorf("Error cleaning up after pause: %s", err)
	}
	return nil
}

// Unpause un-freezes a container so that it can be used to execute commands.
func (c *FirecrackerContainer) Unpause(ctx context.Context) error {
	start := time.Now()
	defer func() {
		log.Debugf("Unpause took %s", time.Since(start))
	}()

	return c.LoadSnapshot(ctx, "" /*=workspaceOverride*/, "" /*=instanceName*/, c.pausedSnapshotDigest)
}

// Wait waits until the underlying VM exits. It returns an error if one is
// encountered while waiting.
func (c *FirecrackerContainer) Wait(ctx context.Context) error {
	if c.externalJailerCmd != nil {
		return c.externalJailerCmd.Wait()
	}
	return c.machine.Wait(ctx)
}

func (c *FirecrackerContainer) Stats(ctx context.Context) (*container.Stats, error) {
	return &container.Stats{}, nil
}

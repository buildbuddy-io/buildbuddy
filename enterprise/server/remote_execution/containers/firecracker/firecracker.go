package firecracker

import (
	"context"
	"crypto/sha256"
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

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	bundle "github.com/buildbuddy-io/buildbuddy/enterprise"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
	fcclient "github.com/firecracker-microvm/firecracker-go-sdk"
	fcmodels "github.com/firecracker-microvm/firecracker-go-sdk/client/models"
)

const (
	// How long to wait for the VMM to listen on the firecracker socket.
	firecrackerSocketWaitTimeout = 3 * time.Second
)

var (
	locateBinariesOnce  sync.Once
	locateBinariesError error
	kernelImagePath     string
	initrdImagePath     string
)

// getStaticFilePath returns the full path to a bundled file. If runfiles are
// present, we'll try to read the file from there. If not, we'll fall back to
// reading the file from the bundle (by writing the contents to a file in the
// user's cache directory). If the file is not found in either place, an error
// is returned.
func getStaticFilePath(fileName string, mode fs.FileMode) (string, error) {
	// If runfiles are included with the binary, bazel will take care of
	// things for us and we can just return the path to the runfile.
	if rfp, err := bazel.RunfilesPath(); err == nil {
		targetFile := filepath.Join(rfp, "enterprise", fileName)
		if exists, err := disk.FileExists(targetFile); err == nil && exists {
			log.Debugf("Found %q in runfiles.", targetFile)
			return targetFile, nil
		}
	}

	// If no runfile is found and we must rely on the bundled version of a
	// file, we need to ensure that we reference the file by sha256, that
	// way subsequent versions of a binary will not reference the same file
	// written on disk.
	if bundleFS, err := bundle.Get(); err == nil {
		if userCacheDir, err := os.UserCacheDir(); err == nil {
			if data, err := fs.ReadFile(bundleFS, fileName); err == nil {
				h := sha256.New()
				if _, err := h.Write(data); err != nil {
					return "", err
				}
				fileHash := fmt.Sprintf("%x", h.Sum(nil))
				casPath := filepath.Join(userCacheDir, "executor", fileHash)
				if _, err := disk.WriteFile(context.Background(), casPath, data); err == nil {
					log.Debugf("Found %q in bundle (and wrote to cached file: %q)", fileName, casPath)
					return casPath, nil
				}
			}
		}
	}

	return "", status.NotFoundErrorf("File %q not found in runfiles or bundle.", fileName)
}

// getOrCreateContainerImage will look for a cached filesystem of the specified
// containerImage in the user's cache directory -- if none is found one will be
// created and cached.
func getOrCreateContainerImage(ctx context.Context, containerImage string) (string, error) {
	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}

	h := sha256.New()
	if _, err := h.Write([]byte(containerImage)); err != nil {
		return "", err
	}
	hashedContainerName := fmt.Sprintf("%x", h.Sum(nil))
	containerImagePath := filepath.Join(userCacheDir, "executor", hashedContainerName)
	if exists, err := disk.FileExists(containerImagePath); err == nil && exists {
		log.Debugf("found cached rootfs at %q", containerImagePath)
		return containerImagePath, nil
	}

	// container not found -- write one!
	tmpImagePath, err := convertContainerToExt4FS(ctx, containerImage)
	if err != nil {
		return "", err
	}
	if err := os.Rename(tmpImagePath, containerImagePath); err != nil {
		return "", err
	}
	log.Debugf("generated rootfs at %q", containerImagePath)
	return containerImagePath, nil
}

// convertContainerToExt4FS uses system tools to generate an ext4 filesystem
// image from an OCI container image reference.
// NB: We use docker because it's already installed - but podman can also be
// used interchangeably and does not require root access.
func convertContainerToExt4FS(ctx context.Context, containerImage string) (string, error) {
	rootFSDir, err := os.MkdirTemp("", "containerfs-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(rootFSDir)

	if err := exec.CommandContext(ctx, "docker", "pull", containerImage).Run(); err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, "docker", "create", containerImage)
	cidBytes, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	containerID := strings.TrimSuffix(string(cidBytes), "\n")

	cmd = exec.CommandContext(ctx, "docker", "cp", fmt.Sprintf("%s:/", containerID), rootFSDir)
	if err := cmd.Run(); err != nil {
		return "", err
	}

	f, err := os.CreateTemp("", "containerfs-*.ext4")
	if err != nil {
		return "", err
	}
	defer f.Close()
	imageFile := f.Name()
	if err := ext4.DirectoryToImageAutoSize(ctx, rootFSDir, imageFile); err != nil {
		return "", err
	}
	log.Debugf("Wrote container %q to image file: %q", containerImage, imageFile)
	return imageFile, nil
}

// firecrackerContainer executes commands inside of a firecracker VM.
type firecrackerContainer struct {
	containerHome  string
	containerImage string
	workDir        string

	// Populated once PullImageIfNecessary is called.
	workspaceFSPath string

	// Populated once Create is called.
	rootFSPath string
	machine    *fcclient.Machine
	fcSocket   string
	vSocket    string

	// Snapshot paths are populated when the container is paused.
	memSnapshotPath  string
	diskSnapshotPath string
}

func NewContainer(ctx context.Context, containerImage, hostRootDir string) (*firecrackerContainer, error) {
	// Ensure our kernel and initrd exist. Bail if they don't.
	initStaticFiles()
	if locateBinariesError != nil {
		return nil, locateBinariesError
	}

	containerHome, err := os.MkdirTemp("", "fc-container-*")
	if err != nil {
		return nil, err
	}
	return &firecrackerContainer{
		containerHome:  containerHome,
		containerImage: containerImage,
		workDir:        hostRootDir,
	}, nil
}

func nonCmdExit(err error) *interfaces.CommandResult {
	return &interfaces.CommandResult{
		Error:    err,
		ExitCode: -2,
	}
}

func (c *firecrackerContainer) getConfig(ctx context.Context, fcSocket, vSocket, containerFS, workspaceFS string) (*fcclient.Config, error) {
	bootArgs := "ro console=ttyS0 noapic reboot=k panic=1 pci=off nomodules=1 random.trust_cpu=on i8042.noaux=1 tsc=reliable ipv6.disable=1 quiet"
	return &fcclient.Config{
		SocketPath:      fcSocket,
		KernelImagePath: kernelImagePath,
		InitrdPath:      initrdImagePath,
		KernelArgs:      bootArgs,
		Drives: []fcmodels.Drive{
			fcmodels.Drive{
				DriveID:      fcclient.String("containerfs"),
				PathOnHost:   &containerFS,
				IsRootDevice: fcclient.Bool(false),
				IsReadOnly:   fcclient.Bool(true),
			},
			fcmodels.Drive{
				DriveID:      fcclient.String("workspacefs"),
				PathOnHost:   &workspaceFS,
				IsRootDevice: fcclient.Bool(false),
				IsReadOnly:   fcclient.Bool(false),
			},
		},
		VsockDevices: []fcclient.VsockDevice{
			fcclient.VsockDevice{
				Path: vSocket,
			},
		},
		MachineCfg: fcmodels.MachineConfiguration{
			VcpuCount:  fcclient.Int64(1),
			MemSizeMib: fcclient.Int64(1000),
			HtEnabled:  fcclient.Bool(false),
		},
	}, nil
}

func initStaticFiles() {
	locateBinariesOnce.Do(func() {
		initrdImagePath, locateBinariesError = getStaticFilePath("vmsupport/bin/initrd.cpio", 0755)
		if locateBinariesError != nil {
			return
		}
		kernelImagePath, locateBinariesError = getStaticFilePath("vmsupport/bin/vmlinux", 0755)
	})
}

// copyOutputsToWorkspace copies output files from the workspace filesystem
// image to the local filesystem workdir. It will not overwrite existing files
// and it will skip copying rootfs-overlay files. Callers should ensure that
// data has already been synced to the workspace filesystem and the VM has
// been paused before calling this.
func (c *firecrackerContainer) copyOutputsToWorkspace(ctx context.Context) error {
	if exists, err := disk.FileExists(c.workspaceFSPath); err != nil || !exists {
		return status.FailedPreconditionErrorf("workspacefs path %q not found", c.workspaceFSPath)
	}
	if exists, err := disk.FileExists(c.workDir); err != nil || !exists {
		return status.FailedPreconditionErrorf("workDir path %q not found", c.workDir)
	}

	unpackDir, err := os.MkdirTemp("", "unpacked-workspacefs-*")
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
		targetLocation := filepath.Join(c.workDir, path)
		alreadyExists, err := disk.FileExists(targetLocation)
		if err != nil {
			return err
		}
		if !alreadyExists {
			if d.IsDir() {
				return disk.EnsureDirectoryExists(filepath.Join(c.workDir, path))
			}
			return os.Rename(filepath.Join(unpackDir, path), targetLocation)
		}
		return nil
	})
	return walkErr
}

// Run the given command within the container and remove the container after
// it is done executing.
//
// It is approximately the same as calling PullImageIfNecessary, Create,
// Exec, then Remove.
func (c *firecrackerContainer) Run(ctx context.Context, command *repb.Command, workDir string) *interfaces.CommandResult {
	start := time.Now()
	defer func() {
		log.Debugf("Run took %s", time.Since(start))
	}()
	if err := c.PullImageIfNecessary(ctx); err != nil {
		return nonCmdExit(err)
	}

	if err := c.Create(ctx, workDir); err != nil {
		return nonCmdExit(err)
	}
	defer c.Remove(ctx)

	cmdResult := c.Exec(ctx, command, nil /*=stdin*/, nil /*=stdout*/)
	return cmdResult
}

// Create creates a new VM and starts a top-level process inside it listening
// for commands to execute.
func (c *firecrackerContainer) Create(ctx context.Context, workDir string) error {
	start := time.Now()
	defer func() {
		log.Debugf("Create took %s", time.Since(start))
	}()
	c.workDir = workDir
	workspaceSizeBytes, err := disk.DirSize(c.workDir)
	if err != nil {
		return err
	}
	wsPath := filepath.Join(c.containerHome, "workspacefs.ext4")
	if err := ext4.DirectoryToImage(ctx, c.workDir, wsPath, workspaceSizeBytes+1e9); err != nil {
		return err
	}
	c.workspaceFSPath = wsPath

	logrusLogger := logrus.New()
	logrusLogger.SetLevel(logrus.ErrorLevel)

	machineOpts := []fcclient.Opt{
		fcclient.WithLogger(logrus.NewEntry(logrusLogger)),
	}

	c.fcSocket = filepath.Join(c.containerHome, "firecracker.sock")
	c.vSocket = filepath.Join(c.containerHome, "v.sock")

	fcCfg, err := c.getConfig(ctx, c.fcSocket, c.vSocket, c.rootFSPath, c.workspaceFSPath)
	if err != nil {
		return err
	}
	cmd := fcclient.VMCommandBuilder{}.
		WithBin("firecracker").
		WithSocketPath(c.fcSocket).
		Build(ctx)
	machineOpts = append(machineOpts, fcclient.WithProcessRunner(cmd))
	m, err := fcclient.NewMachine(ctx, *fcCfg, machineOpts...)
	if err != nil {
		return status.InternalErrorf("Failed creating machine: %s", err)
	}
	if err := m.Start(ctx); err != nil {
		return status.InternalErrorf("Failed starting machine: %s", err)
	}
	c.machine = m
	return nil
}

// Exec runs a command inside a container, with the same working dir set when
// creating the container.
// If stdin is non-nil, the contents of stdin reader will be piped to the stdin of
// the executed process.
// If stdout is non-nil, the stdout of the executed process will be written to the
// stdout writer.
func (c *firecrackerContainer) Exec(ctx context.Context, cmd *repb.Command, stdin io.Reader, stdout io.Writer) *interfaces.CommandResult {
	start := time.Now()
	defer func() {
		log.Debugf("Exec took %s", time.Since(start))
	}()
	result := &interfaces.CommandResult{
		CommandDebugString: fmt.Sprintf("(firecracker) %s", cmd.GetArguments()),
		ExitCode:           commandutil.NoExitCode,
	}

	bufDialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return vsock.DialHostToGuest(ctx, c.vSocket, vsock.DefaultPort)
	}

	// These params are tuned for a fast-reconnect to the vmexec server
	// running inside the VM.
	backoffConfig := backoff.Config{
		BaseDelay:  1.0 * time.Millisecond,
		Multiplier: 1.6,
		Jitter:     0.2,
		MaxDelay:   10 * time.Second,
	}
	connectParams := grpc.ConnectParams{
		Backoff:           backoffConfig,
		MinConnectTimeout: 10 * time.Second,
	}

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithConnectParams(connectParams),
	}

	connectionStart := time.Now()
	conn, err := grpc.DialContext(ctx, "vsock", dialOptions...)
	if err != nil {
		result.Error = err
		return result
	}
	defer conn.Close()
	log.Debugf("Connected after %s", time.Since(connectionStart))

	execClient := vmxpb.NewExecClient(conn)
	execRequest := &vmxpb.ExecRequest{
		Arguments:        cmd.GetArguments(),
		WorkingDirectory: "/workspace/",
	}
	for _, ev := range cmd.GetEnvironmentVariables() {
		execRequest.EnvironmentVariables = append(execRequest.EnvironmentVariables, &vmxpb.ExecRequest_EnvironmentVariable{
			Name: ev.GetName(), Value: ev.GetValue(),
		})
	}

	rsp, err := execClient.Exec(ctx, execRequest)
	if err != nil {
		result.Error = err
		return result
	}

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
	result.ExitCode = int(rsp.GetExitCode())
	result.Stdout = rsp.GetStdout()
	result.Stderr = rsp.GetStderr()
	return result
}

// PullImageIfNecessary pulls the container image if it is not already
// available locally.
func (c *firecrackerContainer) PullImageIfNecessary(ctx context.Context) error {
	start := time.Now()
	defer func() {
		log.Debugf("PullImageIfNecessary took %s", time.Since(start))
	}()
	if c.rootFSPath != "" {
		return nil
	}
	rootFSPath, err := getOrCreateContainerImage(ctx, c.containerImage)
	if err != nil {
		return err
	}
	c.rootFSPath = rootFSPath

	// TODO(tylerw): support loading a VM from snapshot instead for speed.
	return nil
}

// Remove kills any processes currently running inside the container and
// removes any resources associated with the container itself.
func (c *firecrackerContainer) Remove(ctx context.Context) error {
	start := time.Now()
	defer func() {
		log.Debugf("Remove took %s", time.Since(start))
	}()

	if err := c.machine.Shutdown(ctx); err != nil {
		log.Errorf("Error shutting down machine: %s", err)
	}
	if err := c.machine.StopVMM(); err != nil {
		return err
	}

	return os.RemoveAll(c.containerHome)
}

// Pause freezes a container so that it no longer consumes CPU resources.
func (c *firecrackerContainer) Pause(ctx context.Context) error {
	start := time.Now()
	defer func() {
		log.Debugf("Pause took %s", time.Since(start))
	}()
	if err := c.machine.PauseVM(ctx); err != nil {
		return err
	}
	snapshotDir := filepath.Join(c.containerHome, "full-snapshot")
	if err := disk.EnsureDirectoryExists(snapshotDir); err != nil {
		return err
	}
	c.memSnapshotPath = filepath.Join(snapshotDir, "mem.snapshot")
	c.diskSnapshotPath = filepath.Join(snapshotDir, "disk.snapshot")

	// If an older snapshot is present -- nuke it since we're writing a new one.
	disk.DeleteLocalFileIfExists(c.memSnapshotPath)
	disk.DeleteLocalFileIfExists(c.diskSnapshotPath)

	snapStart := time.Now()
	if err := c.machine.CreateSnapshot(ctx, c.memSnapshotPath, c.diskSnapshotPath); err != nil {
		return err
	}
	log.Debugf("CreateSnapshot took %s", time.Since(snapStart))

	if err := c.machine.StopVMM(); err != nil {
		return err
	}
	c.machine = nil

	// Keep the socket and vsocket paths around -- they'll be reused when
	// the machine is unpaused -- but delete the files so the VMM can
	// recreate them.
	disk.DeleteLocalFileIfExists(c.fcSocket)
	disk.DeleteLocalFileIfExists(c.vSocket)
	return nil
}

// Unpause un-freezes a container so that it can be used to execute commands.
func (c *firecrackerContainer) Unpause(ctx context.Context) error {
	start := time.Now()
	defer func() {
		log.Debugf("Unpause took %s", time.Since(start))
	}()
	if exists, err := disk.FileExists(c.memSnapshotPath); err != nil || !exists {
		return status.FailedPreconditionErrorf("memory snapshot %q not found", c.memSnapshotPath)
	}
	if exists, err := disk.FileExists(c.diskSnapshotPath); err != nil || !exists {
		return status.FailedPreconditionErrorf("disk snapshot %q not found", c.diskSnapshotPath)
	}

	cfg := fcclient.Config{
		SocketPath:        c.fcSocket,
		DisableValidation: true,
	}

	cmd := fcclient.VMCommandBuilder{}.
		WithBin("firecracker").
		WithSocketPath(c.fcSocket).
		Build(ctx)

	logrusLogger := logrus.New()
	logrusLogger.SetLevel(logrus.ErrorLevel)
	machineOpts := []fcclient.Opt{
		fcclient.WithLogger(logrus.NewEntry(logrusLogger)),
		fcclient.WithProcessRunner(cmd),
	}
	// Start Firecracker
	err := cmd.Start()
	if err != nil {
		return status.InternalErrorf("Failed starting firecracker binary: %s", err)
	}
	machine, err := fcclient.NewMachine(ctx, cfg, machineOpts...)
	if err != nil {
		return status.InternalErrorf("failed to create new machine: %s", err)
	}
	c.machine = machine

	errCh := make(chan error)
	if err := c.machine.WaitForSocket(firecrackerSocketWaitTimeout, errCh); err != nil {
		return status.InternalErrorf("timeout waiting for firecracker socket: %s", err)
	}
	if err := c.machine.LoadSnapshot(ctx, c.memSnapshotPath, c.diskSnapshotPath); err != nil {
		return status.InternalErrorf("error loading snapshot: %s", err)
	}
	if err := c.machine.ResumeVM(ctx); err != nil {
		return status.InternalErrorf("error resuming VM: %s", err)
	}

	return nil
}

func (c *firecrackerContainer) Stats(ctx context.Context) (*container.Stats, error) {
	return &container.Stats{}, nil
}

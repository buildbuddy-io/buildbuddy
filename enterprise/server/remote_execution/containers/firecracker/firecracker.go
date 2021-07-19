package firecracker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
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
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"

	bundle "github.com/buildbuddy-io/buildbuddy/enterprise"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
	fcclient "github.com/firecracker-microvm/firecracker-go-sdk"
	fcmodels "github.com/firecracker-microvm/firecracker-go-sdk/client/models"
)

const (
	// How long to wait for the VMM to listen on the firecracker socket.
	firecrackerSocketWaitTimeout = 30 * time.Second

	// How long to wait when dialing the vmexec server inside the VM.
	vSocketDialTimeout = 3 * time.Second

	// The firecracker socket path (will be relative to the chroot).
	firecrackerSocketPath = "/run/fc.sock"

	// The vSock path (also relative to the chroot).
	firecrackerVSockPath = "/run/v.sock"

	// The names to use when creating a full snapshot (relative to chroot).
	fullDiskSnapshotName = "full-disk.snap"
	fullMemSnapshotName  = "full-mem.snap"

	// The networking deets for host and vm interfaces.
	// All VMs are configured with the same IP and tap device via boot args,
	// but because they run inside of a network namespace, they do not
	// conflict. More details here:
	// https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/network-for-clones.md
	tapDeviceName = "vmtap0"
	tapDeviceMac  = "7a:a8:fa:dc:76:b7"
	tapAddr       = "192.168.241.1/29"

	vmIP   = "192.168.241.2"
	vmAddr = vmIP + "/29"

	// https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/html/networking_guide/sec-configuring_ip_networking_from_the_kernel_command_line
	// ip<client-IP-number>:[<server-id>]:<gateway-IP-number>:<netmask>:<client-hostname>:<interface>:{dhcp|dhcp6|auto6|on|any|none|off}
	machineIPBootArgs = "ip=" + vmIP + "::192.168.241.1:255.255.255.48::eth0:off"

	// This is pretty arbitrary limit -- when vmIdx gets this big it will
	// roll over to 0, causing new VMs to start re-using old local IPs. But
	// net namespaces are deleted upon VM removal, so this should not cause
	// any issue. If more than this many VMs were active on a single host at
	// once -- it would cause an error.
	maxVMSPerHost = 1000
)

var (
	locateBinariesOnce  sync.Once
	locateBinariesError error

	// kernel + initrd
	kernelImagePath string
	initrdImagePath string

	// firecracker + jailer
	firecrackerBinPath string
	jailerBinPath      string

	vmIdx   int
	vmIdxMu sync.Mutex
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
// NB: We use modern tools (not docker), that do not require root access. This
// allows this binary to convert images even when not running as root.
func convertContainerToExt4FS(ctx context.Context, containerImage string) (string, error) {
	// Make a temp directory to work in. Delete it when this fuction returns.
	rootUnpackDir, err := os.MkdirTemp("", "container-unpack-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(rootUnpackDir)

	// Make a directory to download the OCI image to.
	ociImageDir := filepath.Join(rootUnpackDir, "image")
	if err := disk.EnsureDirectoryExists(ociImageDir); err != nil {
		return "", err
	}

	// in CLI-form, the commands below do this:
	// skopeo copy docker://alpine:lotest oci:/tmp/image_unpack:latest
	// umoci unpack --rootless --image /tmp/image_unpack /tmp/bundle
	// /tmp/bundle/rootfs/ has the goods
	dockerImageRef := fmt.Sprintf("docker://%s", containerImage)
	ociOutputRef := fmt.Sprintf("oci:%s:latest", ociImageDir)
	if out, err := exec.CommandContext(ctx, "skopeo", "copy", dockerImageRef, ociOutputRef).CombinedOutput(); err != nil {
		return "", status.InternalErrorf("skopeo copy error: %q: %s", string(out), err)
	}

	// Make a directory to unpack the bundle to.
	bundleOutputDir := filepath.Join(rootUnpackDir, "bundle")
	if err := disk.EnsureDirectoryExists(bundleOutputDir); err != nil {
		return "", err
	}
	if out, err := exec.CommandContext(ctx, "umoci", "unpack", "--rootless", "--image", ociImageDir, bundleOutputDir).CombinedOutput(); err != nil {
		return "", status.InternalErrorf("umoci unpack error: %q: %s", string(out), err)
	}

	// Take the rootfs and write it into an ext4 image.
	rootFSDir := filepath.Join(bundleOutputDir, "rootfs")
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

func getLogrusLogger() *logrus.Entry {
	logrusLogger := logrus.New()
	logrusLogger.SetLevel(logrus.ErrorLevel)
	return logrus.NewEntry(logrusLogger)
}

// firecrackerContainer executes commands inside of a firecracker VM.
type firecrackerContainer struct {
	id    string // a random GUID, unique per-run of firecracker
	vmIdx int    // the index of this vm on the host machine

	containerImage   string // the OCI container image. ex "alpine:latest"
	jailerRoot       string // the root dir the jailer will work in
	actionWorkingDir string // the action directory with inputs / outputs
	workspaceFSPath  string // the path to the workspace ext4 image
	containerFSPath  string // the path to the container ext4 image

	machine *fcclient.Machine // the firecracker machine object.
}

func NewContainer(ctx context.Context, containerImage, actionWorkingDir string) (*firecrackerContainer, error) {
	// Ensure our kernel and initrd exist.
	if err := locateStaticFiles(); err != nil {
		return nil, err
	}
	// WARNING: because of the limitation on the length of unix sock file
	// paths (103), this directory path needs to be short. Specifically, a
	// full sock path will look like:
	// /tmp/firecracker/217d4de0-4b28-401b-891b-18e087718ad1/root/run/fc.sock
	// everything after "/tmp" is 65 characters, so 38 are left for the
	// jailerRoot.
	jailerRoot := "/tmp/"
	if err := disk.EnsureDirectoryExists(jailerRoot); err != nil {
		return nil, err
	}

	c := &firecrackerContainer{
		jailerRoot:       jailerRoot,
		containerImage:   containerImage,
		actionWorkingDir: actionWorkingDir,
	}

	if err := c.newID(); err != nil {
		return nil, err
	}
	return c, nil
}

func nonCmdExit(err error) *interfaces.CommandResult {
	return &interfaces.CommandResult{
		Error:    err,
		ExitCode: -2,
	}
}

func (c *firecrackerContainer) newID() error {
	vmIdxMu.Lock()
	defer vmIdxMu.Unlock()
	u, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	log.Printf("Container id changing from %q (%d) to %q (%d)", c.id, c.vmIdx, u.String(), vmIdx)
	c.id = u.String()

	c.vmIdx = vmIdx
	vmIdx += 1
	if vmIdx > maxVMSPerHost {
		vmIdx = 0
	}

	return nil
}

func (c *firecrackerContainer) cleanupNets(ctx context.Context) error {
	if c.id == "" || c.vmIdx == 0 {
		return nil
	}
	return nil
}

func (c *firecrackerContainer) getChroot() string {
	// This path matches the path the jailer will use when jailing
	// firecracker. Because we need to copy some (snapshot) files into
	// this directory before starting firecracker, we need to compute
	// the same path.
	return filepath.Join(c.jailerRoot, "firecracker", c.id, "root")
}

func (c *firecrackerContainer) getConfig(ctx context.Context, containerFS, workspaceFS string) (*fcclient.Config, error) {
	bootArgs := "ro console=ttyS0 noapic reboot=k panic=1 pci=off nomodules=1 random.trust_cpu=on i8042.noaux=1 tsc=reliable ipv6.disable=1 quiet"
	bootArgs += " " + machineIPBootArgs

	return &fcclient.Config{
		VMID:            c.id,
		SocketPath:      firecrackerSocketPath,
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
				Path: firecrackerVSockPath,
			},
		},
		NetworkInterfaces: []fcclient.NetworkInterface{
			{
				StaticConfiguration: &fcclient.StaticNetworkConfiguration{
					HostDevName: tapDeviceName,
					MacAddress:  tapDeviceMac,
				},
			},
		},
		JailerCfg: &fcclient.JailerConfig{
			JailerBinary:  jailerBinPath,
			ChrootBaseDir: c.jailerRoot,
			ID:            c.id,
			UID:           fcclient.Int(unix.Geteuid()),
			GID:           fcclient.Int(unix.Getegid()),
			//			Stdout:         os.Stdout, // STOPSHIP(tylerw): remove this?
			//			Stderr:         os.Stderr, // STOPSHIP(tylerw): remove this?
			//			Stdin:          os.Stdin,  // STOPSHIP(tylerw): remove this?
			NumaNode:       fcclient.Int(0),
			ExecFile:       firecrackerBinPath,
			ChrootStrategy: fcclient.NewNaiveChrootStrategy(kernelImagePath),
		},
		MachineCfg: fcmodels.MachineConfiguration{
			VcpuCount:  fcclient.Int64(1),
			MemSizeMib: fcclient.Int64(1000),
			HtEnabled:  fcclient.Bool(false),
		},
	}, nil
}

func locateStaticFiles() error {
	var err error
	initrdImagePath, err = getStaticFilePath("vmsupport/bin/initrd.cpio", 0755)
	if err != nil {
		return err
	}
	kernelImagePath, err = getStaticFilePath("vmsupport/bin/vmlinux", 0755)
	if err != nil {
		return err
	}
	firecrackerBinPath, err = exec.LookPath("firecracker")
	if err != nil {
		return err
	}
	jailerBinPath, err = exec.LookPath("jailer")
	return err
}

// copyOutputsToWorkspace copies output files from the workspace filesystem
// image to the local filesystem workdir. It will not overwrite existing files
// and it will skip copying rootfs-overlay files. Callers should ensure that
// data has already been synced to the workspace filesystem and the VM has
// been paused before calling this.
func (c *firecrackerContainer) copyOutputsToWorkspace(ctx context.Context) error {
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

// Run the given command within the container and remove the container after
// it is done executing.
//
// It is approximately the same as calling PullImageIfNecessary, Create,
// Exec, then Remove.
func (c *firecrackerContainer) Run(ctx context.Context, command *repb.Command, actionWorkingDir string) *interfaces.CommandResult {
	start := time.Now()
	defer func() {
		log.Debugf("Run took %s", time.Since(start))
	}()
	if err := c.PullImageIfNecessary(ctx); err != nil {
		return nonCmdExit(err)
	}

	if err := c.Create(ctx, actionWorkingDir); err != nil {
		return nonCmdExit(err)
	}
	defer c.Remove(ctx)

	cmdResult := c.Exec(ctx, command, nil /*=stdin*/, nil /*=stdout*/)
	return cmdResult
}

// Create creates a new VM and starts a top-level process inside it listening
// for commands to execute.
func (c *firecrackerContainer) Create(ctx context.Context, actionWorkingDir string) error {
	start := time.Now()
	defer func() {
		log.Debugf("Create took %s", time.Since(start))
	}()
	c.actionWorkingDir = actionWorkingDir
	workspaceSizeBytes, err := disk.DirSize(c.actionWorkingDir)
	if err != nil {
		return err
	}

	containerHome, err := os.MkdirTemp("", "fc-container-*")
	if err != nil {
		return err
	}
	wsPath := filepath.Join(containerHome, "workspacefs.ext4")
	if err := ext4.DirectoryToImage(ctx, c.actionWorkingDir, wsPath, workspaceSizeBytes+1e9); err != nil {
		return err
	}
	c.workspaceFSPath = wsPath

	log.Printf("c.containerFSPath: %q", c.containerFSPath)
	log.Printf("c.workspaceFSPath: %q", c.workspaceFSPath)

	fcCfg, err := c.getConfig(ctx, c.containerFSPath, c.workspaceFSPath)
	if err != nil {
		return err
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
	cmd := fcclient.NewJailerCommandBuilder().
		WithBin(jailerBinPath).
		WithChrootBaseDir(c.jailerRoot).
		WithID(c.id).
		WithNetNS("/var/run/netns/"+c.id).
		WithUID(unix.Geteuid()).
		WithGID(unix.Getegid()).
		//		WithStdin(os.Stdin).   // STOPSHIP(tylerw): remove this?
		//		WithStdout(os.Stdout). // STOPSHIP(tylerw): remove this?
		//		WithStderr(os.Stderr). // STOPSHIP(tylerw): remove this?
		WithNumaNode(0).
		WithExecFile(firecrackerBinPath).
		WithFirecrackerArgs("--api-sock", firecrackerSocketPath).
		Build(ctx)

	machineOpts := []fcclient.Opt{
		fcclient.WithLogger(getLogrusLogger()),
		fcclient.WithProcessRunner(cmd),
	}
	// Wait for the jailer directory to be created
	waitUntilExists(ctx, time.Second, c.getChroot())

	m, err := fcclient.NewMachine(ctx, *fcCfg, machineOpts...)
	if err != nil {
		return status.InternalErrorf("Failed creating machine: %s", err)
	}
	if err := m.Start(ctx); err != nil {
		return status.InternalErrorf("Failed starting machine: %s", err)
	}
	c.machine = m
	dialCtx, cancel := context.WithTimeout(ctx, vSocketDialTimeout)
	defer cancel()

	vsockPath := filepath.Join(c.getChroot(), firecrackerVSockPath)
	_, err = vsock.SimpleGRPCDial(dialCtx, vsockPath)
	if err != nil {
		return err
	}
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

	dialCtx, cancel := context.WithTimeout(ctx, vSocketDialTimeout)
	defer cancel()

	vsockPath := filepath.Join(c.getChroot(), firecrackerVSockPath)
	conn, err := vsock.SimpleGRPCDial(dialCtx, vsockPath)
	if err != nil {
		result.Error = err
		return result
	}
	defer conn.Close()

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
	if c.containerFSPath != "" {
		return nil
	}
	containerFSPath, err := getOrCreateContainerImage(ctx, c.containerImage)
	if err != nil {
		return err
	}
	c.containerFSPath = containerFSPath

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
		log.Errorf("Error stopping VM: %s", err)
	}
	if err := networking.RemoveNetNamespace(ctx, c.id); err != nil {
		log.Errorf("Error removing net namespace: %s", err)
	}
	return nil
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
	memSnapshotPath := filepath.Join(c.getChroot(), fullDiskSnapshotName)
	diskSnapshotPath := filepath.Join(c.getChroot(), fullMemSnapshotName)

	// If an older snapshot is present -- nuke it since we're writing a new one.
	disk.DeleteLocalFileIfExists(memSnapshotPath)
	disk.DeleteLocalFileIfExists(diskSnapshotPath)

	if err := c.machine.CreateSnapshot(ctx, fullMemSnapshotName, fullDiskSnapshotName); err != nil {
		return err
	}

	if err := c.machine.StopVMM(); err != nil {
		return err
	}
	c.machine = nil
	return nil
}

func hardlinkFilesIntoDir(targetDir string, files ...string) error {
	for _, f := range files {
		fileName := filepath.Base(f)
		stat, err := os.Stat(f)
		if err != nil {
			return err
		}
		if stat.IsDir() {
			return status.FailedPreconditionErrorf("%q was dir, not file", f)
		}
		if err := os.Link(f, filepath.Join(targetDir, fileName)); err != nil {
			return err
		}
	}
	return nil
}

// Unpause un-freezes a container so that it can be used to execute commands.
func (c *firecrackerContainer) Unpause(ctx context.Context) error {
	start := time.Now()
	defer func() {
		log.Debugf("Unpause took %s", time.Since(start))
	}()

	if err := networking.RemoveNetNamespace(ctx, c.id); err != nil {
		log.Warningf("Error removing old net namespace: %s", err)
	}

	// Capture the previous snapshot path, so we can copy it into the
	// new (jailed) directory where firecracker will be run from.
	memSnapshotPath := filepath.Join(c.getChroot(), fullDiskSnapshotName)
	diskSnapshotPath := filepath.Join(c.getChroot(), fullMemSnapshotName)
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
			ChrootStrategy: fcclient.NewNaiveChrootStrategy(kernelImagePath),
		},
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
	cmd := fcclient.NewJailerCommandBuilder().
		WithBin(jailerBinPath).
		WithChrootBaseDir(c.jailerRoot).
		WithID(c.id).
		WithNetNS("/var/run/netns/"+c.id).
		WithUID(unix.Geteuid()).
		WithGID(unix.Getegid()).
		//		WithStdin(os.Stdin).   // STOPSHIP(tylerw): remove this?
		//		WithStdout(os.Stdout). // STOPSHIP(tylerw): remove this?
		//		WithStderr(os.Stderr). // STOPSHIP(tylerw): remove this?
		WithNumaNode(0).
		WithExecFile(firecrackerBinPath).
		WithFirecrackerArgs("--api-sock", firecrackerSocketPath).
		Build(ctx)

	machineOpts := []fcclient.Opt{
		fcclient.WithLogger(getLogrusLogger()),
		fcclient.WithProcessRunner(cmd),
	}
	// Start Firecracker
	err := cmd.Start()
	if err != nil {
		return status.InternalErrorf("Failed starting firecracker binary: %s", err)
	}

	// Wait for the jailer directory to be created
	waitUntilExists(ctx, time.Second, c.getChroot())

	requiredFiles := []string{
		kernelImagePath,
		initrdImagePath,
		c.containerFSPath,
		c.workspaceFSPath,
		memSnapshotPath,
		diskSnapshotPath,
	}
	if err := hardlinkFilesIntoDir(c.getChroot(), requiredFiles...); err != nil {
		return err
	}

	machine, err := fcclient.NewMachine(ctx, cfg, machineOpts...)
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
	if err := c.machine.ResumeVM(ctx); err != nil {
		return status.InternalErrorf("error resuming VM: %s", err)
	}

	return nil
}

func (c *firecrackerContainer) Wait(ctx context.Context) error {
	return c.machine.Wait(ctx)
}

func (c *firecrackerContainer) Stats(ctx context.Context) (*container.Stats, error) {
	return &container.Stats{}, nil
}

package firecracker

import (
	"context"
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
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/sirupsen/logrus"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bundle "github.com/buildbuddy-io/enterprise/bundle"
	fcclient "github.com/firecracker-microvm/firecracker-go-sdk"
	fcmodels "github.com/firecracker-microvm/firecracker-go-sdk/client/models"
)

var (
	locateBinariesOnce  sync.Once
	locateBinariesError error
	kernelImagePath     string
	initFSPath          string
)

// getStaticFilePath returns the full path to a bundled file. If runfiles are
// present, we'll try to read the file from there. If not, we'll fall back to
// reading the file from the bundle (by writing the contents to a file in the
// user's cache directory). If the file is not found in either place, an error
// is returned.
func getStaticFilePath(fileName string, mode fs.FileMode) (string, error) {
	if rfp, err := bazel.RunfilesPath(); err == nil {
		targetFile := filepath.Join(rfp, "enterprise", fileName)
		if exists, err := disk.FileExists(targetFile); err == nil && exists {
			log.Debugf("Found %q in runfiles.", targetFile)
			return targetFile, nil
		}
	}

	if bundleFS, err := bundle.Get(); err == nil {
		if userCacheDir, err := os.UserCacheDir(); err == nil {
			cachedPath := filepath.Join(userCacheDir, fileName)
			if data, err := fs.ReadFile(bundleFS, fileName); err == nil {
				disk.DeleteLocalFileIfExists(cachedPath)
				if _, err := disk.WriteFile(context.Background(), cachedPath, data); err == nil {
					log.Debugf("Found %q in bundle (and wrote to cached file.)", cachedPath)
					return cachedPath, nil
				}
			}
		}
	}

	return "", status.NotFoundErrorf("File %q not found in runfiles or bundle.", fileName)
}

func directoryToExt4ImageWithSize(ctx context.Context, directory string, sizeBytes int64) (string, error) {
	f, err := os.CreateTemp("", fmt.Sprintf("%s-*.ext4", filepath.Base(directory)))
	imageFileName := f.Name()
	f.Close()
	args := []string{
		"mke2fs",
		"-L", "''",
		"-N", "0",
		"-O", "^64bit",
		"-d", directory,
		"-m", "5",
		"-r", "1",
		"-t", "ext4",
		imageFileName,
		fmt.Sprintf("%dK", sizeBytes/1e3),
	}
	if err := exec.CommandContext(ctx, args[0], args[1:]...).Run(); err != nil {
		return "", err
	}
	return imageFileName, nil
}

// directoryToExt4Image creates a randomly named ext4 image file. Callers are
// responsible for cleaning this file up after use.
func directoryToExt4Image(ctx context.Context, directory string) (string, error) {
	dirSizeBytes, err := disk.DirSize(directory)
	if err != nil {
		return "", err
	}
	log.Debugf("directory size (bytes): %d", dirSizeBytes)

	// 20% larger works in all tested cases I've tested for initfs.
	imageSizeBytes := int64(float64(dirSizeBytes) * 1.2)
	return directoryToExt4ImageWithSize(ctx, directory, imageSizeBytes)
}

// convertContainerToExt4FS uses system tools to generate an ext4 filesystem
// image from an OCI container image reference.
func convertContainerToExt4FS(ctx context.Context, containerImage string) (string, error) {
	rootFSDir, err := os.MkdirTemp("", "ext4-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(rootFSDir)

	if err := exec.CommandContext(ctx, "podman", "pull", containerImage).Run(); err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, "podman", "create", containerImage)
	cidBytes, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	containerID := strings.TrimSuffix(string(cidBytes), "\n")

	cmd = exec.CommandContext(ctx, "podman", "cp", fmt.Sprintf("%s:/", containerID), rootFSDir)
	if err := cmd.Run(); err != nil {
		return "", err
	}

	imageFile, err := directoryToExt4Image(ctx, rootFSDir)
	if err != nil {
		return "", err
	}

	log.Debugf("Wrote container %q to image file: %q", containerImage, imageFile)
	return imageFile, nil
}

func formatCommand(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, fmt.Sprintf("%q", arg))
	}
	return strings.Join(quoted, " ")
}

// firecrackerContainer executes commands inside of a firecracker VM.
type firecrackerContainer struct {
	containerImage string
	workDir        string

	// These are unset when NewContainer is called, but will
	// be populated by PullImageIfNecessary and Create respectively.
	workspaceFSPath string
	rootFSPath      string
}

func NewContainer(ctx context.Context, containerImage, hostRootDir string) container.CommandContainer {
	log.Infof("NewContainer called with image: %q, rootDir: %q", containerImage, hostRootDir)
	return &firecrackerContainer{
		containerImage: containerImage,
		workDir:        hostRootDir,
	}
}

func nonCmdExit(err error) *interfaces.CommandResult {
	return &interfaces.CommandResult{
		Error:    err,
		ExitCode: -2,
	}
}

func (c *firecrackerContainer) getConfig(ctx context.Context, socketPath, rootFSPath, workspaceFSPath string) (*fcclient.Config, error) {
	contextID, err := vsock.GetContextID(ctx)
	if err != nil {
		return nil, err
	}
	bootArgs := "init=/bb/init ro console=ttyS0 noapic reboot=k panic=1 pci=off nomodules=1 random.trust_cpu=on i8042.noaux=1 tsc=reliable ipv6.disable=1"
	return fcclient.Config{
		SocketPath:      socketPath,
		KernelImagePath: kernelImagePath,
		KernelArgs:      bootArgs,
		Drives: []fcmodels.Drive{
			fcmodels.Drive{
				DriveID:      fcclient.String("initfs"),
				PathOnHost:   &initFSPath,
				IsRootDevice: fcclient.Bool(true),
				IsReadOnly:   fcclient.Bool(false),
			},
			fcmodels.Drive{
				DriveID:      fcclient.String("rootfs"),
				PathOnHost:   &rootFSPath,
				IsRootDevice: fcclient.Bool(false),
				IsReadOnly:   fcclient.Bool(false),
			},
			fcmodels.Drive{
				DriveID:      fcclient.String("workspacefs"),
				PathOnHost:   &workspaceFSPath,
				IsRootDevice: fcclient.Bool(false),
				IsReadOnly:   fcclient.Bool(false),
			},
		},
		VsockDevices: []firecracker.VsockDevice{
			fcmodels.VsockDevice{
				Path: "root", // is this right? should this be a path?
				CID:  contextID,
			},
		},
		MachineCfg: fcmodels.MachineConfiguration{
			VcpuCount:  fcclient.Int64(1),
			MemSizeMib: fcclient.Int64(512),
			HtEnabled:  fcclient.Bool(false),
		},
	}, nil
}

func initStaticFiles() {
	locateBinariesOnce.Do(func() {
		initFSPath, locateBinariesError = getStaticFilePath("vmsupport/bin/initfs", 0755)
		if locateBinariesError != nil {
			return
		}
		kernelImagePath, locateBinariesError = getStaticFilePath("vmsupport/bin/vmlinux", 0755)
	})
}

// Run the given command within the container and remove the container after
// it is done executing.
//
// It is approximately the same as calling PullImageIfNecessary, Create,
// Exec, then Remove.
func (c *firecrackerContainer) Run(ctx context.Context, command *repb.Command, workDir string) *interfaces.CommandResult {
	if err := c.PullImageIfNecessary(ctx); err != nil {
		return nonCmdExit(err)
	}

	if err := c.Create(ctx, workDir); err != nil {
		return nonCmdExit(err)
	}

	if cmdResult := c.Exec(ctx, command, nil /*=stdin*/, nil /*=stdout*/); cmdResult.Error != nil {
		return cmdResult
	}

	if err := c.Remove(ctx); err != nil {
		return nonCmdExit(err)
	}
	/*
		m, err := fcclient.NewMachine(ctx, fcCfg, machineOpts...)
		if err != nil {
			return nonCmdExit(status.InternalErrorf("Failed creating machine: %s", err))
		}

		if err := m.Start(ctx); err != nil {
			return nonCmdExit(status.InternalErrorf("Failed starting machine: %s", err))
		}

		if err := m.Wait(ctx); err != nil {
			return nonCmdExit(err)
		}

		result := &interfaces.CommandResult{
			CommandDebugString: fmt.Sprintf("(firecracker) %s", command.GetArguments()),
			ExitCode:           commandutil.NoExitCode,
		}
	*/
	return commandutil.Run(ctx, command, workDir)
}

// Create creates a new container and starts a top-level process inside it
// (`sleep infinity`) so that it stays alive and running until explicitly
// removed. Note, this works slightly differently than commands like
// `docker create` or `ctr containers create` -- in addition to creating the
// container, it also puts it in a "ready to execute" state by starting the
// top level process.
func (c *firecrackerContainer) Create(ctx context.Context, workDir string) error {
	c.workDir = workDir
	workspaceSizeBytes, err := disk.DirSize(c.workDir)
	if err != nil {
		return err
	}
	log.Debugf("workspace size (bytes): %d", workspaceSizeBytes)
	workspaceFSPath, err := directoryToExt4ImageWithSize(ctx, c.workDir, workspaceSizeBytes+1e9)
	if err != nil {
		return err
	}

	// Need our static files now -- ensure they exist.
	initStaticFiles()
	if locateBinariesError != nil {
		return locateBinariesError
	}

	// Start a VM with a kernel boot arg set to the name of the run script.
	logrusLogger := logrus.New()
	logrusLogger.SetLevel(logrus.InfoLevel)

	machineOpts := []fcclient.Opt{
		fcclient.WithLogger(logrus.NewEntry(logrusLogger)),
	}

	socketPath := fmt.Sprintf("/tmp/firecracker-%d.sock", time.Now().UnixNano())
	if randStr, err := random.RandomString(5); err == nil {
		socketPath = fmt.Sprintf("/tmp/firecracker-%s.sock", randStr)
	}

	fcCfg, err := c.getConfig(socketPath, c.rootFSPath, c.workspaceFSPath)
	if err != nil {
		return err
	}

	cmd := fcclient.VMCommandBuilder{}.
		WithBin("firecracker").
		WithSocketPath(socketPath).
		WithStdin(os.Stdin).
		WithStdout(os.Stdout).
		WithStderr(os.Stderr).
		Build(ctx)
	machineOpts = append(machineOpts, fcclient.WithProcessRunner(cmd))

	m, err := fcclient.NewMachine(ctx, *fcCfg, machineOpts...)
	if err != nil {
		return status.InternalErrorf("Failed creating machine: %s", err)
	}

	if err := m.Start(ctx); err != nil {
		return status.InternalErrorf("Failed starting machine: %s", err)
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
	// send command over the VSOCK
	// wait for result
	return c.Run(ctx, cmd, c.workDir)
}

// PullImageIfNecessary pulls the container image if it is not already
// available locally.
func (c *firecrackerContainer) PullImageIfNecessary(ctx context.Context) error {
	if c.rootFSPath != "" {
		return nil
	}
	rootFSPath, err := convertContainerToExt4FS(ctx, c.containerImage)
	if err != nil {
		log.Errorf("Error making rootfs: %s", err)
		return err
	}
	log.Printf("generated rootfs at %q", rootFSPath)
	c.rootFSPath = rootFSPath

	// if a snapshot is cached, use that
	// otherwise, generate a new one from the container image
}

// Remove kills any processes currently running inside the container and
// removes any resources associated with the container itself.
func (c *firecrackerContainer) Remove(ctx context.Context) error {
	// terminate the VM?
	if c.rootFSPath != "" {
		disk.DeleteLocalFileIfExists(c.rootFSPath)
	}
	if c.workspaceFSPath != "" {
		disk.DeleteLocalFileIfExists(c.workspaceFSPath)
	}
	// remove the snapshot?
}

// Pause freezes a container so that it no longer consumes CPU resources.
func (c *firecrackerContainer) Pause(ctx context.Context) error {
	// pause the VM and create a snapshot
}

// Unpause un-freezes a container so that it can be used to execute commands.
func (c *firecrackerContainer) Unpause(ctx context.Context) error {
	// restore from the latest snapshot otherwise error
}

func (c *firecrackerContainer) Stats(ctx context.Context) (*container.Stats, error) {
	return &container.Stats{}, nil
}

package macvm

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var versions = []string{"14.6.1", "15.1.1"}

const (
	vmDir   = "/Users/iainmacdonald/Downloads/machack"
	macosvm = vmDir + "/macosvm"

	username = "buildbuddy"
	password = "abc123"
)

type Provider struct {
	version string
}

// Issues:
// - Everything is slow :-(
//
//   - Especially makign the disk image
//
//   - And starting the VM
//
//   - Is stuff in the VM slow too?
//
//   - I'm not convinced VMs are being correctly cleaned up
//
//   - You can only run two at a time, but nesting might be possible
//     https://khronokernel.com/macos/2023/08/08/AS-VM.html
func (p *Provider) New(ctx context.Context, args *container.Init) (container.CommandContainer, error) {
	requestedVersion := args.Props.MacVMOSVersion
	log.Infof("Provisioning MacVM Provider with OS Version: %s", requestedVersion)
	found := false
	for _, version := range versions {
		if version == requestedVersion {
			found = true
			break
		}
	}
	if !found {
		return nil, status.InvalidArgumentErrorf("Requested MacOS version (%s) is unsupported", requestedVersion)
	}
	return NewVMCommandContainer(requestedVersion), nil
}

type vmCommandContainer struct {
	signal  chan syscall.Signal
	WorkDir string
	version string
}

func NewVMCommandContainer(version string) container.CommandContainer {
	return &vmCommandContainer{
		signal:  make(chan syscall.Signal, 1),
		version: version,
	}
}

func (c *vmCommandContainer) IsolationType() string {
	return "macvm"
}

func (c *vmCommandContainer) Run(ctx context.Context, command *repb.Command, workDir string, creds oci.Credentials) *interfaces.CommandResult {
	return c.exec(ctx, command, workDir, nil /*=stdio*/)
}

func (c *vmCommandContainer) Create(ctx context.Context, workDir string) error {
	c.WorkDir = workDir
	return nil
}

func (c *vmCommandContainer) Exec(ctx context.Context, cmd *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult {
	return c.exec(ctx, cmd, c.WorkDir, stdio)
}

func (c *vmCommandContainer) Signal(ctx context.Context, sig syscall.Signal) error {
	select {
	case c.signal <- sig:
		return nil
	default:
		return status.UnavailableErrorf("failed to send signal %q: channel buffer is full", sig)
	}
}

func makeDiskImage(dir string) (string, error) {
	fname := uuid.New()

	log.Debugf("Creating disk image from directory %s", dir)
	start := time.Now()
	cmd := exec.Command("hdiutil", "create", "-fs", "HFS+", "-srcfolder", dir, "-format", "UDRW", fmt.Sprintf("/tmp/%s", fname))
	if err := cmd.Start(); err != nil {
		return "", err
	}
	if err := cmd.Wait(); err != nil {
		return "", err
	}
	log.Debugf("Successfully created disk image %s.dmg from directory %s", fname, dir)
	log.Debugf("Disk image creation took %s", time.Since(start))
	return fmt.Sprintf("/tmp/%s.dmg", fname), nil
}

// func unpackDiskImage(imageName, outputDir string) error {
// 	// start := time.Now()
// 	log.Debugf("Mounting %s using hdiutil", imageName)
// 	cmd := exec.Command("hdiutil", "attach", imageName)
// 	if err := cmd.Start(); err != nil {
// 		log.Debugf("Error starting 'hdiutil attach %s'", imageName)
// 		return err
// 	}
// 	if err := cmd.Wait(); err != nil {
// 		log.Debugf("Error running 'hdiutil attach %s'", imageName)
// 		return err
// 	}

// 	volumeName := fmt.Sprintf("/Volumes/%s", filepath.Base(outputDir))
// 	log.Debugf("Copying outputs from %s to %s using cp -R", volumeName, outputDir)
// 	cmd = exec.Command("cp", "-R", volumeName, outputDir+"/")
// 	if err := cmd.Start(); err != nil {
// 		log.Debugf("Error starting cp -R %s %s", volumeName, outputDir+"/")
// 		return err
// 	}
// 	if err := cmd.Wait(); err != nil {
// 		log.Debugf("Error running cp -R %s %s", volumeName, outputDir+"/")
// 		return err
// 	}
// 	log.Debugf("Successfully copied outputs from %s to %s", volumeName, outputDir)
// 	inspect()
// 	log.Debugf("Unmounting %s using hdiutil", volumeName)
// 	cmd = exec.Command("hdiutil", "detach", volumeName)
// 	if err := cmd.Start(); err != nil {
// 		log.Debugf("Error starting hdiutil detach %s", volumeName)
// 		return err
// 	}
// 	if err := cmd.Wait(); err != nil {
// 		log.Debugf("Error running hdiutil detach %s", volumeName)
// 		return err
// 	}
// 	// log.Debugf("Unpacking disk image with outputs took %s", time.Since(start))
// 	return nil
// }

func (c *vmCommandContainer) exec(ctx context.Context, cmd *repb.Command, workDir string, stdio *interfaces.Stdio) *interfaces.CommandResult {
	diskImage, err := makeDiskImage(workDir)
	if err != nil {
		panic(err)
	}

	stopVm, ip, err := startVm(c.version, diskImage)
	defer func() {
		if stopVm != nil {
			stopVm()
		}
	}()
	if err != nil {
		panic(err)
	}

	vmWorkDir := fmt.Sprintf("/Volumes/%s", filepath.Base(workDir))
	if err = runCmdInVm(ip, vmWorkDir, cmd); err != nil {
		log.Debugf("Error running command in VM: %s", err)
		return &interfaces.CommandResult{
			Error:    err,
			ExitCode: -2,
		}
	}

	if err = scpOutputsFromVm(ip, vmWorkDir, workDir, cmd); err != nil {
		log.Debugf("Error SCPing outputs from VM to localhost: %s", err)
		return &interfaces.CommandResult{
			Error:    err,
			ExitCode: -2,
		}
	}

	// Gotta stop the VM so we can mount the dmg
	if stopVm != nil {
		stopVm()
		stopVm = nil
	}
	if err = killVm(); err != nil {
		panic(fmt.Sprintf("Error killing VM: %s", err))
	}

	result := &interfaces.CommandResult{
		CommandDebugString: "stufffff",
		Stdout:             []byte{},
		Stderr:             []byte{},

		ExitCode: 0,
	}
	return result
}

func inspect() {
	panic("DUH NUH NUH NUH NUH, INSPECTOR GADGET")
}

// Starts the MacOS VM and returns its IP address. The returned cancel function
// MUST be called if it is not nil.
func startVm(version string, diskImage string) (func() error, string, error) {
	start := time.Now()
	// Note: the --ephemeral command means modifications to the diskImage
	// aren't reflected locally.
	cmd := exec.Command(macosvm, "--ephemeral", "--disk", diskImage, fmt.Sprintf("%s/%s/vm.json", vmDir, version))
	cmd.Dir = fmt.Sprintf("%s/%s", vmDir, version)
	cmdReader, err := cmd.StderrPipe()
	if err != nil {
		return nil, "", err
	}
	scanner := bufio.NewScanner(cmdReader)

	// Get the MAC address that the command prints out.
	// Unfortunately, specifying one via flag doesn't seem to actually work.
	macChan := make(chan string, 1)
	go func() {
		sent := false
		for scanner.Scan() {
			line := scanner.Text()
			log.Debugf(line)
			if !strings.Contains(line, "network: ether ") {
				continue
			}
			macChan <- strings.Split(line, "network: ether ")[1]
			sent = true
		}
		if !sent {
			macChan <- ""
		}
	}()
	cmd.Start()
	mac := <-macChan
	log.Debugf("MacOS VM started with MAC address %s", mac)
	if mac == "" {
		cmd.Cancel()
		return nil, "", status.InternalError("Error starting MacOS VM")
	}

	// lol
	mac = strings.TrimLeft(mac, "0")
	mac = strings.ReplaceAll(mac, ":0", ":")

	// Run 'arp -an' and search for the MAC address to find the IP address.
	backoff := time.Second
	for i := 0; i < 100; i++ {
		cmd2 := exec.Command("arp", "-an")
		var stdout bytes.Buffer
		cmd2.Stdout = &stdout
		err := cmd2.Run()
		if err != nil {
			cmd.Cancel()
			return nil, "", status.InternalErrorf("Error finding MacOS VM IP address: %v", err)
		}
		for _, line := range strings.Split(stdout.String(), "\n") {
			if !strings.Contains(line, mac) {
				continue
			}
			ip := strings.Split(strings.Split(line, "(")[1], ")")[0]
			log.Debugf("MacOS VM came up with IP %s", ip)
			log.Debugf("VM creation took %s", time.Since(start))
			return cmd.Cancel, ip, nil
		}
		time.Sleep(backoff)
		log.Debugf("Waiting for MacOS VM to come up...")
	}
	return cmd.Cancel, "", status.InternalError("Error finding MacOS VM IP address")
}

// TODO(iain): get outputs
func runCmdInVm(ip, workDir string, cmd *repb.Command) error {
	start := time.Now()
	sshCmd := exec.Command(
		"sshpass",
		"-p",
		password,
		"ssh",
		fmt.Sprintf("%s@%s", username, ip),
		"-oStrictHostKeyChecking=no",
		"cd",
		workDir,
		"&&")
	sshCmd.Args = append(sshCmd.Args, cmd.Arguments...)
	log.Debugf("Running command in VM: %s", strings.Join(sshCmd.Args, " "))
	cmdReader, err := sshCmd.StdoutPipe()
	if err != nil {
		log.Debugf("%s", err)
		return err
	}
	scanner := bufio.NewScanner(cmdReader)
	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			log.Debugf(line)
		}
	}()
	if err := sshCmd.Start(); err != nil {
		log.Debugf("%s", err)
		return err
	}
	if err := sshCmd.Wait(); err != nil {
		log.Debugf("%s", err)
		return err
	}
	log.Debugf("Running SSH command took %s", time.Since(start))
	return nil
}

// Blech, the .fseventsd file messes this up.
func scpOutputsFromVm(ip, vmWorkDir, localWorkDir string, cmd *repb.Command) error {
	paths := map[string]struct{}{}
	for _, path := range cmd.GetOutputPaths() {
		first := strings.Split(path, "/")[0]
		paths[first] = struct{}{}
	}

	for path := range paths {
		log.Debugf("SCPing %s out of VM", path)
		// start := time.Now()
		scpCmd := exec.Command(
			"sshpass",
			"-p",
			password,
			"scp",
			"-oStrictHostKeyChecking=no",
			"-r",
			fmt.Sprintf("buildbuddy@%s:%s", ip, filepath.Join(vmWorkDir, path)),
			localWorkDir)
		log.Debugf("SCPing outputs from VM: %s", strings.Join(scpCmd.Args, " "))

		cmdReader, err := scpCmd.StdoutPipe()
		if err != nil {
			log.Debugf("%s", err)
			return err
		}
		scanner := bufio.NewScanner(cmdReader)
		go func() {
			for scanner.Scan() {
				line := scanner.Text()
				log.Debugf(line)
			}
		}()
		if err := scpCmd.Start(); err != nil {
			log.Debugf("%s", err)
			return err
		}
		if err := scpCmd.Wait(); err != nil {
			log.Debugf("%s", err)
			return err
		}
	}
	// log.Debugf("Running SCP command took %s", time.Since(start))
	return nil
}

// yikes
func killVm() error {
	start := time.Now()
	cmd := exec.Command("pkill", "macosvm")
	if err := cmd.Start(); err != nil {
		log.Debug("Error starting 'pkill macosvm'")
		return err
	}
	if err := cmd.Wait(); err != nil {
		log.Debug("Error running 'pkill macosvm'")
		return err
	}
	log.Debugf("Force-killing VM took %s", time.Since(start))
	return nil
}

func (c *vmCommandContainer) IsImageCached(ctx context.Context) (bool, error) { return false, nil }
func (c *vmCommandContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	return nil
}
func (c *vmCommandContainer) Start(ctx context.Context) error   { return nil }
func (c *vmCommandContainer) Remove(ctx context.Context) error  { return nil }
func (c *vmCommandContainer) Pause(ctx context.Context) error   { return nil }
func (c *vmCommandContainer) Unpause(ctx context.Context) error { return nil }

func (c *vmCommandContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
	return nil, nil
}

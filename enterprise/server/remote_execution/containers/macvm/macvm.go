package macvm

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var versions = []string{"15.1.1"}

const (
	vmDir   = "/Users/iainmacdonald/Downloads/machack"
	macosvm = vmDir + "/macosvm"
)

type Provider struct {
	version string
}

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
		return nil, status.InvalidArgumentErrorf("Requested MacOS version %s is unsupported", requestedVersion)
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

func (c *vmCommandContainer) exec(ctx context.Context, cmd *repb.Command, workDir string, stdio *interfaces.Stdio) (result *interfaces.CommandResult) {
	cancel, ip, err := startVm(c.version)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		panic(err)
	}

	fmt.Println("===== Woo, let's hack baby =====")
	fmt.Println(ip)
	fmt.Println("===== Command is =====")
	fmt.Println(strings.Join(cmd.Arguments, " "))
	cancel()

	// run command in vm here!
	return commandutil.RunWithOpts(ctx, cmd, &commandutil.RunOpts{
		Dir:    workDir,
		Stdio:  stdio,
		Signal: c.signal,
	})
}

// Starts the MacOS VM and returns its IP address. The returned cancel function
// MUST be called if it is not nil.
func startVm(version string) (func() error, string, error) {
	cmd := exec.Command(macosvm, "--ephemeral", fmt.Sprintf("%s/%s/vm.json", vmDir, version))
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
			return cmd.Cancel, ip, nil
		}
		time.Sleep(backoff)
		log.Debugf("Waiting for MacOS VM to come up...")
	}
	return cmd.Cancel, "", status.InternalError("Error finding MacOS VM IP address")
}

func runCmdInVm(ip string) {
	cmd := exec.Command(
		"sshpass",
		"-p",
		"\"abc123\"",
		"ssh",
		fmt.Sprintf("buildbuddy@%s", ip),
		"-oStrictHostKeyChecking=no",
		"sw_vers")
	fmt.Println(cmd.Args)
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

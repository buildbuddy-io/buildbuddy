package testpodman

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
)

const existenceFile = "/.testpodman.podman_installed"

// Populated by x_defs in BUILD file.
var podmanArchiveRlocationpath string

func install() error {
	// TODO: make this work even when not running inside a VM. We should be able
	// to run podman-static directly from the runfiles directory and configure
	// podman to only use the tools/configs from this directory rather than the
	// system directories.

	// Install the podman version at HEAD by extracting the podman-static
	// distribution under /.
	if _, err := os.Stat(existenceFile); err == nil {
		return nil // Podman is already installed.
	} else if !os.IsNotExist(err) {
		return err
	}
	// We haven't installed podman through this helper, but the execution image
	// may already provide it.
	if _, err := exec.LookPath("podman"); err == nil {
		return nil
	}

	podmanArchiveAbspath, err := runfiles.Rlocation(podmanArchiveRlocationpath)
	if err != nil {
		return fmt.Errorf("locate podman in runfiles: %w", err)
	}
	cmd := exec.Command("tar", "--extract", "--file", podmanArchiveAbspath, "--directory=/")
	b, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("extract podman-static: %w (output: %q)", err, string(b))
	}
	if err := os.WriteFile(existenceFile, nil, 0644); err != nil {
		return fmt.Errorf("create %s: %w", existenceFile, err)
	}
	return nil
}

func TestMain(m *testing.M, beforeRun func() error) {
	// When running on arm64 github runners, execute the test using sudo.
	// This is for two reasons:
	// - Tests on amd64 (firecracker) run as root, and ideally we'd run as
	//   root on both amd64 and arm64 for consistency.
	// - We need root in order to install podman-static under /.
	if runtime.GOARCH == "arm64" && os.Getuid() != 0 {
		args := append([]string{"sudo", "--non-interactive", "--preserve-env"}, os.Args...)
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			if cmd.Process != nil && cmd.ProcessState.Exited() {
				os.Exit(cmd.ProcessState.ExitCode())
			}
			log.Fatal(err)
		}
		os.Exit(0)
	}

	// Install podman-static in the test VM if it isn't installed already.
	if err := install(); err != nil {
		log.Fatalf("Failed to install podman: %s", err)
	}
	if beforeRun != nil {
		if err := beforeRun(); err != nil {
			log.Fatal(err)
		}
	}
	os.Exit(m.Run())
}

// OCI runtime launcher that is invoked by rootlesskit.
// Rootlesskit gives us a uid/gid namespace with ID mappings applied,
// as well as a new mount namespace.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/ociruntime/ociuser"
	"golang.org/x/sys/unix"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

var (
	mountsJSON = flag.String("mounts", "[]", "JSON mounts to apply to the container")
	bundleDir  = flag.String("bundle", "", "Path to the OCI bundle directory")
	rootfsPath = flag.String("rootfs", "", "Path to the rootfs to use for the container")
	configPath = flag.String("config", "", "Path to the OCI spec JSON file")
	workdir    = flag.String("workdir", "", "Path to the workspace directory")
	user       = flag.String("user", "", "User to run the container as")
)

type Mount struct {
	Source string
	Target string
	Fstype string
	Flags  uintptr
	Data   string
}

type Mounts []Mount

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatalf("Launcher failed: %v", err)
	}
}

func run() error {
	// Set up rootfs now that we're in a mount namespace created by rootlesskit,
	// which gives us permission to perform overlay mounts.
	mounts := Mounts{}
	if err := json.Unmarshal([]byte(*mountsJSON), &mounts); err != nil {
		return fmt.Errorf("unmarshal mounts: %w", err)
	}
	for _, m := range mounts {
		if err := unix.Mount(m.Source, m.Target, m.Fstype, m.Flags, m.Data); err != nil {
			return fmt.Errorf("mount %s: %w", m.Source, err)
		}
	}

	// Resolve user spec string ("USER[:GROUP]") to numeric IDs now that rootfs
	// is mounted.
	userSpec, err := ociuser.Resolve(*user, *rootfsPath)
	if err != nil {
		return fmt.Errorf("resolve user spec: %w", err)
	}

	// Patch the OCI spec with the resolved user.
	// TODO: maybe cleaner to generate the whole spec in the launcher?
	ociSpec, err := readSpec(*configPath)
	if err != nil {
		return fmt.Errorf("read spec: %w", err)
	}
	ociSpec.Process.User = *userSpec
	specJSON, err := json.MarshalIndent(ociSpec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}
	if err := os.WriteFile(*configPath, specJSON, 0644); err != nil {
		return fmt.Errorf("write spec: %w", err)
	}

	// Set up ping_group_range now that we're in the network namespace
	// created by rootlesskit (and also now that we have the resolved gid).
	// TODO: probably simpler to just write "0 <max>" unconditionally?
	if err := os.WriteFile("/proc/sys/net/ipv4/ping_group_range", fmt.Appendf(nil, "%d %d", userSpec.GID, userSpec.GID), 0644); err != nil {
		return fmt.Errorf("write ping_group_range: %w", err)
	}

	// If we're running as a non-root user, we won't be able to access files in
	// the action workspace directory since those would've been created by the
	// host user, which maps to the root user in this namespace. So fix
	// workspace file ownership while we're root in this userns, before we run
	// any actions.
	if userSpec.UID != 0 {
		cmd := exec.Command("chown", "--recursive", "--no-dereference", fmt.Sprintf("%d:%d", userSpec.UID, userSpec.GID), *workdir)
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("chown workspace: %w", err)
		}
	}

	// exec OCI runtime (this will replace the launcher process)
	// TODO: maybe we should exec the runtime directly instead of using
	// rootlesskit? There are various hooks that let us run code during various
	// stages of the container lifecycle.
	args := flag.Args()
	if err := syscall.Exec(args[0], args, os.Environ()); err != nil {
		log.Fatalf("Failed to exec: %v", err)
	}
	panic("unreachable")
}

func readSpec(path string) (*specs.Spec, error) {
	specJSON, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read spec: %w", err)
	}
	var spec specs.Spec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return nil, fmt.Errorf("unmarshal spec: %w", err)
	}
	return &spec, nil
}

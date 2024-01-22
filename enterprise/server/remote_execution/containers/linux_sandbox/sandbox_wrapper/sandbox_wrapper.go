// The sandbox wrapper is a tiny program that wraps around Bazel's
// linux-sandbox. It exists to support running linux-sandbox as a non-root
// executor.
//
// It does 2 things:
//  1. Sets up the overlay mounts required to prepare the sandbox root filesystem
//     (/bin, /usr etc.) so that any writes to the FS are only visible to the
//     sandbox, and do not affect the original files.
//  2. Runs linux-sandbox.
//
// The reason we do not do step 1 in the executor itself is a consequence of the
// fact that mount() requires root. It's possible to work around this
// restriction using a configuration that we'll call "pseudo-root", meaning the
// executor would run in its own user namespace and mount namespace, remapping
// the current user ID to uid 0 in the new user NS. However, this causes various
// issues. For example, podman (if enabled on the same executor) would
// incorrectly think it's root (due to the uid being 0 within the NS), and run
// in rootful mode, resulting in permissions issues because /var/lib/containers
// is not writable. But even if we solved that via some configuration tweaking,
// there seems to be a severe performance penalty (3-4X overhead) when podman is
// run inside a user/mount namespace this way. It's likely there are other,
// not-yet-known issues with running the executor as "pseudo-root", since
// changing the uid to appear as root can have wide-reaching implications.
//
// So, instead of running the entire executor as "pseudo-root", we instead only
// run this tiny wrapper program as pseudo-root instead. This binary sets up the
// overlay mounts for us. This way, the namespace does not affect any executor
// functionality aside from linux-sandbox isolation.
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
)

var (
	execRoot    = flag.String("execroot", "", "Sandbox execution root dir.")
	tmpDir      = flag.String("overlay.tmp", "", "Scratch directory where overlayfs workdirs and upperdirs should go.")
	overlayRoot = flag.String("overlay.src_root", "", "Path to the root FS path.")
	overlaySrcs = flag.Slice("overlay.src", []string{}, "Directory paths to mount into the sandbox root using overlayfs, relative to the overlay root.")
)

func main() {
	flag.Parse()

	t := time.Now()

	if os.Getenv("_PERFTEST_DISABLE_OVERLAY") == "1" {
		return
	}

	// Set up overlayfs mounts.
	for _, src := range *overlaySrcs {
		lowerDir := filepath.Join(*overlayRoot, src)
		upperDir := filepath.Join(*tmpDir, "overlay/upper", src)
		workDir := filepath.Join(*tmpDir, "overlay/work", src)
		targetDir := filepath.Join(*tmpDir, "overlay/mnt", src)
		if err := mountOverlay(lowerDir, upperDir, workDir, targetDir); err != nil {
			log.Fatalf("mount: %s", err)
		}
	}

	fmt.Printf("setting up mounts took %s\n", time.Since(t))

	// Invoke linux-sandbox (the executable path and args should be the non-flag
	// args, following the wrapper args that we parsed).
	argv := flag.Args()

	if os.Getenv("_PERFTEST_DISABLE_EXEC") == "1" {
		return
	}

	if err := syscall.Exec(argv[0], argv, os.Environ()); err != nil {
		log.Fatalf("exec %s: %s", argv[0], err)
	}
}

// mountOverlay sets up an overlay mount with a single lower layer using the
// given dirs. The source ("lower") directory must already exist, but the other
// directories will be created automatically if they don't already exist.
func mountOverlay(lowerDir, upperDir, workDir, targetDir string) error {
	for _, d := range []string{upperDir, workDir, targetDir} {
		if err := os.MkdirAll(d, 0755); err != nil {
			return fmt.Errorf("mkdir: %w", err)
		}
	}
	options := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lowerDir, upperDir, workDir)
	if err := syscall.Mount("", targetDir, "overlay", syscall.MS_RELATIME, options); err != nil {
		return fmt.Errorf("mount overlay: %w", err)
	}
	return nil
}

// Uploads a built guest kernel to GCS.
// See README.md in this dir for instructions.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	destinationPrefix = flag.String("destination_prefix", "gs://buildbuddy-tools/binaries/linux", "GCS path prefix used when uploading built guest kernels")
	kernelArch        = flag.String("kernel_arch", "", "Architecture of the guest kernel to upload, for example x86_64 or aarch64")
	kernelVersion     = flag.String("kernel_version", "", "Version of the guest kernel to upload, for example v5.15")
	kernelPath        = flag.String("kernel_path", "", "Path to the built guest kernel artifact. This may be a filesystem path or a Bazel runfiles rlocationpath.")
)

func main() {
	flag.Parse()
	if err := run(*destinationPrefix, *kernelArch, *kernelVersion, *kernelPath); err != nil {
		fmt.Fprintf(os.Stderr, "upload: %s\n", err)
		os.Exit(1)
	}
}

func run(destinationPrefix, arch, version, kernelPath string) error {
	if arch == "" {
		return fmt.Errorf("kernel_arch is required")
	}
	if version == "" {
		return fmt.Errorf("kernel_version is required")
	}
	if kernelPath == "" {
		return fmt.Errorf("kernel_path is required")
	}

	src, err := resolveKernelPath(kernelPath)
	if err != nil {
		return fmt.Errorf("resolve kernel path %q: %w", kernelPath, err)
	}
	sha256Hex, err := fileSHA256(src)
	if err != nil {
		return fmt.Errorf("hash %q: %w", src, err)
	}
	repoName, err := depsRepoName(arch, version)
	if err != nil {
		return err
	}

	destPrefix := strings.TrimRight(destinationPrefix, "/")
	name := fmt.Sprintf("vmlinux-%s-%s-%s", arch, version, sha256Hex)
	dest := destPrefix + "/" + name

	exists, err := gcsObjectExists(dest)
	if err != nil {
		return fmt.Errorf("check whether %q already exists: %w", dest, err)
	}
	if exists {
		log.Infof("Skipping upload for %s %s guest kernel; object already exists at %s", arch, version, dest)
	} else {
		log.Infof("Uploading %s %s guest kernel to %s", arch, version, dest)
		cmd := exec.Command("gsutil", "cp", src, dest)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("upload %s %s guest kernel to %q: %w", arch, version, dest, err)
		}
	}

	fmt.Println()
	fmt.Printf("deps.bzl update is required to use the new %s %s kernel:\n", arch, version)
	fmt.Println()
	fmt.Println("    http_file(")
	fmt.Printf("        name = %q,\n", repoName)
	fmt.Printf("        sha256 = %q,\n", sha256Hex)
	fmt.Printf("        urls = [%q],\n", depsURL(dest))
	fmt.Println("        executable = True,")
	fmt.Println("    )")

	return nil
}

func resolveKernelPath(path string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	return runfiles.Rlocation(path)
}

func depsRepoName(arch, version string) (string, error) {
	switch {
	case arch == "x86_64" && version == "v5.15":
		return "org_kernel_git_linux_kernel-vmlinux", nil
	case arch == "x86_64" && version == "v6.1":
		return "org_kernel_git_linux_kernel-vmlinux-6.1", nil
	case arch == "aarch64" && version == "v5.10":
		return "org_kernel_git_linux_kernel-vmlinux-arm64", nil
	default:
		return "", fmt.Errorf("unsupported kernel target arch=%q version=%q", arch, version)
	}
}

func depsURL(dest string) string {
	if trimmed, ok := strings.CutPrefix(dest, "gs://"); ok {
		return "https://storage.googleapis.com/" + trimmed
	}
	return dest
}

func gcsObjectExists(dest string) (bool, error) {
	cmd := exec.Command("gsutil", "ls", dest)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return true, nil
	}
	msg := string(output)
	if strings.Contains(msg, "matched no objects") ||
		strings.Contains(msg, "No URLs matched") ||
		strings.Contains(msg, "NotFoundException") {
		return false, nil
	}
	return false, fmt.Errorf("gsutil ls: %w: %s", err, strings.TrimSpace(msg))
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

package container

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	diskImageFileName = "containerfs.ext4"
)

func hashString(input string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(input)))
}

func hashFile(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// CachedDiskImagePath looks for an existing cached disk image and returns the
// path to it, if it exists. It returns "" (with no error) if the disk image
// does not exist and no other errors occurred while looking for the image.
func CachedDiskImagePath(workspaceDir, containerImage string) (string, error) {
	hashedContainerName := hashString(containerImage)
	containerImagesPath := filepath.Join(workspaceDir, "executor", hashedContainerName)
	files, err := os.ReadDir(containerImagesPath)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", nil
	}
	sort.Slice(files, func(i, j int) bool {
		var iUnix int64
		if fi, err := files[i].Info(); err == nil {
			iUnix = fi.ModTime().Unix()
		}
		var jUnix int64
		if fi, err := files[j].Info(); err == nil {
			jUnix = fi.ModTime().Unix()
		}
		return iUnix < jUnix
	})
	diskImagePath := filepath.Join(containerImagesPath, files[len(files)-1].Name(), diskImageFileName)
	exists, err := disk.FileExists(diskImagePath)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", nil
	}
	log.Debugf("Found existing %q disk image at path %q", containerImage, diskImagePath)
	return diskImagePath, nil
}

// CreateDiskImage pulls the image from the container registry and creates an
// ext4 disk image from the container image in the user's local cache directory.
// If the image is already available locally, the image is not re-downloaded
// from the registry, but the credentials are still authenticated with the
// remote registry to ensure that the image can be accessed. The path to the
// disk image is returned.
func CreateDiskImage(ctx context.Context, workspaceDir, containerImage string, creds container.PullCredentials) (string, error) {
	existingPath, err := CachedDiskImagePath(workspaceDir, containerImage)
	if err != nil {
		return "", err
	}
	if existingPath != "" {
		// Image is cached. Authenticate with the remote registry to be sure
		// the credentials are valid.

		inspectArgs := []string{"inspect", "--raw", fmt.Sprintf("docker://%s", containerImage)}
		if !creds.IsEmpty() {
			inspectArgs = append(inspectArgs, "--creds", creds.String())
		}
		cmd := exec.CommandContext(ctx, "skopeo", inspectArgs...)
		b, err := cmd.CombinedOutput()
		if err != nil {
			// We don't know whether an authentication error occurred unless we do
			// brittle parsing of the command output. So for now just return
			// UnavailableError which is the "least common denominator" of errors.
			return "", status.UnavailableErrorf(
				"Failed to authenticate with container registry for image %q: %s: %s",
				containerImage, err, string(b),
			)
		}

		return existingPath, nil
	}

	hashedContainerName := hashString(containerImage)
	containerImagesPath := filepath.Join(workspaceDir, "executor", hashedContainerName)

	// container not found -- write one!
	tmpImagePath, err := convertContainerToExt4FS(ctx, workspaceDir, containerImage, creds)
	if err != nil {
		return "", err
	}
	imageHash, err := hashFile(tmpImagePath)
	if err != nil {
		return "", err
	}
	containerImageHome := filepath.Join(containerImagesPath, imageHash)
	if err := disk.EnsureDirectoryExists(containerImageHome); err != nil {
		return "", err
	}
	containerImagePath := filepath.Join(containerImageHome, diskImageFileName)
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
func convertContainerToExt4FS(ctx context.Context, workspaceDir, containerImage string, creds container.PullCredentials) (string, error) {
	// Make a temp directory to work in. Delete it when this fuction returns.
	rootUnpackDir, err := os.MkdirTemp(workspaceDir, "container-unpack-*")
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
	skopeoArgs := []string{"copy", dockerImageRef, ociOutputRef}
	if srcCreds := creds.String(); srcCreds != "" {
		skopeoArgs = append(skopeoArgs, "--src-creds", srcCreds)
	}
	if out, err := exec.CommandContext(ctx, "skopeo", skopeoArgs...).CombinedOutput(); err != nil {
		return "", status.InternalErrorf("skopeo copy error: %q: %s", string(out), err)
	}

	// Make a directory to unpack the bundle to.
	rootFSDir := filepath.Join(rootUnpackDir, "rootfs")
	if err := disk.EnsureDirectoryExists(rootFSDir); err != nil {
		return "", err
	}
	if out, err := exec.CommandContext(ctx, "umoci", "raw", "unpack", "--rootless", "--image", ociImageDir, rootFSDir).CombinedOutput(); err != nil {
		return "", status.InternalErrorf("umoci unpack error: %q: %s", string(out), err)
	}

	// Take the rootfs and write it into an ext4 image.
	f, err := os.CreateTemp(workspaceDir, "containerfs-*.ext4")
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

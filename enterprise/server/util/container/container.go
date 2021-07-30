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

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func hashString(input string) (string, error) {
	h := sha256.New()
	if _, err := h.Write([]byte(input)); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
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

// getOrCreateContainerImage will look for a cached filesystem of the specified
// containerImage in the user's cache directory -- if none is found one will be
// created and cached.
func GetOrCreateImage(ctx context.Context, containerImage string) (string, error) {
	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}

	hashedContainerName, err := hashString(containerImage)
	if err != nil {
		return "", err
	}
	containerFileName := "containerfs.ext4"
	containerImagesPath := filepath.Join(userCacheDir, "executor", hashedContainerName)
	if files, err := os.ReadDir(containerImagesPath); err == nil {
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
		if len(files) > 0 {
			return filepath.Join(containerImagesPath, files[len(files)-1].Name(), containerFileName), nil
		}
	}

	// container not found -- write one!
	tmpImagePath, err := convertContainerToExt4FS(ctx, containerImage)
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
	containerImagePath := filepath.Join(containerImageHome, containerFileName)
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

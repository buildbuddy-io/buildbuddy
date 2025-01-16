package ociconv

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
)

const (
	diskImageFileName = "containerfs.ext4"

	// Minimum timeout used for background Firecracker disk image conversion.
	imageConversionTimeout = 15 * time.Minute
)

var (
	isRoot = os.Getuid() == 0

	// Single-flight group used to dedupe firecracker image conversions.
	conversionGroup singleflight.Group[string, string]
)

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

// getDiskImagesPath returns the parent directory where disk images are stored
// for a given image ref. There may be multiple images associated with the
// same ref if the ref does not include a content digest (e.g. ":latest" tag).
func getDiskImagesPath(cacheRoot, containerImage string) string {
	hashedContainerName := hash.String(containerImage)
	return filepath.Join(cacheRoot, "images", "ext4", hashedContainerName)
}

// CachedDiskImagePath looks for an existing cached disk image and returns the
// path to it, if it exists. It returns "" (with no error) if the disk image
// does not exist and no other errors occurred while looking for the image.
func CachedDiskImagePath(ctx context.Context, cacheRoot, containerImage string) (string, error) {
	diskImagesPath := getDiskImagesPath(cacheRoot, containerImage)
	files, err := os.ReadDir(diskImagesPath)
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
	diskImagePath := filepath.Join(diskImagesPath, files[len(files)-1].Name(), diskImageFileName)
	exists, err := disk.FileExists(ctx, diskImagePath)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", nil
	}
	log.Debugf("Found existing %q disk image at path %q", containerImage, diskImagePath)
	return diskImagePath, nil
}

// CreateDiskImage pulls the image from the container registry and exports an
// ext4 disk image based on the container image to the configured cache
// directory.
//
// If the image is already cached, the image is not re-downloaded from the
// registry, but the credentials are still authenticated with the remote
// registry to ensure that the image can be accessed. The path to the disk image
// is returned.
func CreateDiskImage(ctx context.Context, cacheRoot, containerImage string, creds oci.Credentials) (string, error) {
	existingPath, err := CachedDiskImagePath(ctx, cacheRoot, containerImage)
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

	log.CtxInfof(ctx, "Downloading image %s and converting to ext4 format", containerImage)
	start := time.Now()
	defer func() {
		log.CtxInfof(ctx, "Converted %s to ext4 format in %s", containerImage, time.Since(start))
	}()

	// Dedupe image conversion operations since they are disk IO-heavy.
	conversionOpKey := hash.Strings(
		cacheRoot, containerImage, creds.Username, creds.Password,
	)
	imageDir, _, err := conversionGroup.Do(ctx, conversionOpKey, func(ctx context.Context) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, imageConversionTimeout)
		defer cancel()
		// NOTE: If more params are added to this func, be sure to update
		// conversionOpKey above (if applicable).
		return createExt4Image(ctx, cacheRoot, containerImage, creds)
	})
	return imageDir, err
}

func createExt4Image(ctx context.Context, cacheRoot, containerImage string, creds oci.Credentials) (string, error) {
	diskImagesPath := getDiskImagesPath(cacheRoot, containerImage)
	// container not found -- write one!
	tmpImagePath, err := convertContainerToExt4FS(ctx, cacheRoot, containerImage, creds)
	if err != nil {
		return "", err
	}
	imageHash, err := hashFile(tmpImagePath)
	if err != nil {
		return "", err
	}
	containerImageHome := filepath.Join(diskImagesPath, imageHash)
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

// convertContainerToExt4FS generates an ext4 filesystem image from an OCI
// container image reference.
func convertContainerToExt4FS(ctx context.Context, workspaceDir, containerImage string, creds oci.Credentials) (string, error) {
	img, err := oci.Resolve(ctx, containerImage, oci.RuntimePlatform(), creds)
	if err != nil {
		return "", err
	}
	rc := mutate.Extract(img)
	defer rc.Close()

	tempUnpackDir, err := os.MkdirTemp(workspaceDir, "unpack-*")
	if err != nil {
		return "", status.UnavailableErrorf("error creating temp unpack dir: %s", err)
	}
	defer os.RemoveAll(tempUnpackDir)

	cmd := exec.CommandContext(ctx, "tar", "--extract", "--directory", tempUnpackDir)
	var stderr bytes.Buffer
	cmd.Stdin = rc
	cmd.Stderr = &stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Run(); err != nil {
		return "", status.UnavailableErrorf("download and extract layer tarball: %s: %q", err, stderr.String())
	}

	// Take the rootfs and write it into an ext4 image.
	f, err := os.CreateTemp(workspaceDir, "containerfs-*.ext4")
	if err != nil {
		return "", err
	}
	defer f.Close()
	imageFile := f.Name()
	if err := ext4.DirectoryToImageAutoSize(ctx, tempUnpackDir, imageFile); err != nil {
		return "", err
	}
	log.Debugf("Wrote container %q to image file: %q", containerImage, imageFile)
	return imageFile, nil
}

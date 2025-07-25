package ociconv

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"golang.org/x/sys/unix"
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
	return hex.EncodeToString(h.Sum(nil)), nil
}

// getDiskImagesPath returns the parent directory where disk images are stored
// for a given image ref. There may be multiple images associated with the
// same ref if the ref does not include a content digest (e.g. ":latest" tag).
func getDiskImagesPath(cacheRoot, containerImage string) string {
	hashedContainerName := hash.String(containerImage)
	return filepath.Join(cacheRoot, "images", "ext4", hashedContainerName)
}

// cachedDiskImagePath looks for an existing cached disk image and returns the
// path to it, if it exists. It returns "" (with no error) if the disk image
// does not exist and no other errors occurred while looking for the image.
func cachedDiskImagePath(ctx context.Context, cacheRoot, containerImage string) (string, error) {
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
func CreateDiskImage(ctx context.Context, resolver *oci.Resolver, cacheRoot, containerImage string, creds oci.Credentials) (string, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	existingPath, err := cachedDiskImagePath(ctx, cacheRoot, containerImage)
	if err != nil {
		return "", err
	}
	if existingPath != "" {
		// Image is cached. Authenticate with the remote registry to be sure
		// the credentials are valid.
		if err := authenticateWithRegistry(ctx, resolver, containerImage, creds); err != nil {
			return "", err
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
		return createExt4Image(ctx, resolver, cacheRoot, containerImage, creds)
	})
	return imageDir, err
}

func authenticateWithRegistry(ctx context.Context, resolver *oci.Resolver, containerImage string, creds oci.Credentials) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// Authenticate with the remote registry using these credentials to ensure they are valid.
	if err := resolver.AuthenticateWithRegistry(ctx, containerImage, oci.RuntimePlatform(), creds); err != nil {
		return status.WrapError(err, "authentice with registry")
	}
	return nil
}

func createExt4Image(ctx context.Context, resolver *oci.Resolver, cacheRoot, containerImage string, creds oci.Credentials) (string, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	diskImagesPath := getDiskImagesPath(cacheRoot, containerImage)
	// container not found -- write one!
	tmpImagePath, err := convertContainerToExt4FS(ctx, resolver, cacheRoot, containerImage, creds)
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
func convertContainerToExt4FS(ctx context.Context, resolver *oci.Resolver, workspaceDir, containerImage string, creds oci.Credentials) (string, error) {
	img, err := resolver.Resolve(ctx, containerImage, oci.RuntimePlatform(), creds)
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

// OverlayfsLayerToTarball converts an extracted overlayfs layer to an
// uncompressed OCI image layer tarball, writing the tarball content to the
// given writer.
//
// Note that this does not perfectly reconstruct the original layer tarball. For
// example, it does not preserve hardlinks, and will instead copy each file.
func OverlayfsLayerToTarball(ctx context.Context, w io.Writer, layerDir string) error {
	const fileWhiteoutPrefix = ".wh."
	const dirWhiteoutChildFileName = ".wh..wh..opq"

	tw := tar.NewWriter(w)
	err := filepath.Walk(layerDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relpath, err := filepath.Rel(layerDir, path)
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, "" /*=linkname*/)
		if err != nil {
			return fmt.Errorf("failed to create tar header for %s: %w", path, err)
		}
		header.Name = relpath

		if info.Mode().IsRegular() {
			if err := tw.WriteHeader(header); err != nil {
				return fmt.Errorf("write header for %s: %w", relpath, err)
			}
			f, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("open file %s: %w", relpath, err)
			}
			defer f.Close()
			if _, err := io.Copy(tw, f); err != nil {
				return fmt.Errorf("copy file %s: %w", relpath, err)
			}
		} else if info.Mode().IsDir() {
			rawXattr := make([]byte, 4)
			sz, err := unix.Getxattr(path, "trusted.overlay.opaque", rawXattr)
			if err == nil && sz >= 0 && string(rawXattr[:sz]) == "y" {
				tw.WriteHeader(&tar.Header{
					Name: filepath.Join(relpath, dirWhiteoutChildFileName),
					Mode: 0644,
					Size: 0,
				})
				return nil
			} else {
				if err := tw.WriteHeader(header); err != nil {
					return fmt.Errorf("write header for %s: %w", relpath, err)
				}
			}
		} else if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("readlink %s: %w", relpath, err)
			}
			header.Linkname = target
			if err := tw.WriteHeader(header); err != nil {
				return fmt.Errorf("write header for %s: %w", relpath, err)
			}
		} else if info.Mode()&os.ModeCharDevice != 0 {
			// Char devices with device number 0 are represented as whiteout
			// (deletion) markers in the tarball, by prefixing the file name
			// with ".wh."
			if info.Sys().(*syscall.Stat_t).Rdev == 0 {
				base := filepath.Base(relpath)
				if err := tw.WriteHeader(&tar.Header{
					Name: filepath.Join(filepath.Dir(relpath), fileWhiteoutPrefix+base),
					Mode: 0644,
					Size: 0,
				}); err != nil {
					return fmt.Errorf("write header for %s: %w", relpath, err)
				}
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk layer dir: %w", err)
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("flush tar writer: %w", err)
	}
	return nil
}

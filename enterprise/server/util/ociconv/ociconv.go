package ociconv

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/singleflight"

	dockerclient "github.com/docker/docker/client"
	registryv1 "github.com/google/go-containerregistry/pkg/v1"
)

var (
	debugSkipAuthCheck = flag.Bool("debug_ociconv_skip_auth", false, "Skip auth checks for these images when converting OCI images to other formats (for debugging and testing only).")
)

const (
	diskImageFileName = "containerfs.ext4"

	// Minimum timeout used for background Firecracker disk image conversion.
	imageConversionTimeout = 15 * time.Minute
)

var (
	// Single-flight group used to dedupe ext4 image conversions.
	ext4ConversionGroup singleflight.Group
	// Single-flight group used to dedupe rootfs image extraction.
	rootfsExtractionGroup singleflight.Group
)

var isUserRoot = sync.OnceValues(func() (bool, error) {
	u, err := user.Current()
	if err != nil {
		return false, err
	}
	return u.Uid == "0", nil
})

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
func CachedDiskImagePath(ctx context.Context, buildRoot, containerImage string) (string, error) {
	hashedContainerName := hash.String(containerImage)
	containerImagesPath := filepath.Join(buildRoot, "executor", hashedContainerName)
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
// If dockerClient is non-nil, then Docker will be used to pull and export
// the image. This ensures that image pulls are de-duped and that image layers
// which are already in the local Docker cache can be reused.
//
// If the image is already cached, the image is not re-downloaded from the
// registry, but the credentials are still authenticated with the remote
// registry to ensure that the image can be accessed. The path to the disk image
// is returned.
func CreateDiskImage(ctx context.Context, dockerClient *dockerclient.Client, buildRoot, containerImage string, creds oci.Credentials) (string, error) {
	// Always authenticate with the remote registry to ensure the credentials
	// are valid.
	if err := authenticate(ctx, buildRoot, containerImage, creds); err != nil {
		return "", err
	}

	existingPath, err := CachedDiskImagePath(ctx, buildRoot, containerImage)
	if err != nil {
		return "", err
	}
	if existingPath != "" {
		return existingPath, nil
	}

	// Dedupe image conversion operations, which are disk IO-heavy. Note, we
	// convert the image in the background so that one client's ctx timeout does
	// not affect other clients. We do apply a timeout to the background
	// conversion though to prevent it from running forever.
	conversionOpKey := hash.Strings(buildRoot, containerImage)
	resultChan := ext4ConversionGroup.DoChan(conversionOpKey, func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), imageConversionTimeout)
		defer cancel()
		// NOTE: If more params are added to this func, be sure to update
		// conversionOpKey above (if applicable).
		return createExt4Image(ctx, dockerClient, buildRoot, containerImage, creds)
	})

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-resultChan:
		if res.Err != nil {
			return "", res.Err
		}
		if res.Shared {
			log.CtxInfof(ctx, "De-duped firecracker disk image conversion for %s", containerImage)
		}
		return res.Val.(string), nil
	}
}

func authenticate(ctx context.Context, buildRoot, containerImage string, creds oci.Credentials) error {
	if *debugSkipAuthCheck {
		return nil
	}
	_, err := oci.Resolve(ctx, containerImage, oci.HostPlatform(), creds)
	if err != nil {
		return status.UnauthenticatedErrorf("authenticate image access: %s", err)
	}
	return nil
}

// DO NOT SUBMIT: the caching part is not needed (yet?). Was planning to use
// this to apply ENV vars from the image for greater podman compatibility - but
// it seems like we actually need to fetch the individual layers in order to do
// that?
func resolveAndCache(ctx context.Context, buildRoot, containerImage string, creds oci.Credentials) error {
	path := filepath.Join(buildRoot, "executor", hash.String(containerImage)+".metadata.json")
	image, err := oci.Resolve(ctx, containerImage, oci.HostPlatform(), creds)
	if err != nil {
		return err
	}
	b, err := image.RawManifest()
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, b, 0644); err != nil {
		return status.WrapError(err, "write manifest to disk")
	}
	return nil
}

// GetCachedManifest returns the manifest that was cached during
// resolveAndCache.
func GetCachedManifest(ctx context.Context, buildRoot, containerImage string) (*registryv1.Manifest, error) {
	path := filepath.Join(buildRoot, "executor", hash.String(containerImage)+".metadata.json")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	m, err := registryv1.ParseManifest(f)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func CachedRootFSPath(ctx context.Context, buildRoot, containerImage string) (string, error) {
	rootFSDir := getRootFSDir(buildRoot, containerImage)
	s, err := os.Stat(rootFSDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	if s.IsDir() {
		return rootFSDir, nil
	}
	return "", status.InternalErrorf("root FS path %q exists but is not a directory", rootFSDir)
}

// ExtractContainerImage extracts all image root FS layers and returns the path
// to the root directory where the image was unpacked.
func ExtractContainerImage(ctx context.Context, dockerClient *dockerclient.Client, buildRoot, containerImage string, creds oci.Credentials) (string, error) {
	// Always authenticate with the remote registry to ensure the credentials
	// are valid.
	if err := authenticate(ctx, buildRoot, containerImage, creds); err != nil {
		return "", err
	}
	return pullImageAndUnpackRootfs(ctx, dockerClient, buildRoot, containerImage, creds)
}

func createExt4Image(ctx context.Context, dockerClient *dockerclient.Client, buildRoot, containerImage string, creds oci.Credentials) (string, error) {
	hashedContainerName := hash.String(containerImage)
	containerImagesPath := filepath.Join(buildRoot, "executor", hashedContainerName)

	// container not found -- write one!
	tmpImagePath, err := convertContainerToExt4FS(ctx, dockerClient, buildRoot, containerImage, creds)
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
func convertContainerToExt4FS(ctx context.Context, dockerClient *dockerclient.Client, buildRoot, containerImage string, creds oci.Credentials) (string, error) {
	rootFSDir, err := pullImageAndUnpackRootfs(ctx, dockerClient, buildRoot, containerImage, creds)
	if err != nil {
		return "", err
	}

	// Take the rootfs and write it into an ext4 image.
	f, err := os.CreateTemp(buildRoot, "containerfs-*.ext4")
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

func pullImageAndUnpackRootfs(ctx context.Context, dockerClient *dockerclient.Client, buildRoot, containerImage string, creds oci.Credentials) (string, error) {
	existingPath, err := CachedRootFSPath(ctx, buildRoot, containerImage)
	if err != nil {
		return "", err
	}
	if existingPath != "" {
		return existingPath, nil
	}

	key := hash.Strings(buildRoot, containerImage)
	resultChan := ext4ConversionGroup.DoChan(key, func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), imageConversionTimeout)
		defer cancel()
		// NOTE: If more params are added to this func, be sure to update
		// conversionOpKey above (if applicable).
		return doPullImageAndUnpackRootfs(ctx, dockerClient, buildRoot, containerImage, creds)
	})
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-resultChan:
		if res.Err != nil {
			return "", res.Err
		}
		if res.Shared {
			log.CtxInfof(ctx, "De-duped rootfs extraction for %s", containerImage)
		}
		return res.Val.(string), nil
	}
}

func getRootFSDir(buildRoot, containerImage string) string {
	hashedContainerName := hash.String(containerImage)
	return filepath.Join(buildRoot, "executor", hashedContainerName+".rootfs")
}

func doPullImageAndUnpackRootfs(ctx context.Context, dockerClient *dockerclient.Client, buildRoot, containerImage string, creds oci.Credentials) (string, error) {
	// Make a temp directory to work in. Delete it when this fuction returns.
	rootUnpackDir, err := os.MkdirTemp(buildRoot, "container-unpack-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(rootUnpackDir)

	// Make the directory where we'll unpack the root FS.
	rootFSUnpackDir := filepath.Join(rootUnpackDir, "rootfs")
	if err := os.MkdirAll(rootFSUnpackDir, 0755); err != nil {
		return "", err
	}

	// Make a directory to download the OCI image to.
	ociImageDir := filepath.Join(rootUnpackDir, "image")
	if err := disk.EnsureDirectoryExists(ociImageDir); err != nil {
		return "", err
	}

	// In CLI-form, the commands below do this:
	//
	// docker pull alpine:latest
	// docker save alpine:latest --output /tmp/image_unpack/docker_image.tar
	// skopeo copy docker-archive:/tmp/image_unpack/docker_image.tar oci:/tmp/image_unpack/oci_image:latest
	// umoci unpack --rootless --image /tmp/image_unpack/oci_image /tmp/image_unpack/bundle
	//
	// If docker is not available then we use skopeo to pull the image, like so:
	//
	// skopeo copy docker://alpine:latest oci:/tmp/image_unpack/oci_image:latest
	//
	// After running these commands, /tmp/image_unpack/bundle/rootfs/ has the
	// unpacked image contents.
	//
	// The reason we use docker pull instead of directly copying with skopeo is
	// that docker handles de-duping for us, and we can make use of existing
	// cached image layers.
	//
	// Also note that we use an intermediate `docker save` rather than using the
	// `docker-daemon:` protocol to export the image directly, due to
	// https://github.com/containers/image/issues/1049
	srcRef := fmt.Sprintf("docker://%s", containerImage)
	if dockerClient != nil {
		log.Debugf("Pulling: %s", containerImage)
		if err := docker.PullImage(ctx, dockerClient, containerImage, creds); err != nil {
			return "", err
		}
		log.Debugf("Exporting image from docker daemon: %s", containerImage)
		dockerImgPath := filepath.Join(rootUnpackDir, "docker_image.tar")
		if err := docker.SaveImage(ctx, dockerClient, containerImage, dockerImgPath); err != nil {
			return "", err
		}
		log.Debugf("Converting to OCI image: %s", containerImage)
		srcRef = fmt.Sprintf("docker-archive:%s", dockerImgPath)
	} else {
		log.Debugf("Downloading image and converting to OCI format: %s", containerImage)
	}
	ociOutputRef := fmt.Sprintf("oci:%s:latest", ociImageDir)
	skopeoArgs := []string{"copy", srcRef, ociOutputRef}
	if srcCreds := creds.String(); srcCreds != "" {
		skopeoArgs = append(skopeoArgs, "--src-creds", srcCreds)
	}
	if out, err := exec.CommandContext(ctx, "skopeo", skopeoArgs...).CombinedOutput(); err != nil {
		return "", status.InternalErrorf("skopeo copy error: %q: %s", string(out), err)
	}

	log.Debugf("Unpacking OCI image: %s", containerImage)
	// Note, the "--rootless" flag causes all unpacked files to be owned by the
	// current uid, regardless of their original owner in the source OCI image.
	// This lets us avoid "permission denied" errors when unpacking, but
	// unfortunately since we're messing with the owner uid, it can cause
	// permissions errors when the image is actually used. For example,
	// "/home/foo" will be owned by uid 0 if the executor is root, and non-root
	// users in the resulting VM won't even be able to write to their own home
	// dir.
	//
	// So, we only set --rootless here if we have to, in order to avoid
	// permissions errors when unpacking. But do note that this messes with file
	// permissions in the resulting image, e.g. "/" could be owned by a non-root
	// user which is typically not correct and may cause unexpected issues.
	//
	// TODO: Find another way to convert OCI -> ext4 so that we don't get
	// incorrect permissions when the executor is not running as root.
	isRoot, err := isUserRoot()
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(
		ctx,
		"umoci", "raw", "unpack",
		fmt.Sprintf("--rootless=%v", !isRoot),
		"--image", ociImageDir,
		rootFSUnpackDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", status.InternalErrorf("umoci unpack error: %q: %s", string(out), err)
	}
	// Now that we've successfully unpacked, move it to the finalized,
	// non-temporary path.
	rootFSDir := getRootFSDir(buildRoot, containerImage)
	if err := os.Rename(rootFSUnpackDir, rootFSDir); err != nil {
		return "", err
	}
	return rootFSDir, nil
}

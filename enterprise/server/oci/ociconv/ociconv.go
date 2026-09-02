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
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"golang.org/x/sys/unix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	diskImageFileName = "containerfs.ext4"

	// Minimum timeout used for background Firecracker disk image conversion.
	imageConversionTimeout = 15 * time.Minute

	// Shared images namespace in the filecache. On disk this will appear as
	// "_SHARED_ext4_images" under the filecache dir.
	fileCacheSharedImagesNamespace = "ext4_images"
)

var (
	isRoot = os.Getuid() == 0

	// Single-flight group used to dedupe firecracker image conversions.
	conversionGroup singleflight.Group[string, string]

	localCacheStoreExt4Images = flag.Bool("executor.local_cache_store_ext4_images", true, "If true, store converted Firecracker ext4 images in filecache instead of cacheRoot/images/ext4.")
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

// sharedFileCacheContext returns a context that stores executor-local ext4
// images under a shared filecache directory.
func sharedFileCacheContext(ctx context.Context, fileCache interfaces.FileCache) context.Context {
	return fileCache.WithSharedDirectory(ctx, fileCacheSharedImagesNamespace)
}

func diskImageFileCacheKey(containerImage string) string {
	// Match the legacy cacheRoot/images/ext4/<hash(containerImage)> layout so
	// startup migration can reuse the existing directory name as the filecache
	// key.
	return hash.String(containerImage)
}

func diskImageFileNodeFromKey(key string) *repb.FileNode {
	return &repb.FileNode{
		Digest: &repb.Digest{
			Hash:      key,
			SizeBytes: 1, // Arbitrary: filecache lookups key off the digest hash.
		},
	}
}

func diskImageFileNode(containerImage string) *repb.FileNode {
	return diskImageFileNodeFromKey(diskImageFileCacheKey(containerImage))
}

// DiskImageCacheKey returns the executor-local cache key for a Firecracker
// image. Images produced by the app's synthetic conversion action are keyed by
// that action's digest, so a salt change on the app invalidates executor-local
// copies as well as remote Action Cache entries. Other images are keyed by
// container image ref.
func DiskImageCacheKey(task *repb.ExecutionTask, containerImage string) string {
	if d := task.GetFirecrackerExt4ImageActionDigest(); d != nil {
		return actionDiskImageCacheKey(d)
	}
	return containerImage
}

func actionDiskImageCacheKey(actionDigest *repb.Digest) string {
	return "firecracker-ext4-action/" + digest.String(actionDigest)
}

// getDiskImagesPath returns the parent directory where legacy disk images are
// stored for a given image ref. There may be multiple images associated with
// the same ref if the ref does not include a content digest (e.g. ":latest"
// tag).
func getDiskImagesPath(cacheRoot, containerImage string) string {
	return filepath.Join(cacheRoot, "images", "ext4", diskImageFileCacheKey(containerImage))
}

func legacyDiskImagesRoot(cacheRoot string) string {
	return filepath.Join(cacheRoot, "images", "ext4")
}

func legacyCachedDiskImagePathForKey(ctx context.Context, cacheRoot, cacheKey string) (string, error) {
	diskImagesPath := filepath.Join(legacyDiskImagesRoot(cacheRoot), cacheKey)
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
	log.CtxDebugf(ctx, "Found existing disk image for cache key %q at path %q", cacheKey, diskImagePath)
	return diskImagePath, nil
}

func legacyCachedDiskImagePath(ctx context.Context, cacheRoot, containerImage string) (string, error) {
	return legacyCachedDiskImagePathForKey(ctx, cacheRoot, diskImageFileCacheKey(containerImage))
}

// MigrateImagesToFileCache scans the legacy images/ext4 tree and migrates the
// newest image for each cached container image ref into filecache.
func MigrateImagesToFileCache(ctx context.Context, fileCache interfaces.FileCache, cacheRoot string) error {
	if !*localCacheStoreExt4Images || fileCache == nil {
		return nil
	}
	entries, err := os.ReadDir(legacyDiskImagesRoot(cacheRoot))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read legacy disk images root: %w", err)
	}

	sharedCtx := sharedFileCacheContext(ctx, fileCache)

	var migratedCount int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		cacheKey := entry.Name()
		legacyPath, err := legacyCachedDiskImagePathForKey(ctx, cacheRoot, cacheKey)
		if err != nil {
			return fmt.Errorf("find legacy disk image for %q: %w", cacheKey, err)
		}
		if legacyPath == "" {
			continue
		}
		node := diskImageFileNodeFromKey(cacheKey)
		if fileCache.ContainsFile(sharedCtx, node) {
			continue
		}
		if err := fileCache.AddFile(sharedCtx, node, legacyPath); err != nil {
			return fmt.Errorf("add legacy disk image %q to filecache: %w", legacyPath, err)
		}
		log.Infof("Migrated legacy ext4 image with cache key %q from %q to filecache", cacheKey, legacyPath)
		migratedCount++
	}
	if migratedCount > 0 {
		log.Infof("Migrated %d legacy ext4 image(s) to filecache", migratedCount)
	}
	if err := os.RemoveAll(legacyDiskImagesRoot(cacheRoot)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove legacy disk images root: %w", err)
	}
	return nil
}

// LinkCachedImage hardlinks the cached ext4 image for cacheKey to outputPath.
// cacheKey is the key returned by DiskImageCacheKey.
//
// IMPORTANT: Callers are responsible for authorizing access to the image.
func LinkCachedImage(ctx context.Context, fileCache interfaces.FileCache, cacheRoot, cacheKey, outputPath string) error {
	if *localCacheStoreExt4Images {
		sharedCtx := sharedFileCacheContext(ctx, fileCache)
		node := diskImageFileNode(cacheKey)
		if ok := fileCache.FastLinkFile(sharedCtx, node, outputPath); ok {
			log.CtxDebugf(ctx, "Linked cached %q disk image to %q", cacheKey, outputPath)
			return nil
		}
		// TODO: FastLinkFile can fail for reasons other than NotFound, so we
		// return "Unknown" here. Since we control FastLinkFile, we should
		// instead have it return (ok, err) and wrap the actual error.
		return status.UnknownErrorf("failed to link image %q from local cache", cacheKey)
	}
	legacyPath, err := legacyCachedDiskImagePath(ctx, cacheRoot, cacheKey)
	if err != nil {
		return status.WrapError(err, "get legacy cached disk image path")
	}
	if legacyPath == "" {
		return status.NotFoundErrorf("image %q not found in local cache", cacheKey)
	}
	if err := os.Link(legacyPath, outputPath); err != nil {
		return status.WrapError(err, "link container image")
	}
	log.CtxDebugf(ctx, "Linked cached %q legacy disk image to %q", cacheKey, outputPath)
	return nil
}

// IsImageCached checks whether the ext4 image for cacheKey is present in
// filecache (if enabled) or the legacy ext4 cache dir (if filecache is not
// enabled). cacheKey is the key returned by DiskImageCacheKey.
func IsImageCached(ctx context.Context, fileCache interfaces.FileCache, cacheRoot, cacheKey string) (bool, error) {
	if *localCacheStoreExt4Images {
		sharedCtx := sharedFileCacheContext(ctx, fileCache)
		node := diskImageFileNode(cacheKey)
		return fileCache.ContainsFile(sharedCtx, node), nil
	}
	legacyPath, err := legacyCachedDiskImagePath(ctx, cacheRoot, cacheKey)
	if err != nil {
		return false, err
	}
	return legacyPath != "", nil
}

// CreateDiskImageFromActionCache downloads the ext4 image produced by the
// task's synthetic conversion action, stores it in the executor-local image
// cache, and hardlinks it to outputPath. Concurrent pulls of the same image
// are already serialized by container.PullImageIfNecessary.
func CreateDiskImageFromActionCache(ctx context.Context, env environment.Env, fileCache interfaces.FileCache, cacheRoot string, task *repb.ExecutionTask, outputPath string) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	actionDigest := task.GetFirecrackerExt4ImageActionDigest()
	if actionDigest == nil {
		return status.FailedPreconditionError("task has no Firecracker ext4 image action digest")
	}
	cacheKey := actionDiskImageCacheKey(actionDigest)
	cached, err := IsImageCached(ctx, fileCache, cacheRoot, cacheKey)
	if err != nil {
		return err
	}
	if !cached {
		if err := downloadDiskImage(ctx, env, fileCache, cacheRoot, task, cacheKey); err != nil {
			return err
		}
	}
	if err := LinkCachedImage(ctx, fileCache, cacheRoot, cacheKey, outputPath); err != nil {
		return status.WrapError(err, "link cached image")
	}
	return nil
}

// downloadDiskImage fetches the first output of the synthetic conversion
// action result and adds it to the executor-local image cache under cacheKey.
func downloadDiskImage(ctx context.Context, env environment.Env, fileCache interfaces.FileCache, cacheRoot string, task *repb.ExecutionTask, cacheKey string) error {
	req := task.GetExecuteRequest()
	actionResourceName := digest.NewACResourceName(task.GetFirecrackerExt4ImageActionDigest(), req.GetInstanceName(), req.GetDigestFunction())
	actionResult, err := cachetools.GetActionResult(ctx, env.GetActionCacheClient(), actionResourceName)
	if err != nil {
		return status.WrapError(err, "get ext4 conversion action result")
	}
	if len(actionResult.GetOutputFiles()) == 0 {
		return status.FailedPreconditionError("ext4 conversion action result has no output files")
	}
	outputFile := actionResult.GetOutputFiles()[0]
	if filepath.Ext(outputFile.GetPath()) != ".ext4" {
		log.CtxErrorf(ctx, "Ext4 conversion action output path %q does not have an .ext4 extension", outputFile.GetPath())
		return status.FailedPreconditionErrorf("ext4 conversion action output path %q does not have an .ext4 extension", outputFile.GetPath())
	}

	tmpImage, err := os.CreateTemp(cacheRoot, "containerfs-*.ext4")
	if err != nil {
		return err
	}
	defer tmpImage.Close()
	blobResourceName := digest.NewCASResourceName(outputFile.GetDigest(), req.GetInstanceName(), req.GetDigestFunction())
	if err := cachetools.GetBlob(ctx, env.GetByteStreamClient(), blobResourceName, tmpImage); err != nil {
		os.Remove(tmpImage.Name())
		return status.WrapError(err, "download ext4 image")
	}
	return addDiskImageToCache(ctx, fileCache, cacheRoot, cacheKey, tmpImage.Name())
}

// CreateDiskImage pulls the image from the container registry, converts it to
// ext4 format, stores it in the configured cache directory, and hardlinks the
// cached image to outputPath.
//
// This function does NOT re-authenticate with the registry if the image is
// already cached. Instead, the caller is responsible for doing so if needed.
func CreateDiskImage(ctx context.Context, resolver *oci.Resolver, fileCache interfaces.FileCache, cacheRoot, containerImage string, creds oci.Credentials, useOCIFetcher bool, outputPath string) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// Note: creds are included in the singleflight key here, so it looks like
	// we might be doing unnecessary concurrent pulls for the same image. In
	// practice however, container.PullImageIfNecessary only allows one
	// concurrent pull per image ref, so this should be fine.
	conversionOpKeyParts := []string{cacheRoot, containerImage, creds.Username, creds.Password}
	if *localCacheStoreExt4Images {
		// Dedupe image conversion operations since they are disk IO-heavy.
		conversionOpKeyParts = append([]string{cacheRoot, fileCache.TempDir()}, conversionOpKeyParts[1:]...)
	} else {
		conversionOpKeyParts = append([]string{"legacy"}, conversionOpKeyParts...)
	}
	_, _, err := conversionGroup.Do(ctx, hash.Strings(conversionOpKeyParts...), func(ctx context.Context) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, imageConversionTimeout)
		defer cancel()

		// Re-check the cache here so we can avoid duplicate conversion work if
		// another caller populated the cache after the caller's initial lookup.
		cached, err := IsImageCached(ctx, fileCache, cacheRoot, containerImage)
		if err != nil {
			return "", err
		}
		if cached {
			return "", nil
		}

		// NOTE: If more params are added to this func, be sure to update
		// conversionOpKeyParts above (if applicable).
		if err := createExt4Image(ctx, resolver, fileCache, cacheRoot, containerImage, creds, useOCIFetcher); err != nil {
			return "", status.WrapErrorf(err, "convert %q from OCI to EXT4 format", containerImage)
		}
		return "", nil
	})
	if err != nil {
		return err
	}
	// Note: there is a potential race condition here: createExt4Image just
	// fetched the image into filecache, but there is a short window where it
	// could potentially get evicted. Since we just pulled, this normally
	// shouldn't happen unless the filecache is under heavy eviction pressure,
	// which most likely means it's too small.
	if err := LinkCachedImage(ctx, fileCache, cacheRoot, containerImage, outputPath); err != nil {
		return status.WrapError(err, "failed to link cached image")
	}
	return nil
}

func createExt4Image(ctx context.Context, resolver *oci.Resolver, fileCache interfaces.FileCache, cacheRoot, containerImage string, creds oci.Credentials, useOCIFetcher bool) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	tmpImagePath, err := convertContainerToExt4FS(ctx, resolver, cacheRoot, containerImage, creds, useOCIFetcher)
	if err != nil {
		return err
	}
	return addDiskImageToCache(ctx, fileCache, cacheRoot, containerImage, tmpImagePath)
}

// addDiskImageToCache moves the ext4 image at tmpImagePath into the
// executor-local image cache under cacheKey.
func addDiskImageToCache(ctx context.Context, fileCache interfaces.FileCache, cacheRoot, cacheKey, tmpImagePath string) error {
	if !*localCacheStoreExt4Images || fileCache == nil {
		imageHash, err := hashFile(tmpImagePath)
		if err != nil {
			return err
		}
		imageHome := filepath.Join(getDiskImagesPath(cacheRoot, cacheKey), imageHash)
		if err := disk.EnsureDirectoryExists(imageHome); err != nil {
			return err
		}
		imagePath := filepath.Join(imageHome, diskImageFileName)
		if err := os.Rename(tmpImagePath, imagePath); err != nil {
			return err
		}
		log.CtxDebugf(ctx, "generated rootfs at %q", imagePath)
		return nil
	}
	defer func() {
		if err := os.Remove(tmpImagePath); err != nil && !os.IsNotExist(err) {
			log.CtxWarningf(ctx, "Delete temp disk image %q: %s", tmpImagePath, err)
		}
	}()
	sharedCtx := sharedFileCacheContext(ctx, fileCache)
	if err := fileCache.AddFile(sharedCtx, diskImageFileNode(cacheKey), tmpImagePath); err != nil {
		return fmt.Errorf("add disk image to filecache: %w", err)
	}
	return nil
}

// ConvertContainerToExt4FS pulls an OCI image and writes its root filesystem
// to an ext4 image at outputPath.
func ConvertContainerToExt4FS(ctx context.Context, resolver *oci.Resolver, workspaceDir, containerImage string, creds oci.Credentials, useOCIFetcher bool, outputPath string) error {
	tmpImagePath, err := convertContainerToExt4FS(ctx, resolver, workspaceDir, containerImage, creds, useOCIFetcher)
	if err != nil {
		return err
	}
	defer os.Remove(tmpImagePath)
	if err := os.Rename(tmpImagePath, outputPath); err != nil {
		return fmt.Errorf("move ext4 image to output path: %w", err)
	}
	return nil
}

// convertContainerToExt4FS generates an ext4 filesystem image from an OCI
// container image reference.
func convertContainerToExt4FS(ctx context.Context, resolver *oci.Resolver, workspaceDir, containerImage string, creds oci.Credentials, useOCIFetcher bool) (string, error) {
	log.CtxInfof(ctx, "Downloading image %s and converting to ext4 format", containerImage)
	start := time.Now()

	img, err := resolver.Resolve(ctx, containerImage, oci.RuntimePlatform(), creds, useOCIFetcher)
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

	cmd := exec.CommandContext(
		ctx,
		"tar",
		"--extract",
		"--directory", tempUnpackDir,
		// Skip device nodes under /dev, which tar cannot create without
		// CAP_MKNOD when the conversion runs as a remote action in an OCI
		// container. Guest init mounts devtmpfs over /dev anyway, so the
		// image does not need them. Exclusion patterns are unanchored by
		// default, so anchor this one to avoid dropping paths like
		// usr/lib/foo/dev/bar.
		"--anchored", "--exclude", "dev/*",
	)
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
	log.CtxInfof(ctx, "Converted %s to ext4 format in %s", containerImage, time.Since(start))
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

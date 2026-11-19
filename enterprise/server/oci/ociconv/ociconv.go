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
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/diskimage"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/erofs/go-erofs"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"golang.org/x/sys/unix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctr "github.com/google/go-containerregistry/pkg/v1"
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
	excludeRootDeviceNodes    = flag.Bool("executor.exclude_root_device_nodes", false, "If true, omit entries under the root /dev directory when converting OCI images to ext4. Intended for conversion actions without mknod support.", flag.Internal)
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

// useFileCache reports whether disk images are stored in fileCache rather than
// the legacy cacheRoot/images/ext4 directory.
func useFileCache(fileCache interfaces.FileCache) bool {
	return *localCacheStoreExt4Images && fileCache != nil
}

// sharedFileCacheContext returns a context that stores executor-local ext4
// images under a shared filecache directory.
func sharedFileCacheContext(ctx context.Context, fileCache interfaces.FileCache) context.Context {
	return fileCache.WithSharedDirectory(ctx, fileCacheSharedImagesNamespace)
}

func diskImageFileCacheKey(containerImage string, format diskimage.Format) string {
	if format == diskimage.Ext4 {
		// Match the legacy cacheRoot/images/ext4/<hash(containerImage)> layout
		// so startup migration can reuse the existing directory name as the
		// filecache key.
		return hash.String(containerImage)
	}
	return hash.Strings(string(format), containerImage)
}

func diskImageFileNodeFromKey(key string) *repb.FileNode {
	return &repb.FileNode{
		Digest: &repb.Digest{
			Hash:      key,
			SizeBytes: 1, // Arbitrary: filecache lookups key off the digest hash.
		},
	}
}

func diskImageFileNode(containerImage string, format diskimage.Format) *repb.FileNode {
	return diskImageFileNodeFromKey(diskImageFileCacheKey(containerImage, format))
}

// checkLegacyFormat returns an error if format can't be stored in the legacy
// cache directory, which only holds ext4 images.
func checkLegacyFormat(format diskimage.Format) error {
	if format != diskimage.Ext4 {
		return status.UnimplementedErrorf("%s images require executor.local_cache_store_ext4_images", format)
	}
	return nil
}

// AddDiskImageToFileCache adds a prebuilt disk image using the same shared
// filecache key that Firecracker uses for converted OCI images.
func AddDiskImageToFileCache(ctx context.Context, fileCache interfaces.FileCache, containerImage string, format diskimage.Format, imagePath string) error {
	sharedCtx := sharedFileCacheContext(ctx, fileCache)
	node := diskImageFileNode(containerImage, format)
	return fileCache.AddFile(sharedCtx, node, imagePath)
}

// getDiskImagesPath returns the parent directory where legacy disk images are
// stored for a given image ref. There may be multiple images associated with
// the same ref if the ref does not include a content digest (e.g. ":latest"
// tag).
func getDiskImagesPath(cacheRoot, containerImage string) string {
	return filepath.Join(cacheRoot, "images", "ext4", diskImageFileCacheKey(containerImage, diskimage.Ext4))
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
	return legacyCachedDiskImagePathForKey(ctx, cacheRoot, diskImageFileCacheKey(containerImage, diskimage.Ext4))
}

// MigrateImagesToFileCache scans the legacy images/ext4 tree and migrates the
// newest image for each cached container image ref into filecache.
func MigrateImagesToFileCache(ctx context.Context, fileCache interfaces.FileCache, cacheRoot string) error {
	if !useFileCache(fileCache) {
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

// LinkCachedImage links a cached OCI disk image.
// If outputPath is non-empty, the cached image is hardlinked there.
//
// IMPORTANT: Callers are responsible for authorizing access to the image.
func LinkCachedImage(ctx context.Context, fileCache interfaces.FileCache, cacheRoot, containerImage string, format diskimage.Format, outputPath string) error {
	if useFileCache(fileCache) {
		sharedCtx := sharedFileCacheContext(ctx, fileCache)
		node := diskImageFileNode(containerImage, format)
		if ok := fileCache.FastLinkFile(sharedCtx, node, outputPath); ok {
			log.CtxDebugf(ctx, "Linked cached %q disk image to %q", containerImage, outputPath)
			return nil
		}
		// TODO: FastLinkFile can fail for reasons other than NotFound, so we
		// return "Unknown" here. Since we control FastLinkFile, we should
		// instead have it return (ok, err) and wrap the actual error.
		return status.UnknownErrorf("failed to link image %q from local cache", containerImage)
	}
	if err := checkLegacyFormat(format); err != nil {
		return err
	}
	legacyPath, err := legacyCachedDiskImagePath(ctx, cacheRoot, containerImage)
	if err != nil {
		return status.WrapError(err, "get legacy cached disk image path")
	}
	if legacyPath == "" {
		return status.NotFoundErrorf("image %q not found in local cache", containerImage)
	}
	if err := os.Link(legacyPath, outputPath); err != nil {
		return status.WrapError(err, "link container image")
	}
	log.CtxDebugf(ctx, "Linked cached %q legacy disk image to %q", containerImage, outputPath)
	return nil
}

// IsImageCached checks whether a disk image is present in filecache (if
// enabled) or the legacy ext4 cache dir (if filecache is not enabled).
func IsImageCached(ctx context.Context, fileCache interfaces.FileCache, cacheRoot, containerImage string, format diskimage.Format) (bool, error) {
	if useFileCache(fileCache) {
		sharedCtx := sharedFileCacheContext(ctx, fileCache)
		node := diskImageFileNode(containerImage, format)
		return fileCache.ContainsFile(sharedCtx, node), nil
	}
	if err := checkLegacyFormat(format); err != nil {
		return false, err
	}
	legacyPath, err := legacyCachedDiskImagePath(ctx, cacheRoot, containerImage)
	if err != nil {
		return false, err
	}
	return legacyPath != "", nil
}

// CreateDiskImage pulls the image from the container registry, converts it to
// the given format, stores it in the configured cache directory, and hardlinks
// the cached image to outputPath.
//
// This function does NOT re-authenticate with the registry if the image is
// already cached. Instead, the caller is responsible for doing so if needed.
func CreateDiskImage(ctx context.Context, resolver *oci.Resolver, fileCache interfaces.FileCache, cacheRoot, containerImage string, format diskimage.Format, creds oci.Credentials, useOCIFetcher bool, outputPath string) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()

	// Note: creds are included in the singleflight key here, so it looks like
	// we might be doing unnecessary concurrent pulls for the same image. In
	// practice however, container.PullImageIfNecessary only allows one
	// concurrent pull per image ref, so this should be fine.
	conversionOpKeyParts := []string{cacheRoot, containerImage, string(format), creds.Username, creds.Password}
	if useFileCache(fileCache) {
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
		cached, err := IsImageCached(ctx, fileCache, cacheRoot, containerImage, format)
		if err != nil {
			return "", err
		}
		if cached {
			return "", nil
		}

		// NOTE: If more params are added to this func, be sure to update
		// conversionOpKeyParts above (if applicable).
		out, err := createDiskImage(ctx, resolver, fileCache, cacheRoot, containerImage, format, creds, useOCIFetcher)
		if err != nil {
			return "", status.WrapErrorf(err, "convert %q from OCI to %s format", containerImage, format)
		}
		return out, nil
	})
	if err != nil {
		return err
	}
	// Note: there is a potential race condition here: createDiskImage just
	// fetched the image into filecache, but there is a short window where it
	// could potentially get evicted. Since we just pulled, this normally
	// shouldn't happen unless the filecache is under heavy eviction pressure,
	// which most likely means it's too small.
	if err := LinkCachedImage(ctx, fileCache, cacheRoot, containerImage, format, outputPath); err != nil {
		return status.WrapError(err, "failed to link cached image")
	}
	return nil
}

func createDiskImage(ctx context.Context, resolver *oci.Resolver, fileCache interfaces.FileCache, cacheRoot, containerImage string, format diskimage.Format, creds oci.Credentials, useOCIFetcher bool) (string, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	legacy := !useFileCache(fileCache)
	if legacy {
		if err := checkLegacyFormat(format); err != nil {
			return "", err
		}
	}
	tmpImage, err := os.CreateTemp(cacheRoot, "containerfs-*."+string(format))
	if err != nil {
		return "", err
	}
	tmpImagePath := tmpImage.Name()
	defer func() {
		if err := os.Remove(tmpImagePath); err != nil && !os.IsNotExist(err) {
			log.CtxWarningf(ctx, "Delete temp disk image %q: %s", tmpImagePath, err)
		}
	}()
	if err := tmpImage.Close(); err != nil {
		return "", fmt.Errorf("close temp disk image: %w", err)
	}
	img, err := resolver.Resolve(ctx, containerImage, oci.RuntimePlatform(), creds, useOCIFetcher)
	if err != nil {
		return "", err
	}
	switch format {
	case diskimage.Ext4:
		err = ConvertContainerToExt4FS(ctx, img, cacheRoot, containerImage, tmpImagePath)
	case diskimage.Erofs:
		err = ConvertContainerToErofs(ctx, img, cacheRoot, containerImage, tmpImagePath)
	default:
		err = status.InvalidArgumentErrorf("unknown disk image format %q", format)
	}
	if err != nil {
		return "", err
	}
	if legacy {
		imageHash, err := hashFile(tmpImagePath)
		if err != nil {
			return "", err
		}
		containerImageHome := filepath.Join(getDiskImagesPath(cacheRoot, containerImage), imageHash)
		if err := disk.EnsureDirectoryExists(containerImageHome); err != nil {
			return "", err
		}
		containerImagePath := filepath.Join(containerImageHome, diskImageFileName)
		if err := os.Rename(tmpImagePath, containerImagePath); err != nil {
			return "", err
		}
		log.CtxDebugf(ctx, "generated rootfs at %q", containerImagePath)
		return containerImagePath, nil
	}
	if err := AddDiskImageToFileCache(ctx, fileCache, containerImage, format, tmpImagePath); err != nil {
		return "", fmt.Errorf("add disk image to filecache: %w", err)
	}
	return "", nil
}

// ConvertContainerToExt4FS writes a resolved OCI image's root filesystem to an
// ext4 image at outputPath. containerImage identifies the image in log messages.
func ConvertContainerToExt4FS(ctx context.Context, img ctr.Image, workspaceDir, containerImage, outputPath string) error {
	log.CtxInfof(ctx, "Downloading image %s and converting to ext4 format", containerImage)
	start := time.Now()

	rc := mutate.Extract(img)
	defer rc.Close()

	tempUnpackDir, err := os.MkdirTemp(workspaceDir, "unpack-*")
	if err != nil {
		return status.UnavailableErrorf("error creating temp unpack dir: %s", err)
	}
	defer os.RemoveAll(tempUnpackDir)

	tarArgs := []string{
		"--extract",
		"--directory", tempUnpackDir,
	}
	if *excludeRootDeviceNodes {
		tarArgs = append(tarArgs,
			"--anchored",
			// Conversion actions may lack CAP_MKNOD even when running as root,
			// so tar cannot recreate OCI device entries. Guest startup
			// mounts devtmpfs over /dev, so entries from this root directory
			// would not be visible anyway.
			"--exclude", "dev/*",
		)
	}
	cmd := exec.CommandContext(ctx, "tar", tarArgs...)
	var stderr bytes.Buffer
	cmd.Stdin = rc
	cmd.Stderr = &stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Run(); err != nil {
		return status.UnavailableErrorf("download and extract layer tarball: %s: %q", err, stderr.String())
	}

	// Take the rootfs and write it into an ext4 image.
	if err := ext4.DirectoryToImageAutoSize(ctx, tempUnpackDir, outputPath); err != nil {
		return err
	}
	log.CtxInfof(ctx, "Converted %s to ext4 format in %s", containerImage, time.Since(start))
	return nil
}

// ConvertContainerToErofs writes a resolved OCI image's root filesystem to an
// uncompressed EROFS image at outputPath. File data is spooled to a temp file
// under workspaceDir. containerImage identifies the image in log messages.
func ConvertContainerToErofs(ctx context.Context, img ctr.Image, workspaceDir, containerImage, outputPath string) error {
	log.CtxInfof(ctx, "Downloading image %s and converting to EROFS format", containerImage)
	start := time.Now()
	if fi, err := os.Stat(outputPath); err == nil && fi.Size() > 0 {
		return status.InternalErrorf("create EROFS image %s: file already exists and is not empty", outputPath)
	}
	f, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return status.UnavailableErrorf("create EROFS image: %s", err)
	}
	defer f.Close()

	rc := mutate.Extract(img)
	defer rc.Close()
	// A fixed build time keeps the image reproducible.
	w := erofs.Create(f, erofs.WithBuildTime(0, 0), erofs.WithTempDir(workspaceDir))
	if err := tarToErofs(ctx, tar.NewReader(rc), w); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return status.UnavailableErrorf("write EROFS image: %s", err)
	}
	if err := f.Close(); err != nil {
		return status.UnavailableErrorf("close EROFS image: %s", err)
	}
	log.CtxInfof(ctx, "Converted %s to EROFS format in %s", containerImage, time.Since(start))
	return nil
}

// tarToErofs adds the entries of a flattened image tarball from
// mutate.Extract to w.
func tarToErofs(ctx context.Context, tr *tar.Reader, w *erofs.Writer) error {
	// mutate.Extract emits upper layers first, so a hardlink can precede its
	// target and a file can precede its parent directory's entry.
	var links []*tar.Header
	for {
		if ctx.Err() != nil {
			return status.FromContextError(ctx)
		}
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.UnavailableErrorf("read image tarball: %s", err)
		}
		name := path.Join("/", hdr.Name)
		if hiddenByUpperLayer(w, name, hdr.Typeflag == tar.TypeDir) {
			continue
		}
		if hdr.Typeflag == tar.TypeLink {
			links = append(links, hdr)
			continue
		}
		if err := addTarEntry(tr, w, name, hdr); err != nil {
			return status.InternalErrorf("add %q to EROFS image: %s", hdr.Name, err)
		}
	}
	for _, hdr := range links {
		if err := w.Link(path.Join("/", hdr.Linkname), path.Join("/", hdr.Name)); err != nil {
			return status.InternalErrorf("add hardlink %q to EROFS image: %s", hdr.Name, err)
		}
	}
	return nil
}

// hiddenByUpperLayer reports whether an entry at name was already added from
// an upper layer, or is under a path that an upper layer replaced with a
// non-directory. Directories that were only created implicitly as parents
// are not hidden.
func hiddenByUpperLayer(w *erofs.Writer, name string, isDir bool) bool {
	if fi, err := w.Lstat(name); err == nil {
		return !(isDir && fi.IsDir())
	}
	for d := path.Dir(name); d != "/"; d = path.Dir(d) {
		if fi, err := w.Lstat(d); err == nil {
			return !fi.IsDir()
		}
	}
	return false
}

func addTarEntry(tr *tar.Reader, w *erofs.Writer, name string, hdr *tar.Header) error {
	mode := hdr.FileInfo().Mode()
	switch hdr.Typeflag {
	case tar.TypeDir:
		if _, err := w.Lstat(name); err == nil {
			if err := w.Chmod(name, mode); err != nil {
				return err
			}
		} else if err := w.Mkdir(name, mode); err != nil {
			return err
		}
	case tar.TypeReg:
		f, err := w.Create(name)
		if err != nil {
			return err
		}
		if err := f.Chmod(mode); err != nil {
			return err
		}
		if _, err := io.Copy(f, tr); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	case tar.TypeSymlink:
		if err := w.Symlink(hdr.Linkname, name); err != nil {
			return err
		}
	case tar.TypeChar, tar.TypeBlock, tar.TypeFifo:
		fileType := map[byte]uint32{tar.TypeChar: unix.S_IFCHR, tar.TypeBlock: unix.S_IFBLK, tar.TypeFifo: unix.S_IFIFO}[hdr.Typeflag]
		rdev := unix.Mkdev(uint32(hdr.Devmajor), uint32(hdr.Devminor))
		if err := w.Mknod(name, uint16(fileType|uint32(hdr.Mode&0o7777)), uint32(rdev)); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported tar entry type %q", hdr.Typeflag)
	}
	if err := w.Chown(name, hdr.Uid, hdr.Gid); err != nil {
		return err
	}
	if err := w.Chtimes(name, hdr.ModTime, hdr.ModTime); err != nil {
		return err
	}
	for k, v := range hdr.PAXRecords {
		if attr, ok := strings.CutPrefix(k, "SCHILY.xattr."); ok {
			if err := w.Setxattr(name, attr, v); err != nil {
				return err
			}
		}
	}
	return nil
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

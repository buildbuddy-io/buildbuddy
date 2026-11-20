package ociconv_test

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ociconv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/diskimage"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmount"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testtar"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/erofs/go-erofs"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestMain(m *testing.M) {
	testmount.RunWithLimitedMountPermissions(m)
}

func writeLegacyImage(t testing.TB, root, containerImage, dirName, content string) string {
	t.Helper()

	legacyPath := filepath.Join(root, "images", "ext4", hash.String(containerImage), dirName, "containerfs.ext4")
	require.NoError(t, os.MkdirAll(filepath.Dir(legacyPath), 0755))
	require.NoError(t, os.WriteFile(legacyPath, []byte(content), 0644))
	return legacyPath
}

func sharedDiskImageNode(containerImage string) *repb.FileNode {
	return &repb.FileNode{
		Digest: &repb.Digest{
			Hash:      hash.String(containerImage),
			SizeBytes: 1,
		},
	}
}

func TestOciconv(t *testing.T) {
	testOciconv(t, diskimage.Ext4, func(ctx context.Context, t *testing.T, imagePath string) map[string]int64 {
		outDir := testfs.MakeTempDir(t)
		require.NoError(t, ext4.ImageToDirectory(ctx, imagePath, outDir, []string{"/"}))
		return regularFileSizes(t, os.DirFS(outDir))
	})
}

func TestOciconvErofs(t *testing.T) {
	fc, root, ref := testOciconv(t, diskimage.Erofs, func(ctx context.Context, t *testing.T, imagePath string) map[string]int64 {
		return regularFileSizes(t, openErofs(t, imagePath))
	})
	// Formats are cached separately.
	cached, err := ociconv.IsImageCached(context.Background(), fc, root, ref, diskimage.Ext4)
	require.NoError(t, err)
	require.False(t, cached)
}

// testOciconv converts a test image to the given format and checks that
// listFiles finds the image's regular files in it. It returns the filecache,
// cache root, and image ref used.
func testOciconv(t *testing.T, format diskimage.Format, listFiles func(ctx context.Context, t *testing.T, imagePath string) map[string]int64) (interfaces.FileCache, string, string) {
	te := testenv.GetTestEnv(t)
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
	flags.Set(t, "executor.local_cache_store_ext4_images", true)

	ctx := context.Background()
	root := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(filepath.Join(root, "filecache"), 1_000_000_000, false)
	require.NoError(t, err)
	te.SetFileCache(fc)

	reg := testregistry.Run(t, testregistry.Opts{})

	ref, img := reg.PushNamedImage(t, "ociconv-test-image:latest", nil)

	resolver, err := oci.NewResolver(te)
	require.NoError(t, err)
	require.NotNil(t, resolver)

	path := filepath.Join(root, "materialized."+string(format))
	err = ociconv.CreateDiskImage(ctx, resolver, fc, root, ref, format, oci.Credentials{}, false /*=useOCIFetcher*/, path)
	require.NoError(t, err)

	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.False(t, fi.IsDir())
	require.Greater(t, fi.Size(), int64(0))

	// Build path->size map from the container image (regular files only).
	rc := mutate.Extract(img)
	defer rc.Close()
	tr := tar.NewReader(rc)
	imageFiles := make(map[string]int64)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if hdr == nil {
			continue
		}
		switch hdr.Typeflag {
		case tar.TypeReg, tar.TypeRegA:
			name := strings.TrimPrefix(hdr.Name, "./")
			name = strings.TrimPrefix(name, "/")
			if name == "" {
				continue
			}
			imageFiles[name] = hdr.Size
		}
	}

	require.Empty(t, cmp.Diff(imageFiles, listFiles(ctx, t, path)))
	return fc, root, ref
}

// regularFileSizes returns the size of each regular file in fsys by path.
func regularFileSizes(t *testing.T, fsys fs.FS) map[string]int64 {
	files := make(map[string]int64)
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		files[p] = info.Size()
		return nil
	})
	require.NoError(t, err)
	return files
}

func openErofs(t *testing.T, imagePath string) fs.FS {
	f, err := os.Open(imagePath)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	fsys, err := erofs.Open(f)
	require.NoError(t, err)
	return fsys
}

func erofsStat(t *testing.T, fsys fs.FS, name string) *erofs.Stat {
	fi, err := fs.Stat(fsys, name)
	require.NoError(t, err, name)
	return fi.Sys().(*erofs.Stat)
}

func TestConvertContainerToErofs_Metadata(t *testing.T) {
	mtime := time.Unix(1700000000, 0)
	hdr := func(h tar.Header) *tar.Header {
		h.ModTime = mtime
		h.Format = tar.FormatPAX
		return &h
	}
	lower := testtar.EntriesBytes(t, []testtar.Entry{
		{Header: hdr(tar.Header{Name: "etc/", Typeflag: tar.TypeDir, Mode: 0o750, Uid: 7, Gid: 8})},
		{Header: hdr(tar.Header{Name: "etc/passwd", Typeflag: tar.TypeReg, Mode: 0o644}), Data: []byte("root:x:0:0")},
		{Header: hdr(tar.Header{Name: "bin/", Typeflag: tar.TypeDir, Mode: 0o755})},
		{Header: hdr(tar.Header{Name: "bin/su", Typeflag: tar.TypeReg, Mode: 0o4755}), Data: []byte("su")},
		{Header: hdr(tar.Header{Name: "bin/ping", Typeflag: tar.TypeReg, Mode: 0o755, Uid: 1234, Gid: 5678, PAXRecords: map[string]string{
			"SCHILY.xattr.user.test": "value",
		}}), Data: []byte("ping")},
		{Header: hdr(tar.Header{Name: "dev/null", Typeflag: tar.TypeChar, Mode: 0o666, Devmajor: 1, Devminor: 3})},
		{Header: hdr(tar.Header{Name: "lib/", Typeflag: tar.TypeDir, Mode: 0o755})},
		{Header: hdr(tar.Header{Name: "lib/libc.so", Typeflag: tar.TypeReg, Mode: 0o644}), Data: []byte("libc")},
		{Header: hdr(tar.Header{Name: "deleted", Typeflag: tar.TypeReg, Mode: 0o644}), Data: []byte("deleted")},
	})
	upper := testtar.EntriesBytes(t, []testtar.Entry{
		// Archive metadata, as written by e.g. `git archive`; not a file.
		{Header: &tar.Header{Name: "pax_global_header", Typeflag: tar.TypeXGlobalHeader, Format: tar.FormatPAX, PAXRecords: map[string]string{"comment": "abc123"}}},
		// GNU tar extracts contiguous files as regular files.
		{Header: hdr(tar.Header{Name: "bin/cont", Typeflag: tar.TypeCont, Mode: 0o644}), Data: []byte("cont")},
		// Precedes its parent dir's entry, which comes from the lower layer.
		{Header: hdr(tar.Header{Name: "etc/hostname", Typeflag: tar.TypeReg, Mode: 0o644, Uid: 9, Gid: 9}), Data: []byte("box")},
		// Precedes its target, which comes from the lower layer.
		{Header: hdr(tar.Header{Name: "bin/su2", Typeflag: tar.TypeLink, Linkname: "bin/su"})},
		// Hides the lower layer's lib/ contents.
		{Header: hdr(tar.Header{Name: "lib", Typeflag: tar.TypeSymlink, Linkname: "usr/lib"})},
		{Header: hdr(tar.Header{Name: ".wh.deleted", Typeflag: tar.TypeReg})},
	})
	img, err := mutate.AppendLayers(empty.Image, testregistry.NewBytesLayer(t, lower), testregistry.NewBytesLayer(t, upper))
	require.NoError(t, err)

	ctx := context.Background()
	dir := testfs.MakeTempDir(t)
	imagePath := filepath.Join(dir, "image.erofs")
	require.NoError(t, ociconv.ConvertContainerToErofs(ctx, img, dir, "test-image", imagePath))
	fsys := openErofs(t, imagePath)

	require.Equal(t, map[string]int64{
		"etc/passwd":   10,
		"etc/hostname": 3,
		"bin/su":       2,
		"bin/su2":      2,
		"bin/ping":     4,
		"bin/cont":     4,
	}, regularFileSizes(t, fsys))

	etc := erofsStat(t, fsys, "etc")
	require.Equal(t, fs.ModeDir|0o750, etc.Mode)
	require.Equal(t, []uint32{7, 8}, []uint32{etc.UID, etc.GID})
	require.Equal(t, uint64(mtime.Unix()), etc.Mtime)

	su := erofsStat(t, fsys, "bin/su")
	require.Equal(t, fs.ModeSetuid|0o755, su.Mode)
	require.Equal(t, 2, su.Nlink)
	require.Equal(t, su.Ino, erofsStat(t, fsys, "bin/su2").Ino)

	ping := erofsStat(t, fsys, "bin/ping")
	require.Equal(t, []uint32{1234, 5678}, []uint32{ping.UID, ping.GID})
	require.Equal(t, "value", ping.Xattrs["user.test"])

	null := erofsStat(t, fsys, "dev/null")
	require.Equal(t, fs.ModeDevice|fs.ModeCharDevice|0o666, null.Mode)
	require.Equal(t, uint32(unix.Mkdev(1, 3)), null.Rdev)

	target, err := fs.ReadLink(fsys, "lib")
	require.NoError(t, err)
	require.Equal(t, "usr/lib", target)

	// Converting again produces an identical image.
	imagePath2 := filepath.Join(dir, "image2.erofs")
	require.NoError(t, ociconv.ConvertContainerToErofs(ctx, img, dir, "test-image", imagePath2))
	b1, err := os.ReadFile(imagePath)
	require.NoError(t, err)
	b2, err := os.ReadFile(imagePath2)
	require.NoError(t, err)
	require.True(t, bytes.Equal(b1, b2), "EROFS images differ")
}

func TestCreateDiskImage_UsesCredentialsOnCacheMiss(t *testing.T) {
	te := testenv.GetTestEnv(t)
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
	flags.Set(t, "executor.local_cache_store_ext4_images", true)

	ctx := context.Background()
	root := testfs.MakeTempDir(t)
	os.Setenv("REGISTRY_AUTH_FILE", "_null")
	fc, err := filecache.NewFileCache(filepath.Join(root, "filecache"), 1_000_000_000, false)
	require.NoError(t, err)
	te.SetFileCache(fc)

	authEnabled := false
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			if !authEnabled {
				return true
			}
			user, pass, ok := r.BasicAuth()
			if !ok {
				// Respond with WWW-Authenticate header to trigger basic auth.
				w.Header().Set("WWW-Authenticate", "Basic")
				w.WriteHeader(http.StatusUnauthorized)
				return false
			}
			if user != "test" || pass != "test" {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return false
			}
			return true
		},
	})
	t.Cleanup(func() { reg.Shutdown() })

	// Bypass auth while pushing the image.
	authEnabled = false
	ref, _ := reg.PushNamedImage(t, "test-empty-image:latest", nil)
	authEnabled = true

	resolver, err := oci.NewResolver(te)
	require.NoError(t, err)
	require.NotNil(t, resolver)

	outputPath := func(name string) string {
		return filepath.Join(root, name+".ext4")
	}

	// This should fail because the credentials are invalid and the image is not
	// cached yet.
	err = ociconv.CreateDiskImage(ctx, resolver, fc, root, ref, diskimage.Ext4, oci.Credentials{}, false /*=useOCIFetcher*/, outputPath("first"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401 Unauthorized")

	// This should succeed because the credentials are valid and the image is not
	// cached yet.
	err = ociconv.CreateDiskImage(ctx, resolver, fc, root, ref, diskimage.Ext4, oci.Credentials{
		Username: "test",
		Password: "test",
	}, false /*=useOCIFetcher*/, outputPath("second"))
	require.NoError(t, err)
	require.FileExists(t, outputPath("second"))

	// Once the image is cached, CreateDiskImage's internal re-check should
	// short-circuit before attempting another registry access, while still
	// linking the cached image to the requested output path.
	err = ociconv.CreateDiskImage(ctx, resolver, fc, root, ref, diskimage.Ext4, oci.Credentials{}, false /*=useOCIFetcher*/, outputPath("third"))
	require.NoError(t, err)
	require.FileExists(t, outputPath("third"))

	// A subsequent call with valid credentials should also succeed when the
	// image is already cached.
	err = ociconv.CreateDiskImage(ctx, resolver, fc, root, ref, diskimage.Ext4, oci.Credentials{
		Username: "test",
		Password: "test",
	}, false /*=useOCIFetcher*/, outputPath("fourth"))
	require.NoError(t, err)
	require.FileExists(t, outputPath("fourth"))
}

func TestLinkCachedImage_SharesAcrossGroups(t *testing.T) {
	te := testenv.GetTestEnv(t)
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
	flags.Set(t, "executor.local_cache_store_ext4_images", true)

	root := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(filepath.Join(root, "filecache"), 1_000_000_000, false)
	require.NoError(t, err)
	te.SetFileCache(fc)

	reg := testregistry.Run(t, testregistry.Opts{})
	ref, _ := reg.PushNamedImage(t, "ociconv-shared-image:latest", nil)

	resolver, err := oci.NewResolver(te)
	require.NoError(t, err)

	ctx1 := claims.AuthContextWithJWT(context.Background(), &claims.Claims{GroupID: "GR1"}, nil)
	require.NoError(t, ociconv.CreateDiskImage(ctx1, resolver, fc, root, ref, diskimage.Ext4, oci.Credentials{}, false /*=useOCIFetcher*/, filepath.Join(root, "group1.ext4")))

	ctx2 := claims.AuthContextWithJWT(context.Background(), &claims.Claims{GroupID: "GR2"}, nil)
	linkedPath := filepath.Join(root, "linked.ext4")
	err = ociconv.LinkCachedImage(ctx2, fc, root, ref, diskimage.Ext4, linkedPath)
	require.NoError(t, err)
	require.FileExists(t, linkedPath)
}

func TestCreateDiskImage_LegacyStorage(t *testing.T) {
	te := testenv.GetTestEnv(t)
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.1/32"})
	flags.Set(t, "executor.local_cache_store_ext4_images", false)

	ctx := context.Background()
	root := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(filepath.Join(root, "filecache"), 1_000_000_000, false)
	require.NoError(t, err)
	te.SetFileCache(fc)

	reg := testregistry.Run(t, testregistry.Opts{})
	ref, _ := reg.PushNamedImage(t, "ociconv-legacy-image:latest", nil)

	resolver, err := oci.NewResolver(te)
	require.NoError(t, err)

	materializedPath := filepath.Join(root, "materialized.ext4")
	require.NoError(t, ociconv.CreateDiskImage(ctx, resolver, fc, root, ref, diskimage.Ext4, oci.Credentials{}, false /*=useOCIFetcher*/, materializedPath))
	require.FileExists(t, materializedPath)

	linkedPath := filepath.Join(root, "linked.ext4")
	err = ociconv.LinkCachedImage(ctx, fc, root, ref, diskimage.Ext4, linkedPath)
	require.NoError(t, err)
	require.FileExists(t, linkedPath)
}

func TestMigrateImagesToFileCache(t *testing.T) {
	ctx := context.Background()
	root := testfs.MakeTempDir(t)
	flags.Set(t, "executor.local_cache_store_ext4_images", true)
	fc, err := filecache.NewFileCache(root, 1_000_000_000, false)
	require.NoError(t, err)
	sharedCtx := fc.WithSharedDirectory(ctx, "ext4_images")

	containerImage := "test.registry.io/library/alpine:latest"
	legacyPathA := writeLegacyImage(t, root, containerImage, "old-image", "legacy-ext4-old")
	legacyPathB := writeLegacyImage(t, root, containerImage, "new-image", "legacy-ext4-new")
	require.NoError(t, os.Chtimes(filepath.Dir(legacyPathA), time.Unix(10, 0), time.Unix(10, 0)))
	require.NoError(t, os.Chtimes(filepath.Dir(legacyPathB), time.Unix(20, 0), time.Unix(20, 0)))

	require.NoError(t, ociconv.MigrateImagesToFileCache(ctx, fc, root))
	require.False(t, fc.ContainsFile(ctx, sharedDiskImageNode(containerImage)))
	require.True(t, fc.ContainsFile(sharedCtx, sharedDiskImageNode(containerImage)))

	linkedPath := filepath.Join(root, "linked.ext4")
	err = ociconv.LinkCachedImage(ctx, fc, root, containerImage, diskimage.Ext4, linkedPath)
	require.NoError(t, err)

	content, err := os.ReadFile(linkedPath)
	require.NoError(t, err)
	require.Equal(t, "legacy-ext4-new", string(content))
	require.NoDirExists(t, filepath.Join(root, "images", "ext4"))
}

func TestLinkCachedImage_LegacyStorageWhenFileCacheDisabled(t *testing.T) {
	ctx := context.Background()
	root := testfs.MakeTempDir(t)
	flags.Set(t, "executor.local_cache_store_ext4_images", false)
	fc, err := filecache.NewFileCache(filepath.Join(root, "filecache"), 1_000_000_000, false)
	require.NoError(t, err)

	containerImage := "test.registry.io/library/alpine:latest"
	legacyPath := writeLegacyImage(t, root, containerImage, "legacy-image", "legacy-ext4")

	require.FileExists(t, legacyPath)

	require.NoError(t, ociconv.MigrateImagesToFileCache(ctx, fc, root))
	require.DirExists(t, filepath.Join(root, "images", "ext4"))

	linkedPath := filepath.Join(root, "linked.ext4")
	err = ociconv.LinkCachedImage(ctx, fc, root, containerImage, diskimage.Ext4, linkedPath)
	require.NoError(t, err)

	content, err := os.ReadFile(linkedPath)
	require.NoError(t, err)
	require.Equal(t, "legacy-ext4", string(content))
}

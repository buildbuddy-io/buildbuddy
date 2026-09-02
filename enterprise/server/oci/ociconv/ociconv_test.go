package ociconv_test

import (
	"archive/tar"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ociconv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

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

func setupRemoteCache(t *testing.T) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	enterprise_testenv.AddClientIdentity(t, te, "app")
	_, runServer, listener := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, listener)
	go runServer()
	return te
}

func uploadExt4ActionResult(t *testing.T, te *testenv.TestEnv, actionDigest *repb.Digest, outputPath string, contents []byte) {
	const instanceName = "ext4-images"
	outputDigest, err := cachetools.UploadBlobToCAS(t.Context(), te.GetByteStreamClient(), instanceName, repb.DigestFunction_SHA256, contents)
	require.NoError(t, err)
	err = cachetools.UploadActionResult(t.Context(), te.GetActionCacheClient(), digest.NewACResourceName(actionDigest, instanceName, repb.DigestFunction_SHA256), &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{{
			Path:   outputPath,
			Digest: outputDigest,
		}},
	})
	require.NoError(t, err)
}

func TestCreateDiskImageFromActionCache(t *testing.T) {
	flags.Set(t, "executor.local_cache_store_ext4_images", true)
	te := setupRemoteCache(t)
	root := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(filepath.Join(root, "filecache"), 1_000_000_000, false)
	require.NoError(t, err)
	te.SetFileCache(fc)

	// Populate an Action Cache result containing the synthetic action's ext4
	// output, as the app would after waiting for conversion to finish.
	actionDigest, err := digest.Compute(strings.NewReader("synthetic action"), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	want := []byte("ext4 image contents")
	uploadExt4ActionResult(t, te, actionDigest, "containerfs.ext4", want)
	task := &repb.ExecutionTask{
		ExecuteRequest: &repb.ExecuteRequest{
			InstanceName:   "ext4-images",
			DigestFunction: repb.DigestFunction_SHA256,
		},
		FirecrackerExt4ImageActionDigest: actionDigest,
	}

	outputPath := filepath.Join(root, "materialized.ext4")
	err = ociconv.CreateDiskImageFromActionCache(t.Context(), te, fc, root, task, outputPath)
	require.NoError(t, err)
	got, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// The downloaded image is now in the executor-local cache under the
	// synthetic action digest, so a task with a different digest (for example
	// after the app changes the salt) does not reuse it.
	cached, err := ociconv.IsImageCached(t.Context(), fc, root, ociconv.DiskImageCacheKey(task, "example.com/image:latest"))
	require.NoError(t, err)
	require.True(t, cached)
	changedActionDigest, err := digest.Compute(strings.NewReader("synthetic action with new salt"), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	task.FirecrackerExt4ImageActionDigest = changedActionDigest
	cached, err = ociconv.IsImageCached(t.Context(), fc, root, ociconv.DiskImageCacheKey(task, "example.com/image:latest"))
	require.NoError(t, err)
	require.False(t, cached)
}

func TestCreateDiskImageFromActionCache_RejectsNonExt4Output(t *testing.T) {
	flags.Set(t, "executor.local_cache_store_ext4_images", true)
	te := setupRemoteCache(t)
	root := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(filepath.Join(root, "filecache"), 1_000_000_000, false)
	require.NoError(t, err)
	te.SetFileCache(fc)

	// Reject a malformed synthetic result before downloading its output.
	actionDigest, err := digest.Compute(strings.NewReader("malformed synthetic action"), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	uploadExt4ActionResult(t, te, actionDigest, "containerfs.img", []byte("not accepted"))
	task := &repb.ExecutionTask{
		ExecuteRequest: &repb.ExecuteRequest{
			InstanceName:   "ext4-images",
			DigestFunction: repb.DigestFunction_SHA256,
		},
		FirecrackerExt4ImageActionDigest: actionDigest,
	}

	err = ociconv.CreateDiskImageFromActionCache(t.Context(), te, fc, root, task, filepath.Join(root, "materialized.ext4"))
	require.ErrorContains(t, err, `output path "containerfs.img" does not have an .ext4 extension`)
}

func TestOciconv(t *testing.T) {
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

	path := filepath.Join(root, "materialized.ext4")
	err = ociconv.CreateDiskImage(ctx, resolver, fc, root, ref, oci.Credentials{}, false /*=useOCIFetcher*/, path)
	require.NoError(t, err)

	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.False(t, fi.IsDir())
	require.Greater(t, fi.Size(), int64(0))

	// Extract ext4 image.
	outDir := testfs.MakeTempDir(t)
	err = ext4.ImageToDirectory(ctx, path, outDir, []string{"/"})
	require.NoError(t, err)

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

	// Build path->size map from the extracted ext4 directory (regular files only).
	extractedFiles := make(map[string]int64)
	err = filepath.Walk(outDir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			rel, err := filepath.Rel(outDir, p)
			if err != nil {
				return err
			}
			if rel == "." {
				return nil
			}
			extractedFiles[rel] = info.Size()
		}
		return nil
	})
	require.NoError(t, err)

	require.Empty(t, cmp.Diff(imageFiles, extractedFiles))
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
	err = ociconv.CreateDiskImage(ctx, resolver, fc, root, ref, oci.Credentials{}, false /*=useOCIFetcher*/, outputPath("first"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401 Unauthorized")

	// This should succeed because the credentials are valid and the image is not
	// cached yet.
	err = ociconv.CreateDiskImage(ctx, resolver, fc, root, ref, oci.Credentials{
		Username: "test",
		Password: "test",
	}, false /*=useOCIFetcher*/, outputPath("second"))
	require.NoError(t, err)
	require.FileExists(t, outputPath("second"))

	// Once the image is cached, CreateDiskImage's internal re-check should
	// short-circuit before attempting another registry access, while still
	// linking the cached image to the requested output path.
	err = ociconv.CreateDiskImage(ctx, resolver, fc, root, ref, oci.Credentials{}, false /*=useOCIFetcher*/, outputPath("third"))
	require.NoError(t, err)
	require.FileExists(t, outputPath("third"))

	// A subsequent call with valid credentials should also succeed when the
	// image is already cached.
	err = ociconv.CreateDiskImage(ctx, resolver, fc, root, ref, oci.Credentials{
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
	require.NoError(t, ociconv.CreateDiskImage(ctx1, resolver, fc, root, ref, oci.Credentials{}, false /*=useOCIFetcher*/, filepath.Join(root, "group1.ext4")))

	ctx2 := claims.AuthContextWithJWT(context.Background(), &claims.Claims{GroupID: "GR2"}, nil)
	linkedPath := filepath.Join(root, "linked.ext4")
	err = ociconv.LinkCachedImage(ctx2, fc, root, ref, linkedPath)
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
	require.NoError(t, ociconv.CreateDiskImage(ctx, resolver, fc, root, ref, oci.Credentials{}, false /*=useOCIFetcher*/, materializedPath))
	require.FileExists(t, materializedPath)

	linkedPath := filepath.Join(root, "linked.ext4")
	err = ociconv.LinkCachedImage(ctx, fc, root, ref, linkedPath)
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
	err = ociconv.LinkCachedImage(ctx, fc, root, containerImage, linkedPath)
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
	err = ociconv.LinkCachedImage(ctx, fc, root, containerImage, linkedPath)
	require.NoError(t, err)

	content, err := os.ReadFile(linkedPath)
	require.NoError(t, err)
	require.Equal(t, "legacy-ext4", string(content))
}

package sociartifactstore

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	socipb "github.com/buildbuddy-io/buildbuddy/proto/soci"
)

func TestNoImage(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.SkipNow()
	}
	_, store, _, ctx := setup(t)
	_, err := store.GetArtifacts(ctx, &socipb.GetArtifactsRequest{})
	assert.True(t, status.IsInvalidArgumentError(err))
}

func TestIndexExists(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.SkipNow()
	}
	env, store, r, ctx := setup(t)

	image := appendLayer(t, empty.Image, "test_data/layers/59fe65d9e601b6db7125a2259b7d64064db081ac6ed94ef24cc961f4438d1008.tar.gz")
	image = appendLayer(t, image, "test_data/layers/6b7e4a5c7c7ad54c76bc4861f476f3b70978beede9e752015202dd223383602b.tar.gz")
	image = appendLayer(t, image, "test_data/layers/ddd3b7c66a0e0090df72a082ae683a96d8ea6d96a689d3bec894feec40880ec5.tar.gz")
	image = appendLayer(t, image, "test_data/layers/6b7e4a5c7c7ad54c76bc4861f476f3b70978beede9e752015202dd223383602b.tar.gz")
	imageName := pushImage(t, r, image, "test")

	sociIndexDigest := repb.Digest{
		Hash:      "2c4c1f7de7a83d2b1b302bce865ed7ba8e14870db155daecabeba08be37eb5c4",
		SizeBytes: 1225,
	}
	ztocDigest1 := repb.Digest{
		Hash:      "5fa40df4606c1d9daa7119a18f7106b672d352f6f56d250547b41572bcf384de",
		SizeBytes: 99024,
	}
	ztocDigest2 := repb.Digest{
		Hash:      "aa58f9f015faed905d18144fe7aaf55bac280a8276f84a08e05b970a95fd56bb",
		SizeBytes: 99024,
	}
	serializedSociIndexResourceName, err := digest.NewResourceName(&sociIndexDigest, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256).DownloadString()
	require.NoError(t, err)

	writeFileContentsToCache(ctx, t, env, &sociIndexDigest, "test_data/soci_indexes/2c4c1f7de7a83d2b1b302bce865ed7ba8e14870db155daecabeba08be37eb5c4.json", rspb.CacheType_CAS)
	writeFileContentsToCache(ctx, t, env, &ztocDigest1, "test_data/ztocs/5fa40df4606c1d9daa7119a18f7106b672d352f6f56d250547b41572bcf384de.ztoc", rspb.CacheType_CAS)
	writeFileContentsToCache(ctx, t, env, &ztocDigest2, "test_data/ztocs/aa58f9f015faed905d18144fe7aaf55bac280a8276f84a08e05b970a95fd56bb.ztoc", rspb.CacheType_CAS)
	writeDataToCache(ctx, t, env,
		getSociIndexKey(t, "sha256:dd04f266fd693e9ae2abee66dd7d3b61b8b42dcf38099cade554c6a34d1ae63b"),
		[]byte(serializedSociIndexResourceName),
		rspb.CacheType_AC)

	actual, err := store.GetArtifacts(ctx, &socipb.GetArtifactsRequest{Image: imageName})
	require.NoError(t, err)

	expected := socipb.GetArtifactsResponse{
		ImageId: "dd04f266fd693e9ae2abee66dd7d3b61b8b42dcf38099cade554c6a34d1ae63b",
		Artifacts: []*socipb.Artifact{
			&socipb.Artifact{
				Digest: &sociIndexDigest,
				Type:   socipb.Type_SOCI_INDEX,
			},
			&socipb.Artifact{
				Digest: &ztocDigest1,
				Type:   socipb.Type_ZTOC,
			},
			&socipb.Artifact{
				Digest: &ztocDigest2,
				Type:   socipb.Type_ZTOC,
			},
		},
	}
	assert.True(t, proto.Equal(&expected, actual))
	assert.Equal(t, 0, r.blobsGot)
	assert.True(t, cacheContains(ctx, t, env, &sociIndexDigest))
	assert.True(t, cacheContains(ctx, t, env, &ztocDigest1))
	assert.True(t, cacheContains(ctx, t, env, &ztocDigest2))
}

func TestIndexPartiallyExists(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.SkipNow()
	}
	env, store, r, ctx := setup(t)

	image := appendLayer(t, empty.Image, "test_data/layers/59fe65d9e601b6db7125a2259b7d64064db081ac6ed94ef24cc961f4438d1008.tar.gz")
	image = appendLayer(t, image, "test_data/layers/6b7e4a5c7c7ad54c76bc4861f476f3b70978beede9e752015202dd223383602b.tar.gz")
	image = appendLayer(t, image, "test_data/layers/ddd3b7c66a0e0090df72a082ae683a96d8ea6d96a689d3bec894feec40880ec5.tar.gz")
	image = appendLayer(t, image, "test_data/layers/6b7e4a5c7c7ad54c76bc4861f476f3b70978beede9e752015202dd223383602b.tar.gz")
	imageName := pushImage(t, r, image, "test")

	sociIndexDigest := repb.Digest{
		Hash:      "2c4c1f7de7a83d2b1b302bce865ed7ba8e14870db155daecabeba08be37eb5c4",
		SizeBytes: 1225,
	}
	ztocDigest1 := repb.Digest{
		Hash:      "5fa40df4606c1d9daa7119a18f7106b672d352f6f56d250547b41572bcf384de",
		SizeBytes: 99024,
	}
	ztocDigest2 := repb.Digest{
		Hash:      "aa58f9f015faed905d18144fe7aaf55bac280a8276f84a08e05b970a95fd56bb",
		SizeBytes: 99024,
	}
	serializedSociIndexResourceName, err := digest.NewResourceName(&sociIndexDigest, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256).DownloadString()
	require.NoError(t, err)

	writeFileContentsToCache(ctx, t, env, &sociIndexDigest, "test_data/soci_indexes/2c4c1f7de7a83d2b1b302bce865ed7ba8e14870db155daecabeba08be37eb5c4.json", rspb.CacheType_CAS)
	writeFileContentsToCache(ctx, t, env, &ztocDigest1, "test_data/ztocs/5fa40df4606c1d9daa7119a18f7106b672d352f6f56d250547b41572bcf384de.ztoc", rspb.CacheType_CAS)
	// Don't write the second ztoc (aa58f...) to the cache.
	writeDataToCache(ctx, t, env,
		getSociIndexKey(t, "sha256:dd04f266fd693e9ae2abee66dd7d3b61b8b42dcf38099cade554c6a34d1ae63b"),
		[]byte(serializedSociIndexResourceName),
		rspb.CacheType_AC)

	actual, err := store.GetArtifacts(ctx, &socipb.GetArtifactsRequest{Image: imageName})
	require.NoError(t, err)

	expected := socipb.GetArtifactsResponse{
		ImageId: "dd04f266fd693e9ae2abee66dd7d3b61b8b42dcf38099cade554c6a34d1ae63b",
		Artifacts: []*socipb.Artifact{
			&socipb.Artifact{
				Digest: &sociIndexDigest,
				Type:   socipb.Type_SOCI_INDEX,
			},
			&socipb.Artifact{
				Digest: &ztocDigest1,
				Type:   socipb.Type_ZTOC,
			},
			&socipb.Artifact{
				Digest: &ztocDigest2,
				Type:   socipb.Type_ZTOC,
			},
		},
	}
	assert.True(t, proto.Equal(&expected, actual))
	assert.Equal(t, 2, r.blobsGot)
	assert.True(t, cacheContains(ctx, t, env, &sociIndexDigest))
	assert.True(t, cacheContains(ctx, t, env, &ztocDigest1))
	assert.True(t, cacheContains(ctx, t, env, &ztocDigest2))
}

func TestIndexDoesNotExist(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.SkipNow()
	}
	env, store, r, ctx := setup(t)

	image := appendLayer(t, empty.Image, "test_data/layers/59fe65d9e601b6db7125a2259b7d64064db081ac6ed94ef24cc961f4438d1008.tar.gz")
	image = appendLayer(t, image, "test_data/layers/6b7e4a5c7c7ad54c76bc4861f476f3b70978beede9e752015202dd223383602b.tar.gz")
	image = appendLayer(t, image, "test_data/layers/ddd3b7c66a0e0090df72a082ae683a96d8ea6d96a689d3bec894feec40880ec5.tar.gz")
	image = appendLayer(t, image, "test_data/layers/6b7e4a5c7c7ad54c76bc4861f476f3b70978beede9e752015202dd223383602b.tar.gz")
	imageName := pushImage(t, r, image, "test")

	sociIndexDigest := repb.Digest{
		Hash:      "2c4c1f7de7a83d2b1b302bce865ed7ba8e14870db155daecabeba08be37eb5c4",
		SizeBytes: 1225,
	}
	ztocDigest1 := repb.Digest{
		Hash:      "5fa40df4606c1d9daa7119a18f7106b672d352f6f56d250547b41572bcf384de",
		SizeBytes: 99024,
	}
	ztocDigest2 := repb.Digest{
		Hash:      "aa58f9f015faed905d18144fe7aaf55bac280a8276f84a08e05b970a95fd56bb",
		SizeBytes: 99024,
	}

	actual, err := store.GetArtifacts(ctx, &socipb.GetArtifactsRequest{Image: imageName})
	require.NoError(t, err)

	expected := socipb.GetArtifactsResponse{
		ImageId: "dd04f266fd693e9ae2abee66dd7d3b61b8b42dcf38099cade554c6a34d1ae63b",
		Artifacts: []*socipb.Artifact{
			&socipb.Artifact{
				Digest: &sociIndexDigest,
				Type:   socipb.Type_SOCI_INDEX,
			},
			&socipb.Artifact{
				Digest: &ztocDigest1,
				Type:   socipb.Type_ZTOC,
			},
			&socipb.Artifact{
				Digest: &ztocDigest2,
				Type:   socipb.Type_ZTOC,
			},
		},
	}
	assert.True(t, proto.Equal(&expected, actual))
	assert.Equal(t, 2, r.blobsGot)
	assert.True(t, cacheContains(ctx, t, env, &sociIndexDigest))
	assert.True(t, cacheContains(ctx, t, env, &ztocDigest1))
	assert.True(t, cacheContains(ctx, t, env, &ztocDigest2))

	r.blobsGot = 0
	actual, err = store.GetArtifacts(ctx, &socipb.GetArtifactsRequest{Image: imageName})
	require.NoError(t, err)
	assert.True(t, proto.Equal(&expected, actual))
	assert.Equal(t, 0, r.blobsGot)
	assert.True(t, cacheContains(ctx, t, env, &sociIndexDigest))
	assert.True(t, cacheContains(ctx, t, env, &ztocDigest1))
	assert.True(t, cacheContains(ctx, t, env, &ztocDigest2))
}

func TestSmallImage(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.SkipNow()
	}
	env, store, r, ctx := setup(t)

	// Make a small image and push it to the registry.
	files := map[string][]byte{}
	buffer := bytes.Buffer{}
	buffer.Grow(1024)
	for i := 0; i < 1024; i++ {
		_, err := buffer.WriteString("0")
		require.NoError(t, err)
	}
	for i := 0; i < 100000; i++ {
		files[fmt.Sprintf("/tmp/%d", i)] = buffer.Bytes()
	}
	image, err := crane.Image(files)
	require.NoError(t, err)
	imageName := pushImage(t, r, image, "test")

	actual, err := store.GetArtifacts(ctx, &socipb.GetArtifactsRequest{Image: imageName})
	require.NoError(t, err)

	sociIndexDigest := repb.Digest{
		Hash:      "4754590de3b842493a00d0d71825e30cd99597e6b06eb46cb68d987279cb3a12",
		SizeBytes: 514,
	}
	expected := socipb.GetArtifactsResponse{
		ImageId: "6b5aaf876d059e6f47fc8a02b721b87b7f1fe94d609e9ba21cf97ce69eff6ed4",
		Artifacts: []*socipb.Artifact{
			&socipb.Artifact{
				Digest: &sociIndexDigest,
				Type:   socipb.Type_SOCI_INDEX,
			},
		},
	}
	assert.True(t, proto.Equal(&expected, actual))
	assert.True(t, cacheContains(ctx, t, env, &sociIndexDigest))
}

func appendLayer(t *testing.T, image v1.Image, filename string) v1.Image {
	file, err := os.Open(filename)
	require.NoError(t, err)
	layer := stream.NewLayer(file)
	image, err = mutate.AppendLayers(image, layer)
	require.NoError(t, err)
	return image
}

func pushImage(t *testing.T, r *containerRegistry, image v1.Image, imageName string) string {
	fullImageName := r.ImageAddress(imageName)
	ref, err := name.ParseReference(fullImageName)
	require.NoError(t, err)
	err = remote.Write(ref, image)
	require.NoError(t, err)
	return fullImageName
}

func getSociIndexKey(t *testing.T, imageConfigHash string) *repb.Digest {
	imageHash, err := v1.NewHash(imageConfigHash)
	require.NoError(t, err)
	indexCacheKey, err := sociIndexKey(imageHash)
	require.NoError(t, err)
	return indexCacheKey
}

func cacheContains(ctx context.Context, t *testing.T, env *testenv.TestEnv, d *repb.Digest) bool {
	resourceName := digest.NewResourceName(d, "" /*=instanceName -- not used */, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	contains, err := env.GetCache().Contains(ctx, resourceName.ToProto())
	require.NoError(t, err)
	return contains
}

func writeFileContentsToCache(ctx context.Context, t *testing.T, env *testenv.TestEnv, d *repb.Digest, filename string, cacheType rspb.CacheType) {
	data, err := ioutil.ReadFile(filename)
	require.NoError(t, err)
	writeDataToCache(ctx, t, env, d, data, cacheType)
}

func writeDataToCache(ctx context.Context, t *testing.T, env *testenv.TestEnv, d *repb.Digest, data []byte, cacheType rspb.CacheType) {
	resourceName := digest.NewResourceName(d, "" /*=instanceName -- not used */, cacheType, repb.DigestFunction_SHA256)
	require.NoError(t, env.GetCache().Set(ctx, resourceName.ToProto(), data))
}

func setup(t *testing.T) (*testenv.TestEnv, *SociArtifactStore, *containerRegistry, context.Context) {
	env := testenv.GetTestEnv(t)
	env.SetDefaultRedisClient(testredis.Start(t).Client())
	env.SetSingleFlightDeduper(&deduper{})
	reg := runContainerRegistry(t)
	err, store := newSociArtifactStore(env)
	require.NoError(t, err)
	ctx, err := prefix.AttachUserPrefixToContext(context.TODO(), env)
	require.NoError(t, err)
	return env, store, reg, ctx
}

func runContainerRegistry(t *testing.T) *containerRegistry {
	handler := registry.New()
	registry := containerRegistry{
		host:     "localhost",
		port:     testport.FindFree(t),
		blobsGot: 0,
	}
	mux := http.NewServeMux()
	mux.Handle("/", handler)

	// Wrap the container registry so we can verify which blobs are fetched.
	f := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			matches, err := regexp.MatchString("/v2/.*/blobs/sha256:.*", r.URL.Path)
			require.NoError(t, err)
			if matches {
				registry.blobsGot = registry.blobsGot + 1
			}
		}
		mux.ServeHTTP(w, r)
	})
	server := &http.Server{Handler: f}
	lis, err := net.Listen("tcp", registry.Address())
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	return &registry
}

type containerRegistry struct {
	host string
	port int

	// Number of blobs requested using GET
	blobsGot int
}

func (r *containerRegistry) Address() string {
	return fmt.Sprintf("%s:%d", r.host, r.port)
}

func (r *containerRegistry) ImageAddress(imageName string) string {
	return fmt.Sprintf("%s:%d/%s", r.host, r.port, imageName)
}

// A deduper implementation that doesn't do any de-duping as it's not required
// for this test.
type deduper struct {
}

func (d *deduper) Do(_ context.Context, _ string, work func() ([]byte, error)) ([]byte, error) {
	return work()
}

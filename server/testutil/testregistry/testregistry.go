package testregistry

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/stretchr/testify/require"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

type Opts struct {
	// An interceptor applied to HTTP calls. Returns true if the request
	// should be processed post-interception, or false if not.
	HttpInterceptor func(w http.ResponseWriter, r *http.Request) bool
}

type Registry struct {
	host string
	port int
}

func Run(t *testing.T, opts Opts) *Registry {
	handler := registry.New()
	registry := Registry{
		host: "localhost",
		port: testport.FindFree(t),
	}
	mux := http.NewServeMux()
	mux.Handle("/", handler)

	f := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			mux.ServeHTTP(w, r)
		})
	if opts.HttpInterceptor != nil {
		f = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if opts.HttpInterceptor(w, r) {
				mux.ServeHTTP(w, r)
			}
		})
	}

	server := &http.Server{Handler: f}
	lis, err := net.Listen("tcp", registry.Address())
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	return &registry
}

func (r *Registry) Address() string {
	return fmt.Sprintf("%s:%d", r.host, r.port)
}

func (r *Registry) ImageAddress(imageName string) string {
	return fmt.Sprintf("%s:%d/%s", r.host, r.port, imageName)
}

func (r *Registry) Push(t *testing.T, image v1.Image, imageName string) string {
	fullImageName := r.ImageAddress(imageName)
	ref, err := name.ParseReference(fullImageName)
	require.NoError(t, err)
	err = remote.Write(ref, image)
	require.NoError(t, err)
	return fullImageName
}

func (r *Registry) PushIndex(t *testing.T, idx v1.ImageIndex, imageName string) string {
	fullImageName := r.ImageAddress(imageName)
	ref, err := name.ParseReference(fullImageName)
	require.NoError(t, err)
	err = remote.WriteIndex(ref, idx)
	require.NoError(t, err)
	return fullImageName
}

func (r *Registry) PushRandomImage(t *testing.T) (string, v1.Image) {
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
	return r.Push(t, image, "test"), image
}

// ImageFromRlocationpath returns an Image from an rlocationpath.
// The rlocationpath should be set via x_defs in the BUILD file, and the
// rlocationpath target should be an OCI image target (e.g. oci.pull)
func ImageFromRlocationpath(t *testing.T, rlocationpath string) v1.Image {
	indexPath, err := runfiles.Rlocation(rlocationpath)
	require.NoError(t, err)
	idx, err := layout.ImageIndexFromPath(indexPath)
	require.NoError(t, err)
	m, err := idx.IndexManifest()
	require.NoError(t, err)
	require.Len(t, m.Manifests, 1)
	require.True(t, m.Manifests[0].MediaType.IsImage())
	img, err := idx.Image(m.Manifests[0].Digest)
	require.NoError(t, err)
	return img
}

// bytesLayer implements partial.UncompressedLayer from raw bytes.
type bytesLayer struct {
	content   []byte
	diffID    v1.Hash
	mediaType types.MediaType
}

// NewBytesLayer returns an image layer representing the given bytes.
//
// testtar.EntryBytes may be useful for constructing tarball contents.
func NewBytesLayer(t *testing.T, b []byte) v1.Layer {
	layer, err := partial.UncompressedToLayer(&bytesLayer{
		mediaType: types.OCILayer,
		diffID: v1.Hash{
			Algorithm: "sha256",
			Hex:       fmt.Sprintf("%x", sha256.Sum256(b)),
		},
		content: b,
	})
	require.NoError(t, err)
	return layer
}

// DiffID implements partial.UncompressedLayer
func (ul *bytesLayer) DiffID() (v1.Hash, error) {
	return ul.diffID, nil
}

// Uncompressed implements partial.UncompressedLayer
func (ul *bytesLayer) Uncompressed() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewBuffer(ul.content)), nil
}

// MediaType returns the media type of the layer
func (ul *bytesLayer) MediaType() (types.MediaType, error) {
	return ul.mediaType, nil
}

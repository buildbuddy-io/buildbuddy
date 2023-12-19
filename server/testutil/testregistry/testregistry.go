package testregistry

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/require"
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

func (r *Registry) PushRandomImage(t *testing.T) string {
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
	return r.Push(t, image, "test")
}

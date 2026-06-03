package ociregistry_test

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/testpodman"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

type pullTestImage struct {
	ref                string
	manifestLatestPath string
	manifestDigestPath string
	configPath         string
	layerPath          string
}

func pushPullTestImage(t *testing.T, reg *testregistry.Registry, repository string) pullTestImage {
	ref, img := reg.PushNamedImage(t, repository, nil)

	resp, err := http.Head("http://" + reg.Address() + "/v2/" + repository + "/manifests/latest")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	manifestDigest := resp.Header.Get("Docker-Content-Digest")
	require.NotEmpty(t, manifestDigest)
	require.NoError(t, resp.Body.Close())

	configDigest, err := img.ConfigName()
	require.NoError(t, err)
	layers, err := img.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)
	layerDigest, err := layers[0].Digest()
	require.NoError(t, err)

	return pullTestImage{
		ref:                ref,
		manifestLatestPath: "/v2/" + repository + "/manifests/latest",
		manifestDigestPath: "/v2/" + repository + "/manifests/" + manifestDigest,
		configPath:         "/v2/" + repository + "/blobs/" + configDigest.String(),
		layerPath:          "/v2/" + repository + "/blobs/" + layerDigest.String(),
	}
}

func runCommand(t *testing.T, env []string, name string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s %s failed:\n%s", name, strings.Join(args, " "), string(out))
}

func runDockerPull(t *testing.T, imageRef string) {
	t.Helper()
	t.Cleanup(func() {
		_ = exec.Command("docker", "image", "rm", "--force", imageRef).Run()
	})
	_ = exec.Command("docker", "image", "rm", "--force", imageRef).Run()
	runCommand(t, nil, "docker", "pull", imageRef)
}

func TestMain(m *testing.M) {
	testpodman.TestMain(m, nil)
}

func runPodmanPull(t *testing.T, imageRef string) {
	t.Helper()
	tmp := t.TempDir()
	authFile := filepath.Join(tmp, "auth.json")
	require.NoError(t, os.WriteFile(authFile, []byte("{}"), 0644))
	env := []string{
		"HOME=" + tmp,
		"REGISTRY_AUTH_FILE=" + authFile,
	}
	runCommand(
		t,
		env,
		"podman",
		"--root", filepath.Join(tmp, "root"),
		"--runroot", filepath.Join(tmp, "runroot"),
		"--storage-driver", "vfs",
		"--events-backend", "file",
		"pull",
		"--tls-verify=false",
		imageRef,
	)
}

func TestDockerPullRequestPattern(t *testing.T) {
	counter := testhttp.NewRequestCounter()
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	t.Cleanup(func() { require.NoError(t, reg.Shutdown()) })

	img := pushPullTestImage(t, reg, "docker_pull_request_pattern")

	counter.Reset()
	runDockerPull(t, img.ref)
	actual := counter.Snapshot()
	expected := map[string]int{
		http.MethodGet + " /v2/":                       1,
		http.MethodHead + " " + img.manifestLatestPath: 1,
		http.MethodGet + " " + img.manifestDigestPath:  1,
		http.MethodGet + " " + img.configPath:          1,
		http.MethodGet + " " + img.layerPath:           1,
	}
	require.Empty(t, cmp.Diff(expected, actual))
}

func TestPodmanPullRequestPattern(t *testing.T) {
	counter := testhttp.NewRequestCounter()
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	t.Cleanup(func() { require.NoError(t, reg.Shutdown()) })

	img := pushPullTestImage(t, reg, "podman_pull_request_pattern")

	counter.Reset()
	runPodmanPull(t, img.ref)
	actual := counter.Snapshot()
	expected := map[string]int{
		http.MethodGet + " /v2/":                      1,
		http.MethodGet + " " + img.manifestLatestPath: 1,
		http.MethodGet + " " + img.configPath:         1,
		http.MethodGet + " " + img.layerPath:          1,
	}
	require.Empty(t, cmp.Diff(expected, actual))
}

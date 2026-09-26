package firecracker_test

import (
	"crypto/rand"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type dockerMirror struct {
	url          string
	host         string
	image        string
	manifestGETs atomic.Int64
	blobGETs     atomic.Int64
}

// newDockerMirror must run on the suite's root t, BEFORE newFirecrackerEnv.
// Cleanup is LIFO: the executor (including its guests) must stop before the
// mirror proxy and its loopback registry are closed.
func newDockerMirror(t *testing.T) *dockerMirror {
	t.Helper()
	registry := testregistry.Run(t, testregistry.Opts{})
	t.Cleanup(func() { require.NoError(t, registry.Shutdown()) })

	require.NotEmpty(t, busyboxOCIImageRlocationpath, "missing @busybox BUILD x_def")
	image := testregistry.ImageFromRlocationpath(t, busyboxOCIImageRlocationpath)
	digest, err := image.Digest()
	require.NoError(t, err)
	var nonce [16]byte
	_, err = rand.Read(nonce[:])
	require.NoError(t, err)
	repository := fmt.Sprintf("firecracker-executor-test/mirror-%x", nonce)
	registry.Push(t, image, repository, nil)
	mirror := &dockerMirror{
		// Pin the selected platform manifest, not the upstream multiarch index.
		// A docker.io reference is essential: naming the mirror directly would
		// bypass the guest daemon's registry-mirror configuration.
		image: "docker.io/" + repository + "@" + digest.String(),
	}

	upstream, err := url.Parse("http://" + registry.Address())
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.ModifyResponse = func(response *http.Response) error {
		request := response.Request
		if request.Method != http.MethodGet || response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil
		}
		// Count successful content GETs, not HEADs, /v2/ probes, failed
		// requests, or host-side pushes. Never require exact request counts:
		// Docker versions can differ in retries and manifest negotiation.
		if strings.HasPrefix(request.URL.Path, "/v2/"+repository+"/manifests/") {
			mirror.manifestGETs.Add(1)
		}
		if strings.HasPrefix(request.URL.Path, "/v2/"+repository+"/blobs/") {
			mirror.blobGETs.Add(1)
		}
		return nil
	}

	// Guest loopback is not host loopback. Expose only the proxy on the
	// host's primary IP; the backing registry remains loopback-only.
	hostIP, err := networking.DefaultIP(t.Context())
	require.NoError(t, err)
	listener, err := net.Listen("tcp4", net.JoinHostPort(hostIP.String(), "0"))
	require.NoError(t, err)
	server := httptest.NewUnstartedServer(proxy)
	require.NoError(t, server.Listener.Close())
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	mirror.url = server.URL
	mirror.host = listener.Addr().String()
	return mirror
}

func (m *dockerMirror) executorArgs() []string {
	return []string{
		"--executor.firecracker_vm_docker_mirrors=" + m.url,
		"--executor.firecracker_vm_docker_insecure_registries=" + m.host,
	}
}

// test is registered as a parallel leaf of the shared-executor suite. Other
// Docker cases use docker load, so they cannot satisfy this mirror's counters.
func (m *dockerMirror) test(t *testing.T, env *firecrackerEnv) {
	rbe := env.forTest(t)
	// dockerCommand uses network=off with init-dockerd=true, selecting LOCAL:
	// the guest can reach the host mirror, but cannot forward to Docker Hub.
	// Thus public-registry fallback cannot make a broken mirror pass.
	command := dockerCommand(fmt.Sprintf(`
		timeout 90 sh -eu <<'DOCKER_MIRROR_SCRIPT'
		export DOCKER_HOST=unix:///var/run/docker.sock
		test -S /var/run/docker.sock
		timeout 60 docker pull '%s' >/tmp/mirror-pull.log 2>&1 || {
			cat /tmp/mirror-pull.log >&2; exit 1;
		}
		mkdir -p output
		timeout 20 docker run --rm --pull=never --network none \
			-v "$PWD/output:/output" '%s' sh -ec \
			'printf "mirror-nested-output\n" > /output/result.txt; printf "mirror-nested-ok\n"'
DOCKER_MIRROR_SCRIPT
	`, m.image, m.image), platform.Ubuntu24_04Image,
		&repb.Platform_Property{Name: "recycle-runner", Value: "false"},
	)
	command.OutputFiles = []string{"output/result.txt"}
	result := rbe.Execute(command, &rbetest.ExecuteOpts{
		APIKey:           rbe.APIKey1,
		ActionTimeout:    2 * time.Minute,
		DoNotCacheAction: true,
	}).Wait()
	require.Equal(t, 0, result.ExitCode, "stdout: %s\nstderr: %s", result.Stdout, result.Stderr)
	require.Empty(t, result.Stderr)
	require.Equal(t, "mirror-nested-ok\n", result.Stdout)
	testfs.AssertExactFileContents(t, rbe.DownloadOutputsToNewTempDir(result), map[string]string{
		"output/result.txt": "mirror-nested-output\n",
	})
	require.Positive(t, m.manifestGETs.Load(), "guest Docker must GET an image manifest through its mirror")
	require.Positive(t, m.blobGETs.Load(), "guest Docker must GET image blobs through its mirror")
}

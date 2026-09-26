package firecracker_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// Configured once by the shared executor fixture; no test mutates its resolver.
const firecrackerTestResolvConf = "# executor integration test\nnameserver 192.0.2.53\nsearch firecracker.test\noptions ndots:2 timeout:1 attempts:1\n"

// serveNetworkProbe listens on the executor host's primary IP, not loopback:
// the latter refers to the guest itself when used by an action.
func serveNetworkProbe(t *testing.T) string {
	t.Helper()
	ip, err := networking.DefaultIP(context.Background())
	require.NoError(t, err)
	listener, err := net.Listen("tcp4", net.JoinHostPort(ip.String(), "0"))
	require.NoError(t, err)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "firecracker-network-probe\n")
	}))
	require.NoError(t, server.Listener.Close())
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return server.URL
}

func testNetworkEnabledAndDisabled(t *testing.T, env *firecrackerEnv) {
	url := serveNetworkProbe(t)
	// Run both successful controls and the denied request against exactly the
	// same live endpoint. A broken HTTP fixture must not pass the negative case.
	for _, mode := range []string{"external", "off", "external"} {
		t.Run(mode, func(t *testing.T) {
			rbe := env.forTest(t)
			script := fmt.Sprintf(`
				set -eu
				command -v wget >/dev/null
				wget -q -T 3 -O - '%s'
			`, url)
			if mode == "off" {
				script = fmt.Sprintf(`
					set -eu
					command -v wget >/dev/null
					set +e
					wget -q -T 3 -O /tmp/probe '%s' 2>/tmp/probe.err
					rc=$?
					set -e
					# BusyBox wget reports connection failures as 1, not a
					# missing executable, shell error, or action timeout.
					test "$rc" -eq 1
					test -s /tmp/probe.err
					test ! -s /tmp/probe
					printf 'network-disabled\n'
				`, url)
			}
			res := rbe.Execute(firecrackerCommand(script,
				&repb.Platform_Property{Name: "network", Value: mode},
			), &rbetest.ExecuteOpts{APIKey: rbe.APIKey1, ActionTimeout: time.Minute}).Wait()
			require.Equal(t, 0, res.ExitCode, "stderr: %s", res.Stderr)
			require.Empty(t, res.Stderr)
			expected := "firecracker-network-probe\n"
			if mode == "off" {
				expected = "network-disabled\n"
			} else {
				stats := res.ActionResult.GetExecutionMetadata().GetUsageStats().GetNetworkStats()
				require.Positive(t, stats.GetBytesSent())
				require.Positive(t, stats.GetBytesReceived())
			}
			require.Equal(t, expected, res.Stdout)
		})
	}
}

func (p *routedNetworkProbe) test(t *testing.T, env *firecrackerEnv) {
	p.runModes(t, env, []string{"external", "local", "off", "external"})
}

// Serial callers can exercise pool transitions using this same FORWARD-path
// endpoint. No runner recycling: each completed VM can return its network to a
// pool. This observes behavior, not whether a particular private pool was reused.
func (p *routedNetworkProbe) runModes(t *testing.T, env *firecrackerEnv, modes []string) {
	t.Helper()
	for i, mode := range modes {
		t.Run(fmt.Sprintf("%d_%s", i, mode), func(t *testing.T) {
			rbe := env.forTest(t)
			p.check(t)
			defer p.check(t)
			props := []*repb.Platform_Property{{Name: "recycle-runner", Value: "false"}}
			opts := &rbetest.ExecuteOpts{APIKey: rbe.APIKey1, ActionTimeout: 2 * time.Minute}
			setup, client := "command -v wget >/dev/null\n", "wget"
			switch mode {
			case "local":
				// OFF + dockerd selects LOCAL. Deliver a known wget through CAS;
				// do not assume Ubuntu includes a client or allow registry pulls.
				props = append(props,
					&repb.Platform_Property{Name: "network", Value: "off"},
					&repb.Platform_Property{Name: "container-image", Value: platform.DockerPrefix + platform.Ubuntu24_04Image},
					&repb.Platform_Property{Name: "init-dockerd", Value: "true"},
					&repb.Platform_Property{Name: platform.EstimatedMemoryPropertyName, Value: "2500MB"},
					&repb.Platform_Property{Name: platform.EstimatedFreeDiskPropertyName, Value: "2GB"},
				)
				opts.InputRootDir = dockerInputRoot(t)
				setup = fmt.Sprintf(`export DOCKER_HOST=unix:///var/run/docker.sock
					timeout 15 docker info >/dev/null 2>/tmp/docker.err
					timeout 30 docker load -i busybox.tar >/tmp/load.log 2>&1
					timeout 20 docker run --rm --pull=never --network host '%s' sh -ec 'command -v wget >/dev/null'
				`, nestedDockerImage)
				client = fmt.Sprintf("timeout 20 docker run --rm --pull=never --network host '%s' wget", nestedDockerImage)
			case "external", "off":
				props = append(props, &repb.Platform_Property{Name: "network", Value: mode})
			default:
				t.Fatalf("unknown network mode %q", mode)
			}
			script := setup + fmt.Sprintf("%s -q -T 3 -O - '%s'", client, p.url)
			expected := routedProbeBody
			if mode != "external" {
				script = setup + fmt.Sprintf(`
					set +e
					%s -q -T 3 -O - '%s' >/tmp/probe 2>/tmp/probe.err
					rc=$?
					set -e
					# BusyBox connection failure is 1, not docker startup failure
					# (125), missing executable (127), or timeout (124).
					test "$rc" -eq 1
					test -s /tmp/probe.err
					test ! -s /tmp/probe
					printf 'forwarding-denied\n'
				`, client, p.url)
				expected = "forwarding-denied\n"
			}
			res := rbe.Execute(firecrackerCommand(script, props...), opts).Wait()
			require.Equal(t, 0, res.ExitCode, "stdout: %s\nstderr: %s", res.Stdout, res.Stderr)
			require.Empty(t, res.Stderr)
			require.Equal(t, expected, res.Stdout)
		})
	}
}

func testResolvConf(t *testing.T, env *firecrackerEnv) {
	rbe := env.forTest(t)
	res := rbe.Execute(firecrackerCommand("cat /etc/resolv.conf",
		&repb.Platform_Property{Name: "network", Value: "external"},
	), &rbetest.ExecuteOpts{APIKey: rbe.APIKey1, ActionTimeout: time.Minute}).Wait()
	require.Equal(t, 0, res.ExitCode, "stderr: %s", res.Stderr)
	require.Empty(t, res.Stderr)
	require.Equal(t, firecrackerTestResolvConf, res.Stdout)
}

func testGuestIPv6(t *testing.T, env *firecrackerEnv) {
	if runtime.GOARCH != "amd64" {
		t.Skip("the arm64 guest kernel does not yet support this IPv6 test")
	}
	rbe := env.forTest(t)
	res := rbe.Execute(firecrackerCommand(`
		set -eu
		ping -c 1 -W 2 127.0.0.1 >/dev/null
		ping6 -c 1 -W 2 ::1 >/dev/null
		mkdir /tmp/http
		printf 'guest-http\n' >/tmp/http/index.html
		httpd -f -p 18080 -h /tmp/http >/tmp/http/log 2>&1 &
		pid=$!
		trap 'kill "$pid" 2>/dev/null || true' EXIT
		for url in 'http://127.0.0.1:18080/' 'http://[::1]:18080/'; do
			ok=false
			for attempt in $(seq 20); do
				if wget -q -T 1 -O /tmp/response "$url" 2>/tmp/wget.err; then
					ok=true
					break
				fi
				sleep 0.1
			done
			if test "$ok" != true; then
				cat /tmp/http/log /tmp/wget.err >&2
				exit 1
			fi
			test "$(cat /tmp/response)" = guest-http
		done
		printf 'ipv4-ipv6-loopback-ok\n'
	`,
		&repb.Platform_Property{Name: "network", Value: "external"},
		&repb.Platform_Property{Name: platform.NetworkEnableIPv6PropertyName, Value: "true"},
	), &rbetest.ExecuteOpts{APIKey: rbe.APIKey1, ActionTimeout: time.Minute}).Wait()
	require.Equal(t, 0, res.ExitCode, "stderr: %s", res.Stderr)
	require.Empty(t, res.Stderr)
	require.Equal(t, "ipv4-ipv6-loopback-ok\n", res.Stdout)
}

package firecracker_test

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// Set from firecracker/testdata:defs.bzl and @busybox via BUILD x_defs.
var (
	dockerLegacyImage                  string
	dockerDindImage                    string
	dockerBusyboxOCIImageRlocationpath string
)

const nestedDockerImage = "firecracker-test-busybox:local"

func dockerCommand(script, image string, properties ...*repb.Platform_Property) *repb.Command {
	props := []*repb.Platform_Property{
		{Name: "container-image", Value: platform.DockerPrefix + image},
		{Name: "init-dockerd", Value: "true"},
		{Name: "network", Value: "off"},
		{Name: platform.EstimatedMemoryPropertyName, Value: "2500MB"},
		{Name: platform.EstimatedFreeDiskPropertyName, Value: "2GB"},
	}
	return firecrackerCommand(script, append(props, properties...)...)
}

// Deliver the nested image through CAS rather than making the guest pull from
// a public registry. This also tests a nontrivial action input.
func dockerInputRoot(t *testing.T) string {
	t.Helper()
	require.NotEmpty(t, dockerBusyboxOCIImageRlocationpath, "missing @busybox BUILD x_def")
	image := testregistry.ImageFromRlocationpath(t, dockerBusyboxOCIImageRlocationpath)
	tag, err := name.NewTag(nestedDockerImage)
	require.NoError(t, err)
	root := t.TempDir()
	require.NoError(t, tarball.WriteToFile(filepath.Join(root, "busybox.tar"), tag, image))
	testfs.WriteAllFileContents(t, root, map[string]string{
		"nested/input.txt": "nested-action-input\n",
	})
	return root
}

type dockerTestCase struct {
	name       string
	image      string
	version    string
	amd64Only  bool
	wantDriver string
}

func testDockerNativeUDS(t *testing.T, env *firecrackerEnv) {
	runDockerUDSCases(t, env, true, []dockerTestCase{
		{"docker20_native", dockerLegacyImage, "20.10.7", true, "native"},
		{"docker28_native", platform.Ubuntu24_04Image, "28.1.0", false, "native"},
		{"docker29_nftables", dockerDindImage, "29.2.1", true, "native"},
	})
}

// Run only in the shared legacy (non-chunked) executor batch.
func testDockerLegacy(t *testing.T, env *firecrackerEnv) {
	runDockerUDSCases(t, env, false, []dockerTestCase{
		{"docker20_vfs", dockerLegacyImage, "20.10.7", true, "vfs"},
	})
}

func runDockerUDSCases(t *testing.T, env *firecrackerEnv, parallel bool, cases []dockerTestCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if parallel {
				t.Parallel()
			}
			if tc.amd64Only && runtime.GOARCH != "amd64" {
				t.Skip("this Docker image/kernel combination is supported only on amd64")
			}
			require.NotEmpty(t, tc.image, "missing Docker image BUILD x_def")
			runDockerUDS(t, env.forTest(t), tc)
		})
	}
}

func runDockerUDS(t *testing.T, rbe *firecrackerEnv, tc dockerTestCase) {
	t.Helper()
	cmd := dockerCommand(fmt.Sprintf(`
		set -eu
		export DOCKER_HOST=unix:///var/run/docker.sock
		test -S /var/run/docker.sock
		timeout 15 docker version --format '{{.Server.Version}}'
		driver=$(timeout 15 docker info --format '{{.Driver}}' 2>/tmp/docker-info.err)
		if test '%s' = native; then
			case "$driver" in overlay2|overlayfs) ;; *) echo "unexpected storage driver: $driver" >&2; exit 1;; esac
		else
			test "$driver" = vfs
		fi
		printf 'storage=%s\n'
		timeout 30 docker load -i busybox.tar >/tmp/docker-load.log 2>&1 || {
			cat /tmp/docker-load.log >&2; exit 1;
		}
		timeout 20 docker run --rm --network none '%s' echo nested-hello

		# Bind action inputs into a nested container and return its
		# output through the executor's ordinary output upload.
		mkdir -p output
		timeout 20 docker run --rm --network none \
			-v "$PWD/nested:/input:ro" -v "$PWD/output:/output" \
			'%s' sh -ec 'cat /input/input.txt > /output/result.txt; printf "nested-output\n" >> /output/result.txt'

		# Actually connect to a published port, rather than merely
		# asking docker to accept the -p option.
		timeout 20 docker run -d --name http-probe \
			-p 127.0.0.1:18080:80 '%s' sh -ec \
			'mkdir /www; printf "published-http\n" >/www/index.html; exec httpd -f -p 80 -h /www' >/dev/null
		trap 'timeout 10 docker rm -f http-probe >/dev/null 2>&1 || true' EXIT
		ok=false
		for attempt in $(seq 20); do
			# Use the nested BusyBox client to work with all three
			# outer images, without assuming curl/wget is installed.
			if timeout 5 docker run --rm --network host '%s' wget -q -T 2 -O - \
				http://127.0.0.1:18080/ > /tmp/published 2>/tmp/published.err; then
				ok=true
				break
			fi
			sleep 0.1
		done
		if test "$ok" != true; then
			cat /tmp/published.err >&2
			timeout 5 docker logs http-probe >&2 || true
			exit 1
		fi
		test "$(cat /tmp/published)" = published-http
		cat /tmp/published
	`, tc.wantDriver, tc.wantDriver, nestedDockerImage, nestedDockerImage, nestedDockerImage, nestedDockerImage), tc.image)
	cmd.OutputFiles = []string{"output/result.txt"}
	res := rbe.Execute(cmd, &rbetest.ExecuteOpts{
		APIKey:        rbe.APIKey1,
		InputRootDir:  dockerInputRoot(t),
		ActionTimeout: 4 * time.Minute,
	}).Wait()
	require.Equal(t, 0, res.ExitCode, "stdout: %s\nstderr: %s", res.Stdout, res.Stderr)
	require.Empty(t, res.Stderr)
	require.Equal(t, tc.version+"\nstorage="+tc.wantDriver+"\nnested-hello\npublished-http\n", res.Stdout)
	out := rbe.DownloadOutputsToNewTempDir(res)
	testfs.AssertExactFileContents(t, out, map[string]string{
		"output/result.txt": "nested-action-input\nnested-output\n",
	})
}

func testDockerOverTCP(t *testing.T, env *firecrackerEnv) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			t.Parallel()
			rbe := env.forTest(t)
			// First prove that dockerd is healthy on UDS. A failed daemon
			// startup must not satisfy the TCP-disabled assertion.
			script := `
				set -eu
				timeout 15 docker -H unix:///var/run/docker.sock info >/dev/null 2>/tmp/uds.err
			`
			expected := "tcp-disabled-uds-ready\n"
			opts := &rbetest.ExecuteOpts{APIKey: rbe.APIKey1, ActionTimeout: 2 * time.Minute}
			if enabled {
				script += fmt.Sprintf(`
					export DOCKER_HOST=tcp://127.0.0.1:2375
					timeout 15 docker version --format '{{.Server.Version}}'
					timeout 30 docker load -i busybox.tar >/tmp/load.log 2>&1 || {
						cat /tmp/load.log >&2; exit 1;
					}
					timeout 20 docker run --rm --network none '%s' echo tcp-nested-ok
				`, nestedDockerImage)
				opts.InputRootDir = dockerInputRoot(t)
				expected = "28.1.0\ntcp-nested-ok\n"
			} else {
				script += `
					set +e
					timeout 10 docker -H tcp://127.0.0.1:2375 info >/tmp/tcp.out 2>/tmp/tcp.err
					rc=$?
					set -e
					test "$rc" -eq 1
					grep -Eq 'Cannot connect to the Docker daemon|connection refused' /tmp/tcp.err
					printf 'tcp-disabled-uds-ready\n'
				`
			}
			res := rbe.Execute(dockerCommand(script, platform.Ubuntu24_04Image,
				&repb.Platform_Property{Name: "enable-dockerd-tcp", Value: fmt.Sprint(enabled)},
			), opts).Wait()
			require.Equal(t, 0, res.ExitCode, "stdout: %s\nstderr: %s", res.Stdout, res.Stderr)
			require.Empty(t, res.Stderr)
			require.Equal(t, expected, res.Stdout)
		})
	}
}

func testDockerInitializationDisabled(t *testing.T, env *firecrackerEnv) {
	rbe := env.forTest(t)
	res := rbe.Execute(dockerCommand(`
		set -eu
		# Establish the client is installed, then require a genuine connection
		# failure rather than treating any nonzero status as success.
		docker --version >/dev/null
		test ! -S /var/run/docker.sock
		set +e
		timeout 10 docker -H unix:///var/run/docker.sock info >/tmp/docker.out 2>/tmp/docker.err
		rc=$?
		set -e
		test "$rc" -eq 1
		grep -Eq 'Cannot connect to the Docker daemon|connection refused' /tmp/docker.err
		printf 'dockerd-not-started\n'
	`, platform.Ubuntu24_04Image,
		&repb.Platform_Property{Name: "init-dockerd", Value: "false"},
	), &rbetest.ExecuteOpts{APIKey: rbe.APIKey1, ActionTimeout: time.Minute}).Wait()
	require.Equal(t, 0, res.ExitCode, "stderr: %s", res.Stderr)
	require.Empty(t, res.Stderr)
	require.Equal(t, "dockerd-not-started\n", res.Stdout)
}

func testDockerNativeSnapshotResume(t *testing.T, env *firecrackerEnv) {
	runDockerSnapshotResume(t, env, dockerTestCase{"docker28_native", platform.Ubuntu24_04Image, "28.1.0", false, "native"})
}

func runDockerSnapshotResume(t *testing.T, env *firecrackerEnv, tc dockerTestCase) {
	t.Helper()
	if tc.amd64Only && runtime.GOARCH != "amd64" {
		t.Skip("the Docker 20 image is supported only on amd64")
	}
	require.NotEmpty(t, tc.image, "missing Docker image BUILD x_def")
	rbe := env.forTest(t)

	// Run identical Docker operations before and after Pause/Unpause:
	// inspect storage, read action inputs, write action outputs, and
	// connect through a newly published port. On the second execution,
	// these bind mounts must refer to the replacement workspace.
	exerciseDocker := fmt.Sprintf(`
		test "$(timeout 15 docker version --format '{{.Server.Version}}')" = '%s'
		driver=$(timeout 15 docker info --format '{{.Driver}}' 2>/tmp/docker-info.err)
		if test '%s' = native; then
			case "$driver" in overlay2|overlayfs) ;; *) echo "unexpected storage driver: $driver" >&2; exit 1;; esac
		else
			test "$driver" = vfs
		fi
		printf 'storage=%s\n'
		mkdir -p output
		cat /proc/sys/kernel/random/boot_id > output/boot_id
		timeout 20 docker run --rm --pull=never --network none \
			-v "$PWD/nested:/input:ro" -v "$PWD/output:/output" \
			'%s' sh -ec 'cat /input/input.txt > /output/result.txt; printf "nested-output\n" >> /output/result.txt'

		timeout 20 docker run -d --pull=never --name http-probe \
			-v "$PWD/nested:/www:ro" -p 127.0.0.1:18080:80 \
			'%s' httpd -f -p 80 -h /www >/dev/null
		trap 'timeout 10 docker rm -f http-probe >/dev/null 2>&1 || true' EXIT
		ok=false
		for attempt in $(seq 20); do
			if timeout 5 docker run --rm --pull=never --network host \
				'%s' wget -q -T 2 -O - http://127.0.0.1:18080/input.txt \
				>/tmp/published 2>/tmp/published.err; then
				ok=true
				break
			fi
			sleep 0.1
		done
		if test "$ok" != true; then
			cat /tmp/published.err >&2
			timeout 5 docker logs http-probe >&2 || true
			exit 1
		fi
		cmp nested/input.txt /tmp/published
		cat /tmp/published
	`, tc.version, tc.wantDriver, tc.wantDriver, nestedDockerImage, nestedDockerImage, nestedDockerImage)

	firstInputs := dockerInputRoot(t)
	testfs.WriteAllFileContents(t, firstInputs, map[string]string{
		"old-only.txt": "old workspace\n",
		"script.sh": fmt.Sprintf(`#!/bin/sh
			set -eu
			export DOCKER_HOST=unix:///var/run/docker.sock
			test -S /var/run/docker.sock
			test ! -e /root/docker-resume-marker
			test "$(cat old-only.txt)" = 'old workspace'
			printf 'persistent-docker-guest\n' > /root/docker-resume-marker
			cat /proc/sys/kernel/random/boot_id > /root/docker-boot-id
			timeout 30 docker load -i busybox.tar >/tmp/docker-load.log 2>&1 || {
				cat /tmp/docker-load.log >&2; exit 1;
			}
			timeout 15 docker image inspect --format '{{.Id}}' '%s' > /root/nested-image-id
			# Leave a stopped container with a modified writable
			# layer to test Docker's persisted storage after resume.
			timeout 20 docker run --name layer-probe --pull=never --network none \
				'%s' sh -ec 'printf "persisted-docker-layer\n" > /state.txt'
		`, nestedDockerImage, nestedDockerImage) + exerciseDocker + "\nprintf 'before-snapshot\\n'\n",
	})
	cmd := dockerCommand("sh script.sh", tc.image,
		&repb.Platform_Property{Name: "recycle-runner", Value: "true"},
		&repb.Platform_Property{Name: platform.RunnerRecyclingKey, Value: t.Name()},
	)
	cmd.OutputFiles = []string{"output/boot_id", "output/result.txt"}
	first := rbe.Execute(cmd, &rbetest.ExecuteOpts{
		APIKey:           rbe.APIKey1,
		InputRootDir:     firstInputs,
		ActionTimeout:    4 * time.Minute,
		DoNotCacheAction: true,
	}).Wait()
	require.Equal(t, 0, first.ExitCode, "stdout: %s\nstderr: %s", first.Stdout, first.Stderr)
	require.Empty(t, first.Stderr)
	require.Equal(t, "storage="+tc.wantDriver+"\nnested-action-input\nbefore-snapshot\n", first.Stdout)
	firstOutputs := rbe.DownloadOutputsToNewTempDir(first)
	bootID := testfs.ReadFileAsString(t, firstOutputs, "output/boot_id")
	require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\n$`, bootID)
	testfs.AssertExactFileContents(t, firstOutputs, map[string]string{
		"output/boot_id":    bootID,
		"output/result.txt": "nested-action-input\nnested-output\n",
	})

	// Synchronize this execution's snapshot save, not an unkeyed pool
	// gauge that can refer to another concurrent test (and stays zero
	// when chunked snapshots bypass the runner pool).
	waitForFirecrackerSnapshot(t, rbe, first)
	secondInputs := t.TempDir()
	testfs.WriteAllFileContents(t, secondInputs, map[string]string{
		"new-only.txt":     "replacement workspace\n",
		"nested/input.txt": "replacement-action-input\n",
		"script.sh": fmt.Sprintf(`#!/bin/sh
			set -eu
			export DOCKER_HOST=unix:///var/run/docker.sock
			test ! -e old-only.txt
			test ! -e busybox.tar
			test ! -e output/boot_id
			test ! -e output/result.txt
			test "$(cat new-only.txt)" = 'replacement workspace'
			test "$(cat /root/docker-resume-marker)" = persistent-docker-guest
			cmp /root/docker-boot-id /proc/sys/kernel/random/boot_id
			test -S /var/run/docker.sock
			# No load or pull is possible in this action: its input
			# tree has no image archive, and networking is local-only.
			test "$(timeout 15 docker image inspect --format '{{.Id}}' '%s')" = "$(cat /root/nested-image-id)"
			timeout 15 docker cp layer-probe:/state.txt /tmp/docker-layer-state
			test "$(cat /tmp/docker-layer-state)" = persisted-docker-layer
		`, nestedDockerImage) + exerciseDocker + "\nprintf 'after-snapshot\\n'\n",
	})
	// Reuse the exact command and platform, changing only the CAS input
	// root. The guest marker, kernel boot ID, cached image, and modified
	// Docker layer all have to survive; a fresh-boot fallback cannot pass.
	second := rbe.Execute(cmd, &rbetest.ExecuteOpts{
		APIKey:           rbe.APIKey1,
		InputRootDir:     secondInputs,
		ActionTimeout:    4 * time.Minute,
		DoNotCacheAction: true,
	}).Wait()
	require.Equal(t, 0, second.ExitCode, "stdout: %s\nstderr: %s", second.Stdout, second.Stderr)
	require.Empty(t, second.Stderr)
	require.Equal(t, "storage="+tc.wantDriver+"\nreplacement-action-input\nafter-snapshot\n", second.Stdout)
	testfs.AssertExactFileContents(t, rbe.DownloadOutputsToNewTempDir(second), map[string]string{
		"output/boot_id":    bootID,
		"output/result.txt": "replacement-action-input\nnested-output\n",
	})
	// A resumed ordinary RBE action intentionally does not save another local
	// snapshot. There is no third action, so do not wait for a nonexistent save.
}

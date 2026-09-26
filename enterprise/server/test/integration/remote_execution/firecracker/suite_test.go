package firecracker_test

import (
	"flag"
	"fmt"
	"testing"
)

var debugVMLogs = flag.Bool("firecracker_test_debug_vm_logs", false, "Stream guest and Firecracker logs for debugging.")

var legacyMode = flag.Bool("firecracker_test_legacy", false, "Run the non-chunked Docker/vfs configuration instead of the default chunked snapshot suite.")

// One production executor and one app/cache fixture serve all cases in this
// test. Each case owns its assertions and inputs; its platform key isolates VM
// snapshots from other cases. The executor scheduler bounds guest concurrency.
func TestFirecracker(t *testing.T) {
	args := []string{
		fmt.Sprintf("--executor.firecracker_debug_stream_vm_logs=%t", *debugVMLogs),
		"--executor.enable_local_snapshot_sharing=true",
		"--executor.firecracker_vmexec_ready_signal=true",
		"--executor.firecracker_network_pool_size=16",
		"--executor.task_allowed_private_ips=default",
		"--executor.container_registry_allowed_private_ips=127.0.0.1/32",
		"--executor.local_cache_store_ext4_images=true",
		"--executor.network_stats_enabled=true",
		"--executor.firecracker_health_check_interval=1s",
		"--executor.firecracker_health_check_timeout=2s",
	}
	if *legacyMode {
		args = append(args,
			"--executor.enable_local_snapshot_sharing=false",
			"--executor.firecracker_vmexec_ready_signal=false",
		)
	}
	mirror := newDockerMirror(t)
	routedProbe := newRoutedNetworkProbe(t)
	args = append(args, mirror.executorArgs()...)
	env := newFirecrackerEnv(t, args...)
	type testCase struct {
		name string
		run  func(*testing.T, *firecrackerEnv)
	}
	cases := []testCase{
		{"Execution", testExecution},
		{"InputsAndOutputs", testInputsAndOutputs},
		{"LargeStdout", testLargeStdout},
		{"TimeoutPreservesDebugOutputs", testTimeoutPreservesDebugOutputs},
		{"OrphanedProcessIsReaped", testOrphanedProcessIsReaped},
		{"ConcurrentIO", testConcurrentIO},
		{"ExecutionsOverlap", testExecutionsOverlap},
		{"NetworkEnabledAndDisabled", testNetworkEnabledAndDisabled},
		{"NetworkForwarding", routedProbe.test},
		{"ResolvConf", testResolvConf},
		{"GuestIPv6", testGuestIPv6},
		{"DockerOverUDS", testDockerNativeUDS},
		{"DockerOverTCP", testDockerOverTCP},
		{"DockerInitializationDisabled", testDockerInitializationDisabled},
		{"DockerMirror", mirror.test},
		{"SnapshotResumeReplacesWorkspace", testSnapshotResumeReplacesWorkspace},
		{"DockerAfterSnapshotResume", testDockerNativeSnapshotResume},
		{"ColdImageCache", testFirecrackerColdImageCache},
		{"CachedPrivateImageRequiresAuthorization", testFirecrackerCachedPrivateImageRequiresAuthorization},
		{"GuestHealthCheckFailure", testFirecrackerGuestHealthCheckFailure},
	}
	if *legacyMode {
		// Executor-wide storage settings cannot vary per action. The optional
		// alternate run still uses exactly one executor, not one per variant.
		cases = []testCase{
			{"DockerLegacyVFS", testDockerLegacy},
			{"Execution", testExecution},
			{"InputsAndOutputs", testInputsAndOutputs},
			{"NetworkEnabledAndDisabled", testNetworkEnabledAndDisabled},
		}
	}
	t.Run("Parallel", func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				tc.run(t, env.forTest(t))
			})
		}
	})
	if !*legacyMode {
		// Global network pool lifecycle observations are intentionally kept
		// outside the parallel group. Action-local checks above remain parallel.
		t.Run("NetworkPoolingMixedModes", func(t *testing.T) {
			routedProbe.runModes(t, env.forTest(t), []string{"local", "external", "off", "external", "local", "external"})
		})
	}
}

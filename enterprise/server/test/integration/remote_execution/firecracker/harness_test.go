package firecracker_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testexecutor"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

// Set by BUILD from the same pinned image reference as the container-level tests.
var busyboxImage string

type firecrackerEnv struct {
	*rbetest.Env
	executor  *testexecutor.Executor
	t         *testing.T
	collector interfaces.ExecutionCollector
}

// forTest scopes execution assertions and output cleanup to a leaf subtest while
// sharing the batch's app and executor. Never copy rbetest.Env (it owns locks).
func (e *firecrackerEnv) forTest(t *testing.T) *firecrackerEnv {
	return &firecrackerEnv{Env: e.Env, executor: e.executor, t: t, collector: e.collector}
}

// Execute clones caller-owned data so parallel cases can reuse command templates.
// test-case participates in command/action digests and snapshot/runner keys; it
// stays stable across the actions in one test's restore sequence.
func (e *firecrackerEnv) Execute(command *repb.Command, opts *rbetest.ExecuteOpts) *rbetest.Command {
	e.t.Helper()
	copiedOpts := rbetest.ExecuteOpts{}
	if opts != nil {
		copiedOpts = *opts
	}
	if copiedOpts.TestingT == nil {
		copiedOpts.TestingT = e.t
	}
	if copiedOpts.Context == nil {
		copiedOpts.Context = copiedOpts.TestingT.Context()
	}
	command = proto.Clone(command).(*repb.Command)
	if command.Platform == nil {
		command.Platform = &repb.Platform{}
	}
	props := command.Platform.Properties[:0]
	for _, p := range command.Platform.Properties {
		if !strings.EqualFold(p.GetName(), "test-case") {
			props = append(props, p)
		}
	}
	command.Platform.Properties = append(props, &repb.Platform_Property{Name: "test-case", Value: e.t.Name()})
	sort.Slice(command.Platform.Properties, func(i, j int) bool {
		return command.Platform.Properties[i].GetName() < command.Platform.Properties[j].GetName()
	})
	return e.Env.Execute(command, &copiedOpts)
}

// These experiments make the app retain the real executor's post-Pause update.
// All other experiment decisions still use the normal default provider.
type snapshotStatsFlags struct {
	interfaces.ExperimentFlagProvider
}

func (f *snapshotStatsFlags) Boolean(ctx context.Context, name string, defaultValue bool, opts ...any) bool {
	if name == "remote_execution.publish_post_completion_stats" || name == "remote_execution.flush_executions_after_cleanup" {
		return true
	}
	return f.ExperimentFlagProvider.Boolean(ctx, name, defaultValue, opts...)
}

// newFirecrackerEnv starts an app fixture and the production executor binary.
// Executor options must be passed as arguments here, not set with flags.Set:
// flags in the test process do not configure the executor subprocess.
//
// Start batches serially: startup changes process-global app flags and host
// networking. Leaf cases may use t.Parallel after scoping with env.forTest(t).
func newFirecrackerEnv(t *testing.T, extraExecutorArgs ...string) *firecrackerEnv {
	t.Helper()
	require.Equal(t, "linux", runtime.GOOS, "Firecracker tests require Linux")
	require.Zero(t, os.Geteuid(), "Firecracker tests must run as root, outside the Bazel sandbox; see README.md")
	kvm, err := os.OpenFile("/dev/kvm", os.O_RDWR, 0)
	require.NoError(t, err, "Firecracker tests require a usable /dev/kvm")
	require.NoError(t, kvm.Close())

	// Host tools such as iptables are often installed outside an ordinary user's
	// PATH. The executor subprocess inherits this environment.
	t.Setenv("PATH", os.Getenv("PATH")+":/usr/sbin:/sbin")
	for _, tool := range []string{"firecracker", "jailer", "ip", "iptables", "mke2fs", "debugfs"} {
		_, err := exec.LookPath(tool)
		require.NoError(t, err, "required host tool %q is missing; see README.md", tool)
	}
	// Do not inherit the outer RBE executor's pool.
	t.Setenv("MY_POOL", "")

	// t.TempDir() (and TEST_TMPDIR) are too long for Firecracker's Unix sockets.
	// Keep the jailer root short, and place the file cache on the same filesystem
	// so that the executor can hard-link cached input files and images.
	root, err := os.MkdirTemp("/tmp", "fc-e2e-")
	require.NoError(t, err)
	rootMounted := false
	t.Cleanup(func() {
		// Registered before the executor's cleanup so its VMs and FUSE mounts
		// are gone first. Do not remove files through a mount that is still busy.
		if rootMounted {
			if err := syscall.Unmount(root, 0); err != nil {
				t.Errorf("unmount Firecracker fixture root %q: %s", root, err)
				return
			}
		}
		require.NoError(t, os.RemoveAll(root))
	})
	// The jailer creates a slave mount namespace. Unlike the boot disks, the
	// memory-export VBD is mounted after the VM starts, so it must propagate
	// into that namespace. A private /tmp would otherwise make Firecracker
	// write an ordinary file underneath the VBD, leaving the memory COW empty.
	//
	// Give this fixture a shared mount without changing / or /tmp's propagation
	// type. Keeping both build and cache under this mount also preserves
	// hard-link compatibility.
	require.NoError(t, syscall.Mount(root, root, "", syscall.MS_BIND, ""), "bind-mount Firecracker fixture root (requires CAP_SYS_ADMIN)")
	rootMounted = true
	require.NoError(t, syscall.Mount("", root, "", syscall.MS_SHARED, ""), "share late VBD mounts with the jailer")

	// The real executor must validate the same signed JWTs the app issues.
	// Without an OAuth provider it silently installs NullAuthenticator, losing
	// group IDs in snapshot and image-cache keys despite valid task JWTs.
	const jwtKey = "firecracker-integration-test-signing-key"
	flags.Set(t, "auth.jwt_key", jwtKey)
	env := rbetest.NewRBETestEnvWithOptions(t, &rbetest.EnvOptions{
		// Unlike in-process fixtures, this executor starts with a cold image
		// cache. Allow OCI download/conversion as well as guest execution.
		CommandTimeout: 5 * time.Minute,
	})
	flags.Set(t, "remote_execution.shared_executor_pool_group_id", env.GroupID1)
	// Keep post-completion execution records in Redis for per-execution snapshot
	// synchronization. No invocation/OLAP lifecycle is exercised by this suite.
	flags.Set(t, "remote_execution.write_execution_progress_state_to_redis", true)
	flags.Set(t, "app.enable_write_executions_to_olap_db", false)
	var collector interfaces.ExecutionCollector
	env.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		SchedulerServerOptions: scheduler_server.Options{
			RequireExecutorAuthorization: true,
		},
		EnvModifier: func(app *testenv.TestEnv) {
			fp, err := experiments.NewFlagProvider(t.Name())
			require.NoError(t, err)
			app.SetExperimentFlagProvider(&snapshotStatsFlags{fp})
			collector = app.GetExecutionCollector()
		},
	})
	resolvConf := filepath.Join(root, "resolv.conf")
	require.NoError(t, os.WriteFile(resolvConf, []byte(firecrackerTestResolvConf), 0644))

	args := []string{
		"--config_file=",
		"--executor.app_target=" + env.GetBuildBuddyServerTarget(),
		"--executor.api_key=" + env.APIKey1,
		"--auth.jwt_key=" + jwtKey,
		"--auth.enable_anonymous_usage=true",
		// OIDC discovery is lazy; no login is performed. This enables the real
		// JWT authenticator instead of the null-auth fallback.
		`--auth.oauth_providers=[{"issuer_url":"http://localhost","client_id":"firecracker-test","client_secret":"test"}]`,
		"--executor.root_directory=" + filepath.Join(root, "build"),
		"--executor.local_cache_directory=" + filepath.Join(root, "cache"),
		"--executor.metadata_directory=" + filepath.Join(root, "metadata"),
		"--executor.local_cache_size_bytes=10000000000",
		"--executor.enable_firecracker=true",
		"--executor.enable_bare_runner=false",
		// Local chunked snapshots support independent concurrent runner sequences.
		"--executor.enable_local_snapshot_sharing=true",
		"--executor.enable_remote_snapshot_sharing=false",
		// Match the direct suite's MMIO configuration. The provider currently
		// enables PCI on snapshot restore but omits --enable-pci on fresh boot;
		// keep the boot/restore transport consistent without changing production.
		"--executor.firecracker_enable_pci=false",
		"--executor.firecracker_overprovision_cpus=0",
		"--executor.mmap_memory_bytes=268435456",
		"--executor.warmup_default_images=false",
		"--executor.warmup_workflow_images=false",
		"--executor.preserve_existing_netns=true",
		"--executor.firecracker_vmexec_ready_signal=true",
		"--executor.firecracker_health_check_interval=1s",
		"--executor.firecracker_health_check_timeout=2s",
		"--executor.task_allowed_private_ips=default",
		"--executor.network_stats_enabled=true",
		"--executor.container_registry_allowed_private_ips=127.0.0.1/32",
		"--executor.firecracker_vm_resolv_conf=" + resolvConf,
		"--max_shutdown_duration=45s",
		// This directory deliberately outlives individual tests. All executor
		// processes on the host must use the same lock files to avoid assigning
		// duplicate IP ranges. Do not remove it while another test is running.
		"--executor.network_lock_directory=/tmp/buildbuddy-fc-e2e-network-locks",
		"--executor.firecracker_network_pool_size=8",
	}
	executor := testexecutor.RunWithOptions(t, &testexecutor.Options{
		GracefulShutdownTimeout: time.Minute,
	}, append(args, extraExecutorArgs...)...)

	// /readyz does not guarantee that the scheduler has processed registration.
	// Query the public API instead of rbetest's internal executor map, which
	// intentionally contains only in-process executors.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ctx = env.WithUserID(ctx, env.UserID1)
	require.Eventually(t, func() bool {
		rsp, err := env.GetBuildBuddyServiceClient().GetExecutionNodes(ctx, &scpb.GetExecutionNodesRequest{
			RequestContext: &ctxpb.RequestContext{GroupId: env.GroupID1},
		})
		if err != nil {
			t.Logf("waiting for executor registration: %s", err)
			return false
		}
		return len(rsp.GetExecutor()) == 1
	}, time.Minute, 100*time.Millisecond, "executor should register with the app")
	return &firecrackerEnv{Env: env, executor: executor, t: t, collector: collector}
}

// waitForFirecrackerSnapshot observes the actual post-completion update from
// this execution. A global pool gauge cannot synchronize chunked snapshots (the
// runner pool is bypassed), and an aggregate snapshot counter races other cases.
// Marker/boot-ID assertions in the next action must still prove restoration.
func waitForFirecrackerSnapshot(t *testing.T, env *firecrackerEnv, result *rbetest.CommandResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	require.Eventually(t, func() bool {
		execution, err := env.collector.GetInProgressExecution(ctx, result.ID)
		if err != nil {
			t.Logf("waiting for snapshot stats for %s: %s", result.ID, err)
			return false
		}
		return execution.GetPauseDurationUsec() > 0 && (execution.GetSnapshotSavedLocally() || execution.GetSnapshotSavedRemotely())
	}, 3*time.Minute, 100*time.Millisecond, "execution %s should finish saving its snapshot", result.ID)
}

// firecrackerCommand builds an RE API command. Properties override defaults
// case-insensitively, with the last supplied value winning. Sorting and removing
// duplicates also makes the command comply with the RE API's platform contract.
func firecrackerCommand(script string, properties ...*repb.Platform_Property) *repb.Command {
	defaults := []*repb.Platform_Property{
		{Name: "OSFamily", Value: "linux"},
		{Name: "Arch", Value: runtime.GOARCH},
		{Name: "workload-isolation-type", Value: "firecracker"},
		{Name: "network", Value: "off"},
		{Name: "container-image", Value: "docker://" + busyboxImage},
		{Name: "EstimatedCPU", Value: "1"},
		{Name: "EstimatedMemory", Value: "512MiB"},
		{Name: "EstimatedFreeDiskBytes", Value: "100000000"},
	}
	byName := make(map[string]*repb.Platform_Property, len(defaults)+len(properties))
	for _, p := range append(defaults, properties...) {
		byName[strings.ToLower(p.GetName())] = p
	}
	props := make([]*repb.Platform_Property, 0, len(byName))
	for _, p := range byName {
		props = append(props, p)
	}
	sort.Slice(props, func(i, j int) bool { return props[i].GetName() < props[j].GetName() })
	return &repb.Command{
		Arguments: []string{"sh", "-eu", "-c", script},
		Platform:  &repb.Platform{Properties: props},
	}
}

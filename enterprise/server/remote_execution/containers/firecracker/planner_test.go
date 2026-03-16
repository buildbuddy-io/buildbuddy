package firecracker

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/platform"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestComputeSupportsRemoteSnapshots(t *testing.T) {
	task := ciTask()
	if !computeSupportsRemoteSnapshots(task, true, false) {
		t.Fatal("expected CI task to support remote snapshots when enabled")
	}
	if computeSupportsRemoteSnapshots(nonCITask(), true, false) {
		t.Fatal("expected non-CI task not to support remote snapshots by default")
	}
	if !computeSupportsRemoteSnapshots(nonCITask(), true, true) {
		t.Fatal("expected force flag to enable remote snapshots")
	}
}

func TestComputeSnapshotPlan(t *testing.T) {
	tests := []struct {
		name          string
		input         snapshotPlannerInput
		wantRemote    bool
		wantLocal     bool
		wantType      string
		wantMemName   string
		wantStateName string
	}{
		{
			name: "always save default snapshot",
			input: snapshotPlannerInput{
				task:                        ciTask(withPlatformProperty(platform.SnapshotSavePolicyPropertyName, platform.AlwaysSaveSnapshot)),
				supportsRemoteSnapshots:     true,
				localSnapshotSharingEnabled: true,
				recyclingEnabled:            true,
				isLikelyDefaultSnapshot:     true,
			},
			wantRemote:    true,
			wantLocal:     true,
			wantType:      fullSnapshotType,
			wantMemName:   fullMemSnapshotName,
			wantStateName: vmStateSnapshotName,
		},
		{
			name: "only save first non default snapshot once",
			input: snapshotPlannerInput{
				task:                        ciTask(withPlatformProperty(platform.SnapshotSavePolicyPropertyName, platform.OnlySaveFirstNonDefaultSnapshot)),
				supportsRemoteSnapshots:     true,
				localSnapshotSharingEnabled: true,
				recyclingEnabled:            true,
				existence: snapshotExistence{
					remoteForPrimary: true,
					localForPrimary:  true,
				},
			},
			wantRemote:    false,
			wantLocal:     false,
			wantType:      fullSnapshotType,
			wantMemName:   fullMemSnapshotName,
			wantStateName: vmStateSnapshotName,
		},
		{
			name: "non default remote fallback suppresses remote save but not local save",
			input: snapshotPlannerInput{
				task:                        ciTask(withPlatformProperty(platform.SnapshotSavePolicyPropertyName, platform.OnlySaveNonDefaultSnapshotIfNoneAvailable)),
				supportsRemoteSnapshots:     true,
				localSnapshotSharingEnabled: true,
				recyclingEnabled:            true,
				existence: snapshotExistence{
					remoteForAny:    true,
					localForPrimary: false,
				},
			},
			wantRemote:    false,
			wantLocal:     true,
			wantType:      fullSnapshotType,
			wantMemName:   fullMemSnapshotName,
			wantStateName: vmStateSnapshotName,
		},
		{
			name: "recycled VM uses diff snapshots",
			input: snapshotPlannerInput{
				task:                        ciTask(withPlatformProperty(platform.SnapshotSavePolicyPropertyName, platform.AlwaysSaveSnapshot)),
				supportsRemoteSnapshots:     true,
				localSnapshotSharingEnabled: true,
				recyclingEnabled:            true,
				recycled:                    true,
			},
			wantRemote:    true,
			wantLocal:     true,
			wantType:      diffSnapshotType,
			wantMemName:   diffMemSnapshotName,
			wantStateName: vmStateSnapshotName,
		},
		{
			name: "non CI action resumed from snapshot does not resave local snapshot",
			input: snapshotPlannerInput{
				task:                        nonCITask(withPlatformProperty(platform.SnapshotSavePolicyPropertyName, platform.AlwaysSaveSnapshot)),
				localSnapshotSharingEnabled: true,
				recyclingEnabled:            true,
				createFromSnapshot:          true,
			},
			wantRemote:    false,
			wantLocal:     false,
			wantType:      fullSnapshotType,
			wantMemName:   fullMemSnapshotName,
			wantStateName: vmStateSnapshotName,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := computeSnapshotPlan(test.input)
			if got.saveRemoteSnapshot != test.wantRemote {
				t.Fatalf("saveRemoteSnapshot = %t, want %t", got.saveRemoteSnapshot, test.wantRemote)
			}
			if got.saveLocalSnapshot != test.wantLocal {
				t.Fatalf("saveLocalSnapshot = %t, want %t", got.saveLocalSnapshot, test.wantLocal)
			}
			if got.snapshotType != test.wantType {
				t.Fatalf("snapshotType = %q, want %q", got.snapshotType, test.wantType)
			}
			if got.memSnapshotName != test.wantMemName {
				t.Fatalf("memSnapshotName = %q, want %q", got.memSnapshotName, test.wantMemName)
			}
			if got.vmStateSnapshotName != test.wantStateName {
				t.Fatalf("vmStateSnapshotName = %q, want %q", got.vmStateSnapshotName, test.wantStateName)
			}
		})
	}
}

func TestComputeSnapshotReadPolicy(t *testing.T) {
	tests := []struct {
		name    string
		task    *repb.ExecutionTask
		want    string
		wantErr bool
	}{
		{
			name: "default",
			task: ciTask(),
			want: platform.ReadLocalSnapshotFirst,
		},
		{
			name: "explicit local only",
			task: ciTask(withPlatformProperty(platform.SnapshotReadPolicyPropertyName, platform.ReadLocalSnapshotOnly)),
			want: platform.ReadLocalSnapshotOnly,
		},
		{
			name:    "invalid",
			task:    ciTask(withPlatformProperty(platform.SnapshotReadPolicyPropertyName, "bogus")),
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := computeSnapshotReadPolicy(test.task)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if got != test.want {
				t.Fatalf("policy = %q, want %q", got, test.want)
			}
		})
	}
}

func TestComputeBootArgs(t *testing.T) {
	args := computeBootArgs(&fcpb.VMConfiguration{
		DebugMode:        true,
		EnableLogging:    true,
		NetworkMode:      fcpb.NetworkMode_NETWORK_MODE_LOCAL,
		InitDockerd:      true,
		EnableDockerdTcp: true,
		EnableVfs:        true,
	}, true, true)

	for _, expected := range []string{
		"-debug_mode",
		"-enable_logging",
		"-set_default_route",
		"-init_dockerd",
		"-enable_dockerd_tcp",
		"-enable_rootfs",
		"-enable_vfs",
		"init_on_alloc=1",
		"init_on_free=1",
		machineIPBootArgs,
		"ipv6.disable=1",
	} {
		if !strings.Contains(args, expected) {
			t.Fatalf("boot args %q missing %q", args, expected)
		}
	}

	ipv6EnabledArgs := computeBootArgs(&fcpb.VMConfiguration{
		Ipv6Enabled: true,
		NetworkMode: fcpb.NetworkMode_NETWORK_MODE_OFF,
	}, false, false)
	if strings.Contains(ipv6EnabledArgs, "ipv6.disable=1") {
		t.Fatalf("boot args %q unexpectedly disabled ipv6", ipv6EnabledArgs)
	}
	if strings.Contains(ipv6EnabledArgs, machineIPBootArgs) {
		t.Fatalf("boot args %q unexpectedly configured networking", ipv6EnabledArgs)
	}
}

func TestComputeWorkspacePathsToExtract(t *testing.T) {
	preserve := ciTask(withPlatformProperty(platform.PreserveWorkspacePropertyName, "true"))
	got := computeWorkspacePathsToExtract(preserve)
	if len(got) != 1 || got[0] != "/" {
		t.Fatalf("preserve-workspace paths = %v, want [/]", got)
	}

	task := ciTask()
	task.Command.OutputDirectories = []string{"dir-out"}
	task.Command.OutputFiles = []string{"file-out"}
	task.Command.OutputPaths = []string{"path-out"}
	got = computeWorkspacePathsToExtract(task)
	want := []string{
		".BUILDBUDDY_DO_NOT_RECYCLE",
		".BUILDBUDDY_INVALIDATE_SNAPSHOT",
		"dir-out",
		"file-out",
		"path-out",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("paths = %v, want %v", got, want)
	}
}

func TestComputeShouldUpgradeGuestKernel(t *testing.T) {
	task := ciTask()
	task.Experiments = []string{"upgrade-fc-guest-kernel"}
	if !computeShouldUpgradeGuestKernel(task, "/path/to/kernel") {
		t.Fatal("expected upgrade when experiment is set and path is non-empty")
	}
	if computeShouldUpgradeGuestKernel(task, "") {
		t.Fatal("expected no upgrade when kernel path is empty")
	}
	task2 := ciTask()
	if computeShouldUpgradeGuestKernel(task2, "/path/to/kernel") {
		t.Fatal("expected no upgrade when experiment is not set")
	}
}

func TestIsBalloonEnabled(t *testing.T) {
	if !isBalloonEnabled(true, true, true) {
		t.Fatal("expected balloon enabled when all flags are true")
	}
	if isBalloonEnabled(false, true, true) {
		t.Fatal("expected balloon disabled when enableBalloon is false")
	}
	if isBalloonEnabled(true, false, true) {
		t.Fatal("expected balloon disabled when recycling is disabled")
	}
	if isBalloonEnabled(true, true, false) {
		t.Fatal("expected balloon disabled when remote snapshots not supported")
	}
}

func TestNetworkMode(t *testing.T) {
	tests := []struct {
		name        string
		network     string
		initDockerd bool
		want        fcpb.NetworkMode
		wantErr     bool
	}{
		{"external explicit", "external", false, fcpb.NetworkMode_NETWORK_MODE_EXTERNAL, false},
		{"empty defaults to external", "", false, fcpb.NetworkMode_NETWORK_MODE_EXTERNAL, false},
		{"off without dockerd", "off", false, fcpb.NetworkMode_NETWORK_MODE_OFF, false},
		{"off with dockerd gets local", "off", true, fcpb.NetworkMode_NETWORK_MODE_LOCAL, false},
		{"unsupported value", "bogus", false, fcpb.NetworkMode_NETWORK_MODE_UNSPECIFIED, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := networkMode(test.network, test.initDockerd)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if got != test.want {
				t.Fatalf("networkMode = %v, want %v", got, test.want)
			}
		})
	}
}

func TestNetworkingEnabled(t *testing.T) {
	if !networkingEnabled(fcpb.NetworkMode_NETWORK_MODE_EXTERNAL) {
		t.Fatal("expected EXTERNAL to be networking enabled")
	}
	if !networkingEnabled(fcpb.NetworkMode_NETWORK_MODE_LOCAL) {
		t.Fatal("expected LOCAL to be networking enabled")
	}
	if !networkingEnabled(fcpb.NetworkMode_NETWORK_MODE_UNSPECIFIED) {
		t.Fatal("expected UNSPECIFIED to be networking enabled (backward compat)")
	}
	if networkingEnabled(fcpb.NetworkMode_NETWORK_MODE_OFF) {
		t.Fatal("expected OFF to not be networking enabled")
	}
}

func TestExternalNetworkingEnabled(t *testing.T) {
	if !externalNetworkingEnabled(fcpb.NetworkMode_NETWORK_MODE_EXTERNAL) {
		t.Fatal("expected EXTERNAL to have external networking")
	}
	if !externalNetworkingEnabled(fcpb.NetworkMode_NETWORK_MODE_UNSPECIFIED) {
		t.Fatal("expected UNSPECIFIED to have external networking (backward compat)")
	}
	if externalNetworkingEnabled(fcpb.NetworkMode_NETWORK_MODE_LOCAL) {
		t.Fatal("expected LOCAL to not have external networking")
	}
	if externalNetworkingEnabled(fcpb.NetworkMode_NETWORK_MODE_OFF) {
		t.Fatal("expected OFF to not have external networking")
	}
}

func TestParseFatalInitError(t *testing.T) {
	if err := parseFatalInitError("some normal log output"); err != nil {
		t.Fatalf("expected nil for normal log, got %s", err)
	}
	err := parseFatalInitError("some log\r\n" + fatalInitLogPrefix + "something went wrong\r\n")
	if err == nil {
		t.Fatal("expected error for fatal init log")
	}
	if !strings.Contains(err.Error(), "something went wrong") {
		t.Fatalf("error message should contain the fatal message, got: %s", err)
	}
}

func TestParseOOMError(t *testing.T) {
	if err := parseOOMError("normal log output"); err != nil {
		t.Fatalf("expected nil for normal log, got %s", err)
	}
	err := parseOOMError("some log\noom-kill: memory cgroup...\nOut of memory: Killed process 123\nmore log")
	if err == nil {
		t.Fatal("expected error for OOM log")
	}
	if !strings.Contains(err.Error(), "oom-kill:") {
		t.Fatalf("error should contain oom-kill line, got: %s", err)
	}
	if !strings.Contains(err.Error(), "Out of memory") {
		t.Fatalf("error should contain Out of memory line, got: %s", err)
	}
}

func TestParseSegFault(t *testing.T) {
	if err := parseSegFault("log tail", []byte("normal stderr")); err != nil {
		t.Fatalf("expected nil for normal stderr, got %s", err)
	}
	err := parseSegFault("kernel log tail", []byte("got SIGSEGV"))
	if err == nil {
		t.Fatal("expected error for SIGSEGV in stderr")
	}
	if !strings.Contains(err.Error(), "segfault") {
		t.Fatalf("error should mention segfault, got: %s", err)
	}
}

func TestCombineHostAndGuestStats(t *testing.T) {
	hostFS := []*repb.UsageStats_FileSystemUsage{{Source: "/dev/host", UsedBytes: 100}}
	guestFS := []*repb.UsageStats_FileSystemUsage{{Source: "/dev/guest", UsedBytes: 200}}
	host := &repb.UsageStats{
		CpuNanos:            1000,
		MemoryBytes:         500,
		PeakMemoryBytes:     600,
		PeakFileSystemUsage: hostFS,
	}
	guest := &repb.UsageStats{
		CpuNanos:            2000,
		MemoryBytes:         800,
		PeakMemoryBytes:     900,
		PeakFileSystemUsage: guestFS,
	}
	result := combineHostAndGuestStats(host, guest)
	// CPU should come from host
	if result.CpuNanos != 1000 {
		t.Fatalf("CpuNanos = %d, want 1000 (from host)", result.CpuNanos)
	}
	// Memory should come from guest
	if result.MemoryBytes != 800 {
		t.Fatalf("MemoryBytes = %d, want 800 (from guest)", result.MemoryBytes)
	}
	if result.PeakMemoryBytes != 900 {
		t.Fatalf("PeakMemoryBytes = %d, want 900 (from guest)", result.PeakMemoryBytes)
	}
	// Disk should come from guest
	if len(result.PeakFileSystemUsage) != 1 || result.PeakFileSystemUsage[0].Source != "/dev/guest" {
		t.Fatalf("PeakFileSystemUsage should come from guest, got: %v", result.PeakFileSystemUsage)
	}
}

func TestAlignToMultiple(t *testing.T) {
	tests := []struct {
		n, multiple, want int64
	}{
		{0, 4096, 0},
		{1, 4096, 4096},
		{4096, 4096, 4096},
		{4097, 4096, 8192},
		{8191, 4096, 8192},
		{8192, 4096, 8192},
	}
	for _, test := range tests {
		got := alignToMultiple(test.n, test.multiple)
		if got != test.want {
			t.Fatalf("alignToMultiple(%d, %d) = %d, want %d", test.n, test.multiple, got, test.want)
		}
	}
}

func TestGetDockerDaemonConfig(t *testing.T) {
	// Empty config
	cfg, err := getDockerDaemonConfig(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if string(cfg) != "{}" {
		t.Fatalf("empty config = %q, want {}", string(cfg))
	}

	// With mirrors and insecure registries
	cfg, err = getDockerDaemonConfig(
		[]string{"https://mirror.example.com"},
		[]string{"insecure.example.com:5000"},
	)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	s := string(cfg)
	if !strings.Contains(s, "registry-mirrors") {
		t.Fatalf("config should contain registry-mirrors, got: %s", s)
	}
	if !strings.Contains(s, "insecure-registries") {
		t.Fatalf("config should contain insecure-registries, got: %s", s)
	}
}

func TestParseDNSOverrides(t *testing.T) {
	// Empty
	result, err := parseDNSOverrides(nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if result != "" {
		t.Fatalf("empty overrides = %q, want empty", result)
	}

	// Valid
	overrides := []DNSOverride{{HostnameToOverride: "example.com."}}
	result, err = parseDNSOverrides(overrides)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if !strings.Contains(result, "example.com.") {
		t.Fatalf("result should contain hostname, got: %s", result)
	}

	// Missing trailing dot
	_, err = parseDNSOverrides([]DNSOverride{{HostnameToOverride: "example.com"}})
	if err == nil {
		t.Fatal("expected error for hostname without trailing dot")
	}

	// Empty hostname
	_, err = parseDNSOverrides([]DNSOverride{{HostnameToOverride: ""}})
	if err == nil {
		t.Fatal("expected error for empty hostname")
	}
}

func ciTask(opts ...taskOpt) *repb.ExecutionTask {
	task := &repb.ExecutionTask{
		Command: &repb.Command{
			Arguments: []string{"./buildbuddy_ci_runner"},
		},
	}
	for _, opt := range opts {
		opt(task)
	}
	return task
}

func nonCITask(opts ...taskOpt) *repb.ExecutionTask {
	task := &repb.ExecutionTask{
		Command: &repb.Command{
			Arguments: []string{"sh", "-c", "echo hello"},
		},
	}
	for _, opt := range opts {
		opt(task)
	}
	return task
}

type taskOpt func(*repb.ExecutionTask)

func withPlatformProperty(name, value string) taskOpt {
	return func(task *repb.ExecutionTask) {
		if task.Command.Platform == nil {
			task.Command.Platform = &repb.Platform{}
		}
		task.Command.Platform.Properties = append(task.Command.Platform.Properties, &repb.Platform_Property{
			Name:  name,
			Value: value,
		})
	}
}

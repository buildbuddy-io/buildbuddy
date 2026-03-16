package firecracker

import (
	"encoding/json"
	"regexp"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const fatalInitLogPrefix = "die: "

var fatalErrPattern = regexp.MustCompile(`\b` + fatalInitLogPrefix + `(.*)`)

type snapshotExistence struct {
	remoteForPrimary bool
	remoteForAny     bool
	localForPrimary  bool
}

type snapshotPlannerInput struct {
	task                        *repb.ExecutionTask
	supportsRemoteSnapshots     bool
	localSnapshotSharingEnabled bool
	recyclingEnabled            bool
	createFromSnapshot          bool
	recycled                    bool
	isLikelyDefaultSnapshot     bool
	existence                   snapshotExistence
}

func computeSupportsRemoteSnapshots(task *repb.ExecutionTask, remoteSnapshotSharingEnabled bool, forceRemoteSnapshotting bool) bool {
	return remoteSnapshotSharingEnabled && (platform.IsCICommand(task.GetCommand(), platform.GetProto(task.GetAction(), task.GetCommand())) || forceRemoteSnapshotting)
}

func computeSnapshotPlan(input snapshotPlannerInput) *snapshotDetails {
	saveRemoteSnapshot := computeShouldSaveRemoteSnapshot(input)
	saveLocalSnapshot := computeShouldSaveLocalSnapshot(input)

	if input.recycled {
		return &snapshotDetails{
			snapshotType:        diffSnapshotType,
			memSnapshotName:     diffMemSnapshotName,
			vmStateSnapshotName: vmStateSnapshotName,
			saveRemoteSnapshot:  saveRemoteSnapshot,
			saveLocalSnapshot:   saveLocalSnapshot,
		}
	}
	return &snapshotDetails{
		snapshotType:        fullSnapshotType,
		memSnapshotName:     fullMemSnapshotName,
		vmStateSnapshotName: vmStateSnapshotName,
		saveRemoteSnapshot:  saveRemoteSnapshot,
		saveLocalSnapshot:   saveLocalSnapshot,
	}
}

func computeShouldSaveRemoteSnapshot(input snapshotPlannerInput) bool {
	if !input.supportsRemoteSnapshots || !input.recyclingEnabled {
		return false
	}

	remoteSavePolicy := platform.FindEffectiveValue(input.task, platform.SnapshotSavePolicyPropertyName)
	if remoteSavePolicy == platform.AlwaysSaveSnapshot || input.isLikelyDefaultSnapshot {
		return true
	}
	if remoteSavePolicy == platform.OnlySaveFirstNonDefaultSnapshot {
		return !input.existence.remoteForPrimary
	}
	return !input.existence.remoteForAny
}

func computeShouldSaveLocalSnapshot(input snapshotPlannerInput) bool {
	if !input.localSnapshotSharingEnabled || !input.recyclingEnabled {
		return false
	}
	if input.createFromSnapshot && !platform.IsCICommand(input.task.GetCommand(), platform.GetProto(input.task.GetAction(), input.task.GetCommand())) {
		return false
	}

	savePolicy := platform.FindEffectiveValue(input.task, platform.SnapshotSavePolicyPropertyName)
	if savePolicy == platform.AlwaysSaveSnapshot || input.isLikelyDefaultSnapshot {
		return true
	}
	return !input.existence.localForPrimary
}

func computeBootArgs(vmConfig *fcpb.VMConfiguration, chunkedSnapshotSharingEnabled bool, initOnAllocAndFree bool) string {
	kernelArgs := []string{
		"ro",
		"console=ttyS0",
		"reboot=k",
		"panic=1",
		"pci=off",
		"nomodules=1",
		"random.trust_cpu=on",
		"i8042.noaux",
		"i8042.nomux",
		"i8042.nopnp",
		"i8042.dumbkbd",
		"tsc=reliable",
	}
	if !vmConfig.GetIpv6Enabled() {
		kernelArgs = append(kernelArgs, "ipv6.disable=1")
	}
	if networkingEnabled(vmConfig.NetworkMode) {
		kernelArgs = append(kernelArgs, machineIPBootArgs)
	}
	if initOnAllocAndFree {
		kernelArgs = append(kernelArgs, "init_on_alloc=1", "init_on_free=1")
	}

	var initArgs []string
	if vmConfig.DebugMode {
		initArgs = append(initArgs, "-debug_mode")
	}
	if vmConfig.EnableLogging {
		initArgs = append(initArgs, "-enable_logging")
	}
	if networkingEnabled(vmConfig.NetworkMode) {
		initArgs = append(initArgs, "-set_default_route")
	}
	if vmConfig.InitDockerd {
		initArgs = append(initArgs, "-init_dockerd")
	}
	if vmConfig.EnableDockerdTcp {
		initArgs = append(initArgs, "-enable_dockerd_tcp")
	}
	if chunkedSnapshotSharingEnabled {
		initArgs = append(initArgs, "-enable_rootfs")
	}
	if vmConfig.EnableVfs {
		initArgs = append(initArgs, "-enable_vfs")
	}
	return strings.Join(append(initArgs, kernelArgs...), " ")
}

func computeWorkspacePathsToExtract(task *repb.ExecutionTask) []string {
	if platform.IsTrue(platform.FindEffectiveValue(task, platform.PreserveWorkspacePropertyName)) {
		return []string{"/"}
	}

	paths := []string{
		".BUILDBUDDY_DO_NOT_RECYCLE",
		".BUILDBUDDY_INVALIDATE_SNAPSHOT",
	}
	paths = append(paths, task.GetCommand().GetOutputDirectories()...)
	paths = append(paths, task.GetCommand().GetOutputFiles()...)
	paths = append(paths, task.GetCommand().GetOutputPaths()...)
	return paths
}

func computeSnapshotReadPolicy(task *repb.ExecutionTask) (string, error) {
	policy := platform.FindEffectiveValue(task, platform.SnapshotReadPolicyPropertyName)
	switch policy {
	case "":
		return platform.ReadLocalSnapshotFirst, nil
	case platform.ReadLocalSnapshotOnly, platform.ReadLocalSnapshotFirst, platform.AlwaysReadNewestSnapshot:
		return policy, nil
	default:
		return "", status.InvalidArgumentErrorf("invalid snapshot read policy %s", policy)
	}
}

func computeShouldUpgradeGuestKernel(task *repb.ExecutionTask, guestKernelImagePath6_1 string) bool {
	return guestKernelImagePath6_1 != "" && slices.Contains(task.Experiments, "upgrade-fc-guest-kernel")
}

// isBalloonEnabled returns whether the memory balloon should be enabled.
// The balloon is intended to reduce memory snapshot size. If recycling is not
// enabled and we don't plan to generate a snapshot, don't enable it.
// Also disable the balloon for firecracker actions with local-only-snapshot-sharing
// (i.e. not workflows), as there seems to be negative performance implications.
func isBalloonEnabled(enableBalloon bool, recyclingEnabled bool, supportsRemoteSnapshots bool) bool {
	return enableBalloon && recyclingEnabled && supportsRemoteSnapshots
}

// networkMode derives the desired Firecracker NetworkMode from the provided
// network and init-dockerd platform properties.
func networkMode(network string, initDockerd bool) (fcpb.NetworkMode, error) {
	if network == "external" || network == "" {
		return fcpb.NetworkMode_NETWORK_MODE_EXTERNAL, nil
	}
	if network == "off" {
		if initDockerd {
			return fcpb.NetworkMode_NETWORK_MODE_LOCAL, nil
		}
		return fcpb.NetworkMode_NETWORK_MODE_OFF, nil
	}
	return fcpb.NetworkMode_NETWORK_MODE_UNSPECIFIED, status.InvalidArgumentErrorf("unsupported network option %q", network)
}

// networkingEnabled returns true if the VM has any networking capability.
func networkingEnabled(mode fcpb.NetworkMode) bool {
	// UNSPECIFIED defaults to EXTERNAL for backward compatibility.
	return mode != fcpb.NetworkMode_NETWORK_MODE_OFF
}

// externalNetworkingEnabled returns true if the VM can access external networks.
func externalNetworkingEnabled(mode fcpb.NetworkMode) bool {
	// UNSPECIFIED defaults to EXTERNAL for backward compatibility.
	return mode == fcpb.NetworkMode_NETWORK_MODE_UNSPECIFIED || mode == fcpb.NetworkMode_NETWORK_MODE_EXTERNAL
}

// parseFatalInitError parses the VM log tail for fatal init errors.
func parseFatalInitError(tail string) error {
	if !strings.Contains(tail, fatalInitLogPrefix) {
		return nil
	}
	// Logs contain "\r\n"; convert these to universal line endings.
	tail = strings.ReplaceAll(tail, "\r\n", "\n")
	lines := strings.SplitSeq(tail, "\n")
	for line := range lines {
		if m := fatalErrPattern.FindStringSubmatch(line); len(m) >= 1 {
			return status.UnavailableErrorf("Firecracker VM crashed: %s", m[1])
		}
	}
	return nil
}

// parseOOMError looks for oom-kill entries in the kernel logs and returns an
// error if found.
func parseOOMError(logTail string) error {
	if !strings.Contains(logTail, "oom-kill:") {
		return nil
	}
	lines := strings.Split(logTail, "\n")
	var oomLines strings.Builder
	for _, line := range lines {
		if strings.Contains(line, "oom-kill:") || strings.Contains(line, "Out of memory: Killed process") {
			oomLines.WriteString(line + "\n")
		}
	}
	return status.ResourceExhaustedErrorf("some processes ran out of memory, and were killed:\n%s", oomLines.String())
}

// parseSegFault looks for segfaults in the stderr output and returns an error if found.
func parseSegFault(logTail string, stderr []byte) error {
	if !strings.Contains(string(stderr), "SIGSEGV") {
		return nil
	}
	return status.UnavailableErrorf("process hit a segfault:\n%s", logTail)
}

// combineHostAndGuestStats merges host and guest usage stats, preferring guest
// stats for memory and disk usage.
func combineHostAndGuestStats(host, guest *repb.UsageStats) *repb.UsageStats {
	stats := host.CloneVT()
	// The guest exports some disk usage stats which we can't easily track on
	// the host without introspection into the ext4 metadata blocks - just
	// continue to get these from the guest for now.
	stats.PeakFileSystemUsage = guest.GetPeakFileSystemUsage()
	// Host memory usage stats might be confusing to the user, because the
	// firecracker process might hold some extra memory that isn't visible to
	// the guest. Use guest stats for memory usage too, for now.
	stats.MemoryBytes = guest.GetMemoryBytes()
	stats.PeakMemoryBytes = guest.GetPeakMemoryBytes()
	return stats
}

// alignToMultiple aligns the value n to a multiple of `multiple`.
// It will round up if necessary.
func alignToMultiple(n int64, multiple int64) int64 {
	remainder := n % multiple
	if remainder == 0 {
		return n
	}
	return n + multiple - remainder
}

// getDockerDaemonConfig builds the Docker daemon config JSON from the given
// mirror and insecure registry lists.
func getDockerDaemonConfig(dockerMirrors []string, dockerInsecureRegistries []string) ([]byte, error) {
	config := map[string][]string{}
	if len(dockerMirrors) > 0 {
		config["registry-mirrors"] = dockerMirrors
	}
	if len(dockerInsecureRegistries) > 0 {
		config["insecure-registries"] = dockerInsecureRegistries
	}
	configJSON, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, err
	}
	return configJSON, nil
}

// DNSOverride mirrors networking.DNSOverride to avoid pulling in the networking
// package (which has heavy system dependencies) into the planner.
type DNSOverride struct {
	HostnameToOverride string `yaml:"hostname_to_override" json:"hostname_to_override"`
	RedirectToHostname string `yaml:"redirect_to_hostname" json:"redirect_to_hostname"`
}

// parseDNSOverrides validates the DNS overrides and marshals them to a JSON string.
func parseDNSOverrides(overrides []DNSOverride) (string, error) {
	if len(overrides) == 0 {
		return "", nil
	}
	for _, o := range overrides {
		if o.HostnameToOverride == "" {
			return "", status.InvalidArgumentErrorf("invalid empty dns override %+v", o)
		}
		// Ensure hostnames end with '.' so they are not resolved as relative names.
		if !strings.HasSuffix(o.HostnameToOverride, ".") {
			return "", status.InvalidArgumentErrorf("hostname_to_override %s should end with a '.'", o.HostnameToOverride)
		}
	}
	marshalledOverrides, err := json.Marshal(overrides)
	if err != nil {
		return "", status.WrapError(err, "marshall dns overrides")
	}
	return string(marshalledOverrides), nil
}

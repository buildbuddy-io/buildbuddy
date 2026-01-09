// Package platform provides the basic data model for platform properties that
// are accepted by BuildBuddy. It mostly provides a Go representation of the
// Platform.Properties proto (from remote_execution.proto), as well as the
// related parsing and validation logic.
//
// For executor-specific platform handling, see the executorplatform package.
package platform

import (
	"context"
	"encoding/base64"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	units "github.com/docker/go-units"
)

const (
	// BuildBuddy ubuntu images. When adding images here, also update
	// the image aliases in enterprise/server/workflow/service/service.go
	Ubuntu16_04Image = "gcr.io/flame-public/executor-docker-default:enterprise-v1.6.0"
	Ubuntu20_04Image = "gcr.io/flame-public/rbe-ubuntu20-04@sha256:09261f2019e9baa7482f7742cdee8e9972a3971b08af27363a61816b2968f622"
	Ubuntu22_04Image = "gcr.io/flame-public/rbe-ubuntu22-04@sha256:0d84a80bb0fc36ba5381942adcf6493249594dcc9044845c617b78c9b621cae3"
	Ubuntu24_04Image = "gcr.io/flame-public/rbe-ubuntu24-04@sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a"

	Ubuntu18_04WorkflowsImage = "gcr.io/flame-public/buildbuddy-ci-runner@sha256:8cf614fc4695789bea8321446402e7d6f84f6be09b8d39ec93caa508fa3e3cfc"
	Ubuntu20_04WorkflowsImage = "gcr.io/flame-public/rbe-ubuntu20-04-workflows@sha256:ba28945426fcdf4310f18e8a8e3c47af670bdcf9ba76bd76b269898c0579089e"
	// Images from 22.04+ do not have separate images for workflows.
	Ubuntu22_04WorkflowsImage = Ubuntu22_04Image
	Ubuntu24_04WorkflowsImage = Ubuntu24_04Image

	// OverrideHeaderPrefix is a prefix used to override platform props via
	// remote headers. The property name immediately follows the prefix in the
	// header key, and the header value is used as the property value.
	//
	// Example header:
	//     x-buildbuddy-platform.container-registry-username: _json_key
	OverrideHeaderPrefix = "x-buildbuddy-platform."

	poolPropertyName = "Pool"

	originalPoolPropertyName = "OriginalPool"

	// DefaultPoolValue is the value for the "Pool" platform property that selects
	// the default executor pool for remote execution.
	DefaultPoolValue = "default"

	containerImagePropertyName = "container-image"
	DockerPrefix               = "docker://"

	containerRegistryUsernamePropertyName = "container-registry-username"
	containerRegistryPasswordPropertyName = "container-registry-password"
	containerRegistryBypassPropertyName   = "container-registry-bypass"
	useOCIFetcherPropertyName             = "use-oci-fetcher"

	// container-image prop value which behaves the same way as if the prop were
	// empty or unset.
	unsetContainerImageVal = "none"

	recycleRunnerPropertyName = "recycle-runner"
	// dockerReuse is treated as an alias for recycle-runner.
	dockerReusePropertyName = "dockerReuse"

	RunnerRecyclingKey                      = "runner-recycling-key"
	RunnerRecyclingMaxWaitPropertyName      = "runner-recycling-max-wait"
	runnerCrashedExitCodesPropertyName      = "runner-crashed-exit-codes"
	transientErrorExitCodes                 = "transient-error-exit-codes"
	SnapshotSavePolicyPropertyName          = "remote-snapshot-save-policy"
	SnapshotReadPolicyPropertyName          = "snapshot-read-policy"
	MaxStaleFallbackSnapshotAgePropertyName = "max-stale-fallback-snapshot-age"
	PreserveWorkspacePropertyName           = "preserve-workspace"
	overlayfsWorkspacePropertyName          = "overlayfs-workspace"
	cleanWorkspaceInputsPropertyName        = "clean-workspace-inputs"
	persistentWorkerPropertyName            = "persistent-workers"
	PersistentWorkerKeyPropertyName         = "persistentWorkerKey"
	persistentWorkerProtocolPropertyName    = "persistentWorkerProtocol"
	WorkflowIDPropertyName                  = "workflow-id"
	WorkloadIsolationPropertyName           = "workload-isolation-type"
	initDockerdPropertyName                 = "init-dockerd"
	enableDockerdTCPPropertyName            = "enable-dockerd-tcp"
	enableVFSPropertyName                   = "enable-vfs"
	HostedBazelAffinityKeyPropertyName      = "hosted-bazel-affinity-key"
	useSelfHostedExecutorsPropertyName      = "use-self-hosted-executors"
	disableMeasuredTaskSizePropertyName     = "debug-disable-measured-task-size"
	disablePredictedTaskSizePropertyName    = "debug-disable-predicted-task-size"
	extraArgsPropertyName                   = "extra-args"
	EnvOverridesPropertyName                = "env-overrides"
	EnvOverridesBase64PropertyName          = "env-overrides-base64"
	IncludeSecretsPropertyName              = "include-secrets"
	DefaultTimeoutPropertyName              = "default-timeout"
	TerminationGracePeriodPropertyName      = "termination-grace-period"
	SnapshotKeyOverridePropertyName         = "snapshot-key-override"
	RetryPropertyName                       = "retry"
	PersistentVolumesPropertyName           = "persistent-volumes"

	OperatingSystemPropertyName = "OSFamily"
	LinuxOperatingSystemName    = "linux"
	defaultOperatingSystemName  = LinuxOperatingSystemName
	DarwinOperatingSystemName   = "darwin"
	WindowsOperatingSystemName  = "windows"

	CPUArchitecturePropertyName = "Arch"
	AMD64ArchitectureName       = "amd64"
	ARM64ArchitectureName       = "arm64"
	defaultCPUArchitecture      = AMD64ArchitectureName

	DockerInitPropertyName = "dockerInit"
	DockerUserPropertyName = "dockerUser"
	// Using the property defined here: https://github.com/bazelbuild/bazel-toolchains/blob/v5.1.0/rules/exec_properties/exec_properties.bzl#L164
	dockerRunAsRootPropertyName = "dockerRunAsRoot"
	// Using the property defined here: https://github.com/bazelbuild/bazel-toolchains/blob/v5.1.0/rules/exec_properties/exec_properties.bzl#L156
	dockerNetworkPropertyName = "dockerNetwork"

	// A BuildBuddy Compute Unit is defined as 1 cpu and 2.5GB of memory.
	EstimatedComputeUnitsPropertyName = "EstimatedComputeUnits"

	// EstimatedFreeDiskPropertyName specifies how much "scratch space" beyond the
	// input files a task requires to be able to function. This is useful for
	// managed Bazel + Firecracker because for Firecracker we need to decide ahead
	// of time how big the workspace filesystem is going to be, and managed Bazel
	// requires a relatively large amount of free space compared to typical actions.
	EstimatedFreeDiskPropertyName = "EstimatedFreeDiskBytes"

	EstimatedCPUPropertyName    = "EstimatedCPU"
	EstimatedMemoryPropertyName = "EstimatedMemory"

	// Property name prefix indicating a custom resource assignment.
	customResourcePrefix = "resources:"

// If you add a container type, also add it to KnownContainerTypes
)

// KnownContainerTypes are all the types that are currently supported, or were
// previously supported.
var KnownContainerTypes []ContainerType = []ContainerType{BareContainerType, PodmanContainerType, DockerContainerType, FirecrackerContainerType, OCIContainerType, SandboxContainerType}

// CoerceContainerType returns t if it's empty or in KnownContainerTypes.
// Otherwise it returns "Unknown".
func CoerceContainerType(t string) string {
	if t == "" {
		return ""
	}
	if slices.Contains(KnownContainerTypes, ContainerType(t)) {
		return t
	}
	return "unknown"
}

// PoolType represents the user's requested executor pool type for an executed
// action.
type PoolType int

const (
	PoolTypeDefault    PoolType = 1 // Respect org preference.
	PoolTypeShared     PoolType = 2 // Use shared executors.
	PoolTypeSelfHosted PoolType = 3 // Use self-hosted executors.
)

// Values for platform.SnapshotSavePolicyPropertyName
const (
	// Every run will save a snapshot.
	AlwaysSaveSnapshot = "always"
	// Only the first run on a non-default ref will save a snapshot.
	// All runs on default refs will save a snapshot.
	OnlySaveFirstNonDefaultSnapshot = "first-non-default-ref"
	// Default. Will only save a snapshot on a non-default ref if there are no
	// snapshots available. If there is a fallback default snapshot, still will not save
	// a snapshot.
	// All runs on default refs will save a snapshot.
	OnlySaveNonDefaultSnapshotIfNoneAvailable = "none-available"
)

// Values for platform.SnapshotReadPolicyPropertyName:
const (
	// Every run will use the newest snapshot available.
	// For snapshots that are only cached locally (i.e. depending on the remote
	// snapshot save policy), a local manifest will be used.
	// For snapshots that are cached remotely, a remote manifest will be used,
	// to guarantee that newer snapshots from other executors are considered.
	AlwaysReadNewestSnapshot = "newest"
	// If a local manifest exists, the local snapshot should be used, even if
	// there is a newer manifest for the snapshot key available in the
	// remote cache.
	ReadLocalSnapshotFirst = "local-first"
	// Only local snapshots should be used.
	ReadLocalSnapshotOnly = "local-only"
)

// Properties represents the platform properties parsed from a command.
type Properties struct {
	OS                        string
	Arch                      string
	Pool                      string
	PoolType                  PoolType
	EstimatedComputeUnits     float64
	EstimatedMilliCPU         int64
	EstimatedMemoryBytes      int64
	EstimatedFreeDiskBytes    int64
	CustomResources           []*scpb.CustomResource
	ContainerImage            string
	ContainerRegistryUsername string
	ContainerRegistryPassword string
	WorkloadIsolationType     string
	DockerForceRoot           bool
	DockerInit                bool
	DockerUser                string
	DockerNetwork             string
	RecycleRunner             bool
	RunnerRecyclingMaxWait    time.Duration

	// Exit codes indicating a runner crashed and should not be recycled since
	// it may be corrupted in some way.
	RunnerCrashedExitCodes []int

	// Exit codes that should be translated to an Unavailable gRPC error so that
	// they can be retried automatically by the client.
	TransientErrorExitCodes []int

	EnableVFS      bool
	IncludeSecrets bool

	// OriginalPool can be set to inform BuildBuddy about the original pool name
	// from another remote execution platform. This allows configuring task
	// sizing/routing based on the original pool name without needing to create
	// dedicated executor pools with each original pool name.
	OriginalPool string

	// DefaultTimeout specifies a remote action timeout to be used if
	// `action.Timeout` is unset. This works around an issue that bazel does not
	// have a way to set timeouts on regular build actions.
	DefaultTimeout time.Duration

	// TerminationGracePeriod is the time to wait between terminating a remote
	// action due to timeout and forcefully shutting it down.
	// This is the remote analog of Bazel's `--local_termination_grace_seconds`.
	TerminationGracePeriod time.Duration

	// InitDockerd specifies whether to initialize dockerd within the execution
	// environment if it is available in the execution image, allowing Docker
	// containers to be spawned by actions. Only available with
	// `workload-isolation-type=firecracker`.
	InitDockerd bool

	// EnableDockerdTCP specifies whether the dockerd initialized by InitDockerd
	// is started with support for connections over TCP.
	EnableDockerdTCP bool

	// PreserveWorkspace specifies whether to delete all files in the workspace
	// before running each action. If true, all files are kept except for output
	// files and directories.
	PreserveWorkspace bool

	// OverlayfsWorkspace specifies whether the action should use an
	// overlayfs-based copy-on-write filesystem for workspace inputs.
	OverlayfsWorkspace bool

	CleanWorkspaceInputs     string
	PersistentWorker         bool
	PersistentWorkerKey      string
	PersistentWorkerProtocol string
	WorkflowID               string
	HostedBazelAffinityKey   string
	RemoteSnapshotSavePolicy string
	SnapshotReadPolicy       string

	// DisableMeasuredTaskSize disables measurement-based task sizing, even if
	// it is enabled via flag, and instead uses the default / platform based
	// sizing. Intended for debugging purposes only and should not generally
	// be used.
	// TODO(bduffany): remove this once measured task sizing is battle-tested
	// and this is no longer needed for debugging
	DisableMeasuredTaskSize bool

	// DisablePredictedTaskSize disables model-based task sizing, even if it
	// is enabled via flag, and instead uses the default / platform based
	// sizing.
	DisablePredictedTaskSize bool

	// ExtraArgs contains arguments to append to the action.
	ExtraArgs []string

	// EnvOverrides contains environment variables in the form NAME=VALUE to be
	// applied as overrides to the action.
	EnvOverrides []string

	// OverrideSnapshotKey specifies a snapshot key that the action should start
	// from.
	// Only applies to recyclable firecracker actions.
	OverrideSnapshotKey *fcpb.SnapshotKey

	// Retry determines whether the scheduler should automatically retry
	// transient errors.
	// Should be set to false for non-idempotent commands, and clients should
	// handle more fine-grained retry behavior.
	// This property is ignored for bazel executions, because the bazel client
	// handles retries itself.
	Retry bool

	// Persistent volumes shared across all actions within a group. Requires
	// `executor.enable_persistent_volumes` to be enabled.
	PersistentVolumes []PersistentVolume

	// ContainerRegistryBypass skips pulling images from the container registry
	// and pulls images only from the cache instead. Note that this does not
	// skip cache authentication.
	//
	// This property can only be used by server admins, otherwise the execution
	// request will be rejected.
	ContainerRegistryBypass bool

	// UseOCIFetcher enables using the OCI fetcher service for pulling container
	// images instead of pulling directly from the registry.
	UseOCIFetcher bool
}

type PersistentVolume struct {
	name          string
	containerPath string
}

// Name is the name of the persistent volume. The returned value is non-empty
// and contains only alphanumeric characters, hyphens, and underscores.
func (v *PersistentVolume) Name() string {
	return v.name
}

// ContainerPath is the path in the container where the persistent volume is
// mounted. The returned value is guaranteed to be non-empty.
func (v *PersistentVolume) ContainerPath() string {
	return v.containerPath
}

// ContainerType indicates the type of containerization required by an executor.
type ContainerType string

const (
	BareContainerType        ContainerType = "none"
	PodmanContainerType      ContainerType = "podman"
	DockerContainerType      ContainerType = "docker"
	FirecrackerContainerType ContainerType = "firecracker"
	OCIContainerType         ContainerType = "oci"
	SandboxContainerType     ContainerType = "sandbox"
)

// ParseProperties parses the client provided properties into a struct.
//
// Within the executor, the returned properties should only be used *after*
// calling executorplatform.ApplyOverrides.
func ParseProperties(task *repb.ExecutionTask) (*Properties, error) {
	m := map[string]string{}
	for _, prop := range GetProto(task.GetAction(), task.GetCommand()).GetProperties() {
		m[strings.ToLower(prop.GetName())] = strings.TrimSpace(prop.GetValue())
	}
	for _, prop := range task.GetPlatformOverrides().GetProperties() {
		m[strings.ToLower(prop.GetName())] = strings.TrimSpace(prop.GetValue())
	}

	pool := stringProp(m, poolPropertyName, "")
	// Treat the explicit default pool value as empty string
	if pool == DefaultPoolValue {
		pool = ""
	}
	// Runner recycling is enabled if any of the following are true:
	// - recycle-runner is true
	// - dockerReuse is true (supported for compatibility reasons)
	// - persistentWorkerKey is set (persistent workers are implemented using
	//   runner recycling)
	recycleRunner := boolProp(m, recycleRunnerPropertyName, false) ||
		boolProp(m, dockerReusePropertyName, false) ||
		stringProp(m, PersistentWorkerKeyPropertyName, "") != ""
	isolationType := stringProp(m, WorkloadIsolationPropertyName, "")

	vfsEnabled := boolProp(m, enableVFSPropertyName, false)
	// Runner recycling is not yet supported in combination with VFS workspaces.
	// Firecracker VFS performance is not good enough yet to be enabled.
	if ContainerType(isolationType) == FirecrackerContainerType {
		vfsEnabled = false
	}

	envOverrides := stringListProp(m, EnvOverridesPropertyName)
	for _, prop := range stringListProp(m, EnvOverridesBase64PropertyName) {
		b, err := base64.StdEncoding.DecodeString(prop)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("decode env override as base64: %s", err)
		}
		envOverrides = append(envOverrides, string(b))
	}

	timeout, err := durationProp(m, DefaultTimeoutPropertyName, 0*time.Second)
	if err != nil {
		return nil, err
	}
	terminationGracePeriod, err := durationProp(m, TerminationGracePeriodPropertyName, 0*time.Second)
	if err != nil {
		return nil, err
	}
	runnerRecyclingMaxWait, err := durationProp(m, RunnerRecyclingMaxWaitPropertyName, 0*time.Second)
	if err != nil {
		return nil, err
	}

	// Parse custom resources
	var customResources []*scpb.CustomResource
	for k, v := range m {
		if strings.HasPrefix(k, customResourcePrefix) {
			name := strings.TrimPrefix(k, customResourcePrefix)
			value, err := strconv.ParseFloat(v, 32)
			if err != nil {
				return nil, status.InvalidArgumentErrorf("parse execution property %q: value is not a valid float32", k)
			}
			customResources = append(customResources, &scpb.CustomResource{
				Name:  name,
				Value: float32(value),
			})
		}
	}

	poolType := PoolTypeDefault
	if val, ok := m[strings.ToLower(useSelfHostedExecutorsPropertyName)]; ok {
		if strings.EqualFold(val, "true") {
			poolType = PoolTypeSelfHosted
		} else {
			poolType = PoolTypeShared
		}
	}

	var overrideSnapshotKey *fcpb.SnapshotKey
	if v, ok := m[strings.ToLower(SnapshotKeyOverridePropertyName)]; ok && v != "" {
		key, err := parseSnapshotKeyJSON(v)
		if err != nil {
			return nil, err
		}
		overrideSnapshotKey = key
	}

	persistentVolumes, err := ParsePersistentVolumes(stringListProp(m, PersistentVolumesPropertyName)...)
	if err != nil {
		return nil, err
	}

	snapshotSavePolicy := stringProp(m, SnapshotSavePolicyPropertyName, "")
	switch snapshotSavePolicy {
	case AlwaysSaveSnapshot, OnlySaveFirstNonDefaultSnapshot, OnlySaveNonDefaultSnapshotIfNoneAvailable, "":
	default:
		return nil, status.InvalidArgumentErrorf("%s is not a valid value for the `remote-snapshot-save-policy` platform property", snapshotSavePolicy)
	}

	snapshotReadPolicy := stringProp(m, SnapshotReadPolicyPropertyName, "")
	switch snapshotReadPolicy {
	case AlwaysReadNewestSnapshot, ReadLocalSnapshotFirst, ReadLocalSnapshotOnly, "":
	default:
		return nil, status.InvalidArgumentErrorf("%s is not a valid value for the `snapshot-read-policy` platform property", snapshotReadPolicy)
	}

	return &Properties{
		OS:                        strings.ToLower(stringProp(m, OperatingSystemPropertyName, defaultOperatingSystemName)),
		Arch:                      strings.ToLower(stringProp(m, CPUArchitecturePropertyName, defaultCPUArchitecture)),
		Pool:                      strings.ToLower(pool),
		PoolType:                  poolType,
		OriginalPool:              stringProp(m, originalPoolPropertyName, ""),
		EstimatedComputeUnits:     float64Prop(m, EstimatedComputeUnitsPropertyName, 0),
		EstimatedMemoryBytes:      iecBytesProp(m, EstimatedMemoryPropertyName, 0),
		EstimatedMilliCPU:         milliCPUProp(m, EstimatedCPUPropertyName, 0),
		EstimatedFreeDiskBytes:    iecBytesProp(m, EstimatedFreeDiskPropertyName, 0),
		CustomResources:           customResources,
		ContainerImage:            stringProp(m, containerImagePropertyName, ""),
		ContainerRegistryUsername: stringProp(m, containerRegistryUsernamePropertyName, ""),
		ContainerRegistryPassword: stringProp(m, containerRegistryPasswordPropertyName, ""),
		WorkloadIsolationType:     isolationType,
		InitDockerd:               boolProp(m, initDockerdPropertyName, false),
		EnableDockerdTCP:          boolProp(m, enableDockerdTCPPropertyName, false),
		DockerForceRoot:           boolProp(m, dockerRunAsRootPropertyName, false),
		DockerInit:                boolProp(m, DockerInitPropertyName, false),
		DockerUser:                stringProp(m, DockerUserPropertyName, ""),
		DockerNetwork:             stringProp(m, dockerNetworkPropertyName, ""),
		RecycleRunner:             recycleRunner,
		DefaultTimeout:            timeout,
		TerminationGracePeriod:    terminationGracePeriod,
		RunnerRecyclingMaxWait:    runnerRecyclingMaxWait,
		EnableVFS:                 vfsEnabled,
		IncludeSecrets:            boolProp(m, IncludeSecretsPropertyName, false),
		PreserveWorkspace:         boolProp(m, PreserveWorkspacePropertyName, false),
		OverlayfsWorkspace:        boolProp(m, overlayfsWorkspacePropertyName, false),
		CleanWorkspaceInputs:      stringProp(m, cleanWorkspaceInputsPropertyName, ""),
		PersistentWorker:          boolProp(m, persistentWorkerPropertyName, false),
		PersistentWorkerKey:       stringProp(m, PersistentWorkerKeyPropertyName, ""),
		PersistentWorkerProtocol:  stringProp(m, persistentWorkerProtocolPropertyName, ""),
		WorkflowID:                stringProp(m, WorkflowIDPropertyName, ""),
		HostedBazelAffinityKey:    stringProp(m, HostedBazelAffinityKeyPropertyName, ""),
		DisableMeasuredTaskSize:   boolProp(m, disableMeasuredTaskSizePropertyName, false),
		DisablePredictedTaskSize:  boolProp(m, disablePredictedTaskSizePropertyName, false),
		ExtraArgs:                 stringListProp(m, extraArgsPropertyName),
		EnvOverrides:              envOverrides,
		OverrideSnapshotKey:       overrideSnapshotKey,
		Retry:                     boolProp(m, RetryPropertyName, true),
		PersistentVolumes:         persistentVolumes,
		SnapshotReadPolicy:        snapshotReadPolicy,
		RemoteSnapshotSavePolicy:  snapshotSavePolicy,
		ContainerRegistryBypass:   boolProp(m, containerRegistryBypassPropertyName, false),
		UseOCIFetcher:             boolProp(m, useOCIFetcherPropertyName, false),
		RunnerCrashedExitCodes:    intListProp(m, runnerCrashedExitCodesPropertyName),
		TransientErrorExitCodes:   intListProp(m, transientErrorExitCodes),
	}, nil
}

// RemoteHeaderOverrides returns the platform properties that should override
// the command's platform properties.
func RemoteHeaderOverrides(ctx context.Context) []*repb.Platform_Property {
	props := []*repb.Platform_Property{}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return props
	}
	mdMap := map[string][]string(md)
	for headerName, headerValues := range mdMap {
		if !strings.HasPrefix(headerName, OverrideHeaderPrefix) {
			continue
		}
		propName := strings.TrimPrefix(headerName, OverrideHeaderPrefix)
		for _, propValue := range headerValues {
			props = append(props, &repb.Platform_Property{
				Name:  propName,
				Value: propValue,
			})
		}
	}

	return props
}

// WithRemoteHeaderOverride sets a remote header for an Execute request that
// overrides the given platform property to the given value. This can be used to
// set platform properties for an execution independently of the cached Action.
func WithRemoteHeaderOverride(ctx context.Context, propertyName, propertyValue string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, OverrideHeaderPrefix+propertyName, propertyValue)
}

func stringProp(props map[string]string, name string, defaultValue string) string {
	val := props[strings.ToLower(name)]
	if val == "" {
		return defaultValue
	}
	return val
}

func boolProp(props map[string]string, name string, defaultValue bool) bool {
	val := props[strings.ToLower(name)]
	if val == "" {
		return defaultValue
	}
	return strings.EqualFold(val, "true")
}

func int64Prop(props map[string]string, name string, defaultValue int64) int64 {
	val := props[strings.ToLower(name)]
	if val == "" {
		return defaultValue
	}
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		log.Warningf("Could not parse platform property %q as int64: %s", name, err)
		return defaultValue
	}
	return i
}

func float64Prop(props map[string]string, name string, defaultValue float64) float64 {
	val := props[strings.ToLower(name)]
	if val == "" {
		return defaultValue
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		log.Warningf("Could not parse platform property %q as float64: %s", name, err)
		return defaultValue
	}
	return f
}

func iecBytesProp(props map[string]string, name string, defaultValue int64) int64 {
	val := props[strings.ToLower(name)]
	if val == "" {
		return defaultValue
	}
	// If it looks like a float (e.g. 1e9), convert to an integer number of
	// bytes.
	f, err := strconv.ParseFloat(val, 64)
	if err == nil {
		return int64(f)
	}
	// Otherwise attempt to parse as an IEC number of bytes.
	// Example: 4GB = 4*(1024^3) bytes
	n, err := units.RAMInBytes(val)
	if err != nil {
		return defaultValue
	}
	return n
}

func milliCPUProp(props map[string]string, name string, defaultValue int64) int64 {
	val := props[strings.ToLower(name)]
	if val == "" {
		return defaultValue
	}
	// Handle "m" suffix - specifies milliCPU directly
	if val[len(val)-1] == 'm' {
		mcpu, err := strconv.ParseFloat(val[:len(val)-1], 64)
		if err != nil {
			log.Debugf("Failed to parse %s=%q", name, val)
			return defaultValue
		}
		return int64(mcpu)
	}
	// Otherwise parse as a float number of CPU cores.
	cpu, err := strconv.ParseFloat(val, 64)
	if err != nil {
		log.Debugf("Failed to parse %s=%q", name, val)
		return defaultValue
	}
	return int64(cpu * 1000)
}

func stringListProp(props map[string]string, name string) []string {
	vals := []string{}
	for _, item := range strings.Split(props[strings.ToLower(name)], ",") {
		item := strings.TrimSpace(item)
		if item != "" {
			vals = append(vals, item)
		}
	}
	return vals
}

func intListProp(props map[string]string, name string) []int {
	p := strings.TrimSpace(props[strings.ToLower(name)])
	if p == "" {
		return nil
	}
	vals := []int{}
	for _, item := range strings.Split(p, ",") {
		item := strings.TrimSpace(item)
		i, err := strconv.Atoi(item)
		if err != nil {
			return nil // TODO: make this fatal
		}
		vals = append(vals, i)
	}
	return vals
}

func durationProp(props map[string]string, name string, defaultValue time.Duration) (time.Duration, error) {
	val := props[strings.ToLower(name)]
	if val == "" {
		return defaultValue, nil
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return 0, status.InvalidArgumentErrorf("execution property value %q: invalid duration format", name)
	}
	return d, nil
}

var volumeNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func ParsePersistentVolumes(values ...string) ([]PersistentVolume, error) {
	volumes := make([]PersistentVolume, 0, len(values))
	for _, v := range values {
		name, containerPath, ok := strings.Cut(v, ":")
		if !ok || name == "" || containerPath == "" {
			return nil, status.InvalidArgumentErrorf(`invalid persistent volume %q: expected "<name>:<container_path>"`, v)
		}
		// Name can only contain alphanumeric characters, hyphens, and
		// underscores. In particular it cannot contain path separators or
		// relative directory references.
		if !volumeNameRegex.MatchString(name) {
			return nil, status.InvalidArgumentErrorf(`invalid persistent volume %q: name can only contain alphanumeric characters, hyphens, and underscores`, v)
		}
		volumes = append(volumes, PersistentVolume{
			name:          name,
			containerPath: containerPath,
		})
	}
	return volumes, nil
}

func parseSnapshotKeyJSON(in string) (*fcpb.SnapshotKey, error) {
	pk := &fcpb.SnapshotKey{}
	if err := protojson.Unmarshal([]byte(in), pk); err != nil {
		return nil, status.WrapError(err, "unmarshal SnapshotKey")
	}
	return pk, nil
}

func findValue(platform *repb.Platform, name string) (value string, ok bool) {
	name = strings.ToLower(name)
	for _, prop := range platform.GetProperties() {
		if strings.ToLower(prop.GetName()) == name {
			return strings.TrimSpace(prop.GetValue()), true
		}
	}
	return "", false
}

// GetProto returns the platform proto from the action if it's present.
// Otherwise it returns the platform from the command. This is the desired
// behaviour as of REAPI 2.2.
func GetProto(action *repb.Action, cmd *repb.Command) *repb.Platform {
	if plat := action.GetPlatform(); plat != nil {
		return plat
	}
	return cmd.GetPlatform()
}

// FindValue scans the platform properties for the given property name (ignoring
// case) and returns the value of that property if it exists, otherwise "".
func FindValue(platform *repb.Platform, name string) string {
	value, _ := findValue(platform, name)
	return value
}

// FindEffectiveValue returns the effective platform property value for the
// given task. If a remote header override was set via
// "x-buildbuddy-platform.<name>", then that value will be returned. Otherwise,
// it returns the original platform property from the action proto.
func FindEffectiveValue(task *repb.ExecutionTask, name string) string {
	override, ok := findValue(task.GetPlatformOverrides(), name)
	if ok {
		return override
	}
	return FindValue(GetProto(task.GetAction(), task.GetCommand()), name)
}

// IsTrue returns whether the given platform property value is truthy.
func IsTrue(value string) bool {
	return strings.EqualFold(value, "true")
}

// IsRecyclingEnabled returns whether runner recycling is enabled for the given
// task.
func IsRecyclingEnabled(task *repb.ExecutionTask) bool {
	parsed, err := ParseProperties(task)
	if err != nil {
		return false
	}
	return parsed.RecycleRunner
}

// IsCICommand returns whether the given command is either a BuildBuddy workflow
// or a GitHub Actions runner task. These commands are longer-running and may
// themselves invoke bazel.
func IsCICommand(cmd *repb.Command, platform *repb.Platform) bool {
	if len(cmd.GetArguments()) > 0 && cmd.GetArguments()[0] == "./buildbuddy_ci_runner" {
		return true
	}
	if FindValue(platform, "github-actions-runner-labels") != "" {
		return true
	}
	return false
}

func Retryable(task *repb.ExecutionTask) bool {
	v := FindEffectiveValue(task, RetryPropertyName)
	return v == "true" || v == ""
}

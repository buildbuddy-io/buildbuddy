package platform

import (
	"context"
	"encoding/base64"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	units "github.com/docker/go-units"
)

var (
	dockerSocket               = flag.String("executor.docker_socket", "", "If set, run execution commands in docker using the provided socket.")
	defaultXcodeVersion        = flag.String("executor.default_xcode_version", "", "Sets the default Xcode version number to use if an action doesn't specify one. If not set, /Applications/Xcode.app/ is used.")
	defaultIsolationType       = flag.String("executor.default_isolation_type", "", "The default workload isolation type when no type is specified in an action. If not set, we use the first of the following that is set: docker, podman, firecracker, or none (bare).")
	enableBareRunner           = flag.Bool("executor.enable_bare_runner", false, "Enables running execution commands directly on the host without isolation.")
	enablePodman               = flag.Bool("executor.enable_podman", false, "Enables running execution commands inside podman containers.")
	enableOCI                  = flag.Bool("executor.enable_oci", false, "Enables running execution commands using an OCI runtime directly.")
	enableSandbox              = flag.Bool("executor.enable_sandbox", false, "Enables running execution commands inside of sandbox-exec.")
	EnableFirecracker          = flag.Bool("executor.enable_firecracker", false, "Enables running execution commands inside of firecracker VMs")
	containerRegistryRegion    = flag.String("executor.container_registry_region", "", "All occurrences of '{{region}}' in container image names will be replaced with this string, if specified.")
	forcedNetworkIsolationType = flag.String("executor.forced_network_isolation_type", "", "If set, run all commands that require networking with this isolation")
	defaultImage               = flag.String("executor.default_image", Ubuntu16_04Image, "The default docker image to use to warm up executors or if no platform property is set. Ex: gcr.io/flame-public/executor-docker-default:enterprise-v1.5.4")
	enableVFS                  = flag.Bool("executor.enable_vfs", false, "Whether FUSE based filesystem is enabled.")
	extraEnvVars               = flag.Slice("executor.extra_env_vars", []string{}, "Additional environment variables to pass to remotely executed actions. i.e. MY_ENV_VAR=foo")
)

const (
	Ubuntu16_04Image = "gcr.io/flame-public/executor-docker-default:enterprise-v1.6.0"
	Ubuntu20_04Image = "gcr.io/flame-public/rbe-ubuntu20-04@sha256:09261f2019e9baa7482f7742cdee8e9972a3971b08af27363a61816b2968f622"

	Ubuntu18_04WorkflowsImage     = "gcr.io/flame-public/buildbuddy-ci-runner@sha256:8cf614fc4695789bea8321446402e7d6f84f6be09b8d39ec93caa508fa3e3cfc"
	Ubuntu20_04WorkflowsImage     = "gcr.io/flame-public/rbe-ubuntu20-04-workflows@sha256:c5092c8cb94471bb7c7fbd046cd80cb596dcc508f0833a748c035a1ba41f1681"
	Ubuntu22_04WorkflowsImage     = "gcr.io/flame-public/rbe-ubuntu22-04@sha256:c7567f84d702b596c592d844adb0570526e262e60e2e1fe50c27177ee6d652ad"
	Ubuntu20_04GitHubActionsImage = "gcr.io/flame-public/rbe-ubuntu20-04-github-actions@sha256:2a3b50fa1aafcb8446c94ab5707270f92fa91abd64a0e049312d4a086d0abb1c"

	// OverrideHeaderPrefix is a prefix used to override platform props via
	// remote headers. The property name immediately follows the prefix in the
	// header key, and the header value is used as the property value.
	//
	// Example header:
	//     x-buildbuddy-platform.container-registry-username: _json_key
	OverrideHeaderPrefix = "x-buildbuddy-platform."

	poolPropertyName = "Pool"
	// DefaultPoolValue is the value for the "Pool" platform property that selects
	// the default executor pool for remote execution.
	DefaultPoolValue = "default"

	registryRegionPlaceholder  = "{{region}}"
	containerImagePropertyName = "container-image"
	DockerPrefix               = "docker://"

	containerRegistryUsernamePropertyName = "container-registry-username"
	containerRegistryPasswordPropertyName = "container-registry-password"

	// container-image prop value which behaves the same way as if the prop were
	// empty or unset.
	unsetContainerImageVal = "none"

	RecycleRunnerPropertyName            = "recycle-runner"
	AffinityRoutingPropertyName          = "affinity-routing"
	RunnerRecyclingMaxWaitPropertyName   = "runner-recycling-max-wait"
	PreserveWorkspacePropertyName        = "preserve-workspace"
	nonrootWorkspacePropertyName         = "nonroot-workspace"
	overlayfsWorkspacePropertyName       = "overlayfs-workspace"
	cleanWorkspaceInputsPropertyName     = "clean-workspace-inputs"
	persistentWorkerPropertyName         = "persistent-workers"
	persistentWorkerKeyPropertyName      = "persistentWorkerKey"
	persistentWorkerProtocolPropertyName = "persistentWorkerProtocol"
	WorkflowIDPropertyName               = "workflow-id"
	workloadIsolationPropertyName        = "workload-isolation-type"
	initDockerdPropertyName              = "init-dockerd"
	enableDockerdTCPPropertyName         = "enable-dockerd-tcp"
	enableVFSPropertyName                = "enable-vfs"
	HostedBazelAffinityKeyPropertyName   = "hosted-bazel-affinity-key"
	useSelfHostedExecutorsPropertyName   = "use-self-hosted-executors"
	disableMeasuredTaskSizePropertyName  = "debug-disable-measured-task-size"
	disablePredictedTaskSizePropertyName = "debug-disable-predicted-task-size"
	extraArgsPropertyName                = "extra-args"
	EnvOverridesPropertyName             = "env-overrides"
	EnvOverridesBase64PropertyName       = "env-overrides-base64"
	IncludeSecretsPropertyName           = "include-secrets"
	DefaultTimeoutPropertyName           = "default-timeout"
	TerminationGracePeriodPropertyName   = "termination-grace-period"

	OperatingSystemPropertyName = "OSFamily"
	LinuxOperatingSystemName    = "linux"
	defaultOperatingSystemName  = LinuxOperatingSystemName
	DarwinOperatingSystemName   = "darwin"

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

	BareContainerType        ContainerType = "none"
	PodmanContainerType      ContainerType = "podman"
	DockerContainerType      ContainerType = "docker"
	FirecrackerContainerType ContainerType = "firecracker"
	OCIContainerType         ContainerType = "oci"
	SandboxContainerType     ContainerType = "sandbox"
	// If you add a container type, also add it to KnownContainerTypes

	// The app will mint a signed client identity token to workflows.
	workflowClientIdentityTokenLifetime = 12 * time.Hour
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

func VFSEnabled() bool {
	return *enableVFS
}

// Properties represents the platform properties parsed from a command.
type Properties struct {
	OS                        string
	Arch                      string
	Pool                      string
	PoolType                  interfaces.PoolType
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
	AffinityRouting           bool
	RunnerRecyclingMaxWait    time.Duration
	EnableVFS                 bool
	IncludeSecrets            bool

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

	// NonrootWorkspace specifies whether workspace directories should be made
	// writable by users other than the executor user (which is the root user for
	// production workloads). This is required to be set when running actions
	// within a container image that has a USER other than root.
	//
	// TODO(bduffany): Consider making this the default behavior, or inferring it
	// by inspecting the image and checking that the USER spec is anything other
	// than "root" or "0".
	NonrootWorkspace bool

	// OverlayfsWorkspace specifies whether the action should use an
	// overlayfs-based copy-on-write filesystem for workspace inputs.
	OverlayfsWorkspace bool

	CleanWorkspaceInputs     string
	PersistentWorker         bool
	PersistentWorkerKey      string
	PersistentWorkerProtocol string
	WorkflowID               string
	HostedBazelAffinityKey   string

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
}

// ContainerType indicates the type of containerization required by an executor.
type ContainerType string

// ExecutorProperties provides the bits of executor configuration that are
// needed to properly interpret the platform properties.
type ExecutorProperties struct {
	SupportedIsolationTypes []ContainerType
	DefaultXcodeVersion     string
}

func (p *ExecutorProperties) SupportsIsolation(c ContainerType) bool {
	for _, s := range p.SupportedIsolationTypes {
		if s == c {
			return true
		}
	}
	return false
}

// ParseProperties parses the client provided properties into a struct.
// Before use, the returned platform.Properties object *must* have
// executor-specific overrides applied via the ApplyOverrides function.
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

	// Only Enable VFS if it is also enabled via flags
	vfsEnabled := boolProp(m, enableVFSPropertyName, false) && *enableVFS

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

	poolType := interfaces.PoolTypeDefault
	if val, ok := m[strings.ToLower(useSelfHostedExecutorsPropertyName)]; ok {
		if strings.EqualFold(val, "true") {
			poolType = interfaces.PoolTypeSelfHosted
		} else {
			poolType = interfaces.PoolTypeShared
		}
	}

	return &Properties{
		OS:                        strings.ToLower(stringProp(m, OperatingSystemPropertyName, defaultOperatingSystemName)),
		Arch:                      strings.ToLower(stringProp(m, CPUArchitecturePropertyName, defaultCPUArchitecture)),
		Pool:                      strings.ToLower(pool),
		PoolType:                  poolType,
		EstimatedComputeUnits:     float64Prop(m, EstimatedComputeUnitsPropertyName, 0),
		EstimatedMemoryBytes:      iecBytesProp(m, EstimatedMemoryPropertyName, 0),
		EstimatedMilliCPU:         milliCPUProp(m, EstimatedCPUPropertyName, 0),
		EstimatedFreeDiskBytes:    iecBytesProp(m, EstimatedFreeDiskPropertyName, 0),
		CustomResources:           customResources,
		ContainerImage:            stringProp(m, containerImagePropertyName, ""),
		ContainerRegistryUsername: stringProp(m, containerRegistryUsernamePropertyName, ""),
		ContainerRegistryPassword: stringProp(m, containerRegistryPasswordPropertyName, ""),
		WorkloadIsolationType:     stringProp(m, workloadIsolationPropertyName, ""),
		InitDockerd:               boolProp(m, initDockerdPropertyName, false),
		EnableDockerdTCP:          boolProp(m, enableDockerdTCPPropertyName, false),
		DockerForceRoot:           boolProp(m, dockerRunAsRootPropertyName, false),
		DockerInit:                boolProp(m, DockerInitPropertyName, false),
		DockerUser:                stringProp(m, DockerUserPropertyName, ""),
		DockerNetwork:             stringProp(m, dockerNetworkPropertyName, ""),
		RecycleRunner:             boolProp(m, RecycleRunnerPropertyName, false),
		AffinityRouting:           boolProp(m, AffinityRoutingPropertyName, false),
		DefaultTimeout:            timeout,
		TerminationGracePeriod:    terminationGracePeriod,
		RunnerRecyclingMaxWait:    runnerRecyclingMaxWait,
		EnableVFS:                 vfsEnabled,
		IncludeSecrets:            boolProp(m, IncludeSecretsPropertyName, false),
		PreserveWorkspace:         boolProp(m, PreserveWorkspacePropertyName, false),
		OverlayfsWorkspace:        boolProp(m, overlayfsWorkspacePropertyName, false),
		NonrootWorkspace:          boolProp(m, nonrootWorkspacePropertyName, false),
		CleanWorkspaceInputs:      stringProp(m, cleanWorkspaceInputsPropertyName, ""),
		PersistentWorker:          boolProp(m, persistentWorkerPropertyName, false),
		PersistentWorkerKey:       stringProp(m, persistentWorkerKeyPropertyName, ""),
		PersistentWorkerProtocol:  stringProp(m, persistentWorkerProtocolPropertyName, ""),
		WorkflowID:                stringProp(m, WorkflowIDPropertyName, ""),
		HostedBazelAffinityKey:    stringProp(m, HostedBazelAffinityKeyPropertyName, ""),
		DisableMeasuredTaskSize:   boolProp(m, disableMeasuredTaskSizePropertyName, false),
		DisablePredictedTaskSize:  boolProp(m, disablePredictedTaskSizePropertyName, false),
		ExtraArgs:                 stringListProp(m, extraArgsPropertyName),
		EnvOverrides:              envOverrides,
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

// GetExecutorProperties returns a struct of properties that the configured
// executor supports. These are matched against the properties sent by the
// client.
func GetExecutorProperties() *ExecutorProperties {
	p := &ExecutorProperties{
		SupportedIsolationTypes: make([]ContainerType, 0),
		DefaultXcodeVersion:     *defaultXcodeVersion,
	}

	// NB: order matters! this list will be used in order to determine the which
	// isolation method to use if none was set.

	if *enableOCI {
		if runtime.GOOS == "darwin" {
			log.Warningf("OCI runtime was enabled, but is unsupported on darwin. Ignoring.")
		} else {
			p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, OCIContainerType)
		}
	}

	if *dockerSocket != "" {
		if runtime.GOOS == "darwin" {
			log.Warning("Docker was enabled, but is unsupported on darwin. Ignoring.")
		} else {
			p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, DockerContainerType)
		}
	}

	if *enablePodman {
		if runtime.GOOS == "darwin" {
			log.Warning("Podman was enabled, but is unsupported on darwin. Ignoring.")
		} else {
			p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, PodmanContainerType)
		}
	}

	if *EnableFirecracker {
		if runtime.GOOS != "linux" {
			log.Warningf("Firecracker was enabled, but is unsupported on %s/%s. Ignoring.", runtime.GOOS, runtime.GOARCH)
		} else {
			p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, FirecrackerContainerType)
		}
	}

	if *enableSandbox {
		if runtime.GOOS == "darwin" {
			p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, SandboxContainerType)
		} else {
			log.Warning("Sandbox was enabled, but is unsupported outside of darwin. Ignoring.")
		}
	}

	// Special case: for backwards compatibility, support bare-runners when docker
	// is not enabled. Typically, this happens for macs.
	if *enableBareRunner || len(p.SupportedIsolationTypes) == 0 {
		p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, BareContainerType)
	}

	return p
}

// ApplyOverrides modifies the platformProps and command as needed to match the
// locally configured executor properties.
func ApplyOverrides(env environment.Env, executorProps *ExecutorProperties, platformProps *Properties, command *repb.Command) error {
	if len(executorProps.SupportedIsolationTypes) == 0 {
		return status.FailedPreconditionError("No workload isolation types configured.")
	}

	// If forcedNetworkIsolationType is set, force isolation (usually to
	// firecracker) for this command.
	if *forcedNetworkIsolationType != "" && platformProps.DockerNetwork != "none" {
		platformProps.WorkloadIsolationType = *forcedNetworkIsolationType
	}

	if platformProps.WorkloadIsolationType == "" {
		if *defaultIsolationType == "" {
			// Backward-compatibility: if no default isolation type was specified; use the first configured one.
			platformProps.WorkloadIsolationType = string(executorProps.SupportedIsolationTypes[0])
		} else {
			platformProps.WorkloadIsolationType = string(*defaultIsolationType)
		}
	}

	// Check that the selected isolation type is supported by this executor.
	if !executorProps.SupportsIsolation(ContainerType(platformProps.WorkloadIsolationType)) {
		return status.InvalidArgumentErrorf("The requested workload isolation type %q is unsupported by this executor. Supported types: %s)", platformProps.WorkloadIsolationType, executorProps.SupportedIsolationTypes)
	}

	// Normalize the container image string
	if platformProps.WorkloadIsolationType == string(BareContainerType) {
		// BareRunner strings become ""
		if strings.EqualFold(platformProps.ContainerImage, "none") || platformProps.ContainerImage == "" {
			platformProps.ContainerImage = ""
		} else {
			// Return an error if a client specified a container
			// image but bare runner is configured.
			return status.InvalidArgumentError("Container images are not supported by this executor.")
		}
	} else {
		// OCI container references lose the "docker://" prefix. If no
		// container was set then we set our default.
		if strings.EqualFold(platformProps.ContainerImage, "none") || platformProps.ContainerImage == "" {
			platformProps.ContainerImage = containerImageName(*defaultImage)
		} else if !strings.HasPrefix(platformProps.ContainerImage, DockerPrefix) {
			// Return an error if a client specified an unparseable
			// container reference.
			return status.InvalidArgumentErrorf("malformed container image string - should be prefixed with '%s'", DockerPrefix)
		}
		// Trim the docker prefix from ContainerImage -- we no longer need it.
		platformProps.ContainerImage = containerImageName(platformProps.ContainerImage)
	}

	if strings.EqualFold(platformProps.OS, DarwinOperatingSystemName) {
		appleSDKVersion := ""
		appleSDKPlatform := "MacOSX"
		xcodeVersion := executorProps.DefaultXcodeVersion

		for _, v := range command.EnvironmentVariables {
			// Environment variables from: https://github.com/bazelbuild/bazel/blob/4ed65b05637cd37f0a6c5e79fdc4dfe0ece3fa68/src/main/java/com/google/devtools/build/lib/rules/apple/AppleConfiguration.java#L43
			switch v.Name {
			case "APPLE_SDK_VERSION_OVERRIDE":
				appleSDKVersion = v.Value
			case "APPLE_SDK_PLATFORM":
				appleSDKPlatform = v.Value
			case "XCODE_VERSION_OVERRIDE":
				xcodeVersion = v.Value
			}
		}

		sdk := fmt.Sprintf("%s%s", appleSDKPlatform, appleSDKVersion)
		developerDir, sdkRoot, err := env.GetXcodeLocator().PathsForVersionAndSDK(xcodeVersion, sdk)
		if err != nil {
			return err
		}

		command.EnvironmentVariables = append(command.EnvironmentVariables, []*repb.Command_EnvironmentVariable{
			{Name: "DEVELOPER_DIR", Value: developerDir},
			{Name: "SDKROOT", Value: sdkRoot},
		}...)
	}

	command.Arguments = append(command.Arguments, platformProps.ExtraArgs...)

	additionalEnvVars := append(*extraEnvVars, platformProps.EnvOverrides...)
	for _, e := range additionalEnvVars {
		parts := strings.Split(e, "=")
		if len(parts) == 0 {
			continue
		}
		name := parts[0]
		value := strings.Join(parts[1:], "=")
		command.EnvironmentVariables = append(command.EnvironmentVariables, &repb.Command_EnvironmentVariable{
			Name:  name,
			Value: value,
		})
	}

	// TODO: find a cleaner way to set the origin header, other than by
	// forwarding an env var to the runner.
	if platformProps.WorkflowID != "" {
		command.EnvironmentVariables = append(command.EnvironmentVariables, &repb.Command_EnvironmentVariable{
			Name:  "BB_GRPC_CLIENT_ORIGIN",
			Value: usageutil.ClientOrigin(),
		})

		if cis := env.GetClientIdentityService(); cis != nil {
			h, err := cis.IdentityHeader(&interfaces.ClientIdentity{
				Origin: interfaces.ClientIdentityInternalOrigin,
				Client: interfaces.ClientIdentityWorkflow,
			}, workflowClientIdentityTokenLifetime)
			if err != nil {
				return err
			}
			command.EnvironmentVariables = append(command.EnvironmentVariables, &repb.Command_EnvironmentVariable{
				Name:  "BB_GRPC_CLIENT_IDENTITY",
				Value: h,
			})
		}
	}

	return nil
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

func containerImageName(input string) string {
	withoutDockerPrefix := strings.TrimPrefix(input, DockerPrefix)
	if *containerRegistryRegion == "" {
		return withoutDockerPrefix
	}
	return strings.ReplaceAll(withoutDockerPrefix, registryRegionPlaceholder, *containerRegistryRegion)
}

func findValue(platform *repb.Platform, name string) (value string, ok bool) {
	name = strings.ToLower(name)
	for _, prop := range platform.GetProperties() {
		if prop.GetName() == name {
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

func DockerSocket() string {
	return *dockerSocket
}

func DefaultImage() string {
	return *defaultImage
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

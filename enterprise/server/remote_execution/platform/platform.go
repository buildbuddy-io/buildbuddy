package platform

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// overrideHeaderPrefix is a prefix used to override platform props via
	// remote headers. The property name immediately follows the prefix in the
	// header key, and the header value is used as the property value.
	//
	// Example header:
	//     x-buildbuddy-platform.container-registry-username: _json_key
	overrideHeaderPrefix = "x-buildbuddy-platform."

	poolPropertyName = "Pool"
	// DefaultPoolValue is the value for the "Pool" platform property that selects
	// the default executor pool for remote execution.
	DefaultPoolValue = "default"

	containerImagePropertyName = "container-image"
	DefaultContainerImage      = "gcr.io/flame-public/executor-docker-default:enterprise-v1.5.4"
	DockerPrefix               = "docker://"

	containerRegistryUsernamePropertyName = "container-registry-username"
	containerRegistryPasswordPropertyName = "container-registry-password"

	// container-image prop value which behaves the same way as if the prop were
	// empty or unset.
	unsetContainerImageVal = "none"

	RecycleRunnerPropertyName            = "recycle-runner"
	preserveWorkspacePropertyName        = "preserve-workspace"
	nonrootWorkspacePropertyName         = "nonroot-workspace"
	cleanWorkspaceInputsPropertyName     = "clean-workspace-inputs"
	persistentWorkerPropertyName         = "persistent-workers"
	persistentWorkerKeyPropertyName      = "persistentWorkerKey"
	persistentWorkerProtocolPropertyName = "persistentWorkerProtocol"
	WorkflowIDPropertyName               = "workflow-id"
	workloadIsolationPropertyName        = "workload-isolation-type"
	enableVFSPropertyName                = "enable-vfs"
	HostedBazelAffinityKeyPropertyName   = "hosted-bazel-affinity-key"
	useSelfHostedExecutorsPropertyName   = "use-self-hosted-executors"

	operatingSystemPropertyName = "OSFamily"
	LinuxOperatingSystemName    = "linux"
	defaultOperatingSystemName  = LinuxOperatingSystemName
	DarwinOperatingSystemName   = "darwin"

	cpuArchitecturePropertyName = "Arch"
	defaultCPUArchitecture      = "amd64"

	// Using the property defined here: https://github.com/bazelbuild/bazel-toolchains/blob/v5.1.0/rules/exec_properties/exec_properties.bzl#L164
	dockerRunAsRootPropertyName = "dockerRunAsRoot"

	// A BuildBuddy Compute Unit is defined as 1 cpu and 2.5GB of memory.
	EstimatedComputeUnitsPropertyName = "EstimatedComputeUnits"

	// EstimatedFreeDiskPropertyName specifies how much "scratch space" beyond the
	// input files a task requires to be able to function. This is useful for
	// managed Bazel + Firecracker because for Firecracker we need to decide ahead
	// of time how big the workspace filesystem is going to be, and managed Bazel
	// requires a relatively large amount of free space compared to typical actions.
	EstimatedFreeDiskPropertyName = "EstimatedFreeDiskBytes"

	BareContainerType        ContainerType = "none"
	DockerContainerType      ContainerType = "docker"
	FirecrackerContainerType ContainerType = "firecracker"
)

// Properties represents the platform properties parsed from a command.
type Properties struct {
	OS                        string
	Arch                      string
	Pool                      string
	EstimatedComputeUnits     int64
	EstimatedFreeDiskBytes    int64
	ContainerImage            string
	ContainerRegistryUsername string
	ContainerRegistryPassword string
	WorkloadIsolationType     string
	DockerForceRoot           bool
	RecycleRunner             bool
	EnableVFS                 bool
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
	NonrootWorkspace         bool
	CleanWorkspaceInputs     string
	PersistentWorker         bool
	PersistentWorkerKey      string
	PersistentWorkerProtocol string
	WorkflowID               string
	HostedBazelAffinityKey   string
	UseSelfHostedExecutors   bool
}

// ContainerType indicates the type of containerization required by an executor.
type ContainerType string

// ExecutorProperties provides the bits of executor configuration that are
// needed to properly interpret the platform properties.
type ExecutorProperties struct {
	SupportedIsolationTypes []ContainerType
	DefaultXcodeVersion     string
}

// ParseProperties parses the client provided properties into a struct.
// Before use, the returned platform.Properties object *must* have
// executor-specific overrides applied via the ApplyOverrides function.
func ParseProperties(task *repb.ExecutionTask) *Properties {
	m := map[string]string{}
	for _, prop := range task.GetCommand().GetPlatform().GetProperties() {
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

	return &Properties{
		OS:                        strings.ToLower(stringProp(m, operatingSystemPropertyName, defaultOperatingSystemName)),
		Arch:                      strings.ToLower(stringProp(m, cpuArchitecturePropertyName, defaultCPUArchitecture)),
		Pool:                      strings.ToLower(pool),
		EstimatedComputeUnits:     int64Prop(m, EstimatedComputeUnitsPropertyName, 0),
		EstimatedFreeDiskBytes:    int64Prop(m, EstimatedFreeDiskPropertyName, 0),
		ContainerImage:            stringProp(m, containerImagePropertyName, ""),
		ContainerRegistryUsername: stringProp(m, containerRegistryUsernamePropertyName, ""),
		ContainerRegistryPassword: stringProp(m, containerRegistryPasswordPropertyName, ""),
		WorkloadIsolationType:     stringProp(m, workloadIsolationPropertyName, ""),
		DockerForceRoot:           boolProp(m, dockerRunAsRootPropertyName, false),
		RecycleRunner:             boolProp(m, RecycleRunnerPropertyName, false),
		EnableVFS:                 boolProp(m, enableVFSPropertyName, false),
		PreserveWorkspace:         boolProp(m, preserveWorkspacePropertyName, false),
		NonrootWorkspace:          boolProp(m, nonrootWorkspacePropertyName, false),
		CleanWorkspaceInputs:      stringProp(m, cleanWorkspaceInputsPropertyName, ""),
		PersistentWorker:          boolProp(m, persistentWorkerPropertyName, false),
		PersistentWorkerKey:       stringProp(m, persistentWorkerKeyPropertyName, ""),
		PersistentWorkerProtocol:  stringProp(m, persistentWorkerProtocolPropertyName, ""),
		WorkflowID:                stringProp(m, WorkflowIDPropertyName, ""),
		HostedBazelAffinityKey:    stringProp(m, HostedBazelAffinityKeyPropertyName, ""),
		UseSelfHostedExecutors:    boolProp(m, useSelfHostedExecutorsPropertyName, false),
	}
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
		if !strings.HasPrefix(headerName, overrideHeaderPrefix) {
			continue
		}
		propName := strings.TrimPrefix(headerName, overrideHeaderPrefix)
		for _, propValue := range headerValues {
			props = append(props, &repb.Platform_Property{
				Name:  propName,
				Value: propValue,
			})
		}
	}

	return props
}

// GetExecutorProperties returns a struct of properties that the configured
// executor supports. These are matched against the properties sent by the
// client.
func GetExecutorProperties(executorConfig *config.ExecutorConfig) *ExecutorProperties {
	p := &ExecutorProperties{
		SupportedIsolationTypes: make([]ContainerType, 0),
		DefaultXcodeVersion:     executorConfig.DefaultXcodeVersion,
	}

	// NB: order matters! this list will be used in order to determine the which
	// isolation method to use if none was set.
	if executorConfig.DockerSocket != "" {
		if runtime.GOOS == "darwin" {
			log.Warning("Docker was enabled, but is unsupported on darwin. Ignoring.")
		} else {
			p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, DockerContainerType)
		}
	}
	if executorConfig.EnableFirecracker {
		if runtime.GOOS == "darwin" {
			log.Warning("Firecracker was enabled, but is unsupported on darwin. Ignoring.")
		} else {
			p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, FirecrackerContainerType)
		}
	}

	// Special case: for backwards compatibility, support bare-runners when docker
	// is not enabled. Typically, this happens for macs.
	if executorConfig.EnableBareRunner || len(p.SupportedIsolationTypes) == 0 {
		p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, BareContainerType)
	}

	return p
}

func contains(haystack []ContainerType, needle ContainerType) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// ApplyOverrides modifies the platformProps and command as needed to match the
// locally configured executor properties.
func ApplyOverrides(env environment.Env, executorProps *ExecutorProperties, platformProps *Properties, command *repb.Command) error {
	if len(executorProps.SupportedIsolationTypes) == 0 {
		return status.FailedPreconditionError("No workload isolation types configured.")
	}

	// If no isolation type was specified; default to the first configured one.
	if platformProps.WorkloadIsolationType == "" {
		platformProps.WorkloadIsolationType = string(executorProps.SupportedIsolationTypes[0])
	}

	// Check that the selected isolation type is supported by this executor.
	if !contains(executorProps.SupportedIsolationTypes, ContainerType(platformProps.WorkloadIsolationType)) {
		return status.InvalidArgumentErrorf("The requested workload isolation type %q is unsupported by this executor. Supported types: %s)", platformProps.WorkloadIsolationType, executorProps.SupportedIsolationTypes)
	}

	defaultContainerImage := DefaultContainerImage
	if env.GetConfigurator().GetExecutorConfig() != nil && env.GetConfigurator().GetExecutorConfig().DefaultImage != "" {
		defaultContainerImage = env.GetConfigurator().GetExecutorConfig().DefaultImage
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
			platformProps.ContainerImage = defaultContainerImage
		} else if !strings.HasPrefix(platformProps.ContainerImage, DockerPrefix) {
			// Return an error if a client specified an unparseable
			// container reference.
			return status.InvalidArgumentError("Malformed container image string.")
		}
		// Trim the docker prefix from ContainerImage -- we no longer need it.
		platformProps.ContainerImage = strings.TrimPrefix(platformProps.ContainerImage, DockerPrefix)
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

// FindValue scans the platform properties for the given property name (ignoring
// case) and returns the value of that property if it exists, otherwise "".
func FindValue(platform *repb.Platform, name string) string {
	name = strings.ToLower(name)
	for _, prop := range platform.GetProperties() {
		if prop.GetName() == name {
			return strings.TrimSpace(prop.GetValue())
		}
	}
	return ""
}

// IsTrue returns whether the given platform property value is truthy.
func IsTrue(value string) bool {
	return strings.EqualFold(value, "true")
}

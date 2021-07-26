package platform

import (
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// DefaultPoolValue is the value for the "Pool" platform property that selects
	// the default executor pool for remote execution.
	DefaultPoolValue = "default"

	containerImagePropertyName = "container-image"
	DefaultContainerImage      = "gcr.io/flame-public/executor-docker-default:enterprise-v1.5.4"
	dockerPrefix               = "docker://"
	// container-image prop value which behaves the same way as if the prop were
	// empty or unset.
	unsetContainerImageVal = "none"

	RecycleRunnerPropertyName       = "recycle-runner"
	preserveWorkspacePropertyName   = "preserve-workspace"
	persistentWorkerPropertyName    = "persistent-workers"
	persistentWorkerKeyPropertyName = "persistentWorkerKey"
	WorkflowIDPropertyName          = "workflow-id"
	workloadIsolationPropertyName   = "workload-isolation-type"

	enableXcodeOverridePropertyName = "enableXcodeOverride"

	operatingSystemPropertyName = "OSFamily"
	defaultOperatingSystemName  = "linux"
	darwinOperatingSystemName   = "darwin"
	// Using the property defined here: https://github.com/bazelbuild/bazel-toolchains/blob/v5.1.0/rules/exec_properties/exec_properties.bzl#L164
	dockerRunAsRootPropertyName = "dockerRunAsRoot"

	BareContainerType        ContainerType = "none"
	DockerContainerType      ContainerType = "docker"
	ContainerdContainerType  ContainerType = "containerd"
	FirecrackerContainerType ContainerType = "firecracker"
)

// Properties represents the platform properties parsed from a command.
type Properties struct {
	OS                    string
	ContainerImage        string
	WorkloadIsolationType string
	DockerForceRoot       bool
	EnableXcodeOverride   bool
	RecycleRunner         bool
	// PreserveWorkspace specifies whether to delete all files in the workspace
	// before running each action. If true, all files are kept except for output
	// files and directories.
	PreserveWorkspace   bool
	PersistentWorker    bool
	PersistentWorkerKey string
	WorkflowID          string
}

// ContainerType indicates the type of containerization required by an executor.
type ContainerType string

// ExecutorProperties provides the bits of executor configuration that are
// needed to properly interpret the platform properties.
type ExecutorProperties struct {
	SupportedIsolationTypes []ContainerType
	DefaultXCodeVersion     string
}

// Parse properties parses the client provided properties into a struct.
// Before use the returned platform.Properties object *must* have overrides
// applied via the ApplyOverrides function.
func ParseProperties(plat *repb.Platform) *Properties {
	m := map[string]string{}
	for _, prop := range plat.GetProperties() {
		m[strings.ToLower(prop.GetName())] = strings.TrimSpace(prop.GetValue())
	}
	return &Properties{
		OS:                    stringProp(m, operatingSystemPropertyName, defaultOperatingSystemName),
		ContainerImage:        stringProp(m, containerImagePropertyName, ""),
		WorkloadIsolationType: stringProp(m, workloadIsolationPropertyName, ""),
		DockerForceRoot:       boolProp(m, dockerRunAsRootPropertyName, false),
		EnableXcodeOverride:   boolProp(m, enableXcodeOverridePropertyName, false),
		RecycleRunner:         boolProp(m, RecycleRunnerPropertyName, false),
		PreserveWorkspace:     boolProp(m, preserveWorkspacePropertyName, false),
		PersistentWorker:      boolProp(m, persistentWorkerPropertyName, false),
		PersistentWorkerKey:   stringProp(m, persistentWorkerKeyPropertyName, ""),
		WorkflowID:            stringProp(m, WorkflowIDPropertyName, ""),
	}
}

// GetExecutorProperties returns a struct of properties that the configured
// executor supports. These are matched against the properties sent by the
// client.
func GetExecutorProperties(executorConfig *config.ExecutorConfig) *ExecutorProperties {
	p := &ExecutorProperties{
		SupportedIsolationTypes: make([]ContainerType, 0),
		DefaultXCodeVersion:     executorConfig.DefaultXCodeVersion,
	}

	// NB: order matters! this list will be used in order to determine the which
	// isolation method to use if none was set.
	if executorConfig.DockerSocket != "" {
		p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, DockerContainerType)
	}
	if executorConfig.EnableFirecracker {
		p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, FirecrackerContainerType)
	}
	if executorConfig.ContainerdSocket != "" {
		p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, ContainerdContainerType)
	}

	// Special case: for backwards compatibility, support bare-runners when neither docker nor
	// containerd are not enabled. Typically, this happens for macs.
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
func ApplyOverrides(executorProps *ExecutorProperties, platformProps *Properties, command *repb.Command) error {
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
			platformProps.ContainerImage = DefaultContainerImage
		} else if !strings.HasPrefix(platformProps.ContainerImage, dockerPrefix) {
			// Return an error if a client specified an unparseable
			// container reference.
			return status.InvalidArgumentError("Malformed container image string.")
		}
		// Trim the docker prefix from ContainerImage -- we no longer need it.
		platformProps.ContainerImage = strings.TrimPrefix(platformProps.ContainerImage, dockerPrefix)
	}

	if strings.EqualFold(platformProps.OS, darwinOperatingSystemName) {
		appleSDKVersion := ""
		appleSDKPlatform := "MacOSX"
		xcodeVersion := executorProps.DefaultXCodeVersion

		for _, v := range command.EnvironmentVariables {
			// Environment variables from: https://github.com/bazelbuild/bazel/blob/4ed65b05637cd37f0a6c5e79fdc4dfe0ece3fa68/src/main/java/com/google/devtools/build/lib/rules/apple/AppleConfiguration.java#L43
			switch v.Name {
			case "APPLE_SDK_VERSION_OVERRIDE":
				if platformProps.EnableXcodeOverride {
					appleSDKVersion = v.Value
				}
			case "APPLE_SDK_PLATFORM":
				appleSDKPlatform = v.Value
			case "XCODE_VERSION_OVERRIDE":
				if versionComponents := strings.Split(v.Value, "."); len(versionComponents) > 1 && platformProps.EnableXcodeOverride {
					// Just grab the first 2 components of the xcode version i.e. "12.4".
					xcodeVersion = versionComponents[0] + "." + versionComponents[1]
				}
			}
		}

		developerDir := "/Applications/Xcode.app/Contents/Developer"
		if executorProps.DefaultXCodeVersion != "" {
			developerDir = fmt.Sprintf("/Applications/Xcode_%s.app/Contents/Developer", xcodeVersion)
		}
		sdkRoot := fmt.Sprintf("%s/Platforms/%s.platform/Developer/SDKs/%s%s.sdk", developerDir, appleSDKPlatform, appleSDKPlatform, appleSDKVersion)

		command.EnvironmentVariables = append(command.EnvironmentVariables, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: sdkRoot},
			{Name: "DEVELOPER_DIR", Value: developerDir},
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

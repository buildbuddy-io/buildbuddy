package platform

import (
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
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

	recycleRunnerPropertyName       = "recycle-runner"
	preserveWorkspacePropertyName   = "preserve-workspace"
	persistentWorkerPropertyName    = "persistent-workers"
	persistentWorkerKeyPropertyName = "persistentWorkerKey"
	WorkflowIDPropertyName          = "workflow-id"

	enableXcodeOverridePropertyName = "enableXcodeOverride"

	operatingSystemPropertyName = "OSFamily"
	defaultOperatingSystemName  = "linux"
	darwinOperatingSystemName   = "darwin"
	// Using the property defined here: https://github.com/bazelbuild/bazel-toolchains/blob/v5.1.0/rules/exec_properties/exec_properties.bzl#L164
	dockerRunAsRootPropertyName = "dockerRunAsRoot"

	BareContainerType       ContainerType = "none"
	DockerContainerType     ContainerType = "docker"
	ContainerdContainerType ContainerType = "containerd"
)

// Properties represents the platform properties parsed from a command.
type Properties struct {
	OS                  string
	ContainerImage      string
	DockerForceRoot     bool
	EnableXcodeOverride bool
	RecycleRunner       bool
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
	ContainerType ContainerType
}

func ParseProperties(plat *repb.Platform, execProps *ExecutorProperties) (*Properties, error) {
	m := map[string]string{}
	for _, prop := range plat.GetProperties() {
		m[strings.ToLower(prop.GetName())] = strings.TrimSpace(prop.GetValue())
	}
	containerImage, err := parseContainerImage(m, execProps)
	if err != nil {
		return nil, err
	}
	return &Properties{
		OS:                  stringProp(m, operatingSystemPropertyName, defaultOperatingSystemName),
		ContainerImage:      containerImage,
		DockerForceRoot:     boolProp(m, dockerRunAsRootPropertyName, false),
		EnableXcodeOverride: boolProp(m, enableXcodeOverridePropertyName, false),
		RecycleRunner:       boolProp(m, recycleRunnerPropertyName, false),
		PreserveWorkspace:   boolProp(m, preserveWorkspacePropertyName, false),
		PersistentWorker:    boolProp(m, persistentWorkerPropertyName, false),
		PersistentWorkerKey: stringProp(m, persistentWorkerKeyPropertyName, ""),
		WorkflowID:          stringProp(m, WorkflowIDPropertyName, ""),
	}, nil
}

// ApplyOverrides modifies the command if needed to match the specified platform
// properties.
func ApplyOverrides(env environment.Env, props *Properties, command *repb.Command) {
	if strings.EqualFold(props.OS, darwinOperatingSystemName) {
		appleSDKVersion := ""
		appleSDKPlatform := "MacOSX"
		xcodeVersion := ""

		executorConfig := env.GetConfigurator().GetExecutorConfig()
		if executorConfig != nil {
			xcodeVersion = executorConfig.DefaultXCodeVersion
		}

		for _, v := range command.EnvironmentVariables {
			// Environment variables from: https://github.com/bazelbuild/bazel/blob/4ed65b05637cd37f0a6c5e79fdc4dfe0ece3fa68/src/main/java/com/google/devtools/build/lib/rules/apple/AppleConfiguration.java#L43
			switch v.Name {
			case "APPLE_SDK_VERSION_OVERRIDE":
				if props.EnableXcodeOverride {
					appleSDKVersion = v.Value
				}
			case "APPLE_SDK_PLATFORM":
				appleSDKPlatform = v.Value
			case "XCODE_VERSION_OVERRIDE":
				if versionComponents := strings.Split(v.Value, "."); len(versionComponents) > 1 && props.EnableXcodeOverride {
					// Just grab the first 2 components of the xcode version i.e. "12.4".
					xcodeVersion = versionComponents[0] + "." + versionComponents[1]
				}
			}
		}

		developerDir := "/Applications/Xcode.app/Contents/Developer"
		if executorConfig != nil && executorConfig.DefaultXCodeVersion != "" {
			developerDir = fmt.Sprintf("/Applications/Xcode_%s.app/Contents/Developer", xcodeVersion)
		}
		sdkRoot := fmt.Sprintf("%s/Platforms/%s.platform/Developer/SDKs/%s%s.sdk", developerDir, appleSDKPlatform, appleSDKPlatform, appleSDKVersion)

		command.EnvironmentVariables = append(command.EnvironmentVariables, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: sdkRoot},
			{Name: "DEVELOPER_DIR", Value: developerDir},
		}...)
	}
}

func parseContainerImage(props map[string]string, execProps *ExecutorProperties) (string, error) {
	key := containerImagePropertyName
	val := props[strings.ToLower(key)]
	if val == "" || strings.EqualFold(val, unsetContainerImageVal) {
		if execProps.ContainerType == BareContainerType {
			return "", nil
		} else {
			return DefaultContainerImage, nil
		}
	}
	if !strings.HasPrefix(val, dockerPrefix) {
		return "", status.InvalidArgumentErrorf("invalid %q platform property value %q: expected \"%s\" prefix", key, val, dockerPrefix)
	}
	if execProps.ContainerType == BareContainerType {
		return "", status.InvalidArgumentErrorf("container-based isolation is unsupported by this executor (platform property %s=%s)", key, val)
	}
	return strings.TrimPrefix(val, dockerPrefix), nil
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

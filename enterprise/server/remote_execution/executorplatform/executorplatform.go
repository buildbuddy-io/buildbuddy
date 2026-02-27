// Package executorplatform contains executor-specific logic relating to
// platform properties. The app should *not* depend on this package.
package executorplatform

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
	defaultImage               = flag.String("executor.default_image", platform.Ubuntu16_04Image, "The default docker image to use to warm up executors or if no platform property is set. Ex: gcr.io/flame-public/executor-docker-default:enterprise-v1.5.4")
	enableVFS                  = flag.Bool("executor.enable_vfs", false, "Whether FUSE based filesystem is enabled.")
	extraEnvVars               = flag.Slice("executor.extra_env_vars", []string{}, "Additional environment variables to pass to remotely executed actions: can be specified either as a `NAME=VALUE` assignment, or just `NAME` to inherit the environment variable from the executor process.")
)

const (
	// The app will mint a signed client identity token to workflows.
	workflowClientIdentityTokenLifetime = 12 * time.Hour

	// Placeholder string in container-image names that will be substituted with
	// the value of the [containerRegistryRegion] flag.
	registryRegionPlaceholder = "{{region}}"
)

func DockerSocket() string {
	return *dockerSocket
}

func DefaultImage() string {
	return *defaultImage
}

// ExecutorProperties provides the bits of executor configuration that are
// needed to properly interpret the platform properties.
type ExecutorProperties struct {
	SupportedIsolationTypes []platform.ContainerType
	DefaultXcodeVersion     string
}

func (p *ExecutorProperties) SupportsIsolation(c platform.ContainerType) bool {
	for _, s := range p.SupportedIsolationTypes {
		if s == c {
			return true
		}
	}
	return false
}

// GetExecutorProperties returns a struct of properties that the configured
// executor supports. These are matched against the properties sent by the
// client.
func GetExecutorProperties() *ExecutorProperties {
	p := &ExecutorProperties{
		SupportedIsolationTypes: make([]platform.ContainerType, 0),
		DefaultXcodeVersion:     *defaultXcodeVersion,
	}

	// NB: order matters! this list will be used in order to determine the which
	// isolation method to use if none was set.

	if *enableOCI {
		if runtime.GOOS == "darwin" {
			log.Warningf("OCI runtime was enabled, but is unsupported on darwin. Ignoring.")
		} else {
			p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, platform.OCIContainerType)
		}
	}

	if *dockerSocket != "" {
		if runtime.GOOS == "darwin" {
			log.Warning("Docker was enabled, but is unsupported on darwin. Ignoring.")
		} else {
			p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, platform.DockerContainerType)
		}
	}

	if *enablePodman {
		if runtime.GOOS == "darwin" {
			log.Warning("Podman was enabled, but is unsupported on darwin. Ignoring.")
		} else {
			p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, platform.PodmanContainerType)
		}
	}

	if *EnableFirecracker {
		if runtime.GOOS != "linux" {
			log.Warningf("Firecracker was enabled, but is unsupported on %s/%s. Ignoring.", runtime.GOOS, runtime.GOARCH)
		} else {
			p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, platform.FirecrackerContainerType)
		}
	}

	if *enableSandbox {
		if runtime.GOOS == "darwin" {
			p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, platform.SandboxContainerType)
		} else {
			log.Warning("Sandbox was enabled, but is unsupported outside of darwin. Ignoring.")
		}
	}

	// Special case: for backwards compatibility, support bare-runners when docker
	// is not enabled. Typically, this happens for macs.
	if *enableBareRunner || len(p.SupportedIsolationTypes) == 0 {
		p.SupportedIsolationTypes = append(p.SupportedIsolationTypes, platform.BareContainerType)
	}

	return p
}

func containerImageName(input string) string {
	withoutDockerPrefix := strings.TrimPrefix(input, platform.DockerPrefix)
	if *containerRegistryRegion == "" {
		return withoutDockerPrefix
	}
	return strings.ReplaceAll(withoutDockerPrefix, registryRegionPlaceholder, *containerRegistryRegion)
}

func ValidateIsolationTypes() error {
	if *defaultIsolationType == "" {
		return nil
	}
	if !slices.Contains(platform.KnownContainerTypes, platform.ContainerType(*defaultIsolationType)) {
		return status.InvalidArgumentErrorf("the configured 'default_isolation_type' %q is invalid. Valid values: %v", *defaultIsolationType, platform.KnownContainerTypes)
	}
	executorProps := GetExecutorProperties()
	if !executorProps.SupportsIsolation(platform.ContainerType(*defaultIsolationType)) {
		return status.InvalidArgumentErrorf("the configured 'default_isolation_type' %q is not enabled for this executor. Enabled isolation types: %v", *defaultIsolationType, executorProps.SupportedIsolationTypes)
	}
	return nil
}

// ApplyOverrides modifies the platformProps and command as needed to match the
// locally configured executor properties.
func ApplyOverrides(env environment.Env, executorProps *ExecutorProperties, platformProps *platform.Properties, command *repb.Command) error {
	if len(executorProps.SupportedIsolationTypes) == 0 {
		return status.FailedPreconditionError("No workload isolation types configured.")
	}

	// If VFS is not enabled then coerce the platform prop to false.
	if !*enableVFS {
		platformProps.EnableVFS = false
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
	if !executorProps.SupportsIsolation(platform.ContainerType(platformProps.WorkloadIsolationType)) {
		return status.InvalidArgumentErrorf("The requested workload isolation type %q is unsupported by this executor. Supported types: %s)", platformProps.WorkloadIsolationType, executorProps.SupportedIsolationTypes)
	}

	// Normalize the container image string
	if platformProps.WorkloadIsolationType == string(platform.BareContainerType) {
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
		} else if !strings.HasPrefix(platformProps.ContainerImage, platform.DockerPrefix) {
			// Return an error if a client specified an unparseable
			// container reference.
			return status.InvalidArgumentErrorf("malformed container image string - should be prefixed with '%s'", platform.DockerPrefix)
		}
		// Trim the docker prefix from ContainerImage -- we no longer need it.
		platformProps.ContainerImage = containerImageName(platformProps.ContainerImage)
	}

	if strings.EqualFold(platformProps.OS, platform.DarwinOperatingSystemName) {
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
		name, value, hasValue := strings.Cut(e, "=")
		if !hasValue {
			// No '=' found; inherit host environment variable value.
			value = os.Getenv(name)
		}
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

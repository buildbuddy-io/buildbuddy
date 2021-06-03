package platform_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	bare   = &platform.ExecutorProperties{ContainerType: platform.BareContainerType}
	docker = &platform.ExecutorProperties{ContainerType: platform.DockerContainerType}
)

func TestParse_ContainerImage_Success(t *testing.T) {
	for _, testCase := range []struct {
		execProps         *platform.ExecutorProperties
		imageProp         string
		containerImageKey string
		expected          string
	}{
		{bare, "", "container-image", ""},
		{bare, "", "Container-Image", ""},
		{bare, "none", "container-image", ""},
		{bare, "none", "Container-Image", ""},
		{bare, "None", "container-image", ""},
		{bare, "None", "Container-Image", ""},
		{docker, "", "container-image", platform.DefaultContainerImage},
		{docker, "", "Container-Image", platform.DefaultContainerImage},
		{docker, "none", "container-image", platform.DefaultContainerImage},
		{docker, "none", "Container-Image", platform.DefaultContainerImage},
		{docker, "None", "container-image", platform.DefaultContainerImage},
		{docker, "None", "Container-Image", platform.DefaultContainerImage},
		{docker, "docker://alpine", "container-image", "alpine"},
		{docker, "docker://alpine", "Container-Image", "alpine"},
		{docker, "docker://caseSensitiveUrl", "container-image", "caseSensitiveUrl"},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: testCase.containerImageKey, Value: testCase.imageProp},
		}}

		props, err := platform.ParseProperties(plat, testCase.execProps)

		require.NoError(t, err)
		assert.Equal(t, testCase.expected, props.ContainerImage)
	}
}

func TestParse_ContainerImage_Error(t *testing.T) {
	for _, testCase := range []struct {
		execProps *platform.ExecutorProperties
		imageProp string
	}{
		{bare, "docker://alpine"},
		{bare, "invalid"},
		{bare, "invalid://alpine"},
		{docker, "invalid"},
		{docker, "invalid://alpine"},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: "container-image", Value: testCase.imageProp},
		}}

		props, err := platform.ParseProperties(plat, testCase.execProps)

		assert.Nil(t, props)
		assert.Error(t, err)
	}
}

func TestParse_ApplyOverrides(t *testing.T) {
	for _, testCase := range []struct {
		platformProps       []*repb.Platform_Property
		startingEnvVars     []*repb.Command_EnvironmentVariable
		expectedEnvVars     []*repb.Command_EnvironmentVariable
		defaultXCodeVersion string
	}{
		// Default darwin platform
		{[]*repb.Platform_Property{
			{Name: "osfamily", Value: "darwin"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "11.1"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		},
			[]*repb.Command_EnvironmentVariable{
				{Name: "SDKROOT", Value: "/Applications/Xcode_12.2.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"},
				{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.2.app/Contents/Developer"},
			},
			"12.2",
		},
		// Case insensitive darwin platform
		{[]*repb.Platform_Property{
			{Name: "OSFAMILY", Value: "dArWiN"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "11.1"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.2.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.2.app/Contents/Developer"},
		},
			"12.2",
		},
		// Darwin with no overrides
		{[]*repb.Platform_Property{
			{Name: "OSFamily", Value: "Darwin"},
			{Name: "enablexcodeoverride", Value: "false"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "11.1"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.2.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.2.app/Contents/Developer"},
		},
			"12.2",
		},
		// Darwin with sdk platform
		{[]*repb.Platform_Property{
			{Name: "OSFamily", Value: "Darwin"},
			{Name: "enablexcodeoverride", Value: "false"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "11.1"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.2.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhone.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.2.app/Contents/Developer"},
		},
			"12.2",
		},
		// Darwin with xcode override
		{[]*repb.Platform_Property{
			{Name: "OSFamily", Value: "Darwin"},
			{Name: "enablexcodeoverride", Value: "true"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "11.1"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhone11.1.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.4.app/Contents/Developer"},
		},
			"12.2",
		},
		// Darwin with xcode override but no sdk version
		{[]*repb.Platform_Property{
			{Name: "OSFamily", Value: "Darwin"},
			{Name: "enablexcodeoverride", Value: "true"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhone.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.4.app/Contents/Developer"},
		},
			"12.2",
		},
		// Case insensitive darwin with xcode override
		{[]*repb.Platform_Property{
			{Name: "OSFAMILY", Value: "dArWiN"},
			{Name: "EnableXcodeOverride", Value: "true"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "11.1"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhone11.1.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.4.app/Contents/Developer"},
		},
			"12.2",
		},
		// Case insensitive darwin with no default xcode version
		{[]*repb.Platform_Property{
			{Name: "OSFAMILY", Value: "dArWiN"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "11.1"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhone.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode.app/Contents/Developer"},
		},
			"",
		},
		// Case insensitive darwin with no default xcode version and xcode override
		{[]*repb.Platform_Property{
			{Name: "OSFAMILY", Value: "dArWiN"},
			{Name: "EnableXcodeOverride", Value: "true"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "11.1"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhone11.1.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode.app/Contents/Developer"},
		},
			"",
		},
		// Default linux
		{[]*repb.Platform_Property{
			{Name: "osfamily", Value: "linux"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "11.1"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{},
			"12.2",
		},
		// Default case-insensitive linux
		{[]*repb.Platform_Property{
			{Name: "oSfAmIlY", Value: "LINUX"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "11.1"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{},
			"12.2",
		},
	} {
		plat := &repb.Platform{Properties: testCase.platformProps}

		props, err := platform.ParseProperties(plat, bare)

		env := testenv.GetTestEnv(t)
		env.GetConfigurator().GetExecutorConfig().DefaultXCodeVersion = testCase.defaultXCodeVersion

		command := &repb.Command{EnvironmentVariables: testCase.startingEnvVars}
		platform.ApplyOverrides(env, props, command)

		require.NoError(t, err)
		assert.ElementsMatch(t, command.EnvironmentVariables, append(testCase.startingEnvVars, testCase.expectedEnvVars...))
	}
}

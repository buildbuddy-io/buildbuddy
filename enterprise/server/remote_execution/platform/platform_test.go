package platform_test

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	bare   = &platform.ExecutorProperties{SupportedIsolationTypes: []platform.ContainerType{platform.BareContainerType}}
	docker = &platform.ExecutorProperties{SupportedIsolationTypes: []platform.ContainerType{platform.DockerContainerType}}
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

		platformProps := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		env := testenv.GetTestEnv(t)
		env.RealEnv.SetXCodeLocator(&xcodeLocator{})
		err := platform.ApplyOverrides(env, testCase.execProps, platformProps, &repb.Command{})
		require.NoError(t, err)
		assert.Equal(t, testCase.expected, platformProps.ContainerImage, testCase)
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

		platformProps := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		env := testenv.GetTestEnv(t)
		env.RealEnv.SetXCodeLocator(&xcodeLocator{})
		err := platform.ApplyOverrides(env, testCase.execProps, platformProps, &repb.Command{})
		assert.Error(t, err)
	}
}

func TestParse_OS(t *testing.T) {
	for _, testCase := range []struct {
		rawValue      string
		expectedValue string
	}{
		{"", "linux"},
		{"linux", "linux"},
		{"darwin", "darwin"},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: "OSFamily", Value: testCase.rawValue},
		}}
		platformProps := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		assert.Equal(t, testCase.expectedValue, platformProps.OS)
	}

	// Empty case
	plat := &repb.Platform{Properties: []*repb.Platform_Property{}}
	platformProps := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
	assert.Equal(t, "linux", platformProps.OS)
}

func TestParse_Arch(t *testing.T) {
	for _, testCase := range []struct {
		rawValue      string
		expectedValue string
	}{
		{"", "amd64"},
		{"amd64", "amd64"},
		{"arm64", "arm64"},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: "Arch", Value: testCase.rawValue},
		}}
		platformProps := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		assert.Equal(t, testCase.expectedValue, platformProps.Arch)
	}

	// Empty case
	plat := &repb.Platform{Properties: []*repb.Platform_Property{}}
	platformProps := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
	assert.Equal(t, "amd64", platformProps.Arch)
}

func TestParse_Pool(t *testing.T) {
	for _, testCase := range []struct {
		rawValue      string
		expectedValue string
	}{
		{"", ""},
		{"default", ""},
		{"my-pool", "my-pool"},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: "Pool", Value: testCase.rawValue},
		}}
		platformProps := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		assert.Equal(t, testCase.expectedValue, platformProps.Pool)
	}

	// Empty case
	plat := &repb.Platform{Properties: []*repb.Platform_Property{}}
	platformProps := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
	assert.Equal(t, "", platformProps.Pool)
}

func TestParse_EstimatedBCU(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		rawValue      string
		expectedValue int64
	}{
		{"EstimatedComputeUnits", "", 0},
		{"EstimatedComputeUnits", "NOT_AN_INT", 0},
		{"EstimatedComputeUnits", "0", 0},
		{"EstimatedComputeUnits", "1", 1},
		{"EstimatedComputeUnits", " 1 ", 1},
		{"estimatedcomputeunits", "1", 1},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: testCase.name, Value: testCase.rawValue},
		}}
		platformProps := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		assert.Equal(t, testCase.expectedValue, platformProps.EstimatedComputeUnits)
	}
}

func TestParse_EstimatedFreeDisk(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		rawValue      string
		expectedValue int64
	}{
		{"EstimatedFreeDiskBytes", "", 0},
		{"EstimatedFreeDiskBytes", "NOT_AN_INT", 0},
		{"EstimatedFreeDiskBytes", "0", 0},
		{"EstimatedFreeDiskBytes", "1", 1},
		{"EstimatedFreeDiskBytes", " 1 ", 1},
		{"estimatedfreediskbytes", "1", 1},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: testCase.name, Value: testCase.rawValue},
		}}
		platformProps := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		assert.Equal(t, testCase.expectedValue, platformProps.EstimatedFreeDiskBytes)
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
				{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"},
				{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.4.app/Contents/Developer"},
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
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.4.app/Contents/Developer"},
		},
			"12.2",
		},
		// Darwin with no overrides
		{[]*repb.Platform_Property{
			{Name: "OSFamily", Value: "Darwin"},
		}, []*repb.Command_EnvironmentVariable{}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.2.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.2.app/Contents/Developer"},
		},
			"12.2",
		},
		// Darwin with sdk platform
		{[]*repb.Platform_Property{
			{Name: "OSFamily", Value: "Darwin"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "11.1"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhone.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.4.app/Contents/Developer"},
		},
			"12.2",
		},
		// Darwin with valid sdk platform
		{[]*repb.Platform_Property{
			{Name: "OSFamily", Value: "Darwin"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "ACTUALLY_EXISTS_11.1"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhoneACTUALLY_EXISTS_11.1.sdk"},
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
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "ACTUALLY_EXISTS_11.1"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhoneACTUALLY_EXISTS_11.1.sdk"},
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
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.5.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhone.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode.app/Contents/Developer"},
		},
			"",
		},
		// Case insensitive darwin with no default xcode version and existing sdk
		{[]*repb.Platform_Property{
			{Name: "OSFAMILY", Value: "dArWiN"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "ACTUALLY_EXISTS_11.1"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhoneACTUALLY_EXISTS_11.1.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.4.app/Contents/Developer"},
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
		platformProps := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		execProps := bare
		execProps.DefaultXCodeVersion = testCase.defaultXCodeVersion
		command := &repb.Command{EnvironmentVariables: testCase.startingEnvVars}
		env := testenv.GetTestEnv(t)
		env.RealEnv.SetXCodeLocator(&xcodeLocator{})
		err := platform.ApplyOverrides(env, execProps, platformProps, command)
		require.NoError(t, err)
		assert.ElementsMatch(t, command.EnvironmentVariables, append(testCase.startingEnvVars, testCase.expectedEnvVars...))
	}
}

type xcodeLocator struct {
}

func (x *xcodeLocator) DeveloperDirForVersion(version string) (string, error) {
	if strings.HasPrefix(version, "12.4") {
		return "/Applications/Xcode_12.4.app/Contents/Developer", nil
	}
	if strings.HasPrefix(version, "12.2") {
		return "/Applications/Xcode_12.2.app/Contents/Developer", nil
	}
	return "/Applications/Xcode.app/Contents/Developer", nil
}

func (x *xcodeLocator) IsSDKPathPresentForVersion(sdkPath, version string) bool {
	return strings.Contains(sdkPath, "ACTUALLY_EXISTS")
}

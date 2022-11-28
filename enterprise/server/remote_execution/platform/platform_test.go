package platform

import (
	"fmt"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	bare   = &ExecutorProperties{SupportedIsolationTypes: []ContainerType{BareContainerType}}
	docker = &ExecutorProperties{SupportedIsolationTypes: []ContainerType{DockerContainerType}}
)

func TestParse_ContainerImage_Success(t *testing.T) {
	for _, testCase := range []struct {
		execProps         *ExecutorProperties
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
		{docker, "", "container-image", *defaultImage},
		{docker, "", "Container-Image", *defaultImage},
		{docker, "none", "container-image", *defaultImage},
		{docker, "none", "Container-Image", *defaultImage},
		{docker, "None", "container-image", *defaultImage},
		{docker, "None", "Container-Image", *defaultImage},
		{docker, "docker://alpine", "container-image", "alpine"},
		{docker, "docker://alpine", "Container-Image", "alpine"},
		{docker, "docker://caseSensitiveUrl", "container-image", "caseSensitiveUrl"},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: testCase.containerImageKey, Value: testCase.imageProp},
		}}

		platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		env := testenv.GetTestEnv(t)
		env.RealEnv.SetXcodeLocator(&xcodeLocator{})
		err := ApplyOverrides(env, testCase.execProps, platformProps, &repb.Command{})
		require.NoError(t, err)
		assert.Equal(t, testCase.expected, platformProps.ContainerImage, testCase)
	}
}

func TestParse_ContainerImage_Error(t *testing.T) {
	for _, testCase := range []struct {
		execProps *ExecutorProperties
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

		platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		env := testenv.GetTestEnv(t)
		env.RealEnv.SetXcodeLocator(&xcodeLocator{})
		err := ApplyOverrides(env, testCase.execProps, platformProps, &repb.Command{})
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
		platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		assert.Equal(t, testCase.expectedValue, platformProps.OS)
	}

	// Empty case
	plat := &repb.Platform{Properties: []*repb.Platform_Property{}}
	platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
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
		platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		assert.Equal(t, testCase.expectedValue, platformProps.Arch)
	}

	// Empty case
	plat := &repb.Platform{Properties: []*repb.Platform_Property{}}
	platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
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
		platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		assert.Equal(t, testCase.expectedValue, platformProps.Pool)
	}

	// Empty case
	plat := &repb.Platform{Properties: []*repb.Platform_Property{}}
	platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
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
		platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
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
		platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		assert.Equal(t, testCase.expectedValue, platformProps.EstimatedFreeDiskBytes)
	}
}

func TestParse_EstimatedCPU(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		rawValue      string
		expectedValue int64
	}{
		{"EstimatedCPU", "", 0},
		{"EstimatedCPU", "0.5", 500},
		{"EstimatedCPU", "1", 1000},
		{"EstimatedCPU", "+0.1e+1", 1000},
		{"EstimatedCPU", "4000m", 4000},
		{"EstimatedCPU", "4e3m", 4000},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: testCase.name, Value: testCase.rawValue},
		}}
		platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		assert.Equal(t, testCase.expectedValue, platformProps.EstimatedMilliCPU)
	}
}

func TestParse_EstimatedMemory(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		rawValue      string
		expectedValue int64
	}{
		{"EstimatedMemory", "", 0},
		{"EstimatedMemory", "1000B", 1000},
		{"EstimatedMemory", "1e3", 1000},
		{"EstimatedMemory", "1M", 1024 * 1024},
		{"EstimatedMemory", "1GB", 1024 * 1024 * 1024},
		{"EstimatedMemory", "2.0GB", 2 * 1024 * 1024 * 1024},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: testCase.name, Value: testCase.rawValue},
		}}
		platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		assert.Equal(t, testCase.expectedValue, platformProps.EstimatedMemoryBytes)
	}
}

func TestParse_ApplyOverrides(t *testing.T) {
	for _, testCase := range []struct {
		platformProps       []*repb.Platform_Property
		startingEnvVars     []*repb.Command_EnvironmentVariable
		expectedEnvVars     []*repb.Command_EnvironmentVariable
		errorExpected       bool
		defaultXcodeVersion string
	}{
		// Default darwin platform
		{[]*repb.Platform_Property{
			{Name: "osfamily", Value: "darwin"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.4.app/Contents/Developer"},
		},
			false,
			"12.2",
		},
		// Case insensitive darwin platform
		{[]*repb.Platform_Property{
			{Name: "OSFAMILY", Value: "dArWiN"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.4.app/Contents/Developer"},
		},
			false,
			"12.2",
		},
		// Darwin with no overrides
		{[]*repb.Platform_Property{
			{Name: "OSFamily", Value: "Darwin"},
		}, []*repb.Command_EnvironmentVariable{}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.2.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.2.app/Contents/Developer"},
		},
			false,
			"12.2",
		},
		// Darwin with invalid sdk platform
		{[]*repb.Platform_Property{
			{Name: "OSFamily", Value: "Darwin"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "14.1"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{},
			true,
			"12.2",
		},
		// Darwin with valid sdk platform
		{[]*repb.Platform_Property{
			{Name: "OSFamily", Value: "Darwin"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "14.3"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhone14.3.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.4.app/Contents/Developer"},
		},
			false,
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
			false,
			"12.2",
		},
		// Darwin with xcode override but invalid sdk version
		{[]*repb.Platform_Property{
			{Name: "OSFamily", Value: "Darwin"},
			{Name: "enablexcodeoverride", Value: "true"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "14.2"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{},
			true,
			"12.2",
		},
		// Case insensitive darwin with xcode override
		{[]*repb.Platform_Property{
			{Name: "OSFAMILY", Value: "dArWiN"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "14.3"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhone14.3.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.4.app/Contents/Developer"},
		},
			false,
			"12.2",
		},
		// Case insensitive darwin with no default xcode version
		{[]*repb.Platform_Property{
			{Name: "OSFAMILY", Value: "dArWiN"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.5.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhone.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode.app/Contents/Developer"},
		},
			false,
			"",
		},
		// Case insensitive darwin with no default xcode version and existing sdk
		{[]*repb.Platform_Property{
			{Name: "OSFAMILY", Value: "dArWiN"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "APPLE_SDK_VERSION_OVERRIDE", Value: "14.3"},
			{Name: "APPLE_SDK_PLATFORM", Value: "iPhone"},
			{Name: "XCODE_VERSION_OVERRIDE", Value: "12.4.123"},
		}, []*repb.Command_EnvironmentVariable{
			{Name: "SDKROOT", Value: "/Applications/Xcode_12.4.app/Contents/Developer/Platforms/iPhone.platform/Developer/SDKs/iPhone14.3.sdk"},
			{Name: "DEVELOPER_DIR", Value: "/Applications/Xcode_12.4.app/Contents/Developer"},
		},
			false,
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
			false,
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
			false,
			"12.2",
		},
	} {
		plat := &repb.Platform{Properties: testCase.platformProps}
		platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		execProps := bare
		execProps.DefaultXcodeVersion = testCase.defaultXcodeVersion
		command := &repb.Command{EnvironmentVariables: testCase.startingEnvVars}
		env := testenv.GetTestEnv(t)
		env.RealEnv.SetXcodeLocator(&xcodeLocator{
			sdks12_2: map[string]string{
				"iPhone":     "Platforms/iPhone.platform/Developer/SDKs/iPhone.sdk",
				"iPhone14.2": "Platforms/iPhone.platform/Developer/SDKs/iPhone14.2.sdk",
				"MacOSX":     "Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk",
				"MacOSX11.0": "Platforms/MacOSX.platform/Developer/SDKs/MacOSX11.0.sdk",
			},
			sdks12_4: map[string]string{
				"iPhone":     "Platforms/iPhone.platform/Developer/SDKs/iPhone.sdk",
				"iPhone14.3": "Platforms/iPhone.platform/Developer/SDKs/iPhone14.3.sdk",
				"MacOSX":     "Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk",
				"MacOSX11.1": "Platforms/MacOSX.platform/Developer/SDKs/MacOSX11.1.sdk",
			},
			sdksDefault: map[string]string{
				"iPhone":     "Platforms/iPhone.platform/Developer/SDKs/iPhone.sdk",
				"iPhone14.4": "Platforms/iPhone.platform/Developer/SDKs/iPhone14.4.sdk",
				"MacOSX":     "Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk",
				"MacOSX11.3": "Platforms/MacOSX.platform/Developer/SDKs/MacOSX11.3.sdk",
			},
		})
		err := ApplyOverrides(env, execProps, platformProps, command)
		if testCase.errorExpected {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		assert.ElementsMatch(t, command.EnvironmentVariables, append(testCase.startingEnvVars, testCase.expectedEnvVars...))
	}
}

func TestEnvAndArgOverrides(t *testing.T) {
	plat := &repb.Platform{Properties: []*repb.Platform_Property{
		{Name: "env-overrides", Value: "A=1,B=2,A=3"},
		{Name: "extra-args", Value: "--foo,--bar=baz"},
	}}
	platformProps := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
	execProps := bare
	command := &repb.Command{
		Arguments: []string{"./some_cmd"},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "A", Value: "0"},
		},
	}
	env := testenv.GetTestEnv(t)
	err := ApplyOverrides(env, execProps, platformProps, command)
	require.NoError(t, err)

	expectedCmd := &repb.Command{
		Arguments: []string{"./some_cmd", "--foo", "--bar=baz"},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			// Should just tack on env vars as-is. Runner implementations will ensure
			// that if there are multiple with the same name, the last one wins.
			{Name: "A", Value: "0"},
			{Name: "A", Value: "1"},
			{Name: "B", Value: "2"},
			{Name: "A", Value: "3"},
		},
	}

	expectedCmdText, err := prototext.Marshal(expectedCmd)
	require.NoError(t, err)
	commandText, err := prototext.Marshal(command)
	require.NoError(t, err)
	require.Equal(t, expectedCmdText, commandText)
}

type xcodeLocator struct {
	sdks12_2    map[string]string
	sdks12_4    map[string]string
	sdksDefault map[string]string
}

func (x *xcodeLocator) PathsForVersionAndSDK(xcodeVersion string, sdk string) (string, string, error) {
	var developerDir string
	var sdkPath string
	if strings.HasPrefix(xcodeVersion, "12.2") {
		developerDir = "/Applications/Xcode_12.2.app/Contents/Developer"
		sdkPath, _ = x.sdks12_2[sdk]
	} else if strings.HasPrefix(xcodeVersion, "12.4") {
		developerDir = "/Applications/Xcode_12.4.app/Contents/Developer"
		sdkPath, _ = x.sdks12_4[sdk]
	} else {
		developerDir = "/Applications/Xcode.app/Contents/Developer"
		sdkPath, _ = x.sdksDefault[sdk]
	}
	if sdkPath == "" {
		return "", "", fmt.Errorf("Invalid SDK '%s' for Xcode '%s'", sdk, xcodeVersion)
	}
	sdkRoot := fmt.Sprintf("%s/%s", developerDir, sdkPath)
	return developerDir, sdkRoot, nil
}

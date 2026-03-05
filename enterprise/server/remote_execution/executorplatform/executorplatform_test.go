package executorplatform

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/testing/protocmp"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	bare                 = &ExecutorProperties{SupportedIsolationTypes: []platform.ContainerType{platform.BareContainerType}}
	docker               = &ExecutorProperties{SupportedIsolationTypes: []platform.ContainerType{platform.DockerContainerType}}
	podmanAndFirecracker = &ExecutorProperties{SupportedIsolationTypes: []platform.ContainerType{platform.PodmanContainerType, platform.FirecrackerContainerType}}
)

func TestParse_ContainerImage_Success(t *testing.T) {
	flags.Set(t, "executor.container_registry_region", "us-test1")
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
		{docker, "docker://{{region}}.gcr.io/{{region}}-ubuntu:latest", "container-image", "us-test1.gcr.io/us-test1-ubuntu:latest"},
		{docker, "docker://{{region}}.gcr.io/{{region}}-ubuntu:latest", "Container-image", "us-test1.gcr.io/us-test1-ubuntu:latest"},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: testCase.containerImageKey, Value: testCase.imageProp},
		}}

		platformProps, err := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		require.NoError(t, err)
		env := testenv.GetTestEnv(t)
		env.SetXcodeLocator(&xcodeLocator{})
		err = ApplyOverrides(env, testCase.execProps, platformProps, &repb.Command{})
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

		platformProps, err := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		require.NoError(t, err)
		env := testenv.GetTestEnv(t)
		env.SetXcodeLocator(&xcodeLocator{})
		err = ApplyOverrides(env, testCase.execProps, platformProps, &repb.Command{})
		assert.Error(t, err)
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
		platformProps, err := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		require.NoError(t, err)
		execProps := bare
		execProps.DefaultXcodeVersion = testCase.defaultXcodeVersion
		command := &repb.Command{EnvironmentVariables: testCase.startingEnvVars}
		env := testenv.GetTestEnv(t)
		env.SetXcodeLocator(&xcodeLocator{
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
		err = ApplyOverrides(env, execProps, platformProps, command)
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
		{Name: "env-overrides-base64", Value: base64.StdEncoding.EncodeToString([]byte(`C={"some":1,"value":2}`))},
		{Name: "extra-args", Value: "--foo,--bar=baz"},
	}}
	platformProps, err := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
	require.NoError(t, err)
	execProps := bare
	command := &repb.Command{
		Arguments: []string{"./some_cmd"},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "A", Value: "0"},
		},
	}
	env := testenv.GetTestEnv(t)
	err = ApplyOverrides(env, execProps, platformProps, command)
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
			{Name: "C", Value: `{"some":1,"value":2}`},
		},
	}

	expectedCmdText, err := prototext.Marshal(expectedCmd)
	require.NoError(t, err)
	commandText, err := prototext.Marshal(command)
	require.NoError(t, err)
	require.Equal(t, expectedCmdText, commandText)
}

func TestExtraEnvVars(t *testing.T) {
	for _, tc := range []struct {
		name            string
		extraEnvVars    []string
		expectedEnvVars []*repb.Command_EnvironmentVariable
	}{
		{
			name:         "set env var to value",
			extraEnvVars: []string{"FOO=bar"},
			expectedEnvVars: []*repb.Command_EnvironmentVariable{
				{Name: "FOO", Value: "bar"},
			},
		},
		{
			name:         "set env var to value containing =",
			extraEnvVars: []string{"FOO=bar=baz"},
			expectedEnvVars: []*repb.Command_EnvironmentVariable{
				{Name: "FOO", Value: "bar=baz"},
			},
		},
		{
			name:         "inherit process env var",
			extraEnvVars: []string{"FOO"},
			expectedEnvVars: []*repb.Command_EnvironmentVariable{
				{Name: "FOO", Value: "process-foo-value"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FOO", "process-foo-value")
			flags.Set(t, "executor.extra_env_vars", tc.extraEnvVars)

			env := testenv.GetTestEnv(t)
			cmd := &repb.Command{}
			platformProps := &platform.Properties{}
			ApplyOverrides(env, podmanAndFirecracker, platformProps, cmd)
			require.Empty(t, cmp.Diff(
				tc.expectedEnvVars,
				cmd.EnvironmentVariables,
				protocmp.Transform(),
			))
		})
	}
}

func TestForceNetworkIsolationType(t *testing.T) {
	for _, testCase := range []struct {
		dockerNetworkValue         string
		workloadIsolationType      string
		forcedNetworkIsolationType string
		expectedIsolationType      string
	}{
		// No override set -- behavior unchanged.
		{"", "podman", "", "podman"},
		{"host", "podman", "", "podman"},
		{"none", "podman", "", "podman"},

		// Override set: everything except "none" should
		// trigger an override.
		{"", "podman", "firecracker", "firecracker"},
		{"host", "podman", "firecracker", "firecracker"},
		{"none", "podman", "firecracker", "podman"},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: "container-image", Value: "docker://alpine"},
			{Name: "dockerNetwork", Value: testCase.dockerNetworkValue},
			{Name: "workload-isolation-type", Value: testCase.workloadIsolationType},
		}}

		flags.Set(t, "executor.forced_network_isolation_type", testCase.forcedNetworkIsolationType)

		platformProps, err := platform.ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		require.NoError(t, err)
		env := testenv.GetTestEnv(t)
		env.SetXcodeLocator(&xcodeLocator{})
		err = ApplyOverrides(env, podmanAndFirecracker, platformProps, &repb.Command{})
		assert.NoError(t, err)
		assert.Equal(t, testCase.expectedIsolationType, platformProps.WorkloadIsolationType, testCase)
	}
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
		sdkPath = x.sdks12_2[sdk]
	} else if strings.HasPrefix(xcodeVersion, "12.4") {
		developerDir = "/Applications/Xcode_12.4.app/Contents/Developer"
		sdkPath = x.sdks12_4[sdk]
	} else {
		developerDir = "/Applications/Xcode.app/Contents/Developer"
		sdkPath = x.sdksDefault[sdk]
	}
	if sdkPath == "" {
		return "", "", fmt.Errorf("Invalid SDK '%s' for Xcode '%s'", sdk, xcodeVersion)
	}
	sdkRoot := fmt.Sprintf("%s/%s", developerDir, sdkPath)
	return developerDir, sdkRoot, nil
}

func (x *xcodeLocator) Versions() []string {
	return []string{}
}

func (x *xcodeLocator) SDKs() []string {
	return []string{}
}

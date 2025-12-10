package platform

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	gstatus "google.golang.org/grpc/status"
)

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
		platformProps, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		require.NoError(t, err)
		assert.Equal(t, testCase.expectedValue, platformProps.OS)
	}

	// Empty case
	plat := &repb.Platform{Properties: []*repb.Platform_Property{}}
	platformProps, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
	require.NoError(t, err)
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
		platformProps, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		require.NoError(t, err)
		assert.Equal(t, testCase.expectedValue, platformProps.Arch)
	}

	// Empty case
	plat := &repb.Platform{Properties: []*repb.Platform_Property{}}
	platformProps, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
	require.NoError(t, err)
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
		platformProps, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		require.NoError(t, err)
		assert.Equal(t, testCase.expectedValue, platformProps.Pool)
	}

	// Empty case
	plat := &repb.Platform{Properties: []*repb.Platform_Property{}}
	platformProps, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
	require.NoError(t, err)
	assert.Equal(t, "", platformProps.Pool)
}

func TestParse_EstimatedBCU(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		rawValue      string
		expectedValue float64
	}{
		{"EstimatedComputeUnits", "", 0},
		{"EstimatedComputeUnits", "NOT_A_VALID_NUMBER", 0},
		{"EstimatedComputeUnits", "0", 0},
		{"EstimatedComputeUnits", "1", 1},
		{"EstimatedComputeUnits", " 1 ", 1},
		{"estimatedcomputeunits", "1", 1},
		{"EstimatedComputeUnits", "0.5", 0.5},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: testCase.name, Value: testCase.rawValue},
		}}
		platformProps, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		require.NoError(t, err)
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
		{"EstimatedFreeDiskBytes", "1000B", 1000},
		{"EstimatedFreeDiskBytes", "1e3", 1000},
		{"EstimatedFreeDiskBytes", "1M", 1024 * 1024},
		{"EstimatedFreeDiskBytes", "1GB", 1024 * 1024 * 1024},
		{"EstimatedFreeDiskBytes", "2.0GB", 2 * 1024 * 1024 * 1024},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: testCase.name, Value: testCase.rawValue},
		}}
		platformProps, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		require.NoError(t, err)
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
		platformProps, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		require.NoError(t, err)
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
		platformProps, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		require.NoError(t, err)
		assert.Equal(t, testCase.expectedValue, platformProps.EstimatedMemoryBytes)
	}
}

func TestParse_Duration(t *testing.T) {
	const durationProperty = "runner-recycling-max-wait"

	// Invalid values:
	for _, rawValue := range []string{
		"100",
		"blah",
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: durationProperty, Value: rawValue},
		}}
		_, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		require.Error(t, err, "parse %q", rawValue)
	}

	// Valid values:
	for _, testCase := range []struct {
		rawValue      string
		expectedValue time.Duration
	}{
		{"", 0},
		{"10ms", 10 * time.Millisecond},
		{"-20ms", -20 * time.Millisecond},
		{"2s", 2 * time.Second},
		{"4m", 4 * time.Minute},
		{"-7m", -7 * time.Minute},
	} {
		plat := &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: durationProperty, Value: testCase.rawValue},
		}}
		platformProps, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
		require.NoError(t, err)
		assert.Equal(t, testCase.expectedValue, platformProps.RunnerRecyclingMaxWait)
	}
}

func TestParse_CustomResources_Valid(t *testing.T) {
	props := []*repb.Platform_Property{
		{Name: "resources:foo", Value: "3.14"},
	}
	task := &repb.ExecutionTask{Command: &repb.Command{Platform: &repb.Platform{Properties: props}}}
	p, err := ParseProperties(task)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff([]*scpb.CustomResource{{
		Name: "foo", Value: 3.14,
	}}, p.CustomResources, protocmp.Transform()))
}

func TestParse_CustomResources_Invalid(t *testing.T) {
	props := []*repb.Platform_Property{
		{Name: "resources:foo", Value: "blah"},
	}
	task := &repb.ExecutionTask{Command: &repb.Command{Platform: &repb.Platform{Properties: props}}}
	_, err := ParseProperties(task)
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got %s", gstatus.Code(err))
}

func TestParse_OverrideSnapshotKey(t *testing.T) {
	key := &fcpb.SnapshotKey{
		SnapshotId:        "snapshot-id",
		InstanceName:      "instance-name",
		PlatformHash:      "platform-hash",
		ConfigurationHash: "config-hash",
		RunnerId:          "runner-id",
		Ref:               "ref",
		VersionId:         "version-id",
	}
	keyBytes, err := json.Marshal(key)
	require.NoError(t, err)
	props := []*repb.Platform_Property{
		{Name: SnapshotKeyOverridePropertyName, Value: string(keyBytes)},
	}
	task := &repb.ExecutionTask{Command: &repb.Command{Platform: &repb.Platform{Properties: props}}}
	p, err := ParseProperties(task)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(key, p.OverrideSnapshotKey, protocmp.Transform()))
}

func TestEnvOverridesError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
	}{
		{"not_base64_encode", "D=123"},
		{"mixed_base64", base64.StdEncoding.EncodeToString([]byte(`C={"some":1,"value":2}`)) + "D=123"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plat := &repb.Platform{Properties: []*repb.Platform_Property{
				{Name: "env-overrides-base64", Value: tc.value},
			}}
			props, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
			require.Error(t, err)
			require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got %s", gstatus.Code(err))
			require.Nil(t, props)
		})
	}
}

func TestPersistentVolumes(t *testing.T) {
	for _, tc := range []struct {
		name          string
		prop          string
		expected      []PersistentVolume
		expectedError error
	}{
		{
			name: "one volume",
			prop: "cache:/tmp/.cache",
			expected: []PersistentVolume{
				{name: "cache", containerPath: "/tmp/.cache"},
			},
			expectedError: nil,
		},
		{
			name: "multiple volumes",
			prop: "tmp_cache:/tmp/.cache,user_cache:/root/.cache",
			expected: []PersistentVolume{
				{name: "tmp_cache", containerPath: "/tmp/.cache"},
				{name: "user_cache", containerPath: "/root/.cache"},
			},
			expectedError: nil,
		},
		{
			name: "invalid volume name",
			prop: "..:/tmp/.cache",
			expected: []PersistentVolume{
				{name: "cache", containerPath: "/tmp/.cache"},
			},
			expectedError: status.InvalidArgumentError(`invalid persistent volume "..:/tmp/.cache": name can only contain alphanumeric characters, hyphens, and underscores`),
		},
		{
			name:          "empty volume name",
			prop:          ":/tmp/.cache",
			expected:      nil,
			expectedError: status.InvalidArgumentError(`invalid persistent volume ":/tmp/.cache": expected "<name>:<container_path>"`),
		},
		{
			name:          "empty mount path",
			prop:          "cache:",
			expected:      nil,
			expectedError: status.InvalidArgumentError(`invalid persistent volume "cache:": expected "<name>:<container_path>"`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plat := &repb.Platform{Properties: []*repb.Platform_Property{
				{Name: "persistent-volumes", Value: tc.prop},
			}}
			platformProps, err := ParseProperties(&repb.ExecutionTask{Command: &repb.Command{Platform: plat}})
			if tc.expectedError == nil {
				require.Equal(t, tc.expected, platformProps.PersistentVolumes)
				require.NoError(t, err)
			} else {
				require.Nil(t, platformProps)
				require.Equal(t, tc.expectedError, err)
			}
		})
	}
}

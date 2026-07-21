package resources_test

import (
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestConfigure(t *testing.T) {
	for _, test := range []struct {
		name  string
		flags map[string]any
		env   map[string]string

		expectedCPUMillis int64
		expectError       bool
	}{
		{
			name:              "millicpu flag allows setting CPU-millis",
			flags:             map[string]any{"executor.millicpu": 1234},
			expectedCPUMillis: 1234,
		},
		{
			name:              "SYS_CPU env var allows setting CPU cores",
			env:               map[string]string{"SYS_CPU": "1.5"},
			expectedCPUMillis: 1500,
		},
		{
			name:              "SYS_CPU env var allows setting CPU-millis",
			env:               map[string]string{"SYS_CPU": "1234m"},
			expectedCPUMillis: 1234,
		},
		{
			// SYS_MILLICPU is deprecated because it's misnamed, but kept for
			// compatibility.
			name:              "SYS_MILLICPU env var allows setting cores despite the name",
			env:               map[string]string{"SYS_MILLICPU": "1.5"},
			expectedCPUMillis: 1500,
		},
		{
			name:        "invalid SYS_CPU core setting returns error",
			env:         map[string]string{"SYS_CPU": "?!"},
			expectError: true,
		},
		{
			name:        "invalid SYS_CPU milliCPU setting returns error",
			env:         map[string]string{"SYS_CPU": "?!m"},
			expectError: true,
		},
		{
			name:        "setting both flag and env var returns error",
			flags:       map[string]any{"executor.millicpu": 1234},
			env:         map[string]string{"SYS_CPU": "1"},
			expectError: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			for flag, value := range test.flags {
				flags.Set(t, flag, value)
			}
			for name, value := range test.env {
				t.Setenv(name, value)
			}

			err := resources.Configure(false /*=mmapLRUEnabled*/)
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectedCPUMillis, resources.GetAllocatedCPUMillis())
			}
		})
	}
}

func TestConfigureDiskCapacity(t *testing.T) {
	t.Run("disk_bytes flag sets capacity directly", func(t *testing.T) {
		flags.Set(t, "executor.disk_bytes", int64(1234567))
		// The ratio should be ignored when disk_bytes is set explicitly.
		flags.Set(t, "executor.disk_capacity_ratio", 0.5)

		require.NoError(t, resources.ConfigureDiskCapacity(t.TempDir()))
		require.Equal(t, int64(1234567), resources.GetAllocatedDiskBytes())
	})

	t.Run("capacity is derived from filesystem size scaled by ratio", func(t *testing.T) {
		buildRoot := t.TempDir()

		flags.Set(t, "executor.disk_capacity_ratio", 1.0)
		require.NoError(t, resources.ConfigureDiskCapacity(buildRoot))
		full := resources.GetAllocatedDiskBytes()
		require.Greater(t, full, int64(0))

		flags.Set(t, "executor.disk_capacity_ratio", 0.5)
		require.NoError(t, resources.ConfigureDiskCapacity(buildRoot))
		half := resources.GetAllocatedDiskBytes()

		// Can't assert an absolute size since it depends on the test machine's
		// filesystem, but halving the ratio should halve the reported capacity.
		require.InDelta(t, float64(full)/2, float64(half), float64(full)*0.01)
	})

	t.Run("out-of-range ratio returns error", func(t *testing.T) {
		for _, ratio := range []float64{0, -1, 1.5} {
			flags.Set(t, "executor.disk_capacity_ratio", ratio)
			require.Error(t, resources.ConfigureDiskCapacity(t.TempDir()))
		}
	})

	t.Run("nonexistent build root returns error", func(t *testing.T) {
		require.Error(t, resources.ConfigureDiskCapacity(filepath.Join(t.TempDir(), "nonexistent")))
	})
}

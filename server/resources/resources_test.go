package resources_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenviron"
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
				testenviron.Set(t, name, value)
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

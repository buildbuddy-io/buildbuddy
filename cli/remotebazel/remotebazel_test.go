package remotebazel

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRemoteCliFlags(t *testing.T) {
	type testCase struct {
		name              string
		inputArgs         []string
		expectedOutput    []string
		expectedFlagValue map[string]string
		expectedError     bool
	}

	testCases := []testCase{
		{
			name: "one remote cli flag",
			inputArgs: []string{
				"--remote_runner=val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
			},
		},
		{
			name: "one remote cli flag - space between val",
			inputArgs: []string{
				"--remote_runner",
				"val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
			},
		},
		{
			name: "multiple remote cli flags",
			inputArgs: []string{
				"--remote_runner=val",
				"--os=val2",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"os":            "val2",
			},
		},
		{
			name: "no flags",
			inputArgs: []string{
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
		},
		{
			name: "startup flags, but no cli flags",
			inputArgs: []string{
				"--output_base=val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base=val",
				"build",
				"//...",
			},
		},
		{
			name: "startup flags, but no cli flags - space between value",
			inputArgs: []string{
				"--output_base",
				"val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base",
				"val",
				"build",
				"//...",
			},
		},
		{
			name: "mix of startup flags and cli flags - starting with cli flag",
			inputArgs: []string{
				"--os",
				"val2",
				"--output_base=val",
				"--remote_runner=val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base=val",
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"os":            "val2",
			},
		},
		{
			name: "mix of startup flags and cli flags - starting with startup flag",
			inputArgs: []string{
				"--output_base=val",
				"--os",
				"val2",
				"--remote_runner=val",
				"--system_rc",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base=val",
				"--system_rc",
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"os":            "val2",
			},
		},
		{
			name:              "empty",
			inputArgs:         []string{},
			expectedOutput:    []string{},
			expectedFlagValue: map[string]string{},
			expectedError:     true,
		},
		{
			name: "flags after the bazel command shouldn't be affected",
			inputArgs: []string{
				"--os",
				"val2",
				"build",
				"//...",
				"--os=untouched",
			},
			expectedOutput: []string{
				"build",
				"//...",
				"--os=untouched",
			},
			expectedFlagValue: map[string]string{
				"os": "val2",
			},
		},
	}
	for _, tc := range testCases {
		actualOutput, err := parseRemoteCliFlags(tc.inputArgs)
		if tc.expectedError {
			require.Error(t, err, tc.name)
		} else {
			require.NoError(t, err, tc.name)
			require.Equal(t, tc.expectedOutput, actualOutput, tc.name)
		}

		for flag, expectedVal := range tc.expectedFlagValue {
			actualVal := remoteFlagset.Lookup(flag).Value
			require.Equal(t, expectedVal, actualVal.String(), tc.name)
		}
	}
}

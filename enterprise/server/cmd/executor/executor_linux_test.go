//go:build linux && !android

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveExecutorCPUFraction(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		value         string
		cpuLimit      int64
		expected      float64
		expectedError bool
	}{
		{name: "unset", value: "", cpuLimit: 64_000, expected: 0},
		// Cores and milliCPU are a share of the CPU limit: 4000 / 64000.
		{name: "cores", value: "4", cpuLimit: 64_000, expected: 0.0625},
		{name: "fractional cores", value: "1.5", cpuLimit: 64_000, expected: 1500.0 / 64_000},
		{name: "milliCPU", value: "4000m", cpuLimit: 64_000, expected: 0.0625},
		// A percentage is used as is, regardless of the CPU limit.
		{name: "percentage", value: "5%", cpuLimit: 64_000, expected: 0.05},
		{name: "fractional percentage with spaces", value: " 2.5 % ", cpuLimit: 64_000, expected: 0.025},
		{name: "all of the CPU limit", value: "64", cpuLimit: 64_000, expectedError: true},
		{name: "more than the CPU limit", value: "65000m", cpuLimit: 64_000, expectedError: true},
		{name: "negative cores", value: "-1", cpuLimit: 64_000, expectedError: true},
		{name: "negative milliCPU", value: "-500m", cpuLimit: 64_000, expectedError: true},
		// Like "0%", zero cores and values that round down to 0m are errors.
		{name: "zero cores", value: "0", cpuLimit: 64_000, expectedError: true},
		{name: "zero milliCPU", value: "0m", cpuLimit: 64_000, expectedError: true},
		{name: "less than 1 milliCPU", value: "0.0004", cpuLimit: 64_000, expectedError: true},
		{name: "NaN cores", value: "NaN", cpuLimit: 64_000, expectedError: true},
		{name: "fractional milliCPU", value: "1.5m", cpuLimit: 64_000, expectedError: true},
		{name: "zero percent", value: "0%", cpuLimit: 64_000, expectedError: true},
		{name: "hundred percent", value: "100%", cpuLimit: 64_000, expectedError: true},
		{name: "NaN percent", value: "NaN%", cpuLimit: 64_000, expectedError: true},
		{name: "not a number", value: "four", cpuLimit: 64_000, expectedError: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fraction, err := resolveExecutorCPUFraction(testCase.value, testCase.cpuLimit)
			if testCase.expectedError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.InDelta(t, testCase.expected, fraction, 1e-9)
		})
	}
}

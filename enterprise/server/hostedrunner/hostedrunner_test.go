package hostedrunner

import (
	"testing"

	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestNormalizePlatform(t *testing.T) {
	tests := map[string]struct {
		input          []*repb.Platform_Property
		expectedOutput []*repb.Platform_Property
	}{
		"nil input": {
			input:          nil,
			expectedOutput: nil,
		},
		"empty input": {
			input:          []*repb.Platform_Property{},
			expectedOutput: []*repb.Platform_Property{},
		},
		"sort and dedupe": {
			input: []*repb.Platform_Property{
				{
					Name:  "B",
					Value: "should get overwritten",
				},
				{
					Name:  "B",
					Value: "2",
				},
				{
					Name:  "A",
					Value: "should get overwritten",
				},
				{
					Name:  "A",
					Value: "1",
				},
				{
					Name:  "C",
					Value: "3",
				},
			},
			expectedOutput: []*repb.Platform_Property{
				{
					Name:  "A",
					Value: "1",
				},
				{
					Name:  "B",
					Value: "2",
				},
				{
					Name:  "C",
					Value: "3",
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			o := normalizePlatform(tc.input)
			require.Equal(t, tc.expectedOutput, o)
		})
	}
}

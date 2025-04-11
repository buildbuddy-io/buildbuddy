package rexec_test

import (
	"testing"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestNormalizeCommand(t *testing.T) {
	for _, test := range []struct {
		name     string
		command  *repb.Command
		expected *repb.Command
	}{
		{
			name:     "handles nil command",
			command:  nil,
			expected: nil,
		},
		{
			name:     "handles nil platform and env",
			command:  &repb.Command{},
			expected: &repb.Command{},
		},
		{
			name: "handles non-nil platform with nil properties",
			command: &repb.Command{
				Platform: &repb.Platform{},
			},
			expected: &repb.Command{
				Platform: &repb.Platform{},
			},
		},
		{
			name: "sorts env vars",
			command: &repb.Command{
				EnvironmentVariables: []*repb.Command_EnvironmentVariable{
					{Name: "B", Value: "2"},
					{Name: "A", Value: "1"},
				},
			},
			expected: &repb.Command{
				EnvironmentVariables: []*repb.Command_EnvironmentVariable{
					{Name: "A", Value: "1"},
					{Name: "B", Value: "2"},
				},
			},
		},
		{
			name: "dedupes env vars",
			command: &repb.Command{
				EnvironmentVariables: []*repb.Command_EnvironmentVariable{
					{Name: "A", Value: "1"},
					{Name: "A", Value: "2"},
				},
			},
			expected: &repb.Command{
				EnvironmentVariables: []*repb.Command_EnvironmentVariable{
					{Name: "A", Value: "2"},
				},
			},
		},
		{
			name: "sorts platform properties",
			command: &repb.Command{
				Platform: &repb.Platform{
					Properties: []*repb.Platform_Property{
						{Name: "B", Value: "2"},
						{Name: "A", Value: "1"},
					},
				},
			},
			expected: &repb.Command{
				Platform: &repb.Platform{
					Properties: []*repb.Platform_Property{
						{Name: "A", Value: "1"},
						{Name: "B", Value: "2"},
					},
				},
			},
		},
		{
			name: "dedupes platform properties",
			command: &repb.Command{
				Platform: &repb.Platform{
					Properties: []*repb.Platform_Property{
						{Name: "A", Value: "1"},
						{Name: "A", Value: "2"},
					},
				},
			},
			expected: &repb.Command{
				Platform: &repb.Platform{
					Properties: []*repb.Platform_Property{
						{Name: "A", Value: "2"},
					},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := proto.Clone(test.command).(*repb.Command)
			rexec.NormalizeCommand(cmd)
			require.Empty(t, cmp.Diff(test.expected, cmd, protocmp.Transform()))
		})
	}
}

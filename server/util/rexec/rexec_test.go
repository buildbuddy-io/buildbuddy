package rexec_test

import (
	"testing"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/anypb"
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

func TestFindFirstAuxiliaryMetadata(t *testing.T) {
	mustMarshalAny := func(t *testing.T, msg proto.Message) *anypb.Any {
		a, err := anypb.New(msg)
		require.NoError(t, err)
		return a
	}

	t.Run("returns first matching entry when multiple exist", func(t *testing.T) {
		first := &repb.Platform{Properties: []*repb.Platform_Property{{Name: "first", Value: "1"}}}
		second := &repb.Platform{Properties: []*repb.Platform_Property{{Name: "second", Value: "2"}}}
		md := &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{
				mustMarshalAny(t, first),
				mustMarshalAny(t, second),
			},
		}

		result := &repb.Platform{}
		ok, err := rexec.FindFirstAuxiliaryMetadata(md, result)

		require.NoError(t, err)
		require.True(t, ok)
		require.Empty(t, cmp.Diff(first, result, protocmp.Transform()))
	})

	t.Run("returns false when no match found", func(t *testing.T) {
		md := &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{
				mustMarshalAny(t, &repb.Command{Arguments: []string{"echo"}}),
			},
		}

		result := &repb.Platform{}
		ok, err := rexec.FindFirstAuxiliaryMetadata(md, result)

		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("returns false for nil metadata", func(t *testing.T) {
		result := &repb.Platform{}
		ok, err := rexec.FindFirstAuxiliaryMetadata(nil, result)

		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("returns false for empty auxiliary metadata", func(t *testing.T) {
		md := &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{},
		}

		result := &repb.Platform{}
		ok, err := rexec.FindFirstAuxiliaryMetadata(md, result)

		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("returns error on unmarshal failure", func(t *testing.T) {
		md := &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{
				{
					TypeUrl: "type.googleapis.com/build.bazel.remote.execution.v2.Platform",
					Value:   []byte("invalid proto bytes"),
				},
			},
		}

		result := &repb.Platform{}
		ok, err := rexec.FindFirstAuxiliaryMetadata(md, result)

		require.Error(t, err)
		require.False(t, ok)
	})
}

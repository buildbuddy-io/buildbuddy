package explain

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
)

func modifiedSpawn(expected bool, diffs ...*spawn_diff.Diff) *spawn_diff.SpawnDiff {
	return &spawn_diff.SpawnDiff{
		Diff: &spawn_diff.SpawnDiff_Modified{Modified: &spawn_diff.Modified{
			Expected: expected,
			Diffs:    diffs,
		}},
	}
}

func TestFilterNondeterministicSpawns(t *testing.T) {
	for _, tc := range []struct {
		name string
		diff *spawn_diff.SpawnDiff
		kept bool
	}{
		{
			name: "modified output contents is non-deterministic",
			diff: modifiedSpawn(false, &spawn_diff.Diff{Diff: &spawn_diff.Diff_OutputContents{OutputContents: &spawn_diff.FileSetDiff{}}}),
			kept: true,
		},
		{
			name: "modified exit code is non-deterministic",
			diff: modifiedSpawn(false, &spawn_diff.Diff{Diff: &spawn_diff.Diff_ExitCode{ExitCode: &spawn_diff.IntDiff{Old: 0, New: 1}}}),
			kept: true,
		},
		{
			name: "modified input contents is a downstream effect, not non-determinism",
			diff: modifiedSpawn(false, &spawn_diff.Diff{Diff: &spawn_diff.Diff_InputContents{InputContents: &spawn_diff.FileSetDiff{}}}),
			kept: false,
		},
		{
			name: "expected non-determinism is dropped",
			diff: modifiedSpawn(true, &spawn_diff.Diff{Diff: &spawn_diff.Diff_OutputContents{OutputContents: &spawn_diff.FileSetDiff{}}}),
			kept: false,
		},
		{
			name: "old-only spawn is non-deterministic",
			diff: &spawn_diff.SpawnDiff{Diff: &spawn_diff.SpawnDiff_OldOnly{OldOnly: &spawn_diff.OldOnly{}}},
			kept: true,
		},
		{
			name: "new-only spawn is non-deterministic",
			diff: &spawn_diff.SpawnDiff{Diff: &spawn_diff.SpawnDiff_NewOnly{NewOnly: &spawn_diff.NewOnly{}}},
			kept: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := filterNondeterministicSpawns([]*spawn_diff.SpawnDiff{tc.diff})
			if tc.kept {
				assert.Equal(t, []*spawn_diff.SpawnDiff{tc.diff}, got)
			} else {
				assert.Empty(t, got)
			}
		})
	}
}

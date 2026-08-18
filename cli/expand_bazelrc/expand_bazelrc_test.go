package expand_bazelrc

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpandsConfigsFromBazelrc(t *testing.T) {
	ws := t.TempDir()
	rc1Path := filepath.Join(ws, "one.bazelrc")
	rc2Path := filepath.Join(ws, "two.bazelrc")
	require.NoError(t, os.WriteFile(rc1Path, []byte(`
common --build_metadata=FROM_RC1=1
build:ci --build_metadata=CI_FROM_RC1=1
build:multi --build_metadata=MULTI_FROM_RC1=1
`), 0644))
	require.NoError(t, os.WriteFile(rc2Path, []byte(`
common --build_metadata=FROM_RC2=1
build:dbg --build_metadata=DBG_FROM_RC2=1
build:multi --build_metadata=MULTI_FROM_RC2=1
`), 0644))

	for _, tc := range []struct {
		name     string
		args     []string
		expected []string
	}{
		{
			name: "single bazelrc and config",
			args: []string{"--bazelrc=" + rc1Path, "build", "--config=ci", "//:target"},
			expected: []string{
				"--ignore_all_rc_files",
				"build",
				"--build_metadata=FROM_RC1=1",
				"--build_metadata=CI_FROM_RC1=1",
				"//:target",
			},
		},
		{
			name: "expands multiple bazelrcs",
			args: []string{"--bazelrc=" + rc1Path, "--bazelrc=" + rc2Path, "build", "//:target"},
			expected: []string{
				"--ignore_all_rc_files",
				"build",
				"--build_metadata=FROM_RC1=1",
				"--build_metadata=FROM_RC2=1",
				"//:target",
			},
		},
		{
			name: "expands multiple configs",
			args: []string{"--bazelrc=" + rc1Path, "build", "--config=ci", "--config=multi", "//:target"},
			expected: []string{
				"--ignore_all_rc_files",
				"build",
				"--build_metadata=FROM_RC1=1",
				"--build_metadata=CI_FROM_RC1=1",
				"--build_metadata=MULTI_FROM_RC1=1",
				"//:target",
			},
		},
		{
			name: "expands multiple configs from multiple bazelrcs",
			args: []string{"--bazelrc=" + rc1Path, "--bazelrc=" + rc2Path, "build", "--config=multi", "--config=dbg", "//:target"},
			expected: []string{
				"--ignore_all_rc_files",
				"build",
				"--build_metadata=FROM_RC1=1",
				"--build_metadata=FROM_RC2=1",
				"--build_metadata=MULTI_FROM_RC1=1",
				"--build_metadata=MULTI_FROM_RC2=1",
				"--build_metadata=DBG_FROM_RC2=1",
				"//:target",
			},
		},
		{
			name: "test command does not apply build config",
			args: []string{"--bazelrc=" + rc1Path, "test", "//:target"},
			expected: []string{
				"--ignore_all_rc_files",
				"test",
				"--build_metadata=FROM_RC1=1",
				"//:target",
			},
		},
		{
			name: "if ignore_all_rc_files already set does not expand bazelrc",
			args: []string{"--ignore_all_rc_files", "--bazelrc=" + rc1Path, "build", "//:target"},
			expected: []string{
				"--bazelrc=" + rc1Path,
				"--ignore_all_rc_files",
				"build",
				"//:target",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expandedArgs, err := expandArgs(hermeticArgsJSON(t, tc.args), ws)
			require.NoError(t, err)
			require.Equal(t, tc.expected, expandedArgs)
		})
	}
}

func hermeticArgsJSON(t *testing.T, args []string) string {
	t.Helper()
	withNoRcFlags := append(
		[]string{"--nosystem_rc", "--nohome_rc", "--noworkspace_rc"},
		args...,
	)
	b, err := json.Marshal(withNoRcFlags)
	require.NoError(t, err)
	return string(b)
}

func TestWorkspaceImports(t *testing.T) {
	ws := t.TempDir()
	mainRC := filepath.Join(ws, "main.bazelrc")
	importedRC := filepath.Join(ws, "imported.bazelrc")
	require.NoError(t, os.WriteFile(mainRC, []byte(`
import %workspace%/imported.bazelrc
`), 0644))
	require.NoError(t, os.WriteFile(importedRC, []byte(`
build:ci --disk_cache=/tmp/cache
`), 0644))

	args := fmt.Sprintf(`["--bazelrc=%s","build","--config=ci","//:target"]`, mainRC)

	_, err := expandArgs(args, "")
	require.Error(t, err)

	expandedArgs, err := expandArgs(args, ws)
	require.NoError(t, err)
	require.True(t, slices.Contains(expandedArgs, "--disk_cache=/tmp/cache"))
}

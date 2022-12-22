package parser

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	log.Configure([]string{"--verbose=1"})
}

func TestParseBazelrc_Basic(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"WORKSPACE":                 "",
		"import.bazelrc":            "",
		"explicit_import_1.bazelrc": "--flag_from_explicit_import_1_bazelrc",
		"explicit_import_2.bazelrc": "--flag_from_explicit_import_2_bazelrc",
		".bazelrc": `

# COMMENT
#ANOTHER COMMENT
#

startup --startup_flag_1
startup:config --startup_configs_are_not_supported_so_this_flag_should_be_ignored

# continuations are allowed \
--this_is_not_a_flag_since_it_is_part_of_the_previous_line

--common_global_flag_1          # trailing comments are allowed
common --common_global_flag_2
common:foo --config_foo_global_flag
common:bar --config_bar_global_flag

build --build_flag_1
build:foo --build_config_foo_flag

# Should be able to refer to the "forward_ref" config even though
# it comes later on in the file
build:foo --config=forward_ref

build:foo --build_config_foo_multi_1 --build_config_foo_multi_2

build:forward_ref --build_config_forward_ref_flag

build:bar --build_config_bar_flag

build:workspace_status_with_space --workspace_status_command="bash workspace_status.sh"

test --config=bar

import     %workspace%/import.bazelrc
try-import %workspace%/NONEXISTENT.bazelrc
`,
	})

	for _, tc := range []struct {
		args                 []string
		expectedExpandedArgs []string
	}{
		{
			[]string{"query"},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"query",
				"--common_global_flag_1",
				"--common_global_flag_2",
			},
		},
		{
			[]string{
				"--bazelrc=" + filepath.Join(ws, "explicit_import_1.bazelrc"),
				"query",
			},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"query",
				"--common_global_flag_1",
				"--common_global_flag_2",
				"--flag_from_explicit_import_1_bazelrc",
			},
		},
		{
			[]string{
				"--bazelrc=" + filepath.Join(ws, "explicit_import_1.bazelrc"),
				"--bazelrc=" + filepath.Join(ws, "explicit_import_2.bazelrc"),
				"query",
			},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"query",
				"--common_global_flag_1",
				"--common_global_flag_2",
				"--flag_from_explicit_import_1_bazelrc",
				"--flag_from_explicit_import_2_bazelrc",
			},
		},
		{
			[]string{
				"--bazelrc=" + filepath.Join(ws, "explicit_import_1.bazelrc"),
				// Passing --bazelrc=/dev/null causes subsequent --bazelrc args
				// to be ignored.
				"--bazelrc=/dev/null",
				"--bazelrc=" + filepath.Join(ws, "explicit_import_2.bazelrc"),
				"query",
			},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"query",
				"--common_global_flag_1",
				"--common_global_flag_2",
				"--flag_from_explicit_import_1_bazelrc",
			},
		},
		{
			[]string{
				"--ignore_all_rc_files",
				"--bazelrc=" + filepath.Join(ws, "explicit_import_1.bazelrc"),
				"build",
				"--config=foo",
			},
			[]string{
				"--ignore_all_rc_files",
				// Note: when `--ignore_all_rc_files` is set, it's OK to leave
				// --bazelrc flags as-is, since Bazel will ignore these when it
				// actually gets invoked. We also don't expand --config args,
				// since bazel will fail anyway due to configs being effectively
				// disabled when --ignore_all_rc_files is set.
				"--bazelrc=" + filepath.Join(ws, "explicit_import_1.bazelrc"),
				"build",
				"--config=foo",
			},
		},
		{
			[]string{"--explicit_startup_flag", "query"},
			[]string{
				"--startup_flag_1",
				"--explicit_startup_flag",
				"--ignore_all_rc_files",
				"query",
				"--common_global_flag_1",
				"--common_global_flag_2",
			},
		},
		{
			[]string{"build"},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"build",
				"--common_global_flag_1",
				"--common_global_flag_2",
				"--build_flag_1",
			},
		},
		{
			[]string{"build", "--explicit_flag"},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"build",
				"--common_global_flag_1",
				"--common_global_flag_2",
				"--build_flag_1",
				"--explicit_flag",
			},
		},
		{
			[]string{"build", "--config=foo"},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"build",
				"--common_global_flag_1",
				"--common_global_flag_2",
				"--build_flag_1",
				"--config_foo_global_flag",
				"--build_config_foo_flag",
				"--build_config_forward_ref_flag",
				"--build_config_foo_multi_1",
				"--build_config_foo_multi_2",
			},
		},
		{
			[]string{"build", "--config=foo", "--config", "bar"},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"build",
				"--common_global_flag_1",
				"--common_global_flag_2",
				"--build_flag_1",
				"--config_foo_global_flag",
				"--build_config_foo_flag",
				"--build_config_forward_ref_flag",
				"--build_config_foo_multi_1",
				"--build_config_foo_multi_2",
				"--config_bar_global_flag",
				"--build_config_bar_flag",
			},
		},
		{
			[]string{"test"},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"test",
				"--common_global_flag_1",
				"--common_global_flag_2",
				"--build_flag_1",
				"--config_bar_global_flag",
				"--build_config_bar_flag",
			},
		},
		{
			[]string{"build", "--config=workspace_status_with_space"},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"build",
				"--common_global_flag_1",
				"--common_global_flag_2",
				"--build_flag_1",
				"--workspace_status_command=bash workspace_status.sh",
			},
		},
	} {
		expandedArgs, err := expandConfigs(ws, tc.args)

		require.NoError(t, err, "error expanding %s", tc.args)
		assert.Equal(t, tc.expectedExpandedArgs, expandedArgs)
	}
}

func TestParseBazelrc_CircularConfigReference(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"WORKSPACE": "",
		".bazelrc": `
build:a --config=b
build:b --config=c
build:c --config=a

build:d --config=d
`,
	})

	_, err := expandConfigs(ws, []string{"build", "--config=a"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circular --config reference detected: a -> b -> c -> a")

	_, err = expandConfigs(ws, []string{"build", "--config=d"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circular --config reference detected: d -> d")
}

func TestParseBazelrc_CircularImport(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"WORKSPACE": "",
		".bazelrc":  `import %workspace%/a.bazelrc`,
		"a.bazelrc": `import %workspace%/b.bazelrc`,
		"b.bazelrc": `import %workspace%/a.bazelrc`,
	})

	_, err := expandConfigs(ws, []string{"build"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circular import detected:")

	_, err = expandConfigs(ws, []string{"build"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circular import detected:")
}

func TestCanonicalizeArgs(t *testing.T) {
	// Use some args that look like bazel commands but are actually
	// specifying flag values.
	args := []string{
		"--output_base", "build",
		"--host_jvm_args", "query",
		"--unknown_plugin_flag", "unknown_plugin_flag_value",
		"--ignore_all_rc_files",
		"test",
		"-c", "opt",
		"--another_unknown_plugin_flag",
		"--cache_test_results",
		"--nocache_test_results",
		"--bes_backend", "remote.buildbuddy.io",
		"--bes_backend=",
		"--remote_header", "x-buildbuddy-foo=1",
		"--remote_header", "x-buildbuddy-bar=2",
	}

	canonicalArgs, err := canonicalizeArgs(args, staticHelpFromTestData)

	require.NoError(t, err)
	expectedCanonicalArgs := []string{
		"--output_base=build",
		"--host_jvm_args=query",
		"--unknown_plugin_flag",
		"unknown_plugin_flag_value",
		"--ignore_all_rc_files",
		"test",
		"--compilation_mode=opt",
		"--another_unknown_plugin_flag",
		"--nocache_test_results",
		"--bes_backend=",
		"--remote_header=x-buildbuddy-foo=1",
		"--remote_header=x-buildbuddy-bar=2",
	}
	require.Equal(t, expectedCanonicalArgs, canonicalArgs)
}

func staticHelpFromTestData(topic string) (string, error) {
	if topic == "startup_options" {
		return test_data.BazelHelpStartupOptionsOutput, nil
	}
	if topic == "test" {
		return test_data.BazelHelpTestOutput, nil
	}
	return "", fmt.Errorf("testHelpProvider: no test data configured for `bazel help %s`", topic)
}

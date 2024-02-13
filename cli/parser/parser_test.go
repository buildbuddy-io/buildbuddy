package parser

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	log.Configure("--verbose=1")
}

func TestParseBazelrc_Basic(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"WORKSPACE": "",
		"import.bazelrc": `
common:import --build_metadata=IMPORTED_FLAG=1
`,
		"explicit_import_1.bazelrc": "--build_metadata=EXPLICIT_IMPORT_1=1",
		"explicit_import_2.bazelrc": "--build_metadata=EXPLICIT_IMPORT_2=1",
		".bazelrc": `

# COMMENT
#ANOTHER COMMENT
#

startup --startup_flag_1
startup:config --startup_configs_are_not_supported_so_this_flag_should_be_ignored

# continuations are allowed \
--build_metadata=THIS_IS_NOT_A_FLAG_SINCE_IT_IS_PART_OF_THE_PREVIOUS_LINE=1

--invalid_common_flag_1          # trailing comments are allowed
--build_metadata=VALID_COMMON_FLAG=1
common --invalid_common_flag_2
common --build_metadata=VALID_COMMON_FLAG=2
common:foo --build_metadata=COMMON_CONFIG_FOO=1
common:bar --build_metadata=COMMON_CONFIG_BAR=1

build --build_flag_1
build:foo --build_config_foo_flag

# Should be able to refer to the "forward_ref" config even though
# it comes later on in the file
build:foo --config=forward_ref

build:foo --build_config_foo_multi_1 --build_config_foo_multi_2

build:forward_ref --build_config_forward_ref_flag

build:bar --build_config_bar_flag

build:workspace_status_with_space --workspace_status_command="bash workspace_status.sh"

common --noverbose_test_summary
test --config=bar

build:no_value_flag --remote_download_minimal

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
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
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
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_metadata=EXPLICIT_IMPORT_1=1",
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
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_metadata=EXPLICIT_IMPORT_1=1",
				"--build_metadata=EXPLICIT_IMPORT_2=1",
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
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_metadata=EXPLICIT_IMPORT_1=1",
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
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
			},
		},
		{
			[]string{"build"},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"build",
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_flag_1",
			},
		},
		{
			[]string{"build", "--explicit_flag"},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"build",
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
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
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_flag_1",
				"--build_metadata=COMMON_CONFIG_FOO=1",
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
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_flag_1",
				"--build_metadata=COMMON_CONFIG_FOO=1",
				"--build_config_foo_flag",
				"--build_config_forward_ref_flag",
				"--build_config_foo_multi_1",
				"--build_config_foo_multi_2",
				"--build_metadata=COMMON_CONFIG_BAR=1",
				"--build_config_bar_flag",
			},
		},
		{
			[]string{"test"},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"test",
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--noverbose_test_summary",
				"--build_flag_1",
				"--build_metadata=COMMON_CONFIG_BAR=1",
				"--build_config_bar_flag",
			},
		},
		{
			[]string{"build", "--config=workspace_status_with_space"},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"build",
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_flag_1",
				"--workspace_status_command=bash workspace_status.sh",
			},
		},
		// Test parsing flags that do not require a value to be set, like
		// --remote_download_minimal or --java_debug
		{
			[]string{
				"build",
				"--config=no_value_flag",
			},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"build",
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_flag_1",
				"--remote_download_minimal",
			},
		},
		// Test parsing a config that should have been imported with
		// try-import %workspace%/<import_name>.bazelrc
		{
			[]string{
				"build",
				"--config=import",
			},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"build",
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_flag_1",
				"--build_metadata=IMPORTED_FLAG=1",
			},
		},
	} {
		expandedArgs, err := expandConfigs(ws, tc.args, staticHelpFromTestData)

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

	_, err := expandConfigs(ws, []string{"build", "--config=a"}, staticHelpFromTestData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circular --config reference detected: a -> b -> c -> a")

	_, err = expandConfigs(ws, []string{"build", "--config=d"}, staticHelpFromTestData)
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

	_, err := expandConfigs(ws, []string{"build"}, staticHelpFromTestData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circular import detected:")

	_, err = expandConfigs(ws, []string{"build"}, staticHelpFromTestData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circular import detected:")
}

func TestParseBazelrc_DedupesBazelrcFilesInArgs(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"WORKSPACE":    "",
		".rc1":         `test --test_arg=1`,
		".rc2":         `test --test_arg=2`,
		".imports-rc1": `import %workspace%/.rc1`,
		".bazelrc":     `run --build_metadata=WORKSPACERC=1`,
	})
	// Make some hardlinks/symlinks for testing as well.
	err := os.Symlink(filepath.Join(ws, ".rc1"), filepath.Join(ws, ".rc1-symlink"))
	require.NoError(t, err)
	err = os.Link(filepath.Join(ws, ".rc1"), filepath.Join(ws, ".rc1-hardlink"))
	require.NoError(t, err)

	workspacerc := filepath.Join(ws, ".bazelrc")
	rc1 := filepath.Join(ws, ".rc1")
	// NOTE: not using filepath.Join() here since it returned the `Clean`ed
	// path, which defeats what we're trying to test here.
	rc1AltPath := ws + "/../" + filepath.Dir(ws) + ".rc1"
	rc2 := filepath.Join(ws, ".rc2")
	rc1Symlink := filepath.Join(ws, ".rc1-symlink")
	rc1Hardlink := filepath.Join(ws, ".rc1-hardlink")
	importsRC1 := filepath.Join(ws, ".imports-rc1")

	for _, test := range []struct {
		name                 string
		args                 []string
		expectedExpandedArgs []string
	}{
		{
			name:                 "ShouldIgnoreDuplicateBazelrcWithExactPathMatch",
			args:                 []string{"--bazelrc=" + rc1, "--bazelrc=" + rc2, "--bazelrc=" + rc1, "test"},
			expectedExpandedArgs: []string{"--ignore_all_rc_files", "test", "--test_arg=1", "--test_arg=2"},
		},
		{
			name:                 "ShouldIgnoreDuplicateBazelrcWithEquivalentPathMatch",
			args:                 []string{"--bazelrc=" + rc1, "--bazelrc=" + rc2, "--bazelrc=" + rc1AltPath, "test"},
			expectedExpandedArgs: []string{"--ignore_all_rc_files", "test", "--test_arg=1", "--test_arg=2"},
		},
		{
			name:                 "ShouldIgnoreDuplicateBazelrcWithEquivalentSymlinkTargetPathMatch",
			args:                 []string{"--bazelrc=" + rc1, "--bazelrc=" + rc2, "--bazelrc=" + rc1Symlink, "test"},
			expectedExpandedArgs: []string{"--ignore_all_rc_files", "test", "--test_arg=1", "--test_arg=2"},
		},
		{
			name:                 "ShouldIgnoreExplicitWorkspacercReference",
			args:                 []string{"--bazelrc=" + workspacerc, "run"},
			expectedExpandedArgs: []string{"--ignore_all_rc_files", "run", "--build_metadata=WORKSPACERC=1"},
		},
		{
			name:                 "ShouldNotIgnoreDuplicateBazelrcWithHardlinkTargetMatch",
			args:                 []string{"--bazelrc=" + rc1, "--bazelrc=" + rc2, "--bazelrc=" + rc1Hardlink, "test"},
			expectedExpandedArgs: []string{"--ignore_all_rc_files", "test", "--test_arg=1", "--test_arg=2", "--test_arg=1"},
		},
		{
			name:                 "ShouldNotIgnoreDuplicateBazelrcImportedExplicitly",
			args:                 []string{"--bazelrc=" + rc1, "--bazelrc=" + rc2, "--bazelrc=" + importsRC1, "test"},
			expectedExpandedArgs: []string{"--ignore_all_rc_files", "test", "--test_arg=1", "--test_arg=2", "--test_arg=1"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			expandedArgs, err := expandConfigs(ws, test.args, staticHelpFromTestData)

			require.NoError(t, err, "error expanding %s", test.args)
			assert.Equal(t, test.expectedExpandedArgs, expandedArgs)
		})
	}
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

	canonicalArgs, err := canonicalizeArgs(args, staticHelpFromTestData, false)

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

func TestCanonicalizeStartupArgs(t *testing.T) {
	// Use some args that look like bazel commands but are actually
	// specifying flag values.
	args := []string{
		"--output_base", "build",
		"--host_jvm_args", "query",
		"--host_jvm_args=another_arg",
		"--unknown_plugin_flag", "unknown_plugin_flag_value",
		"--ignore_all_rc_files",
		"--bazelrc", "/tmp/bazelrc_1",
		"--bazelrc=/tmp/bazelrc_2",
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

	canonicalArgs, err := canonicalizeArgs(args, staticHelpFromTestData, true)

	require.NoError(t, err)
	expectedCanonicalArgs := []string{
		"--output_base=build",
		"--host_jvm_args=query",
		"--host_jvm_args=another_arg",
		"--unknown_plugin_flag",
		"unknown_plugin_flag_value",
		"--ignore_all_rc_files",
		"--bazelrc=/tmp/bazelrc_1",
		"--bazelrc=/tmp/bazelrc_2",
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
	require.Equal(t, expectedCanonicalArgs, canonicalArgs)
}

func TestCanonicalizeArgs_Passthrough(t *testing.T) {
	args := []string{
		"--output_base", "build",
		"test",
		"//:some_target",
		"--",
		"cmd",
		"-foo=bar",
	}

	canonicalArgs, err := canonicalizeArgs(args, staticHelpFromTestData, true)

	require.NoError(t, err)
	expectedCanonicalArgs := []string{
		"--output_base=build",
		"test",
		"//:some_target",
		"--",
		"cmd",
		"-foo=bar",
	}
	require.Equal(t, expectedCanonicalArgs, canonicalArgs)
}

func staticHelpFromTestData(topic string) (string, error) {
	if topic == "startup_options" {
		return test_data.BazelHelpStartupOptionsOutput, nil
	}
	if topic == "build" {
		return test_data.BazelHelpBuildOutput, nil
	}
	if topic == "test" {
		return test_data.BazelHelpTestOutput, nil
	}
	if topic == "query" {
		return test_data.BazelHelpQueryOutput, nil
	}
	if topic == "run" {
		return test_data.BazelHelpRunOutput, nil
	}
	return "", fmt.Errorf("testHelpProvider: no test data configured for `bazel help %s`", topic)
}

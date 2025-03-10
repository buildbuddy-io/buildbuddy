package parser

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	log.Configure("--verbose=1")
	SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

const TEST_PLUGIN_ID = "//test"

func getOptionDefinition(t *testing.T, name string) *OptionDefinition {
	p, err := GetParser()
	require.NoError(t, err)
	require.NotNilf(t, p, "Could not retrieve parser.")
	optionDefinition := p.ByName[name]
	require.NotNilf(t, optionDefinition, "Could not retrieve option definition for %s in parser.", name)
	return optionDefinition
}

func TestParseBazelrc_Simple(t *testing.T) {
	parser, err := GetParser()
	require.NoError(t, err)
	transitionsOptionDefinition := parser.parseStarlarkOptionDefinition("@io_bazel_rules_docker//transitions:enable")
	for _, test := range []struct {
		Name     string
		Bazelrc  string
		Args     []string
		Expanded *ParsedArgs
	}{
		{
			Name:    "ExpandStarlarkFlagsFromCommonConfig",
			Bazelrc: "common --@io_bazel_rules_docker//transitions:enable=false",
			Args:    []string{"build"},
			Expanded: &ParsedArgs{
				StartupOptions: []*Option{
					{
						OptionDefinition: getOptionDefinition(t, "ignore_all_rc_files"),
						Value:            "1",
					},
				},
				Command: "build",
				CommandOptions: []*Option{
					{
						OptionDefinition: transitionsOptionDefinition,
						Value:            "false",
					},
				},
			},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			ws := testfs.MakeTempDir(t)
			testfs.WriteAllFileContents(t, ws, map[string]string{
				"WORKSPACE": "",
				"BUILD":     "",
				".bazelrc":  test.Bazelrc,
			})

			p, err := GetParser()
			require.NoError(t, err)
			defer delete(p.ByName, "@io_bazel_rules_docker//transitions:enable")

			parsedArgs, err := ParseArgs(test.Args, "")
			require.NoError(t, err, "error expanding %s", test.Args)
			parsedConfigs, err := p.consumeAndParseRCFiles(parsedArgs, ws)
			require.NoError(t, err, "error expanding %s", test.Args)
			err = parsedArgs.ExpandConfigs(parsedConfigs)
			require.NoError(t, err, "error expanding %s", test.Args)

			assert.Equal(t, test.Expanded, parsedArgs)
		})
	}
}

func TestParseBazelrc_Complex(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"WORKSPACE": "",
		"import.bazelrc": `
common:import --build_metadata=IMPORTED_FLAG=1
`,
		"explicit_import_1.bazelrc": "common --build_metadata=EXPLICIT_IMPORT_1=1",
		"explicit_import_2.bazelrc": "common --build_metadata=EXPLICIT_IMPORT_2=1",
		".bazelrc": `

# COMMENT
#ANOTHER COMMENT
#

startup --startup_flag_1
startup:config --startup_configs_are_not_supported_so_this_flag_should_be_ignored

# continuations are allowed \
--build_metadata=THIS_IS_NOT_A_FLAG_SINCE_IT_IS_PART_OF_THE_PREVIOUS_LINE=1

--build_metadata=VALID_COMMON_FLAG=1          # trailing comments are allowed
common --build_metadata=VALID_COMMON_FLAG=2
common:foo --build_metadata=COMMON_CONFIG_FOO=1 # trailing comments are allowed
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
	parser, err := GetParser()
	require.NoError(t, err)
	parser.ByName["explicit_startup_flag"] = &OptionDefinition{
		Name:              "explicit_startup_flag",
		HasNegative:       true,
		RequiresValue:     false,
		SupportedCommands: map[string]struct{}{"startup": {}},
		PluginID:          TEST_PLUGIN_ID,
	}
	defer delete(parser.ByName, "explicit_startup_flag")
	parser.ByName["startup_flag_1"] = &OptionDefinition{
		Name:              "startup_flag_1",
		HasNegative:       true,
		RequiresValue:     false,
		SupportedCommands: map[string]struct{}{"startup": {}},
		PluginID:          TEST_PLUGIN_ID,
	}
	defer delete(parser.ByName, "startup_flag_1")
	buildCommands := map[string]struct{}{
		"build":          {},
		"test":           {},
		"run":            {},
		"clean":          {},
		"mobile-install": {},
		"info":           {},
		"print_action":   {},
		"config":         {},
		"cquery":         {},
		"aquery":         {},
		"coverage":       {},
	}
	parser.ByName["explicit_flag"] = &OptionDefinition{
		Name:              "explicit_flag",
		HasNegative:       true,
		RequiresValue:     false,
		SupportedCommands: buildCommands,
		PluginID:          TEST_PLUGIN_ID,
	}
	defer delete(parser.ByName, "explicit_flag")
	parser.ByName["build_flag_1"] = &OptionDefinition{
		Name:              "build_flag_1",
		HasNegative:       true,
		RequiresValue:     false,
		SupportedCommands: buildCommands,
		PluginID:          TEST_PLUGIN_ID,
	}
	defer delete(parser.ByName, "build_flag_1")
	parser.ByName["build_config_foo_multi_1"] = &OptionDefinition{
		Name:              "build_config_foo_multi_1",
		HasNegative:       true,
		RequiresValue:     false,
		SupportedCommands: buildCommands,
		PluginID:          TEST_PLUGIN_ID,
	}
	defer delete(parser.ByName, "build_config_foo_multi_1")
	parser.ByName["build_config_foo_multi_2"] = &OptionDefinition{
		Name:              "build_config_foo_multi_2",
		HasNegative:       true,
		RequiresValue:     false,
		SupportedCommands: buildCommands,
		PluginID:          TEST_PLUGIN_ID,
	}
	defer delete(parser.ByName, "build_config_foo_multi_2")
	parser.ByName["build_config_foo_flag"] = &OptionDefinition{
		Name:              "build_config_foo_flag",
		HasNegative:       true,
		RequiresValue:     false,
		SupportedCommands: buildCommands,
		PluginID:          TEST_PLUGIN_ID,
	}
	defer delete(parser.ByName, "build_config_foo_flag")
	parser.ByName["build_config_bar_flag"] = &OptionDefinition{
		Name:              "build_config_bar_flag",
		HasNegative:       true,
		RequiresValue:     false,
		SupportedCommands: buildCommands,
		PluginID:          TEST_PLUGIN_ID,
	}
	defer delete(parser.ByName, "build_config_bar_flag")
	parser.ByName["build_config_forward_ref_flag"] = &OptionDefinition{
		Name:              "build_config_forward_ref_flag",
		HasNegative:       true,
		RequiresValue:     false,
		SupportedCommands: buildCommands,
		PluginID:          TEST_PLUGIN_ID,
	}
	defer delete(parser.ByName, "build_config_forward_ref_flag")

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
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_metadata=EXPLICIT_IMPORT_1=1",
			},
		},
		{
			[]string{
				"--ignore_all_rc_files",
				"--bazelrc=" + filepath.Join(ws, "explicit_import_1.bazelrc"),
				"build",
			},
			[]string{
				"--ignore_all_rc_files",
				"build",
			},
		},
		{
			[]string{"--explicit_startup_flag", "query"},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"--explicit_startup_flag",
				"query",
				"--build_metadata=VALID_COMMON_FLAG=2",
			},
		},
		{
			[]string{"build"},
			[]string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"build",
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
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_flag_1",
				"--build_metadata=IMPORTED_FLAG=1",
			},
		},
	} {
		p, err := GetParser()
		require.NoError(t, err)

		parsedArgs, err := ParseArgs(tc.args, "")
		require.NoError(t, err, "error expanding %s", tc.args)
		parsedConfigs, err := p.consumeAndParseRCFiles(parsedArgs, ws)
		require.NoError(t, err, "error expanding %s", tc.args)
		err = parsedArgs.ExpandConfigs(parsedConfigs)
		require.NoError(t, err, "error expanding %s", tc.args)

		assert.Equal(t, tc.expectedExpandedArgs, parsedArgs.FormatOptions())
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

	parsedConfigs, err := ParseRCFiles(ws, filepath.Join(ws, ".bazelrc"))
	require.NoError(t, err)
	parsedArgs := &ParsedArgs{
		Command: "build",
		CommandOptions: []*Option{
			{
				OptionDefinition: &OptionDefinition{
					Name: "config",
				},
				Value: "a",
			},
		},
	}
	t.Log("Parsed Configs:")
	for name, config := range parsedConfigs {
		t.Logf("    Config '%s'", name)
		t.Log("        Options:")
		for phase, opts := range config.Options {
			t.Logf("            Phase '%s':", phase)
			for _, o := range opts {
				t.Logf("                %s: %s", o.OptionDefinition.Name, o.Value)
			}
		}
		t.Log("        Args:")
		for phase, args := range config.PositionalArgs {
			t.Logf("            Phase '%s':", phase)
			for _, a := range args {
				t.Logf("                %s", a)
			}
		}
	}
	err = parsedArgs.ExpandConfigs(parsedConfigs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circular --config reference detected: a -> b -> c -> a")

	parsedArgs = &ParsedArgs{
		Command: "build",
		CommandOptions: []*Option{
			{
				OptionDefinition: &OptionDefinition{
					Name: "config",
				},
				Value: "d",
			},
		},
	}
	err = parsedArgs.ExpandConfigs(parsedConfigs)
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

	_, err := ParseRCFiles(ws, filepath.Join(ws, ".bazelrc"))
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

	testArgOptionSchema := getOptionDefinition(t, "test_arg")
	bazelRC1Options := []*Option{
		{
			OptionDefinition: testArgOptionSchema,
			Value:            "1",
		},
	}
	bazelRC2Options := []*Option{
		{
			OptionDefinition: testArgOptionSchema,
			Value:            "2",
		},
	}
	workspacercOptions := []*Option{
		{
			OptionDefinition: getOptionDefinition(t, "build_metadata"),
			Value:            "WORKSPACERC=1",
		},
	}

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

	bazelrcOptionDefinition := getOptionDefinition(t, "bazelrc")
	ignoreAllRCFilesOptionDefinition := getOptionDefinition(t, "ignore_all_rc_files")

	ignoreAllRCFilesOption := &Option{
		OptionDefinition: ignoreAllRCFilesOptionDefinition,
		Value:            "1",
	}

	bazelRC1Option := &Option{
		OptionDefinition: bazelrcOptionDefinition,
		Value:            rc1,
	}
	bazelRC1AltPathOption := &Option{
		OptionDefinition: bazelrcOptionDefinition,
		Value:            rc1AltPath,
	}
	bazelRC1SymlinkOption := &Option{
		OptionDefinition: bazelrcOptionDefinition,
		Value:            rc1Symlink,
	}
	bazelRC1HardlinkOption := &Option{
		OptionDefinition: bazelrcOptionDefinition,
		Value:            rc1Hardlink,
	}
	importsRC1Option := &Option{
		OptionDefinition: bazelrcOptionDefinition,
		Value:            importsRC1,
	}
	bazelRC2Option := &Option{
		OptionDefinition: bazelrcOptionDefinition,
		Value:            rc2,
	}
	workspaceRCOption := &Option{
		OptionDefinition: bazelrcOptionDefinition,
		Value:            workspacerc,
	}
	for _, test := range []struct {
		name           string
		args           *ParsedArgs
		expectedConfig *ParsedConfig
	}{
		{
			name: "ShouldIgnoreDuplicateBazelrcWithExactPathMatch",
			args: &ParsedArgs{
				StartupOptions: []*Option{bazelRC1Option, bazelRC2Option, bazelRC1Option},
				Command:        "test",
			},
			expectedConfig: &ParsedConfig{
				Options: map[string][]*Option{
					"startup": []*Option{ignoreAllRCFilesOption},
					"test":    concat(bazelRC1Options, bazelRC2Options),
					"run":     workspacercOptions,
				},
				PositionalArgs: map[string][]string{},
			},
		},
		{
			name: "ShouldIgnoreDuplicateBazelrcWithEquivalentPathMatch",
			args: &ParsedArgs{
				StartupOptions: []*Option{bazelRC1Option, bazelRC2Option, bazelRC1AltPathOption},
				Command:        "test",
			},
			expectedConfig: &ParsedConfig{
				Options: map[string][]*Option{
					"startup": []*Option{ignoreAllRCFilesOption},
					"test":    concat(bazelRC1Options, bazelRC2Options),
					"run":     workspacercOptions,
				},
				PositionalArgs: map[string][]string{},
			},
		},
		{
			name: "ShouldIgnoreDuplicateBazelrcWithEquivalentSymlinkTargetPathMatch",
			args: &ParsedArgs{
				StartupOptions: []*Option{bazelRC1Option, bazelRC2Option, bazelRC1SymlinkOption},
				Command:        "test",
			},
			expectedConfig: &ParsedConfig{
				Options: map[string][]*Option{
					"startup": []*Option{ignoreAllRCFilesOption},
					"test":    concat(bazelRC1Options, bazelRC2Options),
					"run":     workspacercOptions,
				},
				PositionalArgs: map[string][]string{},
			},
		},
		{
			name: "ShouldIgnoreExplicitWorkspacercReference",
			args: &ParsedArgs{
				StartupOptions: []*Option{workspaceRCOption},
				Command:        "run",
			},
			expectedConfig: &ParsedConfig{
				Options: map[string][]*Option{
					"startup": []*Option{ignoreAllRCFilesOption},
					"run":     workspacercOptions,
				},
				PositionalArgs: map[string][]string{},
			},
		},
		{
			name: "ShouldNotIgnoreDuplicateBazelrcWithHardlinkTargetMatch",
			args: &ParsedArgs{
				StartupOptions: []*Option{bazelRC1Option, bazelRC2Option, bazelRC1HardlinkOption},
				Command:        "test",
			},
			expectedConfig: &ParsedConfig{
				Options: map[string][]*Option{
					"startup": []*Option{ignoreAllRCFilesOption},
					"test":    concat(bazelRC1Options, bazelRC2Options, bazelRC1Options),
					"run":     workspacercOptions,
				},
				PositionalArgs: map[string][]string{},
			},
		},
		{
			name: "ShouldNotIgnoreDuplicateBazelrcImportedExplicitly",
			args: &ParsedArgs{
				StartupOptions: []*Option{bazelRC1Option, bazelRC2Option, importsRC1Option},
				Command:        "test",
			},
			expectedConfig: &ParsedConfig{
				Options: map[string][]*Option{
					"startup": []*Option{ignoreAllRCFilesOption},
					"test":    concat(bazelRC1Options, bazelRC2Options, bazelRC1Options),
					"run":     workspacercOptions,
				},
				PositionalArgs: map[string][]string{},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, err := GetParser()
			require.NoError(t, err)
			parsedConfigs, err := p.consumeAndParseRCFiles(test.args, ws)
			require.NoErrorf(t, err, "Error during test case %s", test.name)
			defaultConfig, ok := parsedConfigs[""]
			require.Truef(t, ok, "Default config absent during test case %s", test.name)

			assert.Equalf(t, test.expectedConfig, defaultConfig, "Config was not as expected during test case %s", test.name)
		})
	}
}

func TestParseArgs(t *testing.T) {
	// Use some args that look like bazel commands but are actually
	// specifying flag values.
	args, err := ParseArgs(
		[]string{
			"--output_base", "build",
			"--host_jvm_args", "query",
			"--host_jvm_args=another_arg",
			"--ignore_all_rc_files",
			"--bazelrc", "/tmp/bazelrc_1",
			"--bazelrc=/tmp/bazelrc_2",
			"--host_jvm_debug",
			"test",
			"-c", "opt",
			"--cache_test_results",
			"--nocache_test_results",
			"--bes_backend", "remote.buildbuddy.io",
			"--bes_backend=",
			"--remote_header", "x-buildbuddy-foo=1",
			"--remote_header", "x-buildbuddy-bar=2",
			"--remote_download_minimal=value",
			"--noexperimental_convenience_symlinks",
			"--subcommands=pretty_print",
		},
		"",
	)
	require.NoError(t, err)
	expectedArgs := &ParsedArgs{
		StartupOptions: []*Option{
			{
				OptionDefinition: getOptionDefinition(t, "output_base"),
				Value:            "build",
			},
			{
				OptionDefinition: getOptionDefinition(t, "host_jvm_args"),
				Value:            "query",
			},
			{
				OptionDefinition: getOptionDefinition(t, "host_jvm_args"),
				Value:            "another_arg",
			},
			{
				OptionDefinition: getOptionDefinition(t, "ignore_all_rc_files"),
				Value:            "1",
			},
			{
				OptionDefinition: getOptionDefinition(t, "bazelrc"),
				Value:            "/tmp/bazelrc_1",
			},
			{
				OptionDefinition: getOptionDefinition(t, "bazelrc"),
				Value:            "/tmp/bazelrc_2",
			},
			{
				OptionDefinition: getOptionDefinition(t, "host_jvm_debug"),
			},
		},
		Command: "test",
		CommandOptions: []*Option{
			{
				OptionDefinition: getOptionDefinition(t, "compilation_mode"),
				Value:            "opt",
			},
			{
				OptionDefinition: getOptionDefinition(t, "cache_test_results"),
				Value:            "1",
			},
			{
				OptionDefinition: getOptionDefinition(t, "cache_test_results"),
				Value:            "0",
			},
			{
				OptionDefinition: getOptionDefinition(t, "bes_backend"),
				Value:            "remote.buildbuddy.io",
			},
			{
				OptionDefinition: getOptionDefinition(t, "bes_backend"),
				Value:            "",
			},
			{
				OptionDefinition: getOptionDefinition(t, "remote_header"),
				Value:            "x-buildbuddy-foo=1",
			},
			{
				OptionDefinition: getOptionDefinition(t, "remote_header"),
				Value:            "x-buildbuddy-bar=2",
			},
			{
				OptionDefinition: getOptionDefinition(t, "remote_download_minimal"),
				Value:            "",
			},
			{
				OptionDefinition: getOptionDefinition(t, "experimental_convenience_symlinks"),
				Value:            "0",
			},
			{
				OptionDefinition: getOptionDefinition(t, "subcommands"),
				Value:            "pretty_print",
			},
		},
	}
	require.Equal(t, expectedArgs, args)
}

func TestCanonicalizeArgs(t *testing.T) {
	// Use some args that look like bazel commands but are actually
	// specifying flag values.
	args := &ParsedArgs{
		StartupOptions: []*Option{
			{
				OptionDefinition: getOptionDefinition(t, "output_base"),
				Value:            "build",
			},
			{
				OptionDefinition: getOptionDefinition(t, "host_jvm_args"),
				Value:            "query",
			},
			{
				OptionDefinition: getOptionDefinition(t, "host_jvm_args"),
				Value:            "another_arg",
			},
			{
				OptionDefinition: &OptionDefinition{
					Name:          "unknown_plugin_flag",
					RequiresValue: true,
				},
				Value: "unknown_plugin_flag_value",
			},
			{
				OptionDefinition: getOptionDefinition(t, "ignore_all_rc_files"),
			},
			{
				OptionDefinition: getOptionDefinition(t, "bazelrc"),
				Value:            "/tmp/bazelrc_1",
			},
			{
				OptionDefinition: getOptionDefinition(t, "bazelrc"),
				Value:            "/tmp/bazelrc_2",
			},
			{
				OptionDefinition: getOptionDefinition(t, "host_jvm_debug"),
			},
		},
		Command: "test",
		CommandOptions: []*Option{
			{
				OptionDefinition: getOptionDefinition(t, "compilation_mode"),
				Value:            "opt",
			},
			{
				OptionDefinition: &OptionDefinition{
					Name: "another_unknown_plugin_flag",
				},
			},
			{
				OptionDefinition: getOptionDefinition(t, "cache_test_results"),
				Value:            "",
			},
			{
				OptionDefinition: getOptionDefinition(t, "cache_test_results"),
				Value:            "0",
			},
			{
				OptionDefinition: getOptionDefinition(t, "bes_backend"),
				Value:            "remote.buildbuddy.io",
			},
			{
				OptionDefinition: getOptionDefinition(t, "bes_backend"),
				Value:            "",
			},
			{
				OptionDefinition: getOptionDefinition(t, "remote_header"),
				Value:            "x-buildbuddy-foo=1",
			},
			{
				OptionDefinition: getOptionDefinition(t, "remote_header"),
				Value:            "x-buildbuddy-bar=2",
			},
			{
				OptionDefinition: getOptionDefinition(t, "remote_download_minimal"),
				Value:            "value",
			},
			{
				OptionDefinition: getOptionDefinition(t, "experimental_convenience_symlinks"),
				Value:            "false",
			},
			{
				OptionDefinition: getOptionDefinition(t, "subcommands"),
				Value:            "pretty_print",
			},
		},
	}

	err := args.CanonicalizeOptions()
	require.NoError(t, err)

	expectedCanonicalArgs := &ParsedArgs{
		StartupOptions: []*Option{
			{
				OptionDefinition: getOptionDefinition(t, "bazelrc"),
				Value:            "/tmp/bazelrc_1",
			},
			{
				OptionDefinition: getOptionDefinition(t, "bazelrc"),
				Value:            "/tmp/bazelrc_2",
			},
			{
				OptionDefinition: getOptionDefinition(t, "host_jvm_args"),
				Value:            "query",
			},
			{
				OptionDefinition: getOptionDefinition(t, "host_jvm_args"),
				Value:            "another_arg",
			},
			{
				OptionDefinition: getOptionDefinition(t, "host_jvm_debug"),
			},
			{
				OptionDefinition: getOptionDefinition(t, "ignore_all_rc_files"),
				Value:            "1",
			},
			{
				OptionDefinition: getOptionDefinition(t, "output_base"),
				Value:            "build",
			},
			{
				OptionDefinition: &OptionDefinition{
					Name:          "unknown_plugin_flag",
					RequiresValue: true,
				},
				Value: "unknown_plugin_flag_value",
			},
		},
		Command: "test",
		CommandOptions: []*Option{
			{
				OptionDefinition: &OptionDefinition{
					Name: "another_unknown_plugin_flag",
				},
			},
			{
				OptionDefinition: getOptionDefinition(t, "bes_backend"),
				Value:            "",
			},
			{
				OptionDefinition: getOptionDefinition(t, "cache_test_results"),
				Value:            "0",
			},
			{
				OptionDefinition: getOptionDefinition(t, "compilation_mode"),
				Value:            "opt",
			},
			{
				OptionDefinition: getOptionDefinition(t, "experimental_convenience_symlinks"),
				Value:            "0",
			},
			{
				OptionDefinition: getOptionDefinition(t, "remote_download_minimal"),
			},
			{
				OptionDefinition: getOptionDefinition(t, "remote_header"),
				Value:            "x-buildbuddy-foo=1",
			},
			{
				OptionDefinition: getOptionDefinition(t, "remote_header"),
				Value:            "x-buildbuddy-bar=2",
			},
			{
				OptionDefinition: getOptionDefinition(t, "subcommands"),
				Value:            "pretty_print",
			},
		},
	}
	assert.Equal(t, expectedCanonicalArgs, args)
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
		"--host_jvm_debug",
		"test",
		"-c", "opt",
		"--another_unknown_plugin_flag",
		"--cache_test_results",
		"--nocache_test_results",
		"--bes_backend", "remote.buildbuddy.io",
		"--bes_backend=",
		"--remote_header", "x-buildbuddy-foo=1",
		"--remote_header", "x-buildbuddy-bar=2",
		"--", "hello",
	}

	p, err := GetParser()
	require.NoError(t, err)
	canonicalArgs, err := p.canonicalizeArgs(args, true)

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
		"--host_jvm_debug",
		"test",
		"-c", "opt",
		"--another_unknown_plugin_flag",
		"--cache_test_results",
		"--nocache_test_results",
		"--bes_backend", "remote.buildbuddy.io",
		"--bes_backend=",
		"--remote_header", "x-buildbuddy-foo=1",
		"--remote_header", "x-buildbuddy-bar=2",
		"--", "hello",
	}
	assert.Equal(t, expectedCanonicalArgs, canonicalArgs)
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

	p, err := GetParser()
	require.NoError(t, err)
	canonicalArgs, err := p.canonicalizeArgs(args, false)

	require.NoError(t, err)
	expectedCanonicalArgs := []string{
		"--output_base=build",
		"test",
		"//:some_target",
		"--",
		"cmd",
		"-foo=bar",
	}
	assert.Equal(t, expectedCanonicalArgs, canonicalArgs)
}

func TestParseArgsWithDoubleDash(t *testing.T) {
	args, err := ParseArgs(
		[]string{
			"--output_base", "build",
			"test",
			"//:some_target",
			"--",
			"cmd",
			"-foo=bar",
		},
		"",
	)
	require.NoError(t, err)

	expectedArgs := &ParsedArgs{
		StartupOptions: []*Option{
			{
				OptionDefinition: getOptionDefinition(t, "output_base"),
				Value:            "build",
			},
		},
		Command: "test",
		PositionalArgs: []string{
			"//:some_target",
			"cmd",
			"-foo=bar",
		},
	}

	canonicalArgs := args.CanonicalizeOptions()
	assert.Equal(t, expectedArgs, canonicalArgs)
}

func TestGetFirstTargetPattern(t *testing.T) {
	for _, tc := range []struct {
		Args            []string
		ExpectedPattern string
	}{
		{
			Args:            []string{"bazel", "build", "//..."},
			ExpectedPattern: "//...",
		},
		{
			Args:            []string{"bazel", "build", "server/..."},
			ExpectedPattern: "server/...",
		},
		{
			Args:            []string{"bazel", "build", ":server"},
			ExpectedPattern: ":server",
		},
		{
			Args:            []string{"bazel", "build", "--config=remote", "-c", "opt", "-g", "server/..."},
			ExpectedPattern: "server/...",
		},
		{
			Args:            []string{"bazel", "help"},
			ExpectedPattern: "",
		},
		{
			Args:            []string{"bazel", "build", "--config=remote", "-c", "opt", "-g"},
			ExpectedPattern: "",
		},
	} {
		assert.Equal(t, tc.ExpectedPattern, GetFirstTargetPattern(tc.Args), strings.Join(tc.Args, " "))
	}
}

func TestCommonUndocumentedOption(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"WORKSPACE": "",
		".bazelrc":  "common --experimental_skip_ttvs_for_genquery",
	})

	args := []string{
		"build",
	}

	expectedExpandedArgs := []string{
		"--ignore_all_rc_files",
		"build",
		"--experimental_skip_ttvs_for_genquery",
	}
	p, err := GetParser()
	require.NoError(t, err)
	parsedArgs, err := ParseArgs(args, "")
	require.NoError(t, err)
	parsedConfigs, err := p.consumeAndParseRCFiles(parsedArgs, ws)
	require.NoError(t, err)
	err = parsedArgs.ExpandConfigs(parsedConfigs)
	require.NoError(t, err, "error expanding %s", args)

	assert.Equal(t, expectedExpandedArgs, parsedArgs.FormatOptions())
}

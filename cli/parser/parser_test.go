package parser

import (
	"bytes"
	"flag"
	stdlog "log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/cli_command"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/bbrc"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	log.Configure("--verbose=1")
	SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

// Set HOME and USERPROFILE to a temp dir to avoid leaking Dev home_rc into
// the test sandbox while running locally.
func TestMain(m *testing.M) {
	tempHome, err := os.MkdirTemp("", "parser-test-home-*")
	if err != nil {
		panic(err)
	}
	if err := os.Setenv("HOME", tempHome); err != nil {
		panic(err)
	}
	if err := os.Setenv("USERPROFILE", tempHome); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(tempHome)
	os.Exit(code)
}

func TestNegativeStarlarkFlagWithValue(t *testing.T) {
	for _, test := range []struct {
		Name     string
		Bazelrc  string
		Args     []string
		Expanded []string
	}{
		{
			Name: "ExpandStarlarkFlagsFromCommonConfig",
			Args: []string{"build", "--no@io_bazel_rules_docker//transitions:enable=foo"},
			Expanded: []string{
				"--ignore_all_rc_files",
				"build",
				"--no@io_bazel_rules_docker//transitions:enable=foo",
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
			defer delete(p.CommandOptionParser.ByName, "@io_bazel_rules_docker//transitions:enable")
			parsedArgs, err := ParseArgs(test.Args)
			require.NoError(t, err)
			expandedArgs, err := resolveArgs(parsedArgs, ws)

			require.NoError(t, err, "error expanding %s", test.Args)
			assert.Equal(t, test.Expanded, expandedArgs.Format())
		})
	}
}

func TestParseBBRCFiles(t *testing.T) {
	previousCommandsByName := cli_command.CommandsByName
	cli_command.CommandsByName = map[string]*cli_command.Command{
		"explain": {Name: "explain"},
	}
	t.Cleanup(func() {
		cli_command.CommandsByName = previousCommandsByName
	})

	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bbrc": `
always --always_flag
common --common_flag
build --build_flag
run --run_flag
explain --explain_flag
run:detailed --detailed_flag --bb_config=nested
build:nested --nested_flag
`,
	})

	p := NewParser(
		[]*options.Definition{
			bbrc.NewConfigOptionDefinition("run", "explain"),
			options.NewDefinition("always_flag", options.WithNegative(), options.WithSupportFor("run", "explain")),
			options.NewDefinition("common_flag", options.WithNegative(), options.WithSupportFor("run", "explain")),
			options.NewDefinition("build_flag", options.WithNegative(), options.WithSupportFor("build")),
			options.NewDefinition("run_flag", options.WithNegative(), options.WithSupportFor("run")),
			options.NewDefinition("explain_flag", options.WithNegative(), options.WithSupportFor("explain")),
			options.NewDefinition("detailed_flag", options.WithNegative(), options.WithSupportFor("run")),
			options.NewDefinition("nested_flag", options.WithNegative(), options.WithSupportFor("build")),
		},
		[]string{"run", "explain"},
		nil,
	)
	namedConfigs, defaultConfig, err := p.ParseBBRCFiles(ws, filepath.Join(ws, ".bbrc"))
	require.NoError(t, err)

	runArgs, err := p.ParseArgs([]string{"run", "--bb_config=detailed", "//:target"})
	require.NoError(t, err)
	expandedRunArgs, err := bbrc.ExpandConfigs(runArgs, namedConfigs, defaultConfig)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"run",
		"--always_flag",
		"--common_flag",
		"--build_flag",
		"--run_flag",
		"--detailed_flag",
		"--nested_flag",
		"//:target",
	}, expandedRunArgs.Format())

	explainArgs, err := p.ParseArgs([]string{"explain", "invocation-id"})
	require.NoError(t, err)
	expandedExplainArgs, err := bbrc.ExpandConfigs(explainArgs, namedConfigs, defaultConfig)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"explain",
		"--always_flag",
		"--common_flag",
		"--explain_flag",
		"invocation-id",
	}, expandedExplainArgs.Format())
}

func TestParseBBRCFiles_MultipleCommands(t *testing.T) {
	remoteFlags := flag.NewFlagSet("remote", flag.ContinueOnError)
	remoteFlags.String("container_image", "", "")

	previousCommandsByName := cli_command.CommandsByName
	cli_command.CommandsByName = map[string]*cli_command.Command{
		"remote": {Name: "remote", Flags: remoteFlags},
	}
	t.Cleanup(func() {
		cli_command.CommandsByName = previousCommandsByName
	})

	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bbrc": `
run --stream_run_logs
remote --container_image=docker://example
`,
	})

	// Simulate parsing a Bazel run invocation. The unrelated remote section
	// should still be parseable and not cause a parsing error.
	p, err := GetBBParserForCommand("run")
	require.NoError(t, err)
	_, defaultConfig, err := p.ParseBBRCFiles(ws, filepath.Join(ws, ".bbrc"))
	require.NoError(t, err)
	require.Equal(t, []string{"--stream_run_logs"}, arguments.FormatAll(defaultConfig.ByPhase["run"]))
	require.Equal(t, []string{"--container_image=docker://example"}, arguments.FormatAll(defaultConfig.ByPhase["remote"]))
}

func TestParseBBRCFiles_CircularConfigReference(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bbrc": `
run:a --bb_config=b
run:b --bb_config=a
`,
	})

	p := NewParser(
		[]*options.Definition{bbrc.NewConfigOptionDefinition("run")},
		[]string{"run"},
		nil,
	)
	namedConfigs, defaultConfig, err := p.ParseBBRCFiles(ws, filepath.Join(ws, ".bbrc"))
	require.NoError(t, err)
	args, err := p.ParseArgs([]string{"run", "--bb_config=a"})
	require.NoError(t, err)

	_, err = bbrc.ExpandConfigs(args, namedConfigs, defaultConfig)
	require.ErrorContains(t, err, "circular --bb_config reference detected: a -> b -> a")
}

func TestParseBBRCFiles_RejectsInvalidStartupOptions(t *testing.T) {
	p, err := GetParser()
	require.NoError(t, err)

	for _, test := range []struct {
		name        string
		contents    string
		errorString string
	}{
		{
			name:     "RCFileControlFlag",
			contents: "startup --ignore_all_rc_files",
		},
		{
			name:     "BBRCFileControlFlag",
			contents: "startup --bbrc=other.bbrc",
		},
		{
			name:     "IgnoreAllBBRCFilesControlFlag",
			contents: "startup --ignore_all_bb_rc_files",
		},
		{
			name:     "NonStartupArgument",
			contents: "startup run",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ws := testfs.MakeTempDir(t)
			testfs.WriteAllFileContents(t, ws, map[string]string{
				".bbrc": test.contents,
			})

			_, _, err := p.ParseBBRCFiles(ws, filepath.Join(ws, ".bbrc"))
			require.ErrorContains(t, err, test.errorString)
		})
	}
}

func TestParseBBRCFiles_RejectsSubcommands(t *testing.T) {
	agentFlags := flag.NewFlagSet("agent", flag.ContinueOnError)
	agentFlags.String("model", "", "")

	previousCommandsByName := cli_command.CommandsByName
	cli_command.CommandsByName = map[string]*cli_command.Command{
		"agent": {Name: "agent", Flags: agentFlags},
	}
	t.Cleanup(func() {
		cli_command.CommandsByName = previousCommandsByName
	})

	for _, test := range []struct {
		name     string
		contents string
	}{
		{
			name:     "rejects subcommands",
			contents: "agent analyze-profile --model=claude-opus-5\n",
		},
		{
			name:     "rejects subcommands after options",
			contents: "agent --model=claude-opus-5 analyze-profile\n",
		},
		{
			name:     "rejects named configs with subcommands",
			contents: "agent:fast analyze-profile --model=claude-opus-5\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ws := testfs.MakeTempDir(t)
			testfs.WriteAllFileContents(t, ws, map[string]string{".bbrc": test.contents})

			p, err := GetBBParserForCommand("agent")
			require.NoError(t, err)
			_, _, err = p.ParseBBRCFiles(ws, filepath.Join(ws, ".bbrc"))
			require.Error(t, err)
		})
	}
}

func TestParseBBRCFiles_AllowsSpaceSeparatedOptionValue(t *testing.T) {
	agentFlags := flag.NewFlagSet("agent", flag.ContinueOnError)
	agentFlags.String("model", "", "")

	previousCommandsByName := cli_command.CommandsByName
	cli_command.CommandsByName = map[string]*cli_command.Command{
		"agent": {Name: "agent", Flags: agentFlags},
	}
	t.Cleanup(func() {
		cli_command.CommandsByName = previousCommandsByName
	})

	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bbrc": "agent --model claude-opus-5\n",
	})

	p, err := GetBBParserForCommand("agent")
	require.NoError(t, err)
	_, defaultConfig, err := p.ParseBBRCFiles(ws, filepath.Join(ws, ".bbrc"))
	require.NoError(t, err)
	require.Equal(t, []string{"--model", "claude-opus-5"}, arguments.FormatAll(defaultConfig.ByPhase["agent"]))
}

func TestConsumeBBRCFileOptions(t *testing.T) {
	p, err := GetParser()
	require.NoError(t, err)

	args, err := p.ParseArgs([]string{
		"--bbrc=first.bbrc",
		"--bbrc", "last.bbrc",
		"run",
	})
	require.NoError(t, err)

	// Canonicalization must preserve all explicit files and their order.
	args, err = p.ParseArgs(args.Canonicalized().Format())
	require.NoError(t, err)
	paths, err := consumeBBRCFileOptions(args, "/workspace", "/home/user")
	require.NoError(t, err)
	require.Equal(t, []string{
		"/workspace/.bbrc",
		"/home/user/.bbrc",
		"first.bbrc",
		"last.bbrc",
	}, paths)
	require.Empty(t, args.GetStartupOptionsByName(bbrc.FileFlagName))
}

func TestConsumeBBRCFileOptions_WarnsForMissingBBRC(t *testing.T) {
	p, err := GetParser()
	require.NoError(t, err)
	missingPath := filepath.Join(t.TempDir(), "missing.bbrc")
	args, err := p.ParseArgs([]string{"--bbrc=" + missingPath, "run"})
	require.NoError(t, err)

	var logs bytes.Buffer
	previousWriter := stdlog.Writer()
	stdlog.SetOutput(&logs)
	t.Cleanup(func() { stdlog.SetOutput(previousWriter) })

	paths, err := consumeBBRCFileOptions(args, "", "")
	require.NoError(t, err)
	require.Equal(t, []string{missingPath}, paths)
	require.Contains(t, logs.String(), "Could not find --bbrc file")
	require.Contains(t, logs.String(), missingPath)
}

func TestConsumeBBRCFileOptions_DevNullStopsExplicitFiles(t *testing.T) {
	p, err := GetParser()
	require.NoError(t, err)
	args, err := p.ParseArgs([]string{
		"--bbrc=first.bbrc",
		"--bbrc=/dev/null",
		"--bbrc=ignored.bbrc",
		"run",
	})
	require.NoError(t, err)

	paths, err := consumeBBRCFileOptions(args, "/workspace", "/home/user")
	require.NoError(t, err)
	require.Equal(t, []string{
		"/workspace/.bbrc",
		"/home/user/.bbrc",
		"first.bbrc",
	}, paths)
}

func TestConsumeBBRCFileOptions_IgnoreAll(t *testing.T) {
	p, err := GetParser()
	require.NoError(t, err)
	args, err := p.ParseArgs([]string{
		"--bbrc=explicit.bbrc",
		"--ignore_all_bb_rc_files",
		"run",
	})
	require.NoError(t, err)

	paths, err := consumeBBRCFileOptions(args, "/workspace", "/home/user")
	require.NoError(t, err)
	require.Empty(t, paths)
	require.Empty(t, args.GetStartupOptionsByName(bbrc.FileFlagName))
	require.Empty(t, args.GetStartupOptionsByName(bbrc.IgnoreAllRCFilesFlagName))
}

func TestConsumeBBRCFileOptions_IgnoreAllLastValueWins(t *testing.T) {
	p, err := GetParser()
	require.NoError(t, err)
	args, err := p.ParseArgs([]string{
		"--ignore_all_bb_rc_files",
		"--noignore_all_bb_rc_files",
		"--bbrc=explicit.bbrc",
		"run",
	})
	require.NoError(t, err)

	paths, err := consumeBBRCFileOptions(args, "/workspace", "/home/user")
	require.NoError(t, err)
	require.Equal(t, []string{
		"/workspace/.bbrc",
		"/home/user/.bbrc",
		"explicit.bbrc",
	}, paths)
}

func TestResolveArgs_ExpandsBBRCBeforeBazelRC(t *testing.T) {
	workspaceDir := testfs.MakeTempDir(t)
	homeDir := testfs.MakeTempDir(t)
	explicitBBRC := filepath.Join(testfs.MakeTempDir(t), "explicit.bbrc")
	t.Setenv("HOME", homeDir)
	testfs.WriteAllFileContents(t, workspaceDir, map[string]string{
		".bbrc": `
run --stream_run_logs
run:ci --on_stream_run_logs_failure=warn
`,
		".bazelrc": `
run:bazel --build_metadata=BAZEL
`,
	})
	testfs.WriteAllFileContents(t, homeDir, map[string]string{
		".bbrc": `
run --nostream_run_logs
run:ci --on_stream_run_logs_failure=ignore
`,
	})
	require.NoError(t, os.WriteFile(explicitBBRC, []byte(`
run:ci --on_stream_run_logs_failure=fail
`), 0644))

	p, err := GetParser()
	require.NoError(t, err)
	args, err := p.ParseArgs([]string{
		"--bbrc=" + explicitBBRC,
		"run",
		"//:target",
		"--bb_config=ci",
		"--config=bazel",
	})
	require.NoError(t, err)
	args, err = p.resolveArgs(args, workspaceDir)
	require.NoError(t, err)
	require.Equal(t, []string{
		"--ignore_all_rc_files",
		"run",
		"--stream_run_logs",
		"--nostream_run_logs",
		"//:target",
		"--on_stream_run_logs_failure=warn",
		"--on_stream_run_logs_failure=ignore",
		"--on_stream_run_logs_failure=fail",
		"--build_metadata=BAZEL",
	}, args.Format())
}

func TestResolveArgs_BBRCRejectsBazelFlags(t *testing.T) {
	workspaceDir := testfs.MakeTempDir(t)
	t.Setenv("HOME", testfs.MakeTempDir(t))
	testfs.WriteAllFileContents(t, workspaceDir, map[string]string{
		".bbrc": "run --build_metadata=NOT_ALLOWED",
	})

	p, err := GetParser()
	require.NoError(t, err)
	args, err := p.ParseArgs([]string{"run", "//:target"})
	require.NoError(t, err)
	_, err = p.resolveArgs(args, workspaceDir)
	require.ErrorContains(t, err, "build_metadata")
}

func TestResolveArgs_IgnoreAllBBRCFiles(t *testing.T) {
	workspaceDir := testfs.MakeTempDir(t)
	homeDir := testfs.MakeTempDir(t)
	t.Setenv("HOME", homeDir)
	testfs.WriteAllFileContents(t, workspaceDir, map[string]string{
		".bbrc": "run --stream_run_logs",
	})
	testfs.WriteAllFileContents(t, homeDir, map[string]string{
		".bbrc": "run --nostream_run_logs",
	})

	p, err := GetParser()
	require.NoError(t, err)
	args, err := p.ParseArgs([]string{"--ignore_all_bb_rc_files", "run", "//:target"})
	require.NoError(t, err)
	args, err = p.resolveArgs(args, workspaceDir)
	require.NoError(t, err)
	require.Equal(t, []string{"--ignore_all_rc_files", "run", "//:target"}, args.Format())
}

func TestParseBBRCFiles_MultipleConfigs(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bbrc": `
run:first --first_flag
run:second --second_flag
`,
	})

	p := NewParser(
		[]*options.Definition{
			bbrc.NewConfigOptionDefinition("run"),
			options.NewDefinition("first_flag", options.WithNegative(), options.WithSupportFor("run")),
			options.NewDefinition("second_flag", options.WithNegative(), options.WithSupportFor("run")),
		},
		[]string{"run"},
		nil,
	)
	namedConfigs, defaultConfig, err := p.ParseBBRCFiles(ws, filepath.Join(ws, ".bbrc"))
	require.NoError(t, err)
	args, err := p.ParseArgs([]string{"run", "--bb_config=first", "--bb_config=second", "//:target"})
	require.NoError(t, err)

	// Canonicalization should retain every --bb_config rather than treating the
	// later flag as an override of the earlier one.
	args, err = p.ParseArgs(args.Canonicalized().Format())
	require.NoError(t, err)
	expanded, err := bbrc.ExpandConfigs(args, namedConfigs, defaultConfig)
	require.NoError(t, err)
	require.Equal(t, []string{"run", "--first_flag", "--second_flag", "//:target"}, expanded.Format())
}

func TestParseBBRCFiles_CommandWithSubcommands(t *testing.T) {
	previousCommandsByName := cli_command.CommandsByName
	cli_command.CommandsByName = map[string]*cli_command.Command{
		"agent": {Name: "agent"},
	}
	t.Cleanup(func() {
		cli_command.CommandsByName = previousCommandsByName
	})

	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bbrc": `
agent --model=claude-opus-5
agent:fast --effort=low
`,
	})

	p := NewParser(
		[]*options.Definition{
			bbrc.NewConfigOptionDefinition("agent"),
			options.NewDefinition("model", options.WithRequiresValue(), options.WithSupportFor("agent")),
			options.NewDefinition("effort", options.WithRequiresValue(), options.WithSupportFor("agent")),
		},
		[]string{"agent"},
		nil,
	)
	namedConfigs, defaultConfig, err := p.ParseBBRCFiles(ws, filepath.Join(ws, ".bbrc"))
	require.NoError(t, err)

	// The `agent` flags should be applied to all subcommands.
	for _, subcommand := range []string{"subcommand-1", "subcommand-2"} {
		t.Run(subcommand, func(t *testing.T) {
			args, err := p.ParseArgs([]string{"agent", subcommand, "invocation-id"})
			require.NoError(t, err)
			expanded, err := bbrc.ExpandConfigs(args, namedConfigs, defaultConfig)
			require.NoError(t, err)
			assert.Equal(t, []string{
				"agent",
				"--model=claude-opus-5",
				subcommand,
				"invocation-id",
			}, expanded.Format())
		})
	}

	// Unlike the default section, whose options are inserted after the command
	// name, a named config expands in place, wherever --bb_config appeared. So
	// options can land on either side of the subcommand name.
	t.Run("NamedConfig", func(t *testing.T) {
		args, err := p.ParseArgs([]string{"agent", "analyze-profile", "--bb_config=fast", "invocation-id"})
		require.NoError(t, err)
		expanded, err := bbrc.ExpandConfigs(args, namedConfigs, defaultConfig)
		require.NoError(t, err)
		assert.Equal(t, []string{
			"agent",
			"--model=claude-opus-5",
			"analyze-profile",
			"--effort=low",
			"invocation-id",
		}, expanded.Format())
	})
}

func TestParseBazelrc_Simple(t *testing.T) {
	for _, test := range []struct {
		Name     string
		Bazelrc  string
		Args     []string
		Expanded []string
	}{
		{
			Name:    "ExpandStarlarkFlagsFromCommonConfig",
			Bazelrc: "common --@io_bazel_rules_docker//transitions:enable=false",
			Args:    []string{"build"},
			Expanded: []string{
				"--ignore_all_rc_files",
				"build",
				"--@io_bazel_rules_docker//transitions:enable=false",
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
			defer delete(p.CommandOptionParser.ByName, "@io_bazel_rules_docker//transitions:enable")
			parsedArgs, err := ParseArgs(test.Args)
			require.NoError(t, err)
			expandedArgs, err := resolveArgs(parsedArgs, ws)

			require.NoError(t, err, "error expanding %s", test.Args)
			assert.Equal(t, test.Expanded, expandedArgs.Format())
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
common --build_metadata=THIS_IS_NOT_A_FLAG_SINCE_IT_IS_PART_OF_THE_PREVIOUS_LINE=1

common --invalid_common_flag_1          # trailing comments are allowed
--build_metadata=INVALID_COMMON_FLAG=1
common --build_metadata=VALID_COMMON_FLAG=1
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
		errorContents        []string
		expectedExpandedArgs []string
	}{
		{
			args: []string{"query"},
			expectedExpandedArgs: []string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"query",
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
			},
		},
		{
			args: []string{
				"--bazelrc=" + filepath.Join(ws, "explicit_import_1.bazelrc"),
				"query",
			},
			expectedExpandedArgs: []string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"query",
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_metadata=EXPLICIT_IMPORT_1=1",
			},
		},
		{
			args: []string{
				"--bazelrc=" + filepath.Join(ws, "explicit_import_1.bazelrc"),
				"--bazelrc=" + filepath.Join(ws, "explicit_import_2.bazelrc"),
				"query",
			},
			expectedExpandedArgs: []string{
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
			args: []string{
				"--bazelrc=" + filepath.Join(ws, "explicit_import_1.bazelrc"),
				// Passing --bazelrc=/dev/null causes subsequent --bazelrc args
				// to be ignored.
				"--bazelrc=/dev/null",
				"--bazelrc=" + filepath.Join(ws, "explicit_import_2.bazelrc"),
				"query",
			},
			expectedExpandedArgs: []string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"query",
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_metadata=EXPLICIT_IMPORT_1=1",
			},
		},
		{
			args: []string{
				"--ignore_all_rc_files",
				"--bazelrc=" + filepath.Join(ws, "explicit_import_1.bazelrc"),
				"build",
				"--config=foo",
			},
			errorContents: []string{
				"config value 'foo' is not defined in any .rc file",
			},
		},
		{
			args: []string{"--explicit_startup_flag", "query"},
			expectedExpandedArgs: []string{
				"--startup_flag_1",
				"--explicit_startup_flag",
				"--ignore_all_rc_files",
				"query",
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
			},
		},
		{
			args: []string{"build"},
			expectedExpandedArgs: []string{
				"--startup_flag_1",
				"--ignore_all_rc_files",
				"build",
				"--build_metadata=VALID_COMMON_FLAG=1",
				"--build_metadata=VALID_COMMON_FLAG=2",
				"--build_flag_1",
			},
		},
		{
			args: []string{"build", "--explicit_flag"},
			expectedExpandedArgs: []string{
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
			args: []string{"build", "--config=foo"},
			expectedExpandedArgs: []string{
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
			args: []string{"build", "--config=foo", "--config", "bar"},
			expectedExpandedArgs: []string{
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
			args: []string{"test"},
			expectedExpandedArgs: []string{
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
			args: []string{"build", "--config=workspace_status_with_space"},
			expectedExpandedArgs: []string{
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
			args: []string{
				"build",
				"--config=no_value_flag",
			},
			expectedExpandedArgs: []string{
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
			args: []string{
				"build",
				"--config=import",
			},
			expectedExpandedArgs: []string{
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
		t.Run("", func(t *testing.T) {
			parsedArgs, err := ParseArgs(tc.args)
			require.NoError(t, err)
			expandedArgs, err := resolveArgs(parsedArgs, ws)

			if tc.errorContents != nil {
				for _, ec := range tc.errorContents {
					require.ErrorContains(t, err, ec)
				}
			} else {
				require.NoError(t, err, "error expanding %s", tc.args)
			}
			if tc.expectedExpandedArgs != nil {
				assert.Equal(t, tc.expectedExpandedArgs, expandedArgs.Format())
			}
		})
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

	parsedArgs, err := ParseArgs([]string{"build", "--config=a"})
	require.NoError(t, err)
	_, err = resolveArgs(parsedArgs, ws)

	assert.Contains(t, err.Error(), "circular --config reference detected: a -> b -> c -> a")

	parsedArgs, err = ParseArgs([]string{"build", "--config=d"})
	require.NoError(t, err)
	_, err = resolveArgs(parsedArgs, ws)
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

	p, err := GetParser()
	require.NoError(t, err)
	args, err := p.ParseArgs([]string{"build"})
	require.NoError(t, err)
	_, _, err = p.consumeAndParseRCFiles(args, ws)
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
			parsedArgs, err := ParseArgs(test.args)
			require.NoError(t, err)
			expandedArgs, err := resolveArgs(parsedArgs, ws)

			require.NoError(t, err, "error expanding %s", test.args)
			assert.Equal(t, test.expectedExpandedArgs, expandedArgs.Format())
		})
	}
}

func TestCanonicalizeArgs(t *testing.T) {
	// Use some args that look like bazel commands but are actually
	// specifying flag values.
	testcases := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name: "Test canonicalize command options",
			input: []string{
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
				"--remote_download_minimal=value",
				"--noexperimental_convenience_symlinks",
				"--subcommands=pretty_print",
			},
			expected: []string{
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
				"--remote_download_minimal",
				"--noexperimental_convenience_symlinks",
				"--subcommands=pretty_print",
			},
		},
		{
			name: "Test canonicalize startup options",
			input: []string{
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
			},
			expected: []string{
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
				"--compilation_mode=opt",
				"--another_unknown_plugin_flag",
				"--nocache_test_results",
				"--bes_backend=",
				"--remote_header=x-buildbuddy-foo=1",
				"--remote_header=x-buildbuddy-bar=2",
				"hello",
			},
		},
		{
			name: "Test canonicalize exec args",
			input: []string{
				"--output_base", "build",
				"run",
				"//:some_target",
				"--",
				"cmd",
				"-foo=bar",
			},
			expected: []string{
				"--output_base=build",
				"run",
				"//:some_target",
				"--",
				"cmd",
				"-foo=bar",
			},
		},
		{
			name: "Test canonicalize negative target",
			input: []string{
				"--output_base", "query",
				"build",
				"//...",
				"--",
				"-//:some_target",
			},
			expected: []string{
				"--output_base=query",
				"build",
				"--",
				"//...",
				"-//:some_target",
			},
		},
		{
			name: "Test canonicalize negative target and exec args",
			input: []string{
				"--output_base", "build",
				"run",
				"--",
				"-//:some_target",
				"cmd",
				"-foo=bar",
			},
			expected: []string{
				"--output_base=build",
				"run",
				"--",
				"-//:some_target",
				"cmd",
				"-foo=bar",
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := GetParser()
			require.NoError(t, err)
			canonicalArgs, err := p.canonicalizeArgs(tc.input)

			require.NoError(t, err)
			assert.Equal(t, tc.expected, canonicalArgs)
		})
	}
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
	parsedArgs, err := ParseArgs(args)
	require.NoError(t, err)
	expandedArgs, err := resolveArgs(parsedArgs, ws)

	require.NoError(t, err, "error expanding %s", args)
	assert.Equal(t, expectedExpandedArgs, expandedArgs.Format())
}

func TestCommonPositionalArgument(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"WORKSPACE": "",
		".bazelrc":  "common foo",
	})

	args := []string{
		"build",
	}

	expectedExpandedArgs := []string{
		"--ignore_all_rc_files",
		"build",
		"foo",
	}
	parsedArgs, err := ParseArgs(args)
	require.NoError(t, err)
	expandedArgs, err := resolveArgs(parsedArgs, ws)

	require.NoError(t, err, "error expanding %s", args)
	assert.Equal(t, expectedExpandedArgs, expandedArgs.Format())
}

func TestBazelrcLexing(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"WORKSPACE": "",
		".bazelrc":  "common targetwitha#",
	})

	args := []string{
		"build",
	}

	expectedExpandedArgs := []string{
		"--ignore_all_rc_files",
		"build",
		"targetwitha#",
	}
	parsedArgs, err := ParseArgs(args)
	require.NoError(t, err)
	expandedArgs, err := resolveArgs(parsedArgs, ws)

	require.NoError(t, err, "error expanding %s", args)
	assert.Equal(t, expectedExpandedArgs, expandedArgs.Format())
}

func TestGetRemoteHeaderVal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		args      []string
		headerKey string
		expected  string
	}{
		{
			name:      "Multiple headers",
			args:      []string{"build", "//...", "--remote_header=unrelated-header-key=XXXXX", "--remote_header=x-buildbuddy-api-key=abc123"},
			headerKey: "x-buildbuddy-api-key",
			expected:  "abc123",
		},
		{
			name:      "Header not set",
			args:      []string{"build", "//...", "--remote_header=x-buildbuddy-api-key=abc123"},
			headerKey: "unrelated-header-key",
			expected:  "",
		},
		{
			name:      "No remote headers",
			args:      []string{"build", "//..."},
			headerKey: "x-buildbuddy-api-key",
			expected:  "",
		},
		{
			name:      "Returns last value set",
			args:      []string{"build", "--remote_header=x-buildbuddy-api-key=first", "--remote_header=x-buildbuddy-api-key=second"},
			headerKey: "x-buildbuddy-api-key",
			expected:  "second",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsedArgs, err := ParseArgs(tc.args)
			require.NoError(t, err)

			result := GetRemoteHeaderVal(parsedArgs, tc.headerKey)
			assert.Equal(t, tc.expected, result)
		})
	}
}

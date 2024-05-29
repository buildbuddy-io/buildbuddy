package redact_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
)

func fileWithURI(uri string) *bespb.File {
	return &bespb.File{
		Name: "foo.txt",
		File: &bespb.File_Uri{Uri: uri},
	}
}

func structuredCommandLineEvent(option *clpb.Option) *bespb.BuildEvent {
	section := &clpb.CommandLineSection{
		SectionLabel: "command",
		SectionType: &clpb.CommandLineSection_OptionList{
			OptionList: &clpb.OptionList{
				Option: []*clpb.Option{option},
			},
		},
	}
	commandLine := &clpb.CommandLine{
		CommandLineLabel: "label",
		Sections:         []*clpb.CommandLineSection{section},
	}
	return &bespb.BuildEvent{
		Payload: &bespb.BuildEvent_StructuredCommandLine{
			StructuredCommandLine: commandLine,
		},
	}
}

func getCommandLineOptions(event *bespb.BuildEvent) []*clpb.Option {
	p, ok := event.Payload.(*bespb.BuildEvent_StructuredCommandLine)
	if !ok {
		return nil
	}
	sections := p.StructuredCommandLine.Sections
	if len(sections) == 0 {
		return nil
	}
	s, ok := sections[0].SectionType.(*clpb.CommandLineSection_OptionList)
	if !ok {
		return nil
	}
	return s.OptionList.Option
}

func TestRedactMetadata_BuildStarted_RedactsOptionsDescription(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	buildStarted := &bespb.BuildStarted{
		OptionsDescription: `--some_flag --another_flag`,
	}

	err := redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Started{Started: buildStarted},
	})
	require.NoError(t, err)

	assert.Equal(t, "<REDACTED>", buildStarted.OptionsDescription)
}

func TestRedactMetadata_UnstructuredCommandLine_RemovesArgs(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	unstructuredCommandLine := &bespb.UnstructuredCommandLine{
		Args: []string{"foo"},
	}

	err := redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_UnstructuredCommandLine{
			UnstructuredCommandLine: unstructuredCommandLine,
		},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{}, unstructuredCommandLine.Args)
}

func TestRedactMetadata_StructuredCommandLine(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	// Started event specified which env vars shouldn't be redacted.
	buildStarted := &bespb.BuildStarted{
		OptionsDescription: "--build_metadata='ALLOW_ENV=FOO_ALLOWED,BAR_ALLOWED_PATTERN_*'",
	}
	err := redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Started{Started: buildStarted},
	})
	require.NoError(t, err)

	for _, testCase := range []struct {
		optionName    string
		inputValue    string
		expectedValue string
	}{
		{"client_env", "SECRET=codez", "SECRET=<REDACTED>"},
		{"client_env", "GITHUB_REPOSITORY=https://username:password@github.com/foo/bar", "GITHUB_REPOSITORY=https://github.com/foo/bar"},
		{"client_env", "FOO_ALLOWED=bar", "FOO_ALLOWED=bar"},
		{"client_env", "BAR_ALLOWED_PATTERN_XYZ=qux", "BAR_ALLOWED_PATTERN_XYZ=qux"},
		{"remote_header", "x-buildbuddy-api-key=abc123", "<REDACTED>"},
		{"remote_cache_header", "x-buildbuddy-api-key=abc123", "<REDACTED>"},
		{"some_url", "https://token@foo.com", "https://foo.com"},
		{"remote_default_exec_properties", "container-registry-username=SECRET_USERNAME", "container-registry-username=<REDACTED>"},
		{"remote_default_exec_properties", "container-registry-password=SECRET_PASSWORD", "container-registry-password=<REDACTED>"},
		{"host_platform", "@buildbuddy_toolchain//:platform", "@buildbuddy_toolchain//:platform"},
		{"build_metadata", "PATTERN=@//foo,NAME=@foo,PASSWORD=SECRET@bar,BAZ=", "PATTERN=@//foo,NAME=@foo,PASSWORD=bar,BAZ="},
		{"build_metadata", "FOO=A=1,BAR=SECRET=SECRET@buildbuddy.io", "FOO=A=1,BAR=buildbuddy.io"},
		{"some_other_flag", "PATTERN=@//foo", "//foo"},
	} {
		option := &clpb.Option{
			OptionName:   testCase.optionName,
			OptionValue:  testCase.inputValue,
			CombinedForm: fmt.Sprintf("--%s=%s", testCase.optionName, testCase.inputValue),
		}

		err := redactor.RedactMetadata(structuredCommandLineEvent(option))
		require.NoError(t, err)

		expectedCombinedForm := fmt.Sprintf("--%s=%s", testCase.optionName, testCase.expectedValue)
		assert.Equal(t, expectedCombinedForm, option.CombinedForm)
		assert.Equal(t, testCase.expectedValue, option.OptionValue)
	}

	// default_override flags should be dropped altogether.

	option := &clpb.Option{
		OptionName:   "default_override",
		OptionValue:  "1:build:remote=--remote_default_exec_properties=container-registry-password=SECRET",
		CombinedForm: "--default_override=1:build:remote=--remote_default_exec_properties=container-registry-password=SECRET",
	}
	event := structuredCommandLineEvent(option)
	assert.NotEmpty(t, getCommandLineOptions(event), "sanity check: --default_override should be an option before redacting")

	err = redactor.RedactMetadata(event)
	require.NoError(t, err)

	assert.Empty(t, getCommandLineOptions(event), "--default_override options should be removed")

	// EXPLICIT_COMMAND_LINE flags should be dropped altogether.

	option = &clpb.Option{
		OptionName:   "build_metadata",
		OptionValue:  `EXPLICIT_COMMAND_LINE=["secrets"]`,
		CombinedForm: `--build_metadata=EXPLICIT_COMMAND_LINE=["secrets"]`,
	}
	event = structuredCommandLineEvent(option)
	assert.NotEmpty(t, getCommandLineOptions(event), "sanity check: EXPLICIT_COMMAND_LINE should be an option before redacting")

	err = redactor.RedactMetadata(event)
	require.NoError(t, err)

	assert.Empty(t, getCommandLineOptions(event), "EXPLICIT_COMMAND_LINE should be removed")
}

func TestRedactMetadata_OptionsParsed_StripsURLSecretsAndRemoteHeaders(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	optionsParsed := &bespb.OptionsParsed{
		CmdLine: []string{
			"213wZJyTUyhXkj381312@foo",
			"--flag=@repo//package",
			"--remote_header=x-buildbuddy-platform.container-registry-password=TOPSECRET",
			"--remote_exec_header=x-buildbuddy-platform.container-registry-password=TOPSECRET2",
			"--bes_header=foo=TOPSECRET",
			"--build_metadata=PATTERN=@//foo,NAME=@bar,SECRET=TOPSECRET@",
			"--some_other_flag=SUBFLAG=@//foo",
			"--build_metadata=EXPLICIT_COMMAND_LINE=[\"SECRET\"]",
		},
		ExplicitCmdLine: []string{
			"213wZJyTUyhXkj381312@explicit",
			"--flag=@repo//package",
			"--remote_header=x-buildbuddy-platform.container-registry-password=TOPSECRET_EXPLICIT",
			"--remote_exec_header=x-buildbuddy-platform.container-registry-password=TOPSECRET2_EXPLICIT",
			"--bes_header=foo=TOPSECRET",
			"--build_metadata=PATTERN=@//foo,NAME=@bar,SECRET=TOPSECRET_EXPLICIT@",
			"--some_other_flag=SUBFLAG=@//foo",
			"--build_metadata=EXPLICIT_COMMAND_LINE=[\"SECRET\"]",
		},
	}

	err := redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_OptionsParsed{OptionsParsed: optionsParsed},
	})
	require.NoError(t, err)

	assert.Equal(
		t,
		[]string{
			"foo",
			"--flag=@repo//package",
			"--remote_header=<REDACTED>",
			"--remote_exec_header=<REDACTED>",
			"--bes_header=<REDACTED>",
			"--build_metadata=PATTERN=@//foo,NAME=@bar,SECRET=",
			"--some_other_flag=//foo",
			"",
		},
		optionsParsed.CmdLine)
	assert.Equal(
		t,
		[]string{
			"explicit",
			"--flag=@repo//package",
			"--remote_header=<REDACTED>",
			"--remote_exec_header=<REDACTED>",
			"--bes_header=<REDACTED>",
			"--build_metadata=PATTERN=@//foo,NAME=@bar,SECRET=",
			"--some_other_flag=//foo",
			"",
		},
		optionsParsed.ExplicitCmdLine)
}

func TestRedactMetadata_ActionExecuted_StripsURLSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	actionExecuted := &bespb.ActionExecuted{
		Stdout:             fileWithURI("213wZJyTUyhXkj381312@uri"),
		Stderr:             fileWithURI("213wZJyTUyhXkj381312@uri"),
		PrimaryOutput:      fileWithURI("213wZJyTUyhXkj381312@uri"),
		ActionMetadataLogs: []*bespb.File{fileWithURI("213wZJyTUyhXkj381312@uri")},
	}

	err := redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Action{Action: actionExecuted},
	})
	require.NoError(t, err)

	assert.Equal(t, "uri", actionExecuted.Stdout.GetUri())
	assert.Equal(t, "uri", actionExecuted.Stderr.GetUri())
	assert.Equal(t, "uri", actionExecuted.PrimaryOutput.GetUri())
	assert.Equal(t, "uri", actionExecuted.ActionMetadataLogs[0].GetUri())
}

func TestRedactMetadata_NamedSetOfFiles_StripsURLSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	namedSetOfFiles := &bespb.NamedSetOfFiles{
		Files: []*bespb.File{fileWithURI("213wZJyTUyhXkj381312@uri")},
	}

	err := redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_NamedSetOfFiles{NamedSetOfFiles: namedSetOfFiles},
	})
	require.NoError(t, err)

	assert.Equal(t, "uri", namedSetOfFiles.Files[0].GetUri())
}

func TestRedactMetadata_TargetComplete_StripsURLSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	targetComplete := &bespb.TargetComplete{
		Success:         true,
		DirectoryOutput: []*bespb.File{fileWithURI("213wZJyTUyhXkj381312@uri")},
	}

	err := redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Completed{Completed: targetComplete},
	})
	require.NoError(t, err)

	assert.Equal(t, "uri", targetComplete.DirectoryOutput[0].GetUri())
}

func TestRedactMetadata_TestResult_StripsURLSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	testResult := &bespb.TestResult{
		Status:           bespb.TestStatus_PASSED,
		TestActionOutput: []*bespb.File{fileWithURI("213wZJyTUyhXkj381312@uri")},
	}

	err := redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_TestResult{TestResult: testResult},
	})
	require.NoError(t, err)

	assert.Equal(t, "uri", testResult.TestActionOutput[0].GetUri())
}

func TestRedactMetadata_TestSummary_StripsURLSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	testSummary := &bespb.TestSummary{
		Passed: []*bespb.File{fileWithURI("213wZJyTUyhXkj381312@uri")},
		Failed: []*bespb.File{fileWithURI("213wZJyTUyhXkj381312@uri")},
	}

	err := redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_TestSummary{TestSummary: testSummary},
	})
	require.NoError(t, err)

	assert.Equal(t, "uri", testSummary.Passed[0].GetUri())
	assert.Equal(t, "uri", testSummary.Failed[0].GetUri())
}

func TestRedactMetadata_BuildMetadata_StripsURLSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	buildMetadata := &bespb.BuildMetadata{
		Metadata: map[string]string{
			"ALLOW_ENV":             "SHELL",
			"ROLE":                  "METADATA_CI",
			"REPO_URL":              "https://USERNAME:PASSWORD@github.com/buildbuddy-io/metadata_repo_url",
			"EXPLICIT_COMMAND_LINE": `["--remote_header=x-buildbuddy-platform.container-registry-password=SECRET", "--foo=SAFE"]`,
		},
	}

	err := redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_BuildMetadata{BuildMetadata: buildMetadata},
	})
	require.NoError(t, err)

	assert.Equal(t, "https://github.com/buildbuddy-io/metadata_repo_url", buildMetadata.Metadata["REPO_URL"])
	assert.Equal(t, `["--remote_header=\u003cREDACTED\u003e","--foo=SAFE"]`, buildMetadata.Metadata["EXPLICIT_COMMAND_LINE"])
}

func TestRedactMetadata_WorkspaceStatus_StripsRepoURLCredentials(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	workspaceStatus := &bespb.WorkspaceStatus{
		Item: []*bespb.WorkspaceStatus_Item{
			{Key: "REPO_URL", Value: "https://USERNAME:PASSWORD@github.com/buildbuddy-io/metadata_repo_url"},
		},
	}

	err := redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_WorkspaceStatus{WorkspaceStatus: workspaceStatus},
	})
	require.NoError(t, err)

	require.Equal(t, 1, len(workspaceStatus.Item))
	assert.Equal(t, "https://github.com/buildbuddy-io/metadata_repo_url", workspaceStatus.Item[0].Value)
}

func TestRedactAPIKey(t *testing.T) {
	apiKey := "abc123"
	notAPIKey := "Hello world!"

	// Declare a list of events to be redacted, where each event contains an
	// API key and a string not containing an API key.
	events := []*bespb.BuildEvent{
		// API key appearing as struct field value
		{Payload: &bespb.BuildEvent_Progress{Progress: &bespb.Progress{
			Stdout: notAPIKey,
			Stderr: apiKey,
		}}},
		// API key appearing as repeated field value
		{Payload: &bespb.BuildEvent_UnstructuredCommandLine{UnstructuredCommandLine: &bespb.UnstructuredCommandLine{
			Args: []string{notAPIKey, apiKey},
		}}},
		// API key appearing as map value
		{Payload: &bespb.BuildEvent_BuildMetadata{BuildMetadata: &bespb.BuildMetadata{
			Metadata: map[string]string{
				"FOO": apiKey,
				"BAR": notAPIKey,
			},
		}}},
		// API key appearing as map key
		{Payload: &bespb.BuildEvent_BuildMetadata{BuildMetadata: &bespb.BuildMetadata{
			Metadata: map[string]string{
				apiKey: notAPIKey,
			},
		}}},
		// API key appearing as a string nested inside a list of structs
		{Payload: &bespb.BuildEvent_Expanded{Expanded: &bespb.PatternExpanded{
			TestSuiteExpansions: []*bespb.PatternExpanded_TestSuiteExpansion{
				{
					SuiteLabel: apiKey + "_" + notAPIKey,
					TestLabels: []string{apiKey, notAPIKey},
				},
			},
		}}},
	}
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	ctx := context.WithValue(context.Background(), "x-buildbuddy-api-key", apiKey)

	for _, e := range events {
		err := redactor.RedactAPIKey(ctx, e)

		require.NoError(t, err)
		b, err := protojson.MarshalOptions{Multiline: true}.Marshal(e)
		require.NoError(t, err)
		json := string(b)
		require.NotContains(t, json, apiKey)
		require.Contains(t, json, "<REDACTED>")
		require.Contains(t, json, notAPIKey)
	}
}

func TestRedactRunResidual(t *testing.T) {
	for _, tc := range []struct {
		name     string
		command  string
		given    []string
		expected []string
	}{
		{
			name:     "withoutSecret",
			command:  "run",
			given:    []string{"-foo=bar", "-baz"},
			expected: []string{"-foo=bar", "-baz"},
		},
		{
			name:     "withNoSplit",
			command:  "run",
			given:    []string{"-api_key=foobar", "-baz"},
			expected: []string{"-api_key=<REDACTED>", "-baz"},
		},
		{
			name:     "withSplit",
			command:  "run",
			given:    []string{"-api_key", "foobar", "-baz"},
			expected: []string{"-api_key", "<REDACTED>", "-baz"},
		},
		{
			name:     "MultipleEqualSigns",
			command:  "run",
			given:    []string{"-api_key=foo=bar=laz", "-baz"},
			expected: []string{"-api_key=<REDACTED>", "-baz"},
		},
		{
			name:     "MultipleEqualSigns",
			command:  "run",
			given:    []string{"-api_key=foo=bar=laz", "-baz"},
			expected: []string{"-api_key=<REDACTED>", "-baz"},
		},
		{
			name:     "withSecret",
			command:  "run",
			given:    []string{"--key", "foobar", "-secret", "loremsumip"},
			expected: []string{"--key", "<REDACTED>", "-secret", "<REDACTED>"},
		},
		{
			name:     "withPass",
			command:  "run",
			given:    []string{"--password=foobar", "--pass=loremsumip"},
			expected: []string{"--password=<REDACTED>", "--pass=<REDACTED>"},
		},
		{
			name:     "withToken",
			command:  "run",
			given:    []string{"--token=foobar", "--baz=loremsumip"},
			expected: []string{"--token=<REDACTED>", "--baz=loremsumip"},
		},
		{
			name:     "withToken",
			command:  "build",
			given:    []string{"//api_keys/...", "//foo/..."},
			expected: []string{"//api_keys/...", "//foo/..."},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))

			chunkList := &clpb.ChunkList{
				Chunk: tc.given,
			}
			err := redactor.RedactMetadata(&bespb.BuildEvent{
				Payload: &bespb.BuildEvent_StructuredCommandLine{
					StructuredCommandLine: &clpb.CommandLine{
						Sections: []*clpb.CommandLineSection{
							{
								SectionLabel: "command",
								SectionType: &clpb.CommandLineSection_ChunkList{
									ChunkList: &clpb.ChunkList{
										Chunk: []string{tc.command},
									},
								},
							},
							{
								SectionLabel: "residual",
								SectionType: &clpb.CommandLineSection_ChunkList{
									ChunkList: chunkList,
								},
							},
						},
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, tc.expected, chunkList.Chunk)
		})
	}
}

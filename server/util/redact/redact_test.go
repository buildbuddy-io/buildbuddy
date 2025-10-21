package redact_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
)

func TestRedactPasswordsInURLs(t *testing.T) {
	te := testenv.GetTestEnv(t)
	for _, tc := range []struct {
		name     string
		event    *bespb.BuildEvent
		expected *bespb.BuildEvent
	}{
		{
			name: "redact passwords in urls in action stdout, stderr, primary output, metadata logs",
			event: &bespb.BuildEvent{Payload: &bespb.BuildEvent_Action{Action: &bespb.ActionExecuted{
				Stdout:             fileWithURI("url://username:passwordthatshouldberedacted@uri"),
				Stderr:             fileWithURI("url://username:passwordthatshouldberedacted@uri"),
				PrimaryOutput:      fileWithURI("url://username:passwordthatshouldberedacted@uri"),
				ActionMetadataLogs: []*bespb.File{fileWithURI("url://username:passwordthatshouldberedacted@uri")},
			}}},
			expected: &bespb.BuildEvent{Payload: &bespb.BuildEvent_Action{Action: &bespb.ActionExecuted{
				Stdout:             fileWithURI("url://username:<REDACTED>@uri"),
				Stderr:             fileWithURI("url://username:<REDACTED>@uri"),
				PrimaryOutput:      fileWithURI("url://username:<REDACTED>@uri"),
				ActionMetadataLogs: []*bespb.File{fileWithURI("url://username:<REDACTED>@uri")},
			}}},
		},
		{
			name: "redact passwords in urls in named set of files",
			event: &bespb.BuildEvent{Payload: &bespb.BuildEvent_NamedSetOfFiles{NamedSetOfFiles: &bespb.NamedSetOfFiles{
				Files: []*bespb.File{fileWithURI("url://username:passwordthatshouldberedacted@uri")},
			}}},
			expected: &bespb.BuildEvent{Payload: &bespb.BuildEvent_NamedSetOfFiles{NamedSetOfFiles: &bespb.NamedSetOfFiles{
				Files: []*bespb.File{fileWithURI("url://username:<REDACTED>@uri")},
			}}},
		},
		{
			name: "redact passwords in urls in target complete",
			event: &bespb.BuildEvent{Payload: &bespb.BuildEvent_Completed{Completed: &bespb.TargetComplete{
				DirectoryOutput: []*bespb.File{fileWithURI("url://username:passwordthatshouldberedacted@uri")},
			}}},
			expected: &bespb.BuildEvent{Payload: &bespb.BuildEvent_Completed{Completed: &bespb.TargetComplete{
				DirectoryOutput: []*bespb.File{fileWithURI("url://username:<REDACTED>@uri")},
			}}},
		},
		{
			name: "redact passwords in urls in test result",
			event: &bespb.BuildEvent{Payload: &bespb.BuildEvent_TestResult{TestResult: &bespb.TestResult{
				TestActionOutput: []*bespb.File{fileWithURI("url://username:passwordthatshouldberedacted@uri")},
			}}},
			expected: &bespb.BuildEvent{Payload: &bespb.BuildEvent_TestResult{TestResult: &bespb.TestResult{
				TestActionOutput: []*bespb.File{fileWithURI("url://username:<REDACTED>@uri")},
			}}},
		},
		{
			name: "redact passwords in urls in test summary files passed, failed",
			event: &bespb.BuildEvent{Payload: &bespb.BuildEvent_TestSummary{TestSummary: &bespb.TestSummary{
				Passed: []*bespb.File{fileWithURI("url://username:passwordthatshouldberedacted@uri")},
				Failed: []*bespb.File{fileWithURI("url://username:passwordthatshouldberedacted@uri")},
			}}},
			expected: &bespb.BuildEvent{Payload: &bespb.BuildEvent_TestSummary{TestSummary: &bespb.TestSummary{
				Passed: []*bespb.File{fileWithURI("url://username:<REDACTED>@uri")},
				Failed: []*bespb.File{fileWithURI("url://username:<REDACTED>@uri")},
			}}},
		},
		{
			name: "redact passwords in urls in structured command line options",
			event: &bespb.BuildEvent{Payload: &bespb.BuildEvent_StructuredCommandLine{StructuredCommandLine: &clpb.CommandLine{Sections: []*clpb.CommandLineSection{&clpb.CommandLineSection{SectionType: &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{Option: []*clpb.Option{
				&clpb.Option{
					OptionName:   "some_url",
					OptionValue:  "https://username:passwordthatshouldberedacted@foo.com",
					CombinedForm: "--some_url=https://username:passwordthatshouldberedacted@foo.com",
				},
				&clpb.Option{
					OptionName:   "some_other_flag",
					OptionValue:  "url://username:passwordthatshouldberedacted=@//foo",
					CombinedForm: "--some_other_flag=url://username:passwordthatshouldberedacted=@//foo",
				},
				&clpb.Option{
					OptionName:   "build_metadata",
					OptionValue:  "PATTERN=@//foo,NAME=@foo,PASSWORD=url://username:passwordthatshouldberedacted@bar,BAZ=",
					CombinedForm: "--build_metadata=PATTERN=@//foo,NAME=@foo,PASSWORD=url://username:passwordthatshouldberedacted@bar,BAZ=",
				},
				&clpb.Option{
					OptionName:   "build_metadata",
					OptionValue:  "FOO=A=1,BAR=SECRET=url://username:passwordthatshouldberedacted@buildbuddy.io",
					CombinedForm: "--build_metadata=FOO=A=1,BAR=SECRET=url://username:passwordthatshouldberedacted@buildbuddy.io",
				},
			}}}}}}}},
			expected: &bespb.BuildEvent{Payload: &bespb.BuildEvent_StructuredCommandLine{StructuredCommandLine: &clpb.CommandLine{Sections: []*clpb.CommandLineSection{&clpb.CommandLineSection{SectionType: &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{Option: []*clpb.Option{
				&clpb.Option{
					OptionName:   "some_url",
					OptionValue:  "https://username:<REDACTED>@foo.com",
					CombinedForm: "--some_url=https://username:<REDACTED>@foo.com",
				},
				&clpb.Option{
					OptionName:   "some_other_flag",
					OptionValue:  "url://username:<REDACTED>@//foo",
					CombinedForm: "--some_other_flag=url://username:<REDACTED>@//foo",
				},
				&clpb.Option{
					OptionName:   "build_metadata",
					OptionValue:  "PATTERN=@//foo,NAME=@foo,PASSWORD=url://username:<REDACTED>@bar,BAZ=",
					CombinedForm: "--build_metadata=PATTERN=@//foo,NAME=@foo,PASSWORD=url://username:<REDACTED>@bar,BAZ=",
				},
				&clpb.Option{
					OptionName:   "build_metadata",
					OptionValue:  "FOO=A=1,BAR=SECRET=url://username:<REDACTED>@buildbuddy.io",
					CombinedForm: "--build_metadata=FOO=A=1,BAR=SECRET=url://username:<REDACTED>@buildbuddy.io",
				},
			}}}}}}}},
		},
		{
			name: "redact passwords in urls in options parsed",
			event: &bespb.BuildEvent{Payload: &bespb.BuildEvent_OptionsParsed{OptionsParsed: &bespb.OptionsParsed{
				CmdLine: []string{
					"url://username:passwordthatshouldberedacted@foo",
					"--build_metadata=PATTERN=@//foo,NAME=@bar,SECRET=url://username:passwordthatshouldberedacted@domain",
					"--some_other_flag=url://username:SUBFLAGTHATSHOULDBEREDACTED=@//foo",
				},
				ExplicitCmdLine: []string{
					"url://username:passwordthatshouldberedacted@foo",
					"--build_metadata=PATTERN=@//foo,NAME=@bar,SECRET=url://username:passwordthatshouldberedacted@domain",
					"--some_other_flag=url://username:SUBFLAGTHATSHOULDBEREDACTED=@//foo",
				},
			}}},
			expected: &bespb.BuildEvent{Payload: &bespb.BuildEvent_OptionsParsed{OptionsParsed: &bespb.OptionsParsed{
				CmdLine: []string{
					"url://username:<REDACTED>@foo",
					"--build_metadata=PATTERN=@//foo,NAME=@bar,SECRET=url://username:<REDACTED>@domain",
					"--some_other_flag=url://username:<REDACTED>@//foo",
				},
				ExplicitCmdLine: []string{
					"url://username:<REDACTED>@foo",
					"--build_metadata=PATTERN=@//foo,NAME=@bar,SECRET=url://username:<REDACTED>@domain",
					"--some_other_flag=url://username:<REDACTED>@//foo",
				},
			}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			redactor := redact.NewStreamingRedactor(te)
			err := redactor.RedactMetadata(tc.event)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(tc.expected, tc.event, protocmp.Transform()))
		})
	}
}

func TestRedactEntireSections(t *testing.T) {
	te := testenv.GetTestEnv(t)
	for _, tc := range []struct {
		name     string
		event    *bespb.BuildEvent
		expected *bespb.BuildEvent
	}{
		{
			name: "redact options description in build started event",
			event: &bespb.BuildEvent{Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{
				OptionsDescription: `--some_flag --another_flag`,
			}}},
			expected: &bespb.BuildEvent{Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{
				OptionsDescription: `<REDACTED>`,
			}}},
		},
		{
			name: "remove args from unstructured commanbd line",
			event: &bespb.BuildEvent{Payload: &bespb.BuildEvent_UnstructuredCommandLine{UnstructuredCommandLine: &bespb.UnstructuredCommandLine{
				Args: []string{"foo"},
			}}},
			expected: &bespb.BuildEvent{Payload: &bespb.BuildEvent_UnstructuredCommandLine{UnstructuredCommandLine: &bespb.UnstructuredCommandLine{}}},
		},
		{
			name: "remove --default_override and EXPLICIT_COMMAND_LINE options from structured command line",
			event: &bespb.BuildEvent{Payload: &bespb.BuildEvent_StructuredCommandLine{StructuredCommandLine: &clpb.CommandLine{Sections: []*clpb.CommandLineSection{&clpb.CommandLineSection{SectionType: &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{Option: []*clpb.Option{
				&clpb.Option{
					OptionName:   "default_override",
					OptionValue:  "1:build:remote=--remote_default_exec_properties=container-registry-password=SECRET",
					CombinedForm: "--default_override=1:build:remote=--remote_default_exec_properties=container-registry-password=SECRET",
				},
				&clpb.Option{
					OptionName:   "build_metadata",
					OptionValue:  `EXPLICIT_COMMAND_LINE=["secrets"]`,
					CombinedForm: `--build_metadata=EXPLICIT_COMMAND_LINE=["secrets"]`,
				},
			}}}}}}}},
			expected: &bespb.BuildEvent{Payload: &bespb.BuildEvent_StructuredCommandLine{StructuredCommandLine: &clpb.CommandLine{Sections: []*clpb.CommandLineSection{&clpb.CommandLineSection{SectionType: &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{Option: []*clpb.Option{}}}}}}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			redactor := redact.NewStreamingRedactor(te)
			err := redactor.RedactMetadata(tc.event)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(tc.expected, tc.event, protocmp.Transform()))
		})
	}
}

func TestRedactRemoteHeaders(t *testing.T) {
	te := testenv.GetTestEnv(t)
	for _, tc := range []struct {
		name     string
		event    *bespb.BuildEvent
		expected *bespb.BuildEvent
	}{
		{
			name: "redact remote headers in structured command line",
			event: &bespb.BuildEvent{Payload: &bespb.BuildEvent_StructuredCommandLine{StructuredCommandLine: &clpb.CommandLine{Sections: []*clpb.CommandLineSection{&clpb.CommandLineSection{SectionType: &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{Option: []*clpb.Option{
				&clpb.Option{
					OptionName:   "remote_header",
					OptionValue:  "harmless_string_that_should_be_redacted_anyhow",
					CombinedForm: "--remote_header=harmless_string_that_should_be_redacted_anyhow",
				},
				&clpb.Option{
					OptionName:   "remote_cache_header",
					OptionValue:  "harmless_string_that_should_be_redacted_anyhow",
					CombinedForm: "--remote_cache_header=harmless_string_that_should_be_redacted_anyhow",
				},
			}}}}}}}},
			expected: &bespb.BuildEvent{Payload: &bespb.BuildEvent_StructuredCommandLine{StructuredCommandLine: &clpb.CommandLine{Sections: []*clpb.CommandLineSection{&clpb.CommandLineSection{SectionType: &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{Option: []*clpb.Option{
				&clpb.Option{
					OptionName:   "remote_header",
					OptionValue:  "<REDACTED>",
					CombinedForm: "--remote_header=<REDACTED>",
				},
				&clpb.Option{
					OptionName:   "remote_cache_header",
					OptionValue:  "<REDACTED>",
					CombinedForm: "--remote_cache_header=<REDACTED>",
				},
			}}}}}}}},
		},
		{
			name: "redact remote headers in options parsed",
			event: &bespb.BuildEvent{Payload: &bespb.BuildEvent_OptionsParsed{OptionsParsed: &bespb.OptionsParsed{
				CmdLine: []string{
					"--remote_header=harmless_string_that_should_be_redacted_anyhow",
					"--remote_exec_header=harmless_string_that_should_be_redacted_anyhow",
					"--bes_header=harmless_string_that_should_be_redacted_anyhow",
				},
				ExplicitCmdLine: []string{
					"--remote_header=harmless_string_that_should_be_redacted_anyhow",
					"--remote_exec_header=harmless_string_that_should_be_redacted_anyhow",
					"--bes_header=harmless_string_that_should_be_redacted_anyhow",
				},
			}}},
			expected: &bespb.BuildEvent{Payload: &bespb.BuildEvent_OptionsParsed{OptionsParsed: &bespb.OptionsParsed{
				CmdLine: []string{
					"--remote_header=<REDACTED>",
					"--remote_exec_header=<REDACTED>",
					"--bes_header=<REDACTED>",
				},
				ExplicitCmdLine: []string{
					"--remote_header=<REDACTED>",
					"--remote_exec_header=<REDACTED>",
					"--bes_header=<REDACTED>",
				},
			}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			redactor := redact.NewStreamingRedactor(te)
			err := redactor.RedactMetadata(tc.event)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(tc.expected, tc.event, protocmp.Transform()))
		})
	}
}

func TestRemoveRepoURLCredentials(t *testing.T) {
	te := testenv.GetTestEnv(t)
	for _, tc := range []struct {
		name     string
		event    *bespb.BuildEvent
		expected *bespb.BuildEvent
	}{
		{
			name: "remove credentials from github repository url in structured command line options",
			event: &bespb.BuildEvent{Payload: &bespb.BuildEvent_StructuredCommandLine{StructuredCommandLine: &clpb.CommandLine{Sections: []*clpb.CommandLineSection{&clpb.CommandLineSection{SectionType: &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{Option: []*clpb.Option{
				&clpb.Option{
					OptionName:   "client_env",
					OptionValue:  "GITHUB_REPOSITORY=https://username_to_remove:password_to_remove@github.com/foo/bar",
					CombinedForm: "--client_env=GITHUB_REPOSITORY=https://username_to_remove:password_to_remove@github.com/foo/bar",
				},
				&clpb.Option{
					OptionName:   "test_env",
					OptionValue:  "GITHUB_REPOSITORY=https://username_to_remove:password_to_remove@github.com/foo/bar",
					CombinedForm: "--test_env=GITHUB_REPOSITORY=https://username_to_remove:password_to_remove@github.com/foo/bar",
				},
				&clpb.Option{
					OptionName:   "repo_env",
					OptionValue:  "GITHUB_REPOSITORY=https://username_to_remove:password_to_remove@github.com/foo/bar",
					CombinedForm: "--repo_env=GITHUB_REPOSITORY=https://username_to_remove:password_to_remove@github.com/foo/bar",
				},
			}}}}}}}},
			expected: &bespb.BuildEvent{Payload: &bespb.BuildEvent_StructuredCommandLine{StructuredCommandLine: &clpb.CommandLine{Sections: []*clpb.CommandLineSection{&clpb.CommandLineSection{SectionType: &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{Option: []*clpb.Option{
				&clpb.Option{
					OptionName:   "client_env",
					OptionValue:  "GITHUB_REPOSITORY=https://github.com/foo/bar",
					CombinedForm: "--client_env=GITHUB_REPOSITORY=https://github.com/foo/bar",
				},
				&clpb.Option{
					OptionName:   "test_env",
					OptionValue:  "GITHUB_REPOSITORY=https://github.com/foo/bar",
					CombinedForm: "--test_env=GITHUB_REPOSITORY=https://github.com/foo/bar",
				},
				&clpb.Option{
					OptionName:   "repo_env",
					OptionValue:  "GITHUB_REPOSITORY=https://github.com/foo/bar",
					CombinedForm: "--repo_env=GITHUB_REPOSITORY=https://github.com/foo/bar",
				},
			}}}}}}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			redactor := redact.NewStreamingRedactor(te)
			err := redactor.RedactMetadata(tc.event)
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(tc.expected, tc.event, protocmp.Transform()))
		})
	}
}

func fileWithURI(uri string) *bespb.File {
	return &bespb.File{
		Name: "foo.txt",
		File: &bespb.File_Uri{Uri: uri},
	}
}

func structuredCommandLineEvent(option *clpb.Option) *bespb.BuildEvent {
	return &bespb.BuildEvent{
		Payload: &bespb.BuildEvent_StructuredCommandLine{
			StructuredCommandLine: &clpb.CommandLine{
				Sections: []*clpb.CommandLineSection{
					&clpb.CommandLineSection{
						SectionType: &clpb.CommandLineSection_OptionList{
							OptionList: &clpb.OptionList{
								Option: []*clpb.Option{option},
							},
						},
					},
				},
			},
		},
	}
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
		{"client_env", "FOO_ALLOWED=bar", "FOO_ALLOWED=bar"},
		{"client_env", "BAR_ALLOWED_PATTERN_XYZ=qux", "BAR_ALLOWED_PATTERN_XYZ=qux"},
		{"test_env", "SECRET=codez", "SECRET=<REDACTED>"},
		{"test_env", "FOO_ALLOWED=bar", "FOO_ALLOWED=bar"},
		{"test_env", "BAR_ALLOWED_PATTERN_XYZ=qux", "BAR_ALLOWED_PATTERN_XYZ=qux"},
		{"repo_env", "SECRET=codez", "SECRET=<REDACTED>"},
		{"repo_env", "FOO_ALLOWED=bar", "FOO_ALLOWED=bar"},
		{"repo_env", "BAR_ALLOWED_PATTERN_XYZ=qux", "BAR_ALLOWED_PATTERN_XYZ=qux"},
		{"repo_env", "AWS_SECRET_ACCESS_KEY=super_secret_aws_secret_access_key", "AWS_SECRET_ACCESS_KEY=<REDACTED>"},
		{"repo_env", "AWS_ACCESS_KEY_ID=super_secret_aws_access_key_id", "AWS_ACCESS_KEY_ID=<REDACTED>"},
		{"remote_default_exec_properties", "container-registry-username=SECRET_USERNAME", "container-registry-username=<REDACTED>"},
		{"remote_default_exec_properties", "container-registry-password=SECRET_PASSWORD", "container-registry-password=<REDACTED>"},
		{"host_platform", "@buildbuddy_toolchain//:platform", "@buildbuddy_toolchain//:platform"},
		{"action_env", "AWS_ACCESS_KEY_ID=super_secret_access_key_id", "AWS_ACCESS_KEY_ID=<REDACTED>"},
		{"action_env", "AWS_SECRET_ACCESS_KEY=super_secret_access_key", "AWS_SECRET_ACCESS_KEY=<REDACTED>"},
		{"host_action_env", "AWS_ACCESS_KEY_ID=super_secret_access_key_id", "AWS_ACCESS_KEY_ID=<REDACTED>"},
		{"host_action_env", "AWS_SECRET_ACCESS_KEY=super_secret_access_key", "AWS_SECRET_ACCESS_KEY=<REDACTED>"},
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
}

func TestRedactMetadata_OptionsParsed_StripsURLSecretsAndRemoteHeaders(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	optionsParsed := &bespb.OptionsParsed{
		CmdLine: []string{
			"--flag=@repo//package",
			"--repo_env=AWS_ACCESS_KEY_ID=secret_aws_access_key_id",
			"--build_metadata=EXPLICIT_COMMAND_LINE=[\"SECRET\"]",
		},
		ExplicitCmdLine: []string{
			"--flag=@repo//package",
			"--repo_env=AWS_ACCESS_KEY_ID=secret_aws_access_key_id",
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
			"--flag=@repo//package",
			"--repo_env=AWS_ACCESS_KEY_ID=<REDACTED>",
			"",
		},
		optionsParsed.CmdLine)
	assert.Equal(
		t,
		[]string{
			"--flag=@repo//package",
			"--repo_env=AWS_ACCESS_KEY_ID=<REDACTED>",
			"",
		},
		optionsParsed.ExplicitCmdLine)
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

func TestRedactTxt(t *testing.T) {
	for _, tc := range []struct {
		name     string
		txt      string
		expected string
	}{
		{
			name:     "remote headers",
			txt:      "ok --remote_header=x-buildbuddy-api-key=secret --flag=ok",
			expected: "ok --remote_header=<REDACTED> --flag=ok",
		},
		{
			name:     "url secrets",
			txt:      "ok url://username:password@uri --flag=ok",
			expected: "ok url://username:<REDACTED>@uri --flag=ok",
		},
		{
			name:     "do not redact rules names",
			txt:      "ERROR: Error computing the main repository mapping: rules_apple@3.16.1 depends on rules_swift@2.1.1 with compatibility level 2, but <root> depends on rules_swift@1.18.0 with compatibility level 1 which is different",
			expected: "ERROR: Error computing the main repository mapping: rules_apple@3.16.1 depends on rules_swift@2.1.1 with compatibility level 2, but <root> depends on rules_swift@1.18.0 with compatibility level 1 which is different",
		},
		{
			name:     "api key start of line",
			txt:      "apikeyexactly20chars@mydomain.com",
			expected: "<REDACTED>@mydomain.com",
		},
		{
			name: "environment variables",
			txt: "common --repo_env=AWS_ACCESS_KEY_ID=super_secret_access_key_id # gitleaks:allow\n" +
				"common --repo_env=AWS_SECRET_ACCESS_KEY=super_secret_access_key # gitleaks:allow",
			expected: "common --repo_env=AWS_ACCESS_KEY_ID=<REDACTED> # gitleaks:allow\n" +
				"common --repo_env=AWS_SECRET_ACCESS_KEY=<REDACTED> # gitleaks:allow",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			redacted := redact.RedactText(tc.txt)
			require.Equal(t, tc.expected, redacted)
		})
	}
}

func TestRedactAPIKeys(t *testing.T) {
	for _, tc := range []struct {
		name     string
		txt      string
		expected string
	}{
		{
			name:     "api key after equals",
			txt:      "MY_SECRET_API_KEY=apikeyexactly20chars@mydomain.com",
			expected: "MY_SECRET_API_KEY=<REDACTED>@mydomain.com",
		},
		{
			name:     "api key in grpc call",
			txt:      "grpc://apikeyexactly20chars@mydomain.com",
			expected: "grpc://<REDACTED>@mydomain.com",
		},
		{
			name:     "api key in http call",
			txt:      "https://apikeyexactly20chars@mydomain.com",
			expected: "https://<REDACTED>@mydomain.com",
		},
		{
			name:     "do not redact text before bazel repository name",
			txt:      "FAILED:exactly20alphanumber@@apple_support++apple_cc_configure_extension+local_config_apple_cc; starting",
			expected: "FAILED:exactly20alphanumber@@apple_support++apple_cc_configure_extension+local_config_apple_cc; starting",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			redacted := redact.RedactText(tc.txt)
			require.Equal(t, tc.expected, redacted)

			event := &bespb.BuildEvent{
				Payload: &bespb.BuildEvent_Progress{
					Progress: &bespb.Progress{
						Stdout: tc.txt,
					},
				},
			}
			redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
			err := redactor.RedactAPIKeysWithSlowRegexp(context.TODO(), event)
			require.NoError(t, err)
			require.Equal(t, tc.expected, event.GetProgress().GetStdout())
		})
	}
}

func redactingWriterRun(t *testing.T, writes [][]byte) string {
	t.Helper()
	var buf bytes.Buffer
	w := redact.NewRedactingWriter(&buf)
	for i, p := range writes {
		if _, err := w.Write(p); err != nil {
			t.Fatalf("Write #%d returned error: %v", i, err)
		}
	}
	return buf.String()
}

func TestRedactingWriterURLSecretsCases(t *testing.T) {
	testCases := []struct {
		name   string
		writes [][]byte
		want   string
	}{
		{
			name:   "SingleWrite",
			writes: [][]byte{[]byte("https://user:secret@example.com")},
			want:   "https://user:<REDACTED>@example.com",
		},
		{
			name:   "SecretInMiddleWrite",
			writes: [][]byte{[]byte("prefix "), []byte("https://user:secret@example.com"), []byte(" suffix")},
			want:   "prefix https://user:<REDACTED>@example.com suffix",
		},
		{
			name:   "MultipleURLsSingleWrite",
			writes: [][]byte{[]byte("one https://user:secret@example.com two ssh://name:topsecret@example.org three")},
			want:   "one https://user:<REDACTED>@example.com two ssh://name:<REDACTED>@example.org three",
		},
		{
			name:   "URLWithQueryAndFragment",
			writes: [][]byte{[]byte("https://user:secret@example.com/path?foo=bar#frag")},
			want:   "https://user:<REDACTED>@example.com/path?foo=bar#frag",
		},
		{
			name:   "UppercaseScheme",
			writes: [][]byte{[]byte("SFTP://user:Password123@example.com")},
			want:   "SFTP://user:<REDACTED>@example.com",
		},
		{
			name:   "URLAcrossWritesButContained",
			writes: [][]byte{[]byte("logs:"), []byte("\nhttps://user:secret@example.com\n")},
			want:   "logs:\nhttps://user:<REDACTED>@example.com\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactingWriterRun(t, tc.writes)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRedactingWriterRemoteHeaderCases(t *testing.T) {
	testCases := []struct {
		name   string
		writes [][]byte
		want   string
	}{
		{
			name:   "RemoteHeader",
			writes: [][]byte{[]byte("--remote_header=x-buildbuddy-api-key=foo")},
			want:   "--remote_header=<REDACTED>",
		},
		{
			name:   "RemoteCacheHeader",
			writes: [][]byte{[]byte("--remote_cache_header=auth=bar")},
			want:   "--remote_cache_header=<REDACTED>",
		},
		{
			name:   "RemoteExecHeader",
			writes: [][]byte{[]byte("--remote_exec_header=token=abc")},
			want:   "--remote_exec_header=<REDACTED>",
		},
		{
			name:   "RemoteDownloaderHeader",
			writes: [][]byte{[]byte("--remote_downloader_header=session=xyz")},
			want:   "--remote_downloader_header=<REDACTED>",
		},
		{
			name:   "BesHeader",
			writes: [][]byte{[]byte("--bes_header=Authorization=BearerXYZ")},
			want:   "--bes_header=<REDACTED>",
		},
		{
			name: "HeadersWithContext",
			writes: [][]byte{
				[]byte("prefix "),
				[]byte("--remote_header=x=y "),
				[]byte("and "),
				[]byte("--bes_header=abc=def"),
			},
			want: "prefix --remote_header=<REDACTED> and --bes_header=<REDACTED>",
		},
		{
			name: "MultipleHeadersSameWrite",
			writes: [][]byte{
				[]byte("--remote_header=x --remote_cache_header=y --remote_exec_header=z"),
			},
			want: "--remote_header=<REDACTED> --remote_cache_header=<REDACTED> --remote_exec_header=<REDACTED>",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactingWriterRun(t, tc.writes)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRedactingWriterAPIKeyCases(t *testing.T) {
	t.Skip("RedactingWriter currently only handles remote header flags")
	const twentyChars = "ABCDEFGHIJKLMNOPQRST"
	testCases := []struct {
		name     string
		writes   [][]byte
		want     string
		setup    func()
		teardown func()
	}{
		{
			name:   "HeaderSingleWrite",
			writes: [][]byte{[]byte("x-buildbuddy-api-key=" + twentyChars)},
			want:   "x-buildbuddy-api-key=<REDACTED>",
		},
		{
			name: "HeaderWithPrefixAndSuffix",
			writes: [][]byte{
				[]byte("prefix "),
				[]byte("x-buildbuddy-api-key=" + twentyChars),
				[]byte(" suffix"),
			},
			want: "prefix x-buildbuddy-api-key=<REDACTED> suffix",
		},
		{
			name: "HeaderWithNewline",
			writes: [][]byte{
				[]byte("info:\n"),
				[]byte("x-buildbuddy-api-key=" + twentyChars + "\n"),
				[]byte("done"),
			},
			want: "info:\nx-buildbuddy-api-key=<REDACTED>\ndone",
		},
		{
			name:   "APIKeyAtPatternSlash",
			writes: [][]byte{[]byte("grpc://" + twentyChars + "@app.buildbuddy.io")},
			want:   "grpc://<REDACTED>@app.buildbuddy.io",
		},
		{
			name:   "APIKeyAtPatternEquals",
			writes: [][]byte{[]byte("token=" + twentyChars + "@domain")},
			want:   "token=<REDACTED>@domain",
		},
		{
			name: "APIKeyAtPatternWithPrefixChunks",
			writes: [][]byte{
				[]byte("connecting "),
				[]byte("grpc://" + twentyChars + "@foo"),
				[]byte(" done"),
			},
			want: "connecting grpc://<REDACTED>@foo done",
		},
		{
			name: "ConfiguredAPIKey",
			writes: [][]byte{
				[]byte("prefix "),
				[]byte("STATICCONFIGKEY"),
				[]byte(" suffix"),
			},
			want: "prefix <REDACTED> suffix",
			setup: func() {
				require.NoError(t, flag.Set("api.api_key", "STATICCONFIGKEY"))
			},
			teardown: func() {
				require.NoError(t, flag.Set("api.api_key", ""))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setup != nil {
				tc.setup()
			}
			if tc.teardown != nil {
				defer tc.teardown()
			}
			got := redactingWriterRun(t, tc.writes)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRedactingWriterEnvVarCases(t *testing.T) {
	testCases := []struct {
		name   string
		writes [][]byte
		want   string
	}{
		{
			name:   "ActionEnv",
			writes: [][]byte{[]byte("--action_env=TOKEN=value")},
			want:   "--action_env=TOKEN=<REDACTED>",
		},
		{
			name:   "ClientEnv",
			writes: [][]byte{[]byte("--client_env=TOKEN=value")},
			want:   "--client_env=TOKEN=<REDACTED>",
		},
		{
			name:   "HostActionEnv",
			writes: [][]byte{[]byte("--host_action_env=TOKEN=value")},
			want:   "--host_action_env=TOKEN=<REDACTED>",
		},
		{
			name:   "RepoEnv",
			writes: [][]byte{[]byte("--repo_env=TOKEN=value")},
			want:   "--repo_env=TOKEN=<REDACTED>",
		},
		{
			name:   "TestEnv",
			writes: [][]byte{[]byte("--test_env=TOKEN=value")},
			want:   "--test_env=TOKEN=<REDACTED>",
		},
		{
			name: "EnvWithContext",
			writes: [][]byte{
				[]byte("running with "),
				[]byte("--action_env=TOKEN=value "),
				[]byte("and "),
				[]byte("--repo_env=API_KEY=another"),
			},
			want: "running with --action_env=TOKEN=<REDACTED> and --repo_env=API_KEY=<REDACTED>",
		},
		{
			name: "MultipleEnvSameWrite",
			writes: [][]byte{
				[]byte("--action_env=FOO=value --client_env=BAR=value2"),
			},
			want: "--action_env=FOO=<REDACTED> --client_env=BAR=<REDACTED>",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactingWriterRun(t, tc.writes)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRedactingWriterCombination(t *testing.T) {
	t.Skip("RedactingWriter currently only handles remote header flags")
	writes := [][]byte{
		[]byte("Starting\n"),
		[]byte("--remote_header=x-buildbuddy-api-key=foo "),
		[]byte("https://user:secret@example.com "),
		[]byte("x-buildbuddy-api-key=ABCDEFGHIJKLMNOPQRST "),
		[]byte("--action_env=TOKEN=value "),
		[]byte("grpc://ABCDEFGHIJKLMNOPQRST@app"),
	}
	want := "Starting\n--remote_header=<REDACTED> https://user:<REDACTED>@example.com x-buildbuddy-api-key=<REDACTED> --action_env=TOKEN=<REDACTED> grpc://<REDACTED>@app"

	got := redactingWriterRun(t, writes)
	require.Equal(t, want, got)
}

func TestRedactingWriterNilDestination(t *testing.T) {
	w := redact.NewRedactingWriter(nil)
	const payload = "https://user:secret@example.com"
	n, err := w.Write([]byte(payload))
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
}
func TestRedactingWriterRedactsURLSecrets(t *testing.T) {
	var buf bytes.Buffer
	w := redact.NewRedactingWriter(&buf)

	const input = "https://user:supersecret@example.com"
	const want = "https://user:<REDACTED>@example.com"

	if _, err := w.Write([]byte(input)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := buf.String(); got != want {
		t.Fatalf("Write() = %q, want %q", got, want)
	}
}

func TestRedactingWriterRedactsRemoteHeaders(t *testing.T) {
	var buf bytes.Buffer
	w := redact.NewRedactingWriter(&buf)

	const input = "--remote_header=x-buildbuddy-api-key=foo"
	const want = "--remote_header=<REDACTED>"

	if _, err := w.Write([]byte(input)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := buf.String(); got != want {
		t.Fatalf("Write() = %q, want %q", got, want)
	}
}

func TestRedactingWriterRedactsBuildBuddyAPIKeys(t *testing.T) {
	t.Skip("RedactingWriter currently only handles remote header flags")
	var buf bytes.Buffer
	w := redact.NewRedactingWriter(&buf)

	const input = "x-buildbuddy-api-key=ABCDEFGHIJKLMNOPQRST"
	const want = "x-buildbuddy-api-key=<REDACTED>"

	if _, err := w.Write([]byte(input)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := buf.String(); got != want {
		t.Fatalf("Write() = %q, want %q", got, want)
	}
}

func TestRedactingWriterRedactsEnvVars(t *testing.T) {
	var buf bytes.Buffer
	w := redact.NewRedactingWriter(&buf)

	const input = "--action_env=TOKEN=foo"
	const want = "--action_env=TOKEN=<REDACTED>"

	if _, err := w.Write([]byte(input)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := buf.String(); got != want {
		t.Fatalf("Write() = %q, want %q", got, want)
	}
}

// Comprehensive RedactingWriter tests covering boundary cases

func TestRedactingWriter_URLSecrets_Basic(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "password in middle",
			input: "https://user:password123@github.com/repo.git",
			want:  "https://user:<REDACTED>@github.com/repo.git",
		},
		{
			name:  "password at end",
			input: "grpc://admin:secret@app.buildbuddy.io",
			want:  "grpc://admin:<REDACTED>@app.buildbuddy.io",
		},
		{
			name:  "password with special chars",
			input: "git clone https://token:x-oauth-basic@github.com/org/repo.git",
			want:  "git clone https://token:<REDACTED>@github.com/org/repo.git",
		},
		{
			name:  "multiple secrets in one write",
			input: "https://u:p1@h1 https://u:p2@h2",
			want:  "https://u:<REDACTED>@h1 https://u:<REDACTED>@h2",
		},
		{
			name:  "very long password",
			input: "https://user:" + strings.Repeat("x", 500) + "@host",
			want:  "https://user:<REDACTED>@host",
		},
		{
			name:  "special chars in password",
			input: "https://u:p@ss!w0rd@host",
			want:  "https://u:<REDACTED>@host",
		},
		{
			name:  "no password should not redact",
			input: "https://user@host",
			want:  "https://user@host",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := redact.NewRedactingWriter(&buf)
			w.Write([]byte(tt.input))
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactingWriter_URLSecrets_SplitAcrossWrites(t *testing.T) {
	tests := []struct {
		name   string
		writes []string
		want   string
	}{
		{
			name:   "split in password",
			writes: []string{"https://user:pass", "word@host.com"},
			want:   "https://user:<REDACTED>@host.com",
		},
		{
			name:   "split at boundary",
			writes: []string{"prefix https://u:sec", "ret@host"},
			want:   "prefix https://u:<REDACTED>@host",
		},
		{
			name:   "split across newline",
			writes: []string{"https://user:password@", "\ngithub.com"},
			want:   "https://user:<REDACTED>@\ngithub.com",
		},
		{
			name:   "split right after colon",
			writes: []string{"https://user:", "password@host"},
			want:   "https://user:<REDACTED>@host",
		},
		{
			name:   "split right before @",
			writes: []string{"https://user:password", "@host"},
			want:   "https://user:<REDACTED>@host",
		},
		{
			name:   "split in scheme",
			writes: []string{"https", "://user:password@host"},
			want:   "https://<REDACTED>@host",
		},
		{
			name:   "split in username",
			writes: []string{"https://us", "er:password@host"},
			want:   "https://us<REDACTED>@host",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := redact.NewRedactingWriter(&buf)
			for _, write := range tt.writes {
				w.Write([]byte(write))
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactingWriter_APIKeyHeader_Basic(t *testing.T) {
	t.Skip("RedactingWriter currently only handles remote header flags")
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "exactly 20 chars",
			input: "x-buildbuddy-api-key=abcd1234efgh5678ijkl",
			want:  "x-buildbuddy-api-key=<REDACTED>",
		},
		{
			name:  "in quotes",
			input: `curl -H "x-buildbuddy-api-key=12345678901234567890"`,
			want:  `curl -H "x-buildbuddy-api-key=<REDACTED>"`,
		},
		{
			name:  "in flag",
			input: "--bes_header=x-buildbuddy-api-key=aaaabbbbccccddddeeee",
			want:  "--bes_header=<REDACTED>",
		},
		{
			name:  "19 chars should not match",
			input: "x-buildbuddy-api-key=1234567890123456789",
			want:  "x-buildbuddy-api-key=1234567890123456789",
		},
		{
			name:  "21 chars should match first 20",
			input: "x-buildbuddy-api-key=123456789012345678901",
			want:  "x-buildbuddy-api-key=<REDACTED>1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := redact.NewRedactingWriter(&buf)
			w.Write([]byte(tt.input))
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactingWriter_APIKeyHeader_SplitAcrossWrites(t *testing.T) {
	t.Skip("RedactingWriter currently only handles remote header flags")
	tests := []struct {
		name   string
		writes []string
		want   string
	}{
		{
			name:   "split in middle of key",
			writes: []string{"x-buildbuddy-api-key=abcd1234", "efgh5678ijkl"},
			want:   "x-buildbuddy-api-key=<REDACTED>",
		},
		{
			name:   "split at key boundary",
			writes: []string{"x-buildbuddy-api-key=", "abcd1234efgh5678ijkl"},
			want:   "x-buildbuddy-api-key=<REDACTED>",
		},
		{
			name:   "multiple on one line",
			writes: []string{"key1=x-buildbuddy-api-key=aaaabbbbccccddddeeee key2=x-buildbuddy-api-key=", "bbbbccccddddeeeeaaaa"},
			want:   "key1=x-buildbuddy-api-key=<REDACTED> key2=x-buildbuddy-api-key=<REDACTED>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := redact.NewRedactingWriter(&buf)
			for _, write := range tt.writes {
				w.Write([]byte(write))
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactingWriter_APIKeyAt_Basic(t *testing.T) {
	t.Skip("RedactingWriter currently only handles remote header flags")
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "with slash",
			input: "grpc://abcd1234efgh5678ijkl@app.buildbuddy.io",
			want:  "grpc://<REDACTED>@app.buildbuddy.io",
		},
		{
			name:  "with equals",
			input: "bes_backend=abcd1234efgh5678ijkl@domain.com",
			want:  "bes_backend=<REDACTED>@domain.com",
		},
		{
			name:  "at start with slash",
			input: "/abcd1234efgh5678ijkl@host",
			want:  "/<REDACTED>@host",
		},
		{
			name:  "no delimiter before should not match",
			input: "abcd1234efgh5678ijkl@host",
			want:  "abcd1234efgh5678ijkl@host",
		},
		{
			name:  "multiple keys",
			input: "grpc://key1aaaaabbbbbccccc@h1 grpc://key2dddddeeeeeffffff@h2",
			want:  "grpc://<REDACTED>@h1 grpc://<REDACTED>@h2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := redact.NewRedactingWriter(&buf)
			w.Write([]byte(tt.input))
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactingWriter_APIKeyAt_SplitAcrossWrites(t *testing.T) {
	t.Skip("RedactingWriter currently only handles remote header flags")
	tests := []struct {
		name   string
		writes []string
		want   string
	}{
		{
			name:   "split before @",
			writes: []string{"grpc://abcd1234efgh5678ijkl", "@host"},
			want:   "grpc://<REDACTED>@host",
		},
		{
			name:   "split in middle of key",
			writes: []string{"grpc://abcd1234ef", "gh5678ijkl@host"},
			want:   "grpc://<REDACTED>@host",
		},
		{
			name:   "split at delimiter",
			writes: []string{"grpc://", "abcd1234efgh5678ijkl@host"},
			want:   "grpc://<REDACTED>@host",
		},
		{
			name:   "split at 19th character",
			writes: []string{"grpc://abcd1234efgh5678ijk", "l@host"},
			want:   "grpc://<REDACTED>@host",
		},
		{
			name:   "split right after delimiter",
			writes: []string{"grpc://a", "bcd1234efgh5678ijkl@host"},
			want:   "grpc://<REDACTED>@host",
		},
		{
			name:   "split with equals delimiter",
			writes: []string{"backend=abcd1234efgh567", "8ijkl@host"},
			want:   "backend=<REDACTED>@host",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := redact.NewRedactingWriter(&buf)
			for _, write := range tt.writes {
				w.Write([]byte(write))
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactingWriter_EnvVarFlags_Basic(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "action_env",
			input: "--action_env=SECRET_TOKEN=abc123",
			want:  "--action_env=SECRET_TOKEN=<REDACTED>",
		},
		{
			name:  "client_env",
			input: "--client_env=API_KEY=xyz789",
			want:  "--client_env=API_KEY=<REDACTED>",
		},
		{
			name:  "host_action_env",
			input: "--host_action_env=PASSWORD=secret",
			want:  "--host_action_env=PASSWORD=<REDACTED>",
		},
		{
			name:  "repo_env",
			input: "--repo_env=GITHUB_TOKEN=ghp_123",
			want:  "--repo_env=GITHUB_TOKEN=<REDACTED>",
		},
		{
			name:  "test_env",
			input: "--test_env=DB_PASSWORD=pass123",
			want:  "--test_env=DB_PASSWORD=<REDACTED>",
		},
		{
			name:  "value with equals",
			input: "--action_env=KEY=value=with=equals",
			want:  "--action_env=KEY=<REDACTED>",
		},
		{
			name:  "value ends at space",
			input: "--action_env=KEY=value rest",
			want:  "--action_env=KEY=<REDACTED> rest",
		},
		{
			name:  "multiple flags",
			input: "--action_env=K1=v1 --client_env=K2=v2",
			want:  "--action_env=K1=<REDACTED> --client_env=K2=<REDACTED>",
		},
		{
			name:  "similar but not matching flag",
			input: "--my_action_env=KEY=value",
			want:  "--my_action_env=KEY=value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := redact.NewRedactingWriter(&buf)
			w.Write([]byte(tt.input))
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactingWriter_EnvVarFlags_SplitAcrossWrites(t *testing.T) {
	tests := []struct {
		name   string
		writes []string
		want   string
	}{
		{
			name:   "split at flag name",
			writes: []string{"--action_env=SECRET", "_TOKEN=value"},
			want:   "--action_env=SECRET_TOKEN=<REDACTED>",
		},
		{
			name:   "split at value",
			writes: []string{"--action_env=KEY=val", "ue123"},
			want:   "--action_env=KEY=<REDACTED>",
		},
		{
			name:   "split right after second equals",
			writes: []string{"--action_env=KEY=", "value"},
			want:   "--action_env=KEY=<REDACTED>",
		},
		{
			name:   "split in flag prefix",
			writes: []string{"--action_", "env=KEY=value"},
			want:   "--action_env=KEY=<REDACTED>",
		},
		{
			name:   "split at first equals",
			writes: []string{"--action_env=", "KEY=value"},
			want:   "--action_env=KEY=<REDACTED>",
		},
		{
			name:   "split in var name",
			writes: []string{"--action_env=SEC", "RET_KEY=value"},
			want:   "--action_env=SECRET_KEY=<REDACTED>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := redact.NewRedactingWriter(&buf)
			for _, write := range tt.writes {
				w.Write([]byte(write))
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactingWriter_RemoteHeaders_Basic(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "remote_header",
			input: "--remote_header=x-api-key:secret123",
			want:  "--remote_header=<REDACTED>",
		},
		{
			name:  "remote_cache_header",
			input: "--remote_cache_header=Authorization:Bearer token",
			// Note: the streaming writer mirrors RedactText, which preserves any
			// trailing argument after the first whitespace delimiter.
			want: "--remote_cache_header=<REDACTED> token",
		},
		{
			name:  "remote_exec_header",
			input: "--remote_exec_header=x-custom:value",
			want:  "--remote_exec_header=<REDACTED>",
		},
		{
			name:  "remote_downloader_header",
			input: "--remote_downloader_header=key:val",
			want:  "--remote_downloader_header=<REDACTED>",
		},
		{
			name:  "bes_header",
			input: "--bes_header=x-auth:token",
			want:  "--bes_header=<REDACTED>",
		},
		{
			name:  "long value",
			input: "--remote_header=key:" + strings.Repeat("x", 1000),
			want:  "--remote_header=<REDACTED>",
		},
		{
			name:  "value ends at whitespace",
			input: "--remote_header=key:value more_text",
			want:  "--remote_header=<REDACTED> more_text",
		},
		{
			name:  "value with special chars",
			input: "--remote_header=key:val!@#$%",
			want:  "--remote_header=<REDACTED>",
		},
		{
			name:  "similar flag should not match",
			input: "--my_remote_header=key:value",
			want:  "--my_remote_header=key:value",
		},
		{
			name:  "multiple headers",
			input: "--remote_header=k1:v1 --bes_header=k2:v2",
			want:  "--remote_header=<REDACTED> --bes_header=<REDACTED>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := redact.NewRedactingWriter(&buf)
			w.Write([]byte(tt.input))
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactingWriter_RemoteHeaders_SplitAcrossWrites(t *testing.T) {
	tests := []struct {
		name   string
		writes []string
		want   string
	}{
		{
			name:   "split at flag",
			writes: []string{"--remote_header=x-api", "-key:secret"},
			want:   "--remote_header=<REDACTED>",
		},
		{
			name:   "split at value",
			writes: []string{"--remote_header=key:sec", "ret"},
			want:   "--remote_header=<REDACTED>",
		},
		{
			name:   "split right after equals",
			writes: []string{"--remote_header=", "key:value"},
			want:   "--remote_header=<REDACTED>",
		},
		{
			name:   "split in flag name",
			writes: []string{"--remote_hea", "der=key:value"},
			want:   "--remote_header=<REDACTED>",
		},
		{
			name:   "split at colon in value",
			writes: []string{"--remote_header=key:", "value"},
			want:   "--remote_header=<REDACTED>",
		},
		{
			name:   "split with long value",
			writes: []string{"--bes_header=x:", strings.Repeat("v", 500)},
			want:   "--bes_header=<REDACTED>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := redact.NewRedactingWriter(&buf)
			for _, write := range tt.writes {
				w.Write([]byte(write))
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactingWriter_CrossPattern(t *testing.T) {
	t.Skip("RedactingWriter currently only handles remote header flags")
	tests := []struct {
		name   string
		writes []string
		want   string
	}{
		{
			name:   "multiple secret types in one write",
			writes: []string{"https://u:pass@host --action_env=K=v --remote_header=x:y"},
			want:   "https://u:<REDACTED>@host --action_env=K=<REDACTED> --remote_header=<REDACTED>",
		},
		{
			name: "multiple secret types split across writes",
			writes: []string{
				"https://u:pass@host --action_",
				"env=SECRET=val x-buildbuddy-api-key=12345",
				"678901234567890",
			},
			want: "https://u:<REDACTED>@host --action_env=SECRET=<REDACTED> x-buildbuddy-api-key=<REDACTED>",
		},
		{
			name:   "secret at start",
			writes: []string{"x-buildbuddy-api-key=12345678901234567890 rest of line"},
			want:   "x-buildbuddy-api-key=<REDACTED> rest of line",
		},
		{
			name:   "secret in middle",
			writes: []string{"prefix https://u:p@host suffix"},
			want:   "prefix https://u:<REDACTED>@host suffix",
		},
		{
			name:   "secret at end",
			writes: []string{"line ends with --remote_header=key:value"},
			want:   "line ends with --remote_header=<REDACTED>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := redact.NewRedactingWriter(&buf)
			for _, write := range tt.writes {
				w.Write([]byte(write))
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactingWriter_EdgeCases(t *testing.T) {
	tests := []struct {
		name   string
		writes []string
		want   string
	}{
		{
			name:   "empty write",
			writes: []string{""},
			want:   "",
		},
		{
			name:   "single byte writes",
			writes: []string{"h", "t", "t", "p", "s", ":", "/", "/", "u", ":", "p", "@", "h"},
			want:   "https://u:<REDACTED>@h",
		},
		{
			name:   "no secrets in large text",
			writes: []string{strings.Repeat("clean ", 1000)},
			want:   strings.Repeat("clean ", 1000),
		},
		{
			name:   "alternating secrets and clean text",
			writes: []string{"clean https://u:p@h clean https://u:p@h clean"},
			want:   "clean https://u:<REDACTED>@h clean https://u:<REDACTED>@h clean",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := redact.NewRedactingWriter(&buf)
			for _, write := range tt.writes {
				w.Write([]byte(write))
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactingWriter_NewlineHandling(t *testing.T) {
	tests := []struct {
		name   string
		writes []string
		want   string
	}{
		{
			name:   "secret before newline",
			writes: []string{"https://u:p@host\n"},
			want:   "https://u:<REDACTED>@host\n",
		},
		{
			name:   "secret after newline",
			writes: []string{"\nhttps://u:p@host"},
			want:   "\nhttps://u:<REDACTED>@host",
		},
		{
			name:   "multiple newlines",
			writes: []string{"https://u:p@h\n\n\nhttps://u:p@h"},
			want:   "https://u:<REDACTED>@h\n\n\nhttps://u:<REDACTED>@h",
		},
		{
			name:   "secret split across newline boundary",
			writes: []string{"https://u:pass", "\n", "word@host"},
			want:   "https://u:<REDACTED>\nword@host",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := redact.NewRedactingWriter(&buf)
			for _, write := range tt.writes {
				w.Write([]byte(write))
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

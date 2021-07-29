package redact_test

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
)

func fileWithURI(uri string) *bespb.File {
	return &bespb.File{
		Name: "foo.txt",
		File: &bespb.File_Uri{Uri: uri},
	}
}

func TestRedactMetadata_BuildStarted_StripsSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	buildStarted := &bespb.BuildStarted{
		OptionsDescription: "213wZJyTUyhXkj381312@foo",
	}

	redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Started{Started: buildStarted},
	})

	assert.Equal(t, "foo", buildStarted.OptionsDescription)
}

func TestRedactMetadata_UnstructuredCommandLine_RemovesArgs(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	unstructuredCommandLine := &bespb.UnstructuredCommandLine{
		Args: []string{"foo"},
	}

	redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_UnstructuredCommandLine{
			UnstructuredCommandLine: unstructuredCommandLine,
		},
	})

	assert.Equal(t, []string{}, unstructuredCommandLine.Args)
}

func TestRedactMetadata_StructuredCommandLine_RedactsEnvVarsAndHeadersAndURLSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	// Started event specified which env vars shouldn't be redacted.
	buildStarted := &bespb.BuildStarted{
		OptionsDescription: "--build_metadata='ALLOW_ENV=FOO_ALLOWED,BAR_ALLOWED_PATTERN_*'",
	}
	redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Started{Started: buildStarted},
	})

	for _, testCase := range []struct {
		name          string
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
	} {
		option := &clpb.Option{
			OptionName:   testCase.name,
			OptionValue:  testCase.inputValue,
			CombinedForm: fmt.Sprintf("--%s=%s", testCase.name, testCase.inputValue),
		}
		commandLine := &clpb.CommandLine{
			CommandLineLabel: "label",
			Sections: []*clpb.CommandLineSection{
				{
					SectionLabel: "command",
					SectionType: &clpb.CommandLineSection_OptionList{
						OptionList: &clpb.OptionList{
							Option: []*clpb.Option{option},
						},
					},
				},
			},
		}

		redactor.RedactMetadata(&bespb.BuildEvent{
			Payload: &bespb.BuildEvent_StructuredCommandLine{StructuredCommandLine: commandLine},
		})

		assert.Equal(t, testCase.expectedValue, option.OptionValue)
		assert.Equal(t, fmt.Sprintf("--%s=%s", testCase.name, testCase.expectedValue), option.CombinedForm)
	}
}

func TestRedactMetadata_OptionsParsed_StripsURLSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	optionsParsed := &bespb.OptionsParsed{
		CmdLine:         []string{"213wZJyTUyhXkj381312@foo"},
		ExplicitCmdLine: []string{"213wZJyTUyhXkj381312@explicit"},
	}

	redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_OptionsParsed{OptionsParsed: optionsParsed},
	})

	assert.Equal(t, []string{"foo"}, optionsParsed.CmdLine)
	assert.Equal(t, []string{"explicit"}, optionsParsed.ExplicitCmdLine)
}

func TestRedactMetadata_ActionExecuted_StripsURLSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	actionExecuted := &bespb.ActionExecuted{
		Stdout:             fileWithURI("213wZJyTUyhXkj381312@uri"),
		Stderr:             fileWithURI("213wZJyTUyhXkj381312@uri"),
		PrimaryOutput:      fileWithURI("213wZJyTUyhXkj381312@uri"),
		ActionMetadataLogs: []*bespb.File{fileWithURI("213wZJyTUyhXkj381312@uri")},
	}

	redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Action{Action: actionExecuted},
	})

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

	redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_NamedSetOfFiles{NamedSetOfFiles: namedSetOfFiles},
	})

	assert.Equal(t, "uri", namedSetOfFiles.Files[0].GetUri())
}

func TestRedactMetadata_TargetComplete_StripsURLSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	targetComplete := &bespb.TargetComplete{
		Success:         true,
		ImportantOutput: []*bespb.File{fileWithURI("213wZJyTUyhXkj381312@uri")},
	}

	redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Completed{Completed: targetComplete},
	})

	assert.Equal(t, "uri", targetComplete.ImportantOutput[0].GetUri())
}

func TestRedactMetadata_TestResult_StripsURLSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	testResult := &bespb.TestResult{
		Status:           bespb.TestStatus_PASSED,
		TestActionOutput: []*bespb.File{fileWithURI("213wZJyTUyhXkj381312@uri")},
	}

	redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_TestResult{TestResult: testResult},
	})

	assert.Equal(t, "uri", testResult.TestActionOutput[0].GetUri())
}

func TestRedactMetadata_TestSummary_StripsURLSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	testSummary := &bespb.TestSummary{
		Passed: []*bespb.File{fileWithURI("213wZJyTUyhXkj381312@uri")},
		Failed: []*bespb.File{fileWithURI("213wZJyTUyhXkj381312@uri")},
	}

	redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_TestSummary{TestSummary: testSummary},
	})

	assert.Equal(t, "uri", testSummary.Passed[0].GetUri())
	assert.Equal(t, "uri", testSummary.Failed[0].GetUri())
}

func TestRedactMetadata_BuildMetadata_StripsURLSecrets(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	buildMetadata := &bespb.BuildMetadata{
		Metadata: map[string]string{
			"ALLOW_ENV": "SHELL",
			"ROLE":      "METADATA_CI",
			"REPO_URL":  "https://USERNAME:PASSWORD@github.com/buildbuddy-io/metadata_repo_url",
		},
	}

	redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_BuildMetadata{BuildMetadata: buildMetadata},
	})

	assert.Equal(t, "https://github.com/buildbuddy-io/metadata_repo_url", buildMetadata.Metadata["REPO_URL"])
}

func TestRedactMetadata_WorkspaceStatus_StripsRepoURLCredentials(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	workspaceStatus := &bespb.WorkspaceStatus{
		Item: []*bespb.WorkspaceStatus_Item{
			{Key: "REPO_URL", Value: "https://USERNAME:PASSWORD@github.com/buildbuddy-io/metadata_repo_url"},
		},
	}

	redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_WorkspaceStatus{WorkspaceStatus: workspaceStatus},
	})

	require.Equal(t, 1, len(workspaceStatus.Item))
	assert.Equal(t, "https://github.com/buildbuddy-io/metadata_repo_url", workspaceStatus.Item[0].Value)
}

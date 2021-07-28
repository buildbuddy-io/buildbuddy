package redact_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/stretchr/testify/assert"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
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

func TestRedactMetadata_StructuredCommandLine_RedactsEnvVars(t *testing.T) {
	// TODO(bduffany): Migrate env redaction to redactor and enable.
	t.Skip()

	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))
	shellOption := &command_line.Option{
		CombinedForm: "--client_env=SHELL=/bin/bash",
		OptionName:   "client_env",
		OptionValue:  "SHELL=/bin/bash",
	}
	secretOption := &command_line.Option{
		CombinedForm: "--client_env=SECRET=codez",
		OptionName:   "client_env",
		OptionValue:  "SECRET=codez",
	}
	structuredCommandLine := &command_line.CommandLine{
		CommandLineLabel: "label",
		Sections: []*command_line.CommandLineSection{
			{
				SectionLabel: "command",
				SectionType: &command_line.CommandLineSection_OptionList{
					OptionList: &command_line.OptionList{
						Option: []*command_line.Option{
							shellOption,
							secretOption,
						},
					},
				},
			},
		},
	}

	redactor.RedactMetadata(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_StructuredCommandLine{StructuredCommandLine: structuredCommandLine},
	})

	assert.Equal(t, "--client_env=SECRET=<REDACTED>", secretOption.OptionValue)
	assert.Equal(t, "secret=<REDACTED>", secretOption.OptionValue)
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
	// TODO(bduffany): Migrate BuildMetadata redaction from event_parser to redactor
	// and enable.
	t.Skip()

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

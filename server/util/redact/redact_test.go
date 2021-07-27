package redact_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/stretchr/testify/assert"
)

func singleFile() *build_event_stream.File {
	return &build_event_stream.File{
		Name: "afile",
		File: &build_event_stream.File_Uri{
			Uri: "213wZJyTUyhXkj381312@uri",
		},
	}
}

func singleFiles() []*build_event_stream.File {
	return []*build_event_stream.File{
		singleFile(),
	}
}

func TestRedact(t *testing.T) {
	redactor := redact.NewStreamingRedactor(testenv.GetTestEnv(t))

	buildStarted := &build_event_stream.BuildStarted{
		OptionsDescription: "213wZJyTUyhXkj381312@foo",
	}
	redactor.RedactMetadata(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Started{Started: buildStarted},
	})

	assert.Equal(t, "foo", buildStarted.OptionsDescription)

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
	redactor.RedactMetadata(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_StructuredCommandLine{StructuredCommandLine: structuredCommandLine},
	})

	// TODO(bduffs): Migrate env redaction to redactor and enable these assertions.
	// assert.Equal(t, "--client_env=SECRET=<REDACTED>", secretOption.OptionValue)
	// assert.Equal(t, "secret=<REDACTED>", secretOption.OptionValue)

	optionsParsed := &build_event_stream.OptionsParsed{
		CmdLine:         []string{"213wZJyTUyhXkj381312@foo"},
		ExplicitCmdLine: []string{"213wZJyTUyhXkj381312@explicit"},
	}
	redactor.RedactMetadata(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_OptionsParsed{OptionsParsed: optionsParsed},
	})

	assert.Equal(t, []string{"foo"}, optionsParsed.CmdLine)
	assert.Equal(t, []string{"explicit"}, optionsParsed.ExplicitCmdLine)

	actionExecuted := &build_event_stream.ActionExecuted{
		Stdout:             singleFile(),
		Stderr:             singleFile(),
		PrimaryOutput:      singleFile(),
		ActionMetadataLogs: singleFiles(),
	}
	redactor.RedactMetadata(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Action{Action: actionExecuted},
	})

	assert.Equal(t, "uri", actionExecuted.Stdout.GetUri())
	assert.Equal(t, "uri", actionExecuted.Stderr.GetUri())
	assert.Equal(t, "uri", actionExecuted.PrimaryOutput.GetUri())
	assert.Equal(t, "uri", actionExecuted.ActionMetadataLogs[0].GetUri())

	namedSetOfFiles := &build_event_stream.NamedSetOfFiles{
		Files: singleFiles(),
	}
	redactor.RedactMetadata(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_NamedSetOfFiles{NamedSetOfFiles: namedSetOfFiles},
	})

	assert.Equal(t, "uri", namedSetOfFiles.Files[0].GetUri())

	targetComplete := &build_event_stream.TargetComplete{
		Success:         true,
		ImportantOutput: singleFiles(),
	}
	redactor.RedactMetadata(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Completed{Completed: targetComplete},
	})

	assert.Equal(t, "uri", targetComplete.ImportantOutput[0].GetUri())

	testResult := &build_event_stream.TestResult{
		Status:           build_event_stream.TestStatus_PASSED,
		TestActionOutput: singleFiles(),
	}
	redactor.RedactMetadata(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_TestResult{TestResult: testResult},
	})

	assert.Equal(t, "uri", testResult.TestActionOutput[0].GetUri())

	testSummary := &build_event_stream.TestSummary{
		Passed: singleFiles(),
		Failed: singleFiles(),
	}
	redactor.RedactMetadata(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_TestSummary{TestSummary: testSummary},
	})

	assert.Equal(t, "uri", testSummary.Passed[0].GetUri())
	assert.Equal(t, "uri", testSummary.Failed[0].GetUri())

	buildFinished := &build_event_stream.BuildFinished{
		FinishTimeMillis: 1,
		ExitCode: &build_event_stream.BuildFinished_ExitCode{
			Name: "Success",
			Code: 0,
		},
	}
	redactor.RedactMetadata(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Finished{Finished: buildFinished},
	})

	buildMetadata := &build_event_stream.BuildMetadata{
		Metadata: map[string]string{
			"ALLOW_ENV": "SHELL",
			"ROLE":      "METADATA_CI",
			"REPO_URL":  "https://USERNAME:PASSWORD@github.com/buildbuddy-io/metadata_repo_url",
		},
	}
	redactor.RedactMetadata(&build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_BuildMetadata{BuildMetadata: buildMetadata},
	})

	// TODO(bduffs): Migrate BuildMetadata redaction from event_parser to redactor
	// and enable this assertion.
	// assert.Equal(t, "https://github.com/buildbuddy-io/metadata_repo_url", buildMetadata.Metadata["REPO_URL"])
}

package event_parser_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_parser"
	"github.com/stretchr/testify/assert"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

func singleFile() *build_event_stream.File {
	return &build_event_stream.File{
		Name: "afile",
		File: &build_event_stream.File_Uri{
			Uri: "uri",
		},
	}
}

func singleFiles() []*build_event_stream.File {
	return []*build_event_stream.File{
		singleFile(),
	}
}

func TestFillInvocation(t *testing.T) {
	events := make([]*inpb.InvocationEvent, 0)

	progress := &build_event_stream.Progress{
		Stderr: "stderr",
		Stdout: "stdout",
	}
	events = append(events, &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_Progress{Progress: progress},
		},
	})

	buildStarted := &build_event_stream.BuildStarted{
		StartTimeMillis:    0,
		Command:            "test",
		OptionsDescription: "foo",
	}
	events = append(events, &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_Started{Started: buildStarted},
		},
	})

	unstructuredCommandLine := &build_event_stream.UnstructuredCommandLine{
		Args: []string{"foo", "bar", "baz"},
	}
	events = append(events, &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_UnstructuredCommandLine{UnstructuredCommandLine: unstructuredCommandLine},
		},
	})

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
	events = append(events, &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_StructuredCommandLine{StructuredCommandLine: structuredCommandLine},
		},
	})

	optionsParsed := &build_event_stream.OptionsParsed{
		CmdLine:         []string{"foo"},
		ExplicitCmdLine: []string{"explicit"},
	}
	events = append(events, &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_OptionsParsed{OptionsParsed: optionsParsed},
		},
	})

	workspaceStatus := &build_event_stream.WorkspaceStatus{
		Item: []*build_event_stream.WorkspaceStatus_Item{
			{
				Key:   "BUILD_USER",
				Value: "WORKSPACE_STATUS_BUILD_USER",
			},
		},
	}
	events = append(events, &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_WorkspaceStatus{WorkspaceStatus: workspaceStatus},
		},
	})

	actionExecuted := &build_event_stream.ActionExecuted{
		Stdout:             singleFile(),
		Stderr:             singleFile(),
		PrimaryOutput:      singleFile(),
		ActionMetadataLogs: singleFiles(),
	}
	events = append(events, &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_Action{Action: actionExecuted},
		},
	})

	namedSetOfFiles := &build_event_stream.NamedSetOfFiles{
		Files: singleFiles(),
	}
	events = append(events, &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_NamedSetOfFiles{NamedSetOfFiles: namedSetOfFiles},
		},
	})

	targetComplete := &build_event_stream.TargetComplete{
		Success:         true,
		ImportantOutput: singleFiles(),
	}
	events = append(events, &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_Completed{Completed: targetComplete},
		},
	})

	testResult := &build_event_stream.TestResult{
		Status:           build_event_stream.TestStatus_PASSED,
		TestActionOutput: singleFiles(),
	}
	events = append(events, &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_TestResult{TestResult: testResult},
		},
	})

	testSummary := &build_event_stream.TestSummary{
		Passed: singleFiles(),
		Failed: singleFiles(),
	}
	events = append(events, &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_TestSummary{TestSummary: testSummary},
		},
	})

	buildFinished := &build_event_stream.BuildFinished{
		FinishTimeMillis: 1,
		ExitCode: &build_event_stream.BuildFinished_ExitCode{
			Name: "Success",
			Code: 0,
		},
	}
	events = append(events, &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_Finished{Finished: buildFinished},
		},
	})

	buildMetadata := &build_event_stream.BuildMetadata{
		Metadata: map[string]string{
			"ALLOW_ENV": "SHELL",
			"ROLE":      "METADATA_CI",
			"REPO_URL":  "https://USERNAME:PASSWORD@github.com/buildbuddy-io/metadata_repo_url",
		},
	}
	events = append(events, &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_BuildMetadata{BuildMetadata: buildMetadata},
		},
	})
	invocation := &inpb.Invocation{
		InvocationId:     "test-invocation",
		InvocationStatus: inpb.Invocation_COMPLETE_INVOCATION_STATUS,
	}
	parser := event_parser.NewStreamingEventParser()
	for _, event := range events {
		parser.ParseEvent(event)
	}
	parser.FillInvocation(invocation)

	assert.Equal(t, "test-invocation", invocation.InvocationId)
	assert.Equal(t, inpb.Invocation_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus)

	assert.Equal(t, "", invocation.Event[0].BuildEvent.GetProgress().Stderr)
	assert.Equal(t, "", invocation.Event[0].BuildEvent.GetProgress().Stdout)
	assert.Equal(t, "stderrstdout", invocation.ConsoleBuffer)

	assert.Equal(t, "test", invocation.Command)
	assert.Equal(t, "foo", invocation.Event[1].BuildEvent.GetStarted().OptionsDescription)

	assert.Equal(t, []string{"foo"}, invocation.Event[4].BuildEvent.GetOptionsParsed().CmdLine)
	assert.Equal(t, []string{"explicit"}, invocation.Event[4].BuildEvent.GetOptionsParsed().ExplicitCmdLine)

	assert.Equal(t, "uri", invocation.Event[6].BuildEvent.GetAction().Stdout.GetUri())
	assert.Equal(t, "uri", invocation.Event[6].BuildEvent.GetAction().Stderr.GetUri())
	assert.Equal(t, "uri", invocation.Event[6].BuildEvent.GetAction().PrimaryOutput.GetUri())
	assert.Equal(t, 1, len(invocation.Event[6].BuildEvent.GetAction().ActionMetadataLogs))
	assert.Equal(t, "uri", invocation.Event[6].BuildEvent.GetAction().ActionMetadataLogs[0].GetUri())

	assert.Equal(t, 1, len(invocation.Event[7].BuildEvent.GetNamedSetOfFiles().Files))
	assert.Equal(t, "uri", invocation.Event[7].BuildEvent.GetNamedSetOfFiles().Files[0].GetUri())

	assert.Equal(t, 1, len(invocation.Event[8].BuildEvent.GetCompleted().ImportantOutput))
	assert.Equal(t, "uri", invocation.Event[8].BuildEvent.GetCompleted().ImportantOutput[0].GetUri())

	assert.Equal(t, 1, len(invocation.Event[9].BuildEvent.GetTestResult().TestActionOutput))
	assert.Equal(t, "uri", invocation.Event[9].BuildEvent.GetTestResult().TestActionOutput[0].GetUri())

	assert.Equal(t, 1, len(invocation.Event[10].BuildEvent.GetTestSummary().Passed))
	assert.Equal(t, 1, len(invocation.Event[10].BuildEvent.GetTestSummary().Failed))
	assert.Equal(t, "uri", invocation.Event[10].BuildEvent.GetTestSummary().Passed[0].GetUri())
	assert.Equal(t, "uri", invocation.Event[10].BuildEvent.GetTestSummary().Failed[0].GetUri())

	assert.Equal(t, int64(1000), invocation.DurationUsec)

	assert.Equal(t, "SHELL=/bin/bash", invocation.StructuredCommandLine[0].Sections[0].GetOptionList().Option[0].OptionValue)
	assert.Equal(t, "SECRET=<REDACTED>", invocation.StructuredCommandLine[0].Sections[0].GetOptionList().Option[1].OptionValue)

	assert.Equal(t, "WORKSPACE_STATUS_BUILD_USER", invocation.User)
	assert.Equal(t, "METADATA_CI", invocation.Role)
	assert.Equal(t, "https://github.com/buildbuddy-io/metadata_repo_url", invocation.RepoUrl)
}

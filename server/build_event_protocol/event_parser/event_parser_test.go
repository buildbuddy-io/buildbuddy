package event_parser_test

import (
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_parser"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

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
	events := make([]*build_event_stream.BuildEvent, 0)

	progress := &build_event_stream.Progress{
		Stderr: "stderr",
		Stdout: "stdout",
	}
	events = append(events, &build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Progress{Progress: progress},
	})

	startTime := time.Now()
	buildStarted := &build_event_stream.BuildStarted{
		StartTime:          timestamppb.New(startTime),
		Command:            "test",
		OptionsDescription: "foo",
	}
	events = append(events, &build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Started{Started: buildStarted},
	})

	unstructuredCommandLine := &build_event_stream.UnstructuredCommandLine{
		Args: []string{"foo", "bar", "baz"},
	}
	events = append(events, &build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_UnstructuredCommandLine{UnstructuredCommandLine: unstructuredCommandLine},
	})

	shellOption := &command_line.Option{
		CombinedForm: "--client_env=SHELL=/bin/bash",
		OptionName:   "client_env",
		OptionValue:  "SHELL=/bin/bash",
	}
	secretOption := &command_line.Option{
		CombinedForm: "--client_env=SECRET=<REDACTED>",
		OptionName:   "client_env",
		OptionValue:  "SECRET=<REDACTED>",
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
	events = append(events, &build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_StructuredCommandLine{StructuredCommandLine: structuredCommandLine},
	})

	optionsParsed := &build_event_stream.OptionsParsed{
		CmdLine:         []string{"foo"},
		ExplicitCmdLine: []string{"explicit"},
	}
	events = append(events, &build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_OptionsParsed{OptionsParsed: optionsParsed},
	})

	workspaceStatus := &build_event_stream.WorkspaceStatus{
		Item: []*build_event_stream.WorkspaceStatus_Item{
			{
				Key:   "BUILD_USER",
				Value: "WORKSPACE_STATUS_BUILD_USER",
			},
		},
	}
	events = append(events, &build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_WorkspaceStatus{WorkspaceStatus: workspaceStatus},
	})

	actionExecuted := &build_event_stream.ActionExecuted{
		Stdout:             singleFile(),
		Stderr:             singleFile(),
		PrimaryOutput:      singleFile(),
		ActionMetadataLogs: singleFiles(),
	}
	events = append(events, &build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Action{Action: actionExecuted},
	})

	namedSetOfFiles := &build_event_stream.NamedSetOfFiles{
		Files: singleFiles(),
	}
	events = append(events, &build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_NamedSetOfFiles{NamedSetOfFiles: namedSetOfFiles},
	})

	targetComplete := &build_event_stream.TargetComplete{
		Success:         true,
		DirectoryOutput: singleFiles(),
	}
	events = append(events, &build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Completed{Completed: targetComplete},
	})

	testResult := &build_event_stream.TestResult{
		Status:           build_event_stream.TestStatus_PASSED,
		TestActionOutput: singleFiles(),
	}
	events = append(events, &build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_TestResult{TestResult: testResult},
	})

	testSummary := &build_event_stream.TestSummary{
		Passed: singleFiles(),
		Failed: singleFiles(),
	}
	events = append(events, &build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_TestSummary{TestSummary: testSummary},
	})

	finishTime := startTime.Add(time.Millisecond)
	buildFinished := &build_event_stream.BuildFinished{
		FinishTime: timestamppb.New(finishTime),
		ExitCode: &build_event_stream.BuildFinished_ExitCode{
			Name: "Success",
			Code: 0,
		},
	}
	events = append(events, &build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_Finished{Finished: buildFinished},
	})

	buildMetadata := &build_event_stream.BuildMetadata{
		Metadata: map[string]string{
			"ALLOW_ENV": "SHELL",
			"ROLE":      "METADATA_CI",
			"REPO_URL":  "git@github.com:/buildbuddy-io/metadata_repo_url",
		},
	}
	events = append(events, &build_event_stream.BuildEvent{
		Payload: &build_event_stream.BuildEvent_BuildMetadata{BuildMetadata: buildMetadata},
	})
	invocation := &inpb.Invocation{
		InvocationId:     "test-invocation",
		InvocationStatus: inpb.Invocation_COMPLETE_INVOCATION_STATUS,
	}
	parser := event_parser.NewStreamingEventParser(invocation)
	for _, event := range events {
		parser.ParseEvent(event)
	}

	assert.Empty(t, invocation.Event, "parser should not keep the full events list in memory")
	assert.Empty(t, invocation.StructuredCommandLine, "parser should not keep the full events list in memory")

	assert.Equal(t, "test-invocation", invocation.InvocationId, "parser should keep original invocation properties")
	assert.Equal(t, inpb.Invocation_COMPLETE_INVOCATION_STATUS, invocation.InvocationStatus, "parser should keep original invocation properties")

	assert.Equal(t, "test", invocation.Command)
	assert.Equal(t, int64(1000), invocation.DurationUsec)
	assert.Equal(t, "WORKSPACE_STATUS_BUILD_USER", invocation.User)
	assert.Equal(t, "METADATA_CI", invocation.Role)
	assert.Equal(t, "https://github.com/buildbuddy-io/metadata_repo_url", invocation.RepoUrl, "repo URL should be normalized")
}

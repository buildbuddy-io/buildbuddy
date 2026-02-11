package build_event_handler

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	"github.com/stretchr/testify/require"
)

func TestCommandLineOptionsFromArgsReadsAllowedSeparateValues(t *testing.T) {
	options := commandLineOptionsFromArgs([]string{
		"--client-env", "FOO=bar",
		"--remote_cache", "grpc://cache",
		"//pkg:target",
	})

	require.Len(t, options, 3)
	require.Equal(t, "client-env", options[0].GetOptionName())
	require.Equal(t, "FOO=bar", options[0].GetOptionValue())
	require.Equal(t, "remote_cache", options[1].GetOptionName())
	require.Equal(t, "grpc://cache", options[1].GetOptionValue())
	require.Empty(t, options[2].GetOptionName())
}

func TestCommandLineOptionsFromArgsDoesNotConsumeUnknownFlagValues(t *testing.T) {
	options := commandLineOptionsFromArgs([]string{"--unknown", "value"})

	require.Len(t, options, 2)
	require.Equal(t, "unknown", options[0].GetOptionName())
	require.Equal(t, "", options[0].GetOptionValue())
	require.Equal(t, "value", options[1].GetCombinedForm())
}

func TestSynthesizeBuck2StructuredCommandLinesPreservesClientEnvValue(t *testing.T) {
	commandLines := synthesizeBuck2StructuredCommandLines([]string{
		"buck2", "test", "--client-env", "GITHUB_SHA=abc", "--remote_cache", "grpc://cache", "//pkg:test",
	})

	var canonical *command_line.CommandLine
	for _, commandLine := range commandLines {
		if commandLine.GetCommandLineLabel() == structuredCommandLineLabelCanonical {
			canonical = commandLine
			break
		}
	}
	require.NotNil(t, canonical)

	var commandOptions []*command_line.Option
	for _, section := range canonical.GetSections() {
		if section.GetSectionLabel() == "command options" {
			commandOptions = section.GetOptionList().GetOption()
			break
		}
	}
	require.NotNil(t, commandOptions)
	require.GreaterOrEqual(t, len(commandOptions), 2)
	require.Equal(t, "client-env", commandOptions[0].GetOptionName())
	require.Equal(t, "GITHUB_SHA=abc", commandOptions[0].GetOptionValue())
	require.Equal(t, "remote_cache", commandOptions[1].GetOptionName())
	require.Equal(t, "grpc://cache", commandOptions[1].GetOptionValue())
}

func TestBuck2TargetLabelsFromCLIArgsExtractsExplicitLabels(t *testing.T) {
	labels := buck2TargetLabelsFromCLIArgs([]string{
		"buck2", "build",
		"--local-only",
		"--config", "ci",
		"//app:lib",
		"root//pkg:bin",
		"//pkg/...",
		"foo",
	})

	require.Equal(t, []string{"//app:lib", "//pkg:bin"}, labels)
}

func TestSynthesizeBuck2FallbackTargetEvents(t *testing.T) {
	channel := &EventChannel{
		startedEvent: &build_event_stream.BuildEvent_Started{
			Started: &build_event_stream.BuildStarted{
				BuildToolVersion:   buck2BuildToolVersion,
				OptionsDescription: "buck2 build --local-only //:main",
			},
		},
	}
	finished := &inpb.InvocationEvent{
		EventTime:      nil,
		SequenceNumber: 42,
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_Finished{
				Finished: &build_event_stream.BuildFinished{
					ExitCode: &build_event_stream.BuildFinished_ExitCode{Code: 0},
				},
			},
		},
	}

	syntheticEvents := channel.synthesizeBuck2FallbackTargetEvents(finished)
	require.Len(t, syntheticEvents, 2)
	require.Equal(t, "//:main", syntheticEvents[0].GetBuildEvent().GetId().GetTargetConfigured().GetLabel())
	require.Equal(t, buck2GenericTargetKind, syntheticEvents[0].GetBuildEvent().GetConfigured().GetTargetKind())
	require.Equal(t, "//:main", syntheticEvents[1].GetBuildEvent().GetId().GetTargetCompleted().GetLabel())
	require.Equal(t, buck2ConfigurationID, syntheticEvents[1].GetBuildEvent().GetId().GetTargetCompleted().GetConfiguration().GetId())
	require.True(t, syntheticEvents[1].GetBuildEvent().GetCompleted().GetSuccess())
}

func TestSynthesizeBuck2FallbackTargetEventsSkippedWhenTargetsSeen(t *testing.T) {
	channel := &EventChannel{
		startedEvent: &build_event_stream.BuildEvent_Started{
			Started: &build_event_stream.BuildStarted{
				BuildToolVersion:   buck2BuildToolVersion,
				OptionsDescription: "buck2 build //:main",
			},
		},
		hasBuck2ConfiguredOrCompletedEvents: true,
	}
	finished := &inpb.InvocationEvent{
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_Finished{
				Finished: &build_event_stream.BuildFinished{
					ExitCode: &build_event_stream.BuildFinished_ExitCode{Code: 1},
				},
			},
		},
	}

	require.Nil(t, channel.synthesizeBuck2FallbackTargetEvents(finished))
}

func TestSynthesizeBuck2FallbackTargetEventsForTestCommand(t *testing.T) {
	channel := &EventChannel{
		startedEvent: &build_event_stream.BuildEvent_Started{
			Started: &build_event_stream.BuildStarted{
				BuildToolVersion:   buck2BuildToolVersion,
				OptionsDescription: "buck2 test --local-only //:library_test",
			},
		},
	}
	finished := &inpb.InvocationEvent{
		EventTime:      nil,
		SequenceNumber: 77,
		BuildEvent: &build_event_stream.BuildEvent{
			Payload: &build_event_stream.BuildEvent_Finished{
				Finished: &build_event_stream.BuildFinished{
					ExitCode: &build_event_stream.BuildFinished_ExitCode{Code: 0},
				},
			},
		},
	}

	syntheticEvents := channel.synthesizeBuck2FallbackTargetEvents(finished)
	require.Len(t, syntheticEvents, 2)
	require.Equal(t, "//:library_test", syntheticEvents[0].GetBuildEvent().GetId().GetTargetConfigured().GetLabel())
	require.Equal(t, buck2TestTargetKind, syntheticEvents[0].GetBuildEvent().GetConfigured().GetTargetKind())
	require.Equal(t, "//:library_test", syntheticEvents[1].GetBuildEvent().GetId().GetTargetCompleted().GetLabel())
	require.True(t, syntheticEvents[1].GetBuildEvent().GetCompleted().GetSuccess())
}

func TestSynthesizeBuck2ProgressEventsFromAction(t *testing.T) {
	channel := &EventChannel{
		startedEvent: &build_event_stream.BuildEvent_Started{
			Started: &build_event_stream.BuildStarted{
				BuildToolVersion: buck2BuildToolVersion,
			},
		},
	}
	event := &inpb.InvocationEvent{
		SequenceNumber: 12,
		BuildEvent: &build_event_stream.BuildEvent{
			Id: &build_event_stream.BuildEventId{
				Id: &build_event_stream.BuildEventId_ActionCompleted{
					ActionCompleted: &build_event_stream.BuildEventId_ActionCompletedId{
						Label: "//:main",
					},
				},
			},
			Payload: &build_event_stream.BuildEvent_Action{
				Action: &build_event_stream.ActionExecuted{
					Success: true,
					Type:    "cxx_compile",
					Label:   "//:main",
				},
			},
		},
	}

	progressEvents := channel.synthesizeBuck2ProgressEvents(event)
	require.Len(t, progressEvents, 1)
	require.Contains(t, progressEvents[0].GetBuildEvent().GetProgress().GetStdout(), "ACTION SUCCESS")
	require.Equal(t, int32(121), progressEvents[0].GetBuildEvent().GetId().GetProgress().GetOpaqueCount())
}

func TestSynthesizeBuck2ProgressEventsFromFailedTestResult(t *testing.T) {
	channel := &EventChannel{
		startedEvent: &build_event_stream.BuildEvent_Started{
			Started: &build_event_stream.BuildStarted{
				BuildToolVersion: buck2BuildToolVersion,
			},
		},
	}
	event := &inpb.InvocationEvent{
		SequenceNumber: 7,
		BuildEvent: &build_event_stream.BuildEvent{
			Id: &build_event_stream.BuildEventId{
				Id: &build_event_stream.BuildEventId_TestResult{
					TestResult: &build_event_stream.BuildEventId_TestResultId{
						Label: "//:library_test",
					},
				},
			},
			Payload: &build_event_stream.BuildEvent_TestResult{
				TestResult: &build_event_stream.TestResult{
					Status:        build_event_stream.TestStatus_FAILED,
					StatusDetails: "boom",
				},
			},
		},
	}

	progressEvents := channel.synthesizeBuck2ProgressEvents(event)
	require.Len(t, progressEvents, 1)
	require.Contains(t, progressEvents[0].GetBuildEvent().GetProgress().GetStderr(), "TEST FAILED")
	require.Contains(t, progressEvents[0].GetBuildEvent().GetProgress().GetStderr(), "boom")
}

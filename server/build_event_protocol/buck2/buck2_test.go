package buck2_test

import (
	"testing"
	"time"

	bdpb "github.com/buildbuddy-io/buildbuddy/proto/buckdata"
	bspb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/buck2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func marshalAny(t *testing.T, event *bdpb.BuckEvent) *anypb.Any {
	t.Helper()
	anyEvent, err := anypb.New(event)
	require.NoError(t, err)
	return anyEvent
}

func TestReadBuildEventStarted(t *testing.T) {
	event := &bdpb.BuckEvent{
		Timestamp: timestamppb.New(time.Unix(1_700_000_000, 0)),
		TraceId:   "trace-id",
		Data: &bdpb.BuckEvent_SpanStart{
			SpanStart: &bdpb.SpanStartEvent{
				Data: &bdpb.SpanStartEvent_Command{
					Command: &bdpb.CommandStart{
						Metadata: map[string]string{
							"HOST": "host-a",
							"USER": "user-a",
						},
						CliArgs: []string{"buck2", "build", "//pkg:target"},
						Data:    &bdpb.CommandStart_Build{Build: &bdpb.BuildCommandStart{}},
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(1, marshalAny(t, event), true, &mapped)
	require.NoError(t, err)

	started := mapped.GetStarted()
	require.NotNil(t, started)
	require.Equal(t, "trace-id", started.GetUuid())
	require.Equal(t, "build", started.GetCommand())
	require.Equal(t, "buck2 build //pkg:target", started.GetOptionsDescription())
	require.Equal(t, "host-a", started.GetHost())
	require.Equal(t, "user-a", started.GetUser())
	require.Len(t, mapped.GetChildren(), 1)
	require.Equal(t, []string{"//pkg:target"}, mapped.GetChildren()[0].GetPattern().GetPattern())
}

func TestReadBuildEventStartedExtractsMultipleTargetPatterns(t *testing.T) {
	event := &bdpb.BuckEvent{
		Timestamp: timestamppb.New(time.Unix(1_700_000_000, 0)),
		Data: &bdpb.BuckEvent_SpanStart{
			SpanStart: &bdpb.SpanStartEvent{
				Data: &bdpb.SpanStartEvent_Command{
					Command: &bdpb.CommandStart{
						CliArgs: []string{"buck2", "test", "//pkg:target", "//foo:bar", "--", "ignored"},
						Data:    &bdpb.CommandStart_Test{Test: &bdpb.TestCommandStart{}},
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(1, marshalAny(t, event), true, &mapped)
	require.NoError(t, err)
	require.Len(t, mapped.GetChildren(), 2)
	require.Equal(t, []string{"//pkg:target"}, mapped.GetChildren()[0].GetPattern().GetPattern())
	require.Equal(t, []string{"//foo:bar"}, mapped.GetChildren()[1].GetPattern().GetPattern())
}

func TestReadBuildEventFinishedBuildCompletedSuccess(t *testing.T) {
	event := &bdpb.BuckEvent{
		Timestamp: timestamppb.New(time.Unix(1_700_000_000, 0)),
		Data: &bdpb.BuckEvent_SpanEnd{
			SpanEnd: &bdpb.SpanEndEvent{
				Data: &bdpb.SpanEndEvent_Command{
					Command: &bdpb.CommandEnd{
						IsSuccess: false,
						BuildResult: &bdpb.BuildResult{
							BuildCompleted: true,
						},
						Data: &bdpb.CommandEnd_Build{Build: &bdpb.BuildCommandEnd{}},
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(2, marshalAny(t, event), false, &mapped)
	require.NoError(t, err)
	finished := mapped.GetFinished()
	require.NotNil(t, finished)
	require.Equal(t, int32(0), finished.GetExitCode().GetCode())
	require.Equal(t, "SUCCESS", finished.GetExitCode().GetName())
}

func TestReadBuildEventFinishedBuildIncompleteFailed(t *testing.T) {
	event := &bdpb.BuckEvent{
		Timestamp: timestamppb.New(time.Unix(1_700_000_000, 0)),
		Data: &bdpb.BuckEvent_SpanEnd{
			SpanEnd: &bdpb.SpanEndEvent{
				Data: &bdpb.SpanEndEvent_Command{
					Command: &bdpb.CommandEnd{
						IsSuccess: false,
						BuildResult: &bdpb.BuildResult{
							BuildCompleted: false,
						},
						Data: &bdpb.CommandEnd_Build{Build: &bdpb.BuildCommandEnd{}},
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(3, marshalAny(t, event), false, &mapped)
	require.NoError(t, err)
	finished := mapped.GetFinished()
	require.NotNil(t, finished)
	require.Equal(t, int32(1), finished.GetExitCode().GetCode())
	require.Equal(t, "FAILED", finished.GetExitCode().GetName())
}

func TestReadBuildEventFinishedFromInvocationRecordFailed(t *testing.T) {
	event := &bdpb.BuckEvent{
		Timestamp: timestamppb.New(time.Unix(1_700_000_000, 0)),
		Data: &bdpb.BuckEvent_Record{
			Record: &bdpb.RecordEvent{
				Data: &bdpb.RecordEvent_InvocationRecord{
					InvocationRecord: &bdpb.InvocationRecord{
						ExitCode:       proto.Uint32(32),
						ExitResultName: proto.String("TESTS_FAILED"),
						Outcome:        bdpb.InvocationOutcome_Failed.Enum(),
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(3, marshalAny(t, event), false, &mapped)
	require.NoError(t, err)
	finished := mapped.GetFinished()
	require.NotNil(t, finished)
	require.Equal(t, int32(32), finished.GetExitCode().GetCode())
	require.Equal(t, "TESTS_FAILED", finished.GetExitCode().GetName())
}

func TestReadBuildEventFinishedFromInvocationRecordSuccess(t *testing.T) {
	event := &bdpb.BuckEvent{
		Timestamp: timestamppb.New(time.Unix(1_700_000_000, 0)),
		Data: &bdpb.BuckEvent_Record{
			Record: &bdpb.RecordEvent{
				Data: &bdpb.RecordEvent_InvocationRecord{
					InvocationRecord: &bdpb.InvocationRecord{
						Outcome: bdpb.InvocationOutcome_Success.Enum(),
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(3, marshalAny(t, event), false, &mapped)
	require.NoError(t, err)
	finished := mapped.GetFinished()
	require.NotNil(t, finished)
	require.Equal(t, int32(0), finished.GetExitCode().GetCode())
	require.Equal(t, "SUCCESS", finished.GetExitCode().GetName())
}

func TestReadBuildEventTestResult(t *testing.T) {
	event := &bdpb.BuckEvent{
		Timestamp: timestamppb.New(time.Unix(1_700_000_100, 0)),
		Data: &bdpb.BuckEvent_Instant{
			Instant: &bdpb.InstantEvent{
				Data: &bdpb.InstantEvent_TestResult{
					TestResult: &bdpb.TestResult{
						Name:    "test_case",
						Status:  bdpb.TestStatus_PASS,
						Details: "details",
						Duration: &durationpb.Duration{
							Seconds: 3,
						},
						TargetLabel: &bdpb.ConfiguredTargetLabel{
							Label: &bdpb.TargetLabel{
								Package: "pkg",
								Name:    "my_test",
							},
							Configuration: &bdpb.Configuration{FullName: "cfg"},
						},
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(2, marshalAny(t, event), false, &mapped)
	require.NoError(t, err)

	testResult := mapped.GetTestResult()
	require.NotNil(t, testResult)
	require.Equal(t, bspb.TestStatus_PASSED, testResult.GetStatus())
	require.Equal(t, "details", testResult.GetStatusDetails())
	require.Equal(t, "//pkg:my_test", mapped.GetId().GetTestResult().GetLabel())
	require.Equal(t, "cfg", mapped.GetId().GetTestResult().GetConfiguration().GetId())
	require.NotNil(t, testResult.GetTestAttemptStart())
	require.Equal(t, int64(3), testResult.GetTestAttemptDuration().GetSeconds())
}

func TestReadBuildEventBuildMetadataFromTargetPatterns(t *testing.T) {
	event := &bdpb.BuckEvent{
		Data: &bdpb.BuckEvent_Instant{
			Instant: &bdpb.InstantEvent{
				Data: &bdpb.InstantEvent_TargetPatterns{
					TargetPatterns: &bdpb.ParsedTargetPatterns{
						TargetPatterns: []*bdpb.TargetPattern{
							{Value: "//a/..."},
							{Value: "//b:c"},
						},
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(3, marshalAny(t, event), false, &mapped)
	require.NoError(t, err)

	metadata := mapped.GetBuildMetadata()
	require.NotNil(t, metadata)
	require.Equal(t, "//a/... //b:c", metadata.GetMetadata()["PATTERN"])
}

func TestReadBuildEventConfiguredFromTestDiscovery(t *testing.T) {
	event := &bdpb.BuckEvent{
		Data: &bdpb.BuckEvent_Instant{
			Instant: &bdpb.InstantEvent{
				Data: &bdpb.InstantEvent_TestDiscovery{
					TestDiscovery: &bdpb.TestDiscovery{
						Data: &bdpb.TestDiscovery_Tests{
							Tests: &bdpb.TestSuite{
								TargetLabel: &bdpb.ConfiguredTargetLabel{
									Label: &bdpb.TargetLabel{
										Package: "foo/bar",
										Name:    "my_test",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(4, marshalAny(t, event), false, &mapped)
	require.NoError(t, err)

	configured := mapped.GetConfigured()
	require.NotNil(t, configured)
	require.Equal(t, "buck2_test rule", configured.GetTargetKind())
	require.Equal(t, "//foo/bar:my_test", mapped.GetId().GetTargetConfigured().GetLabel())
}

func TestReadBuildEventConfiguredFromAnalysisStart(t *testing.T) {
	event := &bdpb.BuckEvent{
		Data: &bdpb.BuckEvent_SpanStart{
			SpanStart: &bdpb.SpanStartEvent{
				Data: &bdpb.SpanStartEvent_Analysis{
					Analysis: &bdpb.AnalysisStart{
						Target: &bdpb.AnalysisStart_StandardTarget{
							StandardTarget: &bdpb.ConfiguredTargetLabel{
								Label: &bdpb.TargetLabel{
									Package: "root//pkg",
									Name:    "lib",
								},
							},
						},
						Rule: "go_library",
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(6, marshalAny(t, event), false, &mapped)
	require.NoError(t, err)

	configured := mapped.GetConfigured()
	require.NotNil(t, configured)
	require.Equal(t, "go_library rule", configured.GetTargetKind())
	require.Equal(t, "//pkg:lib", mapped.GetId().GetTargetConfigured().GetLabel())
}

func TestReadBuildEventCompletedFromAnalysisEnd(t *testing.T) {
	event := &bdpb.BuckEvent{
		Data: &bdpb.BuckEvent_SpanEnd{
			SpanEnd: &bdpb.SpanEndEvent{
				Data: &bdpb.SpanEndEvent_Analysis{
					Analysis: &bdpb.AnalysisEnd{
						Target: &bdpb.AnalysisEnd_StandardTarget{
							StandardTarget: &bdpb.ConfiguredTargetLabel{
								Label: &bdpb.TargetLabel{
									Package: "root//pkg",
									Name:    "lib",
								},
								Configuration: &bdpb.Configuration{
									FullName: "cfg",
								},
							},
						},
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(7, marshalAny(t, event), false, &mapped)
	require.NoError(t, err)

	completed := mapped.GetCompleted()
	require.NotNil(t, completed)
	require.True(t, completed.GetSuccess())
	require.Equal(t, "//pkg:lib", mapped.GetId().GetTargetCompleted().GetLabel())
	require.Equal(t, "cfg", mapped.GetId().GetTargetCompleted().GetConfiguration().GetId())
}

func TestReadBuildEventActionFromActionExecutionEnd(t *testing.T) {
	eventTimestamp := timestamppb.New(time.Unix(1_700_000_200, 0))
	event := &bdpb.BuckEvent{
		Timestamp: eventTimestamp,
		Data: &bdpb.BuckEvent_SpanEnd{
			SpanEnd: &bdpb.SpanEndEvent{
				Duration: durationpb.New(5 * time.Second),
				Data: &bdpb.SpanEndEvent_ActionExecution{
					ActionExecution: &bdpb.ActionExecutionEnd{
						Key: &bdpb.ActionKey{
							Key: "_42",
							Owner: &bdpb.ActionKey_TargetLabel{
								TargetLabel: &bdpb.ConfiguredTargetLabel{
									Label: &bdpb.TargetLabel{
										Package: "root//pkg",
										Name:    "bin",
									},
									Configuration: &bdpb.Configuration{
										FullName: "cfg",
									},
								},
							},
						},
						Name: &bdpb.ActionName{
							Category:   "cxx_link",
							Identifier: "main",
						},
						Failed: true,
						Outputs: []*bdpb.ActionOutput{
							{TinyDigest: "abc123"},
						},
						Commands: []*bdpb.CommandExecution{
							{
								Details: &bdpb.CommandExecutionDetails{
									SignedExitCode: proto.Int32(17),
									CmdStdout:      "stdout body",
									CmdStderr:      "stderr body",
									CommandKind: &bdpb.CommandExecutionKind{
										Command: &bdpb.CommandExecutionKind_LocalCommand{
											LocalCommand: &bdpb.LocalCommand{
												Argv: []string{"clang", "-o", "bin"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(8, marshalAny(t, event), false, &mapped)
	require.NoError(t, err)

	action := mapped.GetAction()
	require.NotNil(t, action)
	require.False(t, action.GetSuccess())
	require.Equal(t, int32(17), action.GetExitCode())
	require.Equal(t, "cxx_link", action.GetType())
	require.Equal(t, "clang", action.GetCommandLine()[0])
	require.Equal(t, "stdout body", string(action.GetStdout().GetContents()))
	require.Equal(t, "stderr body", string(action.GetStderr().GetContents()))
	require.Equal(t, "//pkg:bin", mapped.GetId().GetActionCompleted().GetLabel())
	require.Equal(t, "cfg", mapped.GetId().GetActionCompleted().GetConfiguration().GetId())
	require.Equal(t, "abc123", mapped.GetId().GetActionCompleted().GetPrimaryOutput())
	require.Equal(t, eventTimestamp.AsTime(), action.GetEndTime().AsTime())
	require.Equal(t, eventTimestamp.AsTime().Add(-5*time.Second), action.GetStartTime().AsTime())
}

func TestReadBuildEventTestSummaryFromTestRunEnd(t *testing.T) {
	eventTimestamp := timestamppb.New(time.Unix(1_700_000_260, 0))
	event := &bdpb.BuckEvent{
		Timestamp: eventTimestamp,
		Data: &bdpb.BuckEvent_SpanEnd{
			SpanEnd: &bdpb.SpanEndEvent{
				Duration: durationpb.New(2 * time.Second),
				Data: &bdpb.SpanEndEvent_TestEnd{
					TestEnd: &bdpb.TestRunEnd{
						Suite: &bdpb.TestSuite{
							TestNames: []string{"a", "b", "c"},
							TargetLabel: &bdpb.ConfiguredTargetLabel{
								Label: &bdpb.TargetLabel{
									Package: "pkg",
									Name:    "suite_test",
								},
								Configuration: &bdpb.Configuration{
									FullName: "cfg",
								},
							},
						},
						CommandReport: &bdpb.CommandExecution{
							Details: &bdpb.CommandExecutionDetails{
								CmdStdout: "passed",
							},
							Status: &bdpb.CommandExecution_Success_{
								Success: &bdpb.CommandExecution_Success{},
							},
						},
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(9, marshalAny(t, event), false, &mapped)
	require.NoError(t, err)

	summary := mapped.GetTestSummary()
	require.NotNil(t, summary)
	require.Equal(t, bspb.TestStatus_PASSED, summary.GetOverallStatus())
	require.Equal(t, int32(3), summary.GetTotalRunCount())
	require.Equal(t, "//pkg:suite_test", mapped.GetId().GetTestSummary().GetLabel())
	require.Equal(t, "cfg", mapped.GetId().GetTestSummary().GetConfiguration().GetId())
	require.Equal(t, eventTimestamp.AsTime().Add(-2*time.Second), summary.GetFirstStartTime().AsTime())
	require.Equal(t, eventTimestamp.AsTime(), summary.GetLastStopTime().AsTime())
	require.Equal(t, int64(2), summary.GetTotalRunDuration().GetSeconds())
	require.NotEmpty(t, summary.GetPassed())
}

func TestReadBuildEventBuildMetadataFromTargetCfgAndCommandOptions(t *testing.T) {
	targetCfgEvent := &bdpb.BuckEvent{
		Data: &bdpb.BuckEvent_Instant{
			Instant: &bdpb.InstantEvent{
				Data: &bdpb.InstantEvent_TargetCfg{
					TargetCfg: &bdpb.TargetCfg{
						TargetPlatforms: []string{"prelude//platforms:default"},
						CliModifiers:    []string{"opt", "linux"},
					},
				},
			},
		},
	}
	commandOptionsEvent := &bdpb.BuckEvent{
		Data: &bdpb.BuckEvent_Instant{
			Instant: &bdpb.InstantEvent{
				Data: &bdpb.InstantEvent_ComandOptions{
					ComandOptions: &bdpb.CommandOptions{
						ConfiguredParallelism: 42,
						AvailableParallelism:  64,
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(10, marshalAny(t, targetCfgEvent), false, &mapped)
	require.NoError(t, err)
	metadata := mapped.GetBuildMetadata()
	require.NotNil(t, metadata)
	require.Equal(t, "prelude//platforms:default", metadata.GetMetadata()["TARGET_PLATFORM"])
	require.Equal(t, "opt,linux", metadata.GetMetadata()["BUCK2_CLI_MODIFIERS"])

	err = buck2.ReadBuildEvent(11, marshalAny(t, commandOptionsEvent), false, &mapped)
	require.NoError(t, err)
	metadata = mapped.GetBuildMetadata()
	require.NotNil(t, metadata)
	require.Equal(t, "42", metadata.GetMetadata()["BUCK2_CONFIGURED_PARALLELISM"])
	require.Equal(t, "64", metadata.GetMetadata()["BUCK2_AVAILABLE_PARALLELISM"])
}

func TestReadBuildEventProgressConsoleMessageToStderr(t *testing.T) {
	event := &bdpb.BuckEvent{
		Data: &bdpb.BuckEvent_Instant{
			Instant: &bdpb.InstantEvent{
				Data: &bdpb.InstantEvent_ConsoleMessage{
					ConsoleMessage: &bdpb.ConsoleMessage{
						Message: "hello from buck2",
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(5, marshalAny(t, event), false, &mapped)
	require.NoError(t, err)

	progress := mapped.GetProgress()
	require.NotNil(t, progress)
	require.Equal(t, "hello from buck2", progress.GetStderr())
	require.Equal(t, "", progress.GetStdout())
}

func TestReadBuildEventProgressActionErrorIncludesCommandStderr(t *testing.T) {
	event := &bdpb.BuckEvent{
		Data: &bdpb.BuckEvent_Instant{
			Instant: &bdpb.InstantEvent{
				Data: &bdpb.InstantEvent_ActionError{
					ActionError: &bdpb.ActionError{
						LastCommand: &bdpb.CommandExecution{
							Details: &bdpb.CommandExecutionDetails{
								CmdStderr: "compile failed",
							},
						},
					},
				},
			},
		},
	}

	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(6, marshalAny(t, event), false, &mapped)
	require.NoError(t, err)

	progress := mapped.GetProgress()
	require.NotNil(t, progress)
	require.Equal(t, "compile failed", progress.GetStderr())
}

func TestReadBuildEventRejectsUnexpectedTypeURL(t *testing.T) {
	var mapped bspb.BuildEvent
	err := buck2.ReadBuildEvent(1, &anypb.Any{
		TypeUrl: "type.googleapis.com/build_event_stream.BuildEvent",
		Value:   []byte{},
	}, false, &mapped)
	require.ErrorContains(t, err, "Unsupported experimental_build_tool_event type_url")
}

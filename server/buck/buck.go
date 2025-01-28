package buck

import (
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/buckdata"
	buck_error "github.com/buildbuddy-io/buildbuddy/proto/buckerror"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	fdpb "github.com/buildbuddy-io/buildbuddy/proto/failure_details"
)

func ToBazelEvents(buckBuildEvent *bepb.BuildEvent_BuckEvent) ([]*bespb.BuildEvent, error) {
	var buckEvent buckdata.BuckEvent
	// if err := buckBuildEvent.BuckEvent.UnmarshalTo(&buckEvent); err != nil {
	// 	return nil, err
	// }
	// b, err := protojson.MarshalOptions{Multiline: true}.Marshal(&buckEvent)
	// if err != nil {
	// 	return nil, err
	// }
	// var bb bytes.Buffer
	// if err = json.Indent(&bb, b, "", "  "); err != nil {
	// 	return nil, err
	// }
	// TODO: Remove debug logging
	// log.Infof("BuckEvent: %s", bb.String())

	switch buckEvent.GetData().(type) {
	case *buckdata.BuckEvent_SpanStart:
		spanStart := buckEvent.GetSpanStart()
		switch spanStart.GetData().(type) {
		case *buckdata.SpanStartEvent_Command:
			commandStart := spanStart.GetCommand()
			var command string
			switch commandStart.GetData().(type) {
			case *buckdata.CommandStart_Build:
				command = "build"
			case *buckdata.CommandStart_Targets:
				command = "targets"
			case *buckdata.CommandStart_Query:
				command = "query"
			case *buckdata.CommandStart_Cquery:
				cquery := commandStart.GetCquery()
				commands := []string{"cquery"}
				if cquery.GetTargetUniverse() != "" {
					commands = append(commands, fmt.Sprintf("--target-universe=%s", cquery.GetTargetUniverse()))
				}
				commands = append(commands, cquery.GetQuery())
				commands = append(commands, cquery.GetQueryArgs())
				command = strings.Join(commands, " ")
			case *buckdata.CommandStart_Test:
				command = "test"
			case *buckdata.CommandStart_Audit:
				command = "audit"
			case *buckdata.CommandStart_Docs:
				command = "docs"
			case *buckdata.CommandStart_Clean:
				command = "clean"
			case *buckdata.CommandStart_Aquery:
				command = "aquery"
			case *buckdata.CommandStart_Install:
				command = "install"
			case *buckdata.CommandStart_Materialize:
				command = "materialize"
			case *buckdata.CommandStart_Profile:
				command = "profile"
			case *buckdata.CommandStart_Bxl:
				bxl := commandStart.GetBxl()
				command = strings.Join([]string{"bxl", bxl.GetBxlLabel()}, " ")
			case *buckdata.CommandStart_Lsp:
				command = "lsp"
			case *buckdata.CommandStart_FileStatus:
				command = "filestatus"
			case *buckdata.CommandStart_Starlark:
				command = "starlark"
			case *buckdata.CommandStart_Subscribe:
				command = "subscribe"
			case *buckdata.CommandStart_Trace:
				command = "trace"
			case *buckdata.CommandStart_Ctargets:
				command = "ctargets"
			case *buckdata.CommandStart_StarlarkDebugAttach:
				command = "starlarkdebugattach"
			case *buckdata.CommandStart_Explain:
				command = "explain"
			case *buckdata.CommandStart_ExpandExternalCell:
				command = "expandexternalcell"
			case *buckdata.CommandStart_Complete:
				command = "complete"
			default:
			}
			started := &bespb.BuildEvent{
				Id: &bespb.BuildEventId{
					Id: &bespb.BuildEventId_Started{
						Started: &bespb.BuildEventId_BuildStartedId{},
					},
				},
				Children:    []*bespb.BuildEventId{},
				LastMessage: false,
				Payload: &bespb.BuildEvent_Started{
					Started: &bespb.BuildStarted{
						Uuid:               buckEvent.GetTraceId(),
						StartTime:          buckEvent.GetTimestamp(),
						BuildToolVersion:   "buck2",
						OptionsDescription: "",
						Command:            command,
						WorkingDirectory:   "",
						WorkspaceDirectory: "",
						ServerPid:          0,
					},
				},
			}
			optionsParsed := &bespb.BuildEvent{
				Id: &bespb.BuildEventId{
					Id: &bespb.BuildEventId_OptionsParsed{
						OptionsParsed: &bespb.BuildEventId_OptionsParsedId{},
					},
				},
				Children:    []*bespb.BuildEventId{},
				LastMessage: false,
				Payload: &bespb.BuildEvent_OptionsParsed{
					OptionsParsed: &bespb.OptionsParsed{
						StartupOptions:         []string{},
						ExplicitStartupOptions: []string{},
						CmdLine:                []string{},
						ExplicitCmdLine:        []string{},
						ToolTag:                "",
					},
				},
			}
			return []*bespb.BuildEvent{started, optionsParsed}, nil
		case *buckdata.SpanStartEvent_ActionExecution:
		case *buckdata.SpanStartEvent_Analysis:
			analysisStart := spanStart.GetAnalysis()

			var label, kind string
			switch analysisStart.GetTarget().(type) {
			case *buckdata.AnalysisStart_StandardTarget:
				standardTarget := analysisStart.GetStandardTarget()
				label = standardTarget.GetLabel().GetPackage() + ":" + standardTarget.GetLabel().GetName()
				kind = "StandardTarget"
			case *buckdata.AnalysisStart_AnonTarget:
				anonTarget := analysisStart.GetAnonTarget()
				label = anonTarget.GetName().GetPackage() + ":" + anonTarget.GetName().GetName()
				kind = "AnonTarget"
			case *buckdata.AnalysisStart_DynamicLambda:
				dynamicLambda := analysisStart.GetDynamicLambda()

				switch dynamicLambda.GetOwner().(type) {
				case *buckdata.DynamicLambdaOwner_TargetLabel:
					targetLabel := dynamicLambda.GetTargetLabel()
					label = targetLabel.GetLabel().GetPackage() + ":" + targetLabel.GetLabel().GetName()
					kind = "DynamicLambda-TargetLabelOwner"
				case *buckdata.DynamicLambdaOwner_BxlKey:
					bxlKey := dynamicLambda.GetBxlKey()
					label = bxlKey.GetLabel().GetBxlPath() + ":" + bxlKey.GetLabel().GetName()
					kind = "DynamicLambda-BxlKeyOwner"
				case *buckdata.DynamicLambdaOwner_AnonTarget:
					anonTarget := dynamicLambda.GetAnonTarget()
					label = anonTarget.GetName().GetPackage() + ":" + anonTarget.GetName().GetName()
					kind = "DynamicLambda-AnonTargetOwner"
				}
			}

			targetConfiguredEvent := &bespb.BuildEvent{
				Id: &bespb.BuildEventId{
					Id: &bespb.BuildEventId_TargetConfigured{
						TargetConfigured: &bespb.BuildEventId_TargetConfiguredId{
							Label:  label,
							Aspect: "",
						},
					},
				},
				Children:    []*bespb.BuildEventId{},
				LastMessage: false,
				Payload: &bespb.BuildEvent_Configured{
					Configured: &bespb.TargetConfigured{
						TargetKind: kind,
						TestSize:   bespb.TestSize_UNKNOWN,
						Tag:        []string{},
					},
				},
			}
			targetExpandedEvent := &bespb.BuildEvent{
				Id: &bespb.BuildEventId{
					Id: &bespb.BuildEventId_Pattern{
						Pattern: &bespb.BuildEventId_PatternExpandedId{
							Pattern: []string{label},
						},
					},
				},
				Children:    []*bespb.BuildEventId{targetConfiguredEvent.GetId()},
				LastMessage: false,
				Payload: &bespb.BuildEvent_Expanded{
					Expanded: &bespb.PatternExpanded{
						TestSuiteExpansions: []*bespb.PatternExpanded_TestSuiteExpansion{},
					},
				},
			}
			return []*bespb.BuildEvent{targetConfiguredEvent, targetExpandedEvent}, nil

		case *buckdata.SpanStartEvent_AnalysisResolveQueries:
		case *buckdata.SpanStartEvent_Load:
		case *buckdata.SpanStartEvent_ExecutorStage:
		case *buckdata.SpanStartEvent_TestDiscovery:
		case *buckdata.SpanStartEvent_TestStart:
		case *buckdata.SpanStartEvent_FileWatcher:
		case *buckdata.SpanStartEvent_FinalMaterialization:
		case *buckdata.SpanStartEvent_AnalysisStage:
		case *buckdata.SpanStartEvent_MatchDepFiles:
		case *buckdata.SpanStartEvent_LoadPackage:
		case *buckdata.SpanStartEvent_SharedTask:
		case *buckdata.SpanStartEvent_CacheUpload:
		case *buckdata.SpanStartEvent_CreateOutputSymlinks:
		case *buckdata.SpanStartEvent_CommandCritical:
		case *buckdata.SpanStartEvent_InstallEventInfo:
		case *buckdata.SpanStartEvent_DiceStateUpdate:
		case *buckdata.SpanStartEvent_Materialization:
		case *buckdata.SpanStartEvent_DiceCriticalSection:
		case *buckdata.SpanStartEvent_DiceBlockConcurrentCommand:
		case *buckdata.SpanStartEvent_DiceSynchronizeSection:
		case *buckdata.SpanStartEvent_DiceCleanup:
		case *buckdata.SpanStartEvent_ExclusiveCommandWait:
		case *buckdata.SpanStartEvent_DeferredPreparationStage:
		case *buckdata.SpanStartEvent_DynamicLambda:
		case *buckdata.SpanStartEvent_BxlExecution:
		case *buckdata.SpanStartEvent_BxlDiceInvocation:
		case *buckdata.SpanStartEvent_ReUpload:
		case *buckdata.SpanStartEvent_ConnectToInstaller:
		case *buckdata.SpanStartEvent_LocalResources:
		case *buckdata.SpanStartEvent_ReleaseLocalResources:
		case *buckdata.SpanStartEvent_BxlEnsureArtifacts:
		case *buckdata.SpanStartEvent_CreateOutputHashesFile:
		case *buckdata.SpanStartEvent_ActionErrorHandlerExecution:
		case *buckdata.SpanStartEvent_CqueryUniverseBuild:
		case *buckdata.SpanStartEvent_DepFileUpload:
		case *buckdata.SpanStartEvent_Fake:

		default:
		}
	case *buckdata.BuckEvent_SpanEnd:
		spanEndEvent := buckEvent.GetSpanEnd()
		switch spanEndEvent.GetData().(type) {
		case *buckdata.SpanEndEvent_Command:
			commandEnd := spanEndEvent.GetCommand()
			switch commandEnd.GetData().(type) {
			case *buckdata.CommandEnd_Build:
			case *buckdata.CommandEnd_Targets:
			case *buckdata.CommandEnd_Query:
			case *buckdata.CommandEnd_Cquery:
			case *buckdata.CommandEnd_Test:
			case *buckdata.CommandEnd_Audit:
			case *buckdata.CommandEnd_Docs:
			case *buckdata.CommandEnd_Clean:
			case *buckdata.CommandEnd_Aquery:
			case *buckdata.CommandEnd_Install:
			case *buckdata.CommandEnd_Materialize:
			case *buckdata.CommandEnd_Profile:
			case *buckdata.CommandEnd_Bxl:
			case *buckdata.CommandEnd_Lsp:
			case *buckdata.CommandEnd_FileStatus:
			case *buckdata.CommandEnd_Starlark:
			case *buckdata.CommandEnd_Subscribe:
			case *buckdata.CommandEnd_Trace:
			case *buckdata.CommandEnd_Ctargets:
			case *buckdata.CommandEnd_StarlarkDebugAttach:
			case *buckdata.CommandEnd_Explain:
			case *buckdata.CommandEnd_ExpandExternalCell:
			case *buckdata.CommandEnd_Complete:
			default:
			}
			var exitCode *bespb.BuildFinished_ExitCode
			var failureDetail *fdpb.FailureDetail
			if commandEnd.GetIsSuccess() {
				exitCode = &bespb.BuildFinished_ExitCode{
					Name: "SUCCESS",
					Code: 0,
				}
			} else {
				exitCode = &bespb.BuildFinished_ExitCode{
					Name: "FAILURE",
					Code: 1,
				}
				firstError := commandEnd.GetErrors()[0]
				failureDetail = getFailureDetail(firstError)
			}
			return []*bespb.BuildEvent{&bespb.BuildEvent{
				Id: &bespb.BuildEventId{
					Id: &bespb.BuildEventId_BuildFinished{
						BuildFinished: &bespb.BuildEventId_BuildFinishedId{},
					},
				},
				Children: []*bespb.BuildEventId{},
				// TODO: with BuckEvent, the last event could be an instan InstantEvent_Snapshot
				// message with useful metrics inside. Handle it.
				LastMessage: true,
				Payload: &bespb.BuildEvent_Finished{
					Finished: &bespb.BuildFinished{
						OverallSuccess: commandEnd.GetIsSuccess(),
						ExitCode:       exitCode,
						FinishTime:     buckEvent.GetTimestamp(),
						FailureDetail:  failureDetail,
					},
				},
			}}, nil

		case *buckdata.SpanEndEvent_ActionExecution:
		case *buckdata.SpanEndEvent_Analysis:
		case *buckdata.SpanEndEvent_AnalysisResolveQueries:
		case *buckdata.SpanEndEvent_Load:
		case *buckdata.SpanEndEvent_ExecutorStage:
		case *buckdata.SpanEndEvent_TestDiscovery:
		case *buckdata.SpanEndEvent_TestEnd:
		case *buckdata.SpanEndEvent_SpanCancelled:
		case *buckdata.SpanEndEvent_FileWatcher:
		case *buckdata.SpanEndEvent_FinalMaterialization:
		case *buckdata.SpanEndEvent_AnalysisStage:
		case *buckdata.SpanEndEvent_MatchDepFiles:
		case *buckdata.SpanEndEvent_LoadPackage:
		case *buckdata.SpanEndEvent_SharedTask:
		case *buckdata.SpanEndEvent_CacheUpload:
		case *buckdata.SpanEndEvent_CreateOutputSymlinks:
		case *buckdata.SpanEndEvent_CommandCritical:
		case *buckdata.SpanEndEvent_InstallEventInfo:
		case *buckdata.SpanEndEvent_DiceStateUpdate:
		case *buckdata.SpanEndEvent_Materialization:
		case *buckdata.SpanEndEvent_DiceCriticalSection:
		case *buckdata.SpanEndEvent_DiceBlockConcurrentCommand:
		case *buckdata.SpanEndEvent_DiceSynchronizeSection:
		case *buckdata.SpanEndEvent_DiceCleanup:
		case *buckdata.SpanEndEvent_ExclusiveCommandWait:
		case *buckdata.SpanEndEvent_DeferredPreparationStage:
		case *buckdata.SpanEndEvent_DeferredEvaluation:
		case *buckdata.SpanEndEvent_BxlExecution:
		case *buckdata.SpanEndEvent_BxlDiceInvocation:
		case *buckdata.SpanEndEvent_ReUpload:
		case *buckdata.SpanEndEvent_ConnectToInstaller:
		case *buckdata.SpanEndEvent_LocalResources:
		case *buckdata.SpanEndEvent_ReleaseLocalResources:
		case *buckdata.SpanEndEvent_BxlEnsureArtifacts:
		case *buckdata.SpanEndEvent_CreateOutputHashesFile:
		case *buckdata.SpanEndEvent_ActionErrorHandlerExecution:
		case *buckdata.SpanEndEvent_CqueryUniverseBuild:
		case *buckdata.SpanEndEvent_DepFileUpload:
		case *buckdata.SpanEndEvent_Fake:
		default:
		}
	case *buckdata.BuckEvent_Instant:
		instant := buckEvent.GetInstant()
		switch instant.GetData().(type) {
		case *buckdata.InstantEvent_StructuredError:
		case *buckdata.InstantEvent_ConsoleMessage:
		case *buckdata.InstantEvent_BuildGraphInfo:
		case *buckdata.InstantEvent_ReSession:
		case *buckdata.InstantEvent_TestDiscovery:
		case *buckdata.InstantEvent_TestResult:
		case *buckdata.InstantEvent_Snapshot:
		case *buckdata.InstantEvent_DiceStateSnapshot:
		case *buckdata.InstantEvent_TagEvent:
		case *buckdata.InstantEvent_TargetPatterns:
		case *buckdata.InstantEvent_DiceEqualityCheck:
		case *buckdata.InstantEvent_NoActiveDiceState:
		case *buckdata.InstantEvent_MaterializerStateInfo:
		case *buckdata.InstantEvent_DaemonShutdown:
		case *buckdata.InstantEvent_RageResult:
		case *buckdata.InstantEvent_ConsolePreferences:
		case *buckdata.InstantEvent_IoProviderInfo:
		case *buckdata.InstantEvent_StarlarkFailNoStacktrace:
		case *buckdata.InstantEvent_DebugAdapterSnapshot:
		case *buckdata.InstantEvent_RestartConfiguration:
		case *buckdata.InstantEvent_UntrackedFile:
		case *buckdata.InstantEvent_StarlarkUserEvent:
		case *buckdata.InstantEvent_ComandOptions:
		case *buckdata.InstantEvent_ConcurrentCommands:
		case *buckdata.InstantEvent_PersistEventLogSubprocess:
		case *buckdata.InstantEvent_ActionError:
		case *buckdata.InstantEvent_ConsoleWarning:
		case *buckdata.InstantEvent_MaterializerCommand:
		case *buckdata.InstantEvent_CleanStaleResult:
		case *buckdata.InstantEvent_CellConfigDiff:
		case *buckdata.InstantEvent_InstallFinished:
		case *buckdata.InstantEvent_SystemInfo:
		case *buckdata.InstantEvent_VersionControlRevision:
		case *buckdata.InstantEvent_TargetCfg:
		case *buckdata.InstantEvent_UnstableE2EData:
		case *buckdata.InstantEvent_EndOfTestResults:
		case *buckdata.InstantEvent_ConfigurationCreated:
		case *buckdata.InstantEvent_BuckconfigInputValues:
		default:
		}
	case *buckdata.BuckEvent_Record:
		record := buckEvent.GetRecord()
		switch record.GetData().(type) {
		case *buckdata.RecordEvent_InvocationRecord:
		case *buckdata.RecordEvent_BuildGraphStats:
		default:
		}
	}

	return []*bespb.BuildEvent{}, nil
}

func getFailureDetail(error *buckdata.ErrorReport) *fdpb.FailureDetail {
	fd := &fdpb.FailureDetail{
		Message:  error.GetMessage(),
		Category: &fdpb.FailureDetail_ActionCache{},
	}
	switch error.GetTags()[0] {
	case buck_error.ErrorTag_RE_UNKNOWN_TCODE,
		buck_error.ErrorTag_RE_CANCELLED,
		buck_error.ErrorTag_RE_UNKNOWN,
		buck_error.ErrorTag_RE_INVALID_ARGUMENT,
		buck_error.ErrorTag_RE_DEADLINE_EXCEEDED,
		buck_error.ErrorTag_RE_NOT_FOUND,
		buck_error.ErrorTag_RE_ALREADY_EXISTS,
		buck_error.ErrorTag_RE_PERMISSION_DENIED,
		buck_error.ErrorTag_RE_RESOURCE_EXHAUSTED,
		buck_error.ErrorTag_RE_FAILED_PRECONDITION,
		buck_error.ErrorTag_RE_ABORTED,
		buck_error.ErrorTag_RE_OUT_OF_RANGE,
		buck_error.ErrorTag_RE_UNIMPLEMENTED,
		buck_error.ErrorTag_RE_INTERNAL,
		buck_error.ErrorTag_RE_UNAVAILABLE,
		buck_error.ErrorTag_RE_DATA_LOSS,
		buck_error.ErrorTag_RE_UNAUTHENTICATED:
		fd.Category = &fdpb.FailureDetail_RemoteExecution{
			RemoteExecution: &fdpb.RemoteExecution{
				Code: fdpb.RemoteExecution_REMOTE_EXECUTION_UNKNOWN,
			},
		}
	case buck_error.ErrorTag_STARLARK_FAIL,
		buck_error.ErrorTag_STARLARK_ERROR,
		buck_error.ErrorTag_STARLARK_STACK_OVERFLOW,
		buck_error.ErrorTag_STARLARK_INTERNAL,
		buck_error.ErrorTag_STARLARK_VALUE,
		buck_error.ErrorTag_STARLARK_FUNCTION,
		buck_error.ErrorTag_STARLARK_SCOPE,
		buck_error.ErrorTag_STARLARK_NATIVE_INPUT:
		fd.Category = &fdpb.FailureDetail_StarlarkLoading{
			StarlarkLoading: &fdpb.StarlarkLoading{
				Code: fdpb.StarlarkLoading_STARLARK_LOADING_UNKNOWN,
			},
		}
	case buck_error.ErrorTag_VISIBILITY:
		fd.Category = &fdpb.FailureDetail_StarlarkLoading{
			StarlarkLoading: &fdpb.StarlarkLoading{
				Code: fdpb.StarlarkLoading_VISIBILITY_ERROR,
			},
		}
	case buck_error.ErrorTag_STARLARK_PARSER:
		fd.Category = &fdpb.FailureDetail_StarlarkLoading{
			StarlarkLoading: &fdpb.StarlarkLoading{
				Code: fdpb.StarlarkLoading_PARSE_ERROR,
			},
		}
	case buck_error.ErrorTag_ENVIRONMENT:
		fd.Category = &fdpb.FailureDetail_ClientEnvironment{
			ClientEnvironment: &fdpb.ClientEnvironment{
				Code: fdpb.ClientEnvironment_CLIENT_ENVIRONMENT_UNKNOWN,
			},
		}
	default:
	}

	return fd
}

func handleCommandSpan(buckEndEvent *buckdata.BuckEvent, spanStart *buckdata.CommandStart, spanEnd *buckdata.CommandEnd) (*bespb.BuildEvent, error) {
	var bazelBuildEvent = &bespb.BuildEvent{}

	switch spanStart.GetData().(type) {
	case *buckdata.CommandStart_Build:
		bazelBuildEvent.Id = &bespb.BuildEventId{
			Id: &bespb.BuildEventId_Started{
				Started: &bespb.BuildEventId_BuildStartedId{},
			},
		}
		bazelBuildEvent.Payload = &bespb.BuildEvent_Started{
			Started: &bespb.BuildStarted{
				Uuid:               buckEndEvent.GetTraceId(),
				StartTime:          buckEndEvent.GetTimestamp(),
				BuildToolVersion:   "",
				OptionsDescription: "buck2",
				Command:            "build",
				WorkingDirectory:   "",
				WorkspaceDirectory: "",
				ServerPid:          0,
			},
		}
	case *buckdata.CommandStart_Targets:
	case *buckdata.CommandStart_Query:
	case *buckdata.CommandStart_Cquery:
	case *buckdata.CommandStart_Test:
	case *buckdata.CommandStart_Audit:
	case *buckdata.CommandStart_Docs:
	case *buckdata.CommandStart_Clean:
	case *buckdata.CommandStart_Aquery:
	case *buckdata.CommandStart_Install:
	case *buckdata.CommandStart_Materialize:
	case *buckdata.CommandStart_Profile:
	case *buckdata.CommandStart_Bxl:
	case *buckdata.CommandStart_Lsp:
	case *buckdata.CommandStart_FileStatus:
	case *buckdata.CommandStart_Starlark:
	case *buckdata.CommandStart_Subscribe:
	case *buckdata.CommandStart_Trace:
	case *buckdata.CommandStart_Ctargets:
	case *buckdata.CommandStart_StarlarkDebugAttach:
	case *buckdata.CommandStart_Explain:
	case *buckdata.CommandStart_ExpandExternalCell:
	case *buckdata.CommandStart_Complete:
	default:
	}

	return bazelBuildEvent, nil
}

package buck

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/proto/buckdata"
	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
)

func ToBazelEvent(buckBuildEvent *bepb.BuildEvent_BuckEvent) (*build_event_stream.BuildEvent, error) {
	var buckEvent *buckdata.BuckEvent
	err := buckBuildEvent.BuckEvent.UnmarshalTo(buckEvent)
	if err != nil {
		return nil, err
	}

	switch buckEvent.GetData().(type) {
	case *buckdata.BuckEvent_SpanStart:
		spanStart := buckEvent.GetSpanStart()
		switch spanStart.GetData().(type) {
		case *buckdata.SpanStartEvent_Command:
		case *buckdata.SpanStartEvent_ActionExecution:
		case *buckdata.SpanStartEvent_Analysis:
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
			return nil, fmt.Errorf("Unknown buck event type: %v", buckEvent.Data)
		}
	case *buckdata.BuckEvent_SpanEnd:
		spanEnd := buckEvent.GetSpanEnd()
		switch spanEnd.GetData().(type) {
		case *buckdata.SpanEndEvent_Command:
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
			return nil, fmt.Errorf("Unknown buck event type: %v", buckEvent.Data)
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
			return nil, fmt.Errorf("Unknown buck event type: %v", buckEvent.Data)
		}
	case *buckdata.BuckEvent_Record:
		record := buckEvent.GetRecord()
		switch record.GetData().(type) {

		case *buckdata.RecordEvent_InvocationRecord:
		case *buckdata.RecordEvent_BuildGraphStats:
		default:
			return nil, fmt.Errorf("Unknown buck event type: %v", buckEvent.Data)
		}
	default:
		return nil, fmt.Errorf("Unknown buck event type: %v", buckEvent.Data)
	}

	return nil, nil
}

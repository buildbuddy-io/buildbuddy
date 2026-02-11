package buck2

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	bdpb "github.com/buildbuddy-io/buildbuddy/proto/buckdata"
	bspb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	BuildToolEventMessageName = "buck.data.BuckEvent"

	buildToolVersion       = "buck2"
	buck2TestTargetKind    = "buck2_test rule"
	buck2GenericTargetKind = "buck2 rule"
	defaultConfigurationID = "buck2"
	interruptedExitCode    = int32(8)
	maxInlineFileBytes     = 16 * 1024
)

func ReadBuildEvent(sequenceNumber int64, anyEvent *anypb.Any, synthesizeStarted bool, out *bspb.BuildEvent) error {
	if anyEvent == nil {
		return fmt.Errorf("Missing experimental_build_tool_event payload")
	}
	if !IsBuck2BuildToolEvent(anyEvent) {
		return fmt.Errorf(
			"Unsupported experimental_build_tool_event type_url %q; expected %q",
			anyEvent.GetTypeUrl(),
			BuildToolEventMessageName,
		)
	}

	buckEvent := &bdpb.BuckEvent{}
	if err := anyEvent.UnmarshalTo(buckEvent); err != nil {
		return fmt.Errorf("Failed to decode experimental_build_tool_event: %w", err)
	}

	if synthesizeStarted {
		populateStartedEvent(out, buckEvent)
		return nil
	}
	if populateFinishedEvent(out, buckEvent) {
		return nil
	}
	if populateBuildMetadataEvent(out, buckEvent) {
		return nil
	}
	if populateConfiguredEvent(out, buckEvent) {
		return nil
	}
	if populateCompletedEvent(out, buckEvent) {
		return nil
	}
	if populateActionEvent(out, buckEvent) {
		return nil
	}
	if populateTestSummaryEvent(out, buckEvent) {
		return nil
	}
	if populateTestResultEvent(out, buckEvent) {
		return nil
	}

	populateProgressEvent(out, sequenceNumber, buckEvent)
	return nil
}

func IsBuck2BuildToolEvent(anyEvent *anypb.Any) bool {
	if anyEvent == nil {
		return false
	}
	typeURL := anyEvent.GetTypeUrl()
	if typeURL == "" {
		return false
	}
	if idx := strings.LastIndex(typeURL, "/"); idx >= 0 {
		typeURL = typeURL[idx+1:]
	}
	return typeURL == BuildToolEventMessageName
}

func populateStartedEvent(out *bspb.BuildEvent, event *bdpb.BuckEvent) {
	out.Reset()
	timestamp := event.GetTimestamp()
	if timestamp == nil {
		timestamp = timestamppb.Now()
	}
	commandStart := event.GetSpanStart().GetCommand()
	optionsDescription := buildToolVersion
	cliArgs := commandStart.GetCliArgs()
	if len(cliArgs) > 0 {
		optionsDescription = strings.Join(cliArgs, " ")
	}
	metadata := commandStart.GetMetadata()
	out.Id = &bspb.BuildEventId{
		Id: &bspb.BuildEventId_Started{
			Started: &bspb.BuildEventId_BuildStartedId{},
		},
	}
	out.Payload = &bspb.BuildEvent_Started{
		Started: &bspb.BuildStarted{
			Uuid:               event.GetTraceId(),
			StartTime:          timestamp,
			BuildToolVersion:   buildToolVersion,
			OptionsDescription: optionsDescription,
			Command:            commandName(commandStart),
			Host:               firstNonEmpty(metadata["HOST"], metadata["BUILD_HOST"]),
			User:               firstNonEmpty(metadata["USER"], metadata["BUILD_USER"]),
		},
	}
	patterns := targetPatternsFromCLIArgs(cliArgs)
	out.Children = make([]*bspb.BuildEventId, 0, len(patterns))
	for _, pattern := range patterns {
		out.Children = append(out.Children, &bspb.BuildEventId{
			Id: &bspb.BuildEventId_Pattern{
				Pattern: &bspb.BuildEventId_PatternExpandedId{
					Pattern: []string{pattern},
				},
			},
		})
	}
}

func populateFinishedEvent(out *bspb.BuildEvent, event *bdpb.BuckEvent) bool {
	if populateFinishedEventFromInvocationRecord(out, event) {
		return true
	}

	commandEnd := event.GetSpanEnd().GetCommand()
	if commandEnd == nil {
		return false
	}
	out.Reset()
	exitCode := int32(1)
	exitName := "FAILED"
	// `is_success` is deprecated and may be unset or inaccurate. Prefer
	// `build_result.build_completed` when available.
	if buildResult := commandEnd.GetBuildResult(); buildResult != nil {
		if buildResult.GetBuildCompleted() {
			exitCode = 0
			exitName = "SUCCESS"
		}
	} else if commandEnd.GetIsSuccess() {
		exitCode = 0
		exitName = "SUCCESS"
	}
	timestamp := event.GetTimestamp()
	if timestamp == nil {
		timestamp = timestamppb.Now()
	}
	out.Id = &bspb.BuildEventId{
		Id: &bspb.BuildEventId_BuildFinished{
			BuildFinished: &bspb.BuildEventId_BuildFinishedId{},
		},
	}
	out.Payload = &bspb.BuildEvent_Finished{
		Finished: &bspb.BuildFinished{
			ExitCode: &bspb.BuildFinished_ExitCode{
				Name: exitName,
				Code: exitCode,
			},
			FinishTime: timestamp,
		},
	}
	return true
}

func populateFinishedEventFromInvocationRecord(out *bspb.BuildEvent, event *bdpb.BuckEvent) bool {
	invocationRecord := event.GetRecord().GetInvocationRecord()
	if invocationRecord == nil {
		return false
	}

	exitCode := int32(1)
	exitName := "FAILED"
	switch invocationRecord.GetOutcome() {
	case bdpb.InvocationOutcome_Success:
		exitCode = 0
		exitName = "SUCCESS"
	case bdpb.InvocationOutcome_Failed:
		// Keep default FAILED exit status.
	case bdpb.InvocationOutcome_Cancelled:
		exitCode = interruptedExitCode
		exitName = "INTERRUPTED"
	case bdpb.InvocationOutcome_Crashed:
		exitName = "CRASHED"
	case bdpb.InvocationOutcome_Unknown:
		// Unknown outcomes are resolved from exitCode below.
	}

	if invocationRecord.ExitCode != nil {
		exitCode = saturatingInt32FromUint32(invocationRecord.GetExitCode())
	}
	if exitResultName := strings.TrimSpace(invocationRecord.GetExitResultName()); exitResultName != "" {
		exitName = exitResultName
	} else if invocationRecord.GetOutcome() == bdpb.InvocationOutcome_Unknown {
		if exitCode == 0 {
			exitName = "SUCCESS"
		} else {
			exitName = "FAILED"
		}
	}

	timestamp := event.GetTimestamp()
	if timestamp == nil {
		timestamp = timestamppb.Now()
	}
	out.Reset()
	out.Id = &bspb.BuildEventId{
		Id: &bspb.BuildEventId_BuildFinished{
			BuildFinished: &bspb.BuildEventId_BuildFinishedId{},
		},
	}
	out.Payload = &bspb.BuildEvent_Finished{
		Finished: &bspb.BuildFinished{
			ExitCode: &bspb.BuildFinished_ExitCode{
				Name: exitName,
				Code: exitCode,
			},
			FinishTime: timestamp,
		},
	}
	return true
}

func populateBuildMetadataEvent(out *bspb.BuildEvent, event *bdpb.BuckEvent) bool {
	metadata := map[string]string{}

	if commandStart := event.GetSpanStart().GetCommand(); commandStart != nil {
		mergeMetadata(metadata, commandStart.GetMetadata())
		if tags := commandStart.GetTags(); len(tags) > 0 && metadata["TAGS"] == "" {
			metadata["TAGS"] = strings.Join(tags, ",")
		}
	}
	if commandCritical := event.GetSpanStart().GetCommandCritical(); commandCritical != nil {
		mergeMetadata(metadata, commandCritical.GetMetadata())
	}

	instant := event.GetInstant()
	if buildGraphInfo := instant.GetBuildGraphInfo(); buildGraphInfo != nil {
		if metadata["BUCK2_BUILD_GRAPH_NUM_NODES"] == "" && buildGraphInfo.GetNumNodes() > 0 {
			metadata["BUCK2_BUILD_GRAPH_NUM_NODES"] = strconv.FormatUint(buildGraphInfo.GetNumNodes(), 10)
		}
		if metadata["BUCK2_BUILD_GRAPH_NUM_EDGES"] == "" && buildGraphInfo.GetNumEdges() > 0 {
			metadata["BUCK2_BUILD_GRAPH_NUM_EDGES"] = strconv.FormatUint(buildGraphInfo.GetNumEdges(), 10)
		}
		if command := strings.TrimSpace(buildGraphInfo.GetCommandName()); command != "" && metadata["BUCK2_BUILD_GRAPH_COMMAND"] == "" {
			metadata["BUCK2_BUILD_GRAPH_COMMAND"] = command
		}
		if backend := strings.TrimSpace(buildGraphInfo.GetBackendName()); backend != "" && metadata["BUCK2_BUILD_GRAPH_BACKEND"] == "" {
			metadata["BUCK2_BUILD_GRAPH_BACKEND"] = backend
		}
		if isolationDir := strings.TrimSpace(buildGraphInfo.GetIsolationDir()); isolationDir != "" && metadata["BUCK2_ISOLATION_DIR"] == "" {
			metadata["BUCK2_ISOLATION_DIR"] = isolationDir
		}
	}
	if targetCfg := instant.GetTargetCfg(); targetCfg != nil {
		if platforms := targetCfg.GetTargetPlatforms(); len(platforms) > 0 && metadata["TARGET_PLATFORM"] == "" {
			metadata["TARGET_PLATFORM"] = strings.Join(platforms, ",")
		}
		if modifiers := targetCfg.GetCliModifiers(); len(modifiers) > 0 && metadata["BUCK2_CLI_MODIFIERS"] == "" {
			metadata["BUCK2_CLI_MODIFIERS"] = strings.Join(modifiers, ",")
		}
	}
	if options := instant.GetComandOptions(); options != nil {
		if configured := options.GetConfiguredParallelism(); configured > 0 && metadata["BUCK2_CONFIGURED_PARALLELISM"] == "" {
			metadata["BUCK2_CONFIGURED_PARALLELISM"] = strconv.FormatUint(configured, 10)
		}
		if available := options.GetAvailableParallelism(); available > 0 && metadata["BUCK2_AVAILABLE_PARALLELISM"] == "" {
			metadata["BUCK2_AVAILABLE_PARALLELISM"] = strconv.FormatUint(available, 10)
		}
	}
	if created := instant.GetConfigurationCreated(); created != nil {
		if cfg := strings.TrimSpace(created.GetCfg().GetFullName()); cfg != "" && metadata["BUCK2_CONFIGURATION"] == "" {
			metadata["BUCK2_CONFIGURATION"] = cfg
		}
	}
	if parsedTargetPatterns := instant.GetTargetPatterns(); parsedTargetPatterns != nil {
		patterns := targetPatternValues(parsedTargetPatterns.GetTargetPatterns())
		if len(patterns) > 0 {
			metadata["PATTERN"] = strings.Join(patterns, " ")
		}
	}
	if revision := instant.GetVersionControlRevision(); revision != nil {
		if metadata["COMMIT_SHA"] == "" {
			if rev := strings.TrimSpace(revision.GetHgRevision()); rev != "" {
				metadata["COMMIT_SHA"] = rev
			}
		}
	}

	if len(metadata) == 0 {
		return false
	}

	out.Reset()
	out.Id = &bspb.BuildEventId{
		Id: &bspb.BuildEventId_BuildMetadata{
			BuildMetadata: &bspb.BuildEventId_BuildMetadataId{},
		},
	}
	out.Payload = &bspb.BuildEvent_BuildMetadata{
		BuildMetadata: &bspb.BuildMetadata{
			Metadata: metadata,
		},
	}
	return true
}

func populateConfiguredEvent(out *bspb.BuildEvent, event *bdpb.BuckEvent) bool {
	if analysisStart := event.GetSpanStart().GetAnalysis(); analysisStart != nil {
		target := firstConfiguredTarget(analysisStart.GetStandardTarget(), analysisStart.GetDynamicLambda().GetTargetLabel())
		label := labelForConfiguredTarget(target)
		if label == "" {
			return false
		}
		targetKind := strings.TrimSpace(analysisStart.GetRule())
		if targetKind == "" {
			targetKind = buck2GenericTargetKind
		} else if !strings.HasSuffix(targetKind, " rule") {
			targetKind = targetKind + " rule"
		}

		out.Reset()
		out.Id = &bspb.BuildEventId{
			Id: &bspb.BuildEventId_TargetConfigured{
				TargetConfigured: &bspb.BuildEventId_TargetConfiguredId{
					Label: label,
				},
			},
		}
		out.Payload = &bspb.BuildEvent_Configured{
			Configured: &bspb.TargetConfigured{
				TargetKind: targetKind,
			},
		}
		return true
	}

	testSuite := event.GetInstant().GetTestDiscovery().GetTests()
	if testSuite != nil {
		label := labelForConfiguredTarget(testSuite.GetTargetLabel())
		if label == "" {
			return false
		}

		out.Reset()
		out.Id = &bspb.BuildEventId{
			Id: &bspb.BuildEventId_TargetConfigured{
				TargetConfigured: &bspb.BuildEventId_TargetConfiguredId{
					Label: label,
				},
			},
		}
		out.Payload = &bspb.BuildEvent_Configured{
			Configured: &bspb.TargetConfigured{
				TargetKind: buck2TestTargetKind,
			},
		}
		return true
	}

	testDiscovery := event.GetSpanEnd().GetTestDiscovery()
	if testDiscovery == nil {
		return false
	}
	label := labelForConfiguredTarget(testDiscovery.GetTargetLabel())
	if label == "" {
		return false
	}

	out.Reset()
	out.Id = &bspb.BuildEventId{
		Id: &bspb.BuildEventId_TargetConfigured{
			TargetConfigured: &bspb.BuildEventId_TargetConfiguredId{
				Label: label,
			},
		},
	}
	out.Payload = &bspb.BuildEvent_Configured{
		Configured: &bspb.TargetConfigured{
			TargetKind: buck2TestTargetKind,
		},
	}
	return true
}

func populateCompletedEvent(out *bspb.BuildEvent, event *bdpb.BuckEvent) bool {
	analysisEnd := event.GetSpanEnd().GetAnalysis()
	if analysisEnd == nil {
		return false
	}
	target := firstConfiguredTarget(analysisEnd.GetStandardTarget(), analysisEnd.GetDynamicLambda().GetTargetLabel())
	label := labelForConfiguredTarget(target)
	if label == "" {
		return false
	}
	configurationID := configurationIDForTarget(target)

	out.Reset()
	out.Id = &bspb.BuildEventId{
		Id: &bspb.BuildEventId_TargetCompleted{
			TargetCompleted: &bspb.BuildEventId_TargetCompletedId{
				Label: label,
				Configuration: &bspb.BuildEventId_ConfigurationId{
					Id: configurationID,
				},
			},
		},
	}
	out.Payload = &bspb.BuildEvent_Completed{
		Completed: &bspb.TargetComplete{
			Success: true,
		},
	}
	return true
}

func populateActionEvent(out *bspb.BuildEvent, event *bdpb.BuckEvent) bool {
	action := event.GetSpanEnd().GetActionExecution()
	if action == nil {
		return false
	}

	target := firstConfiguredTarget(
		action.GetKey().GetTargetLabel(),
		action.GetKey().GetTestTargetLabel(),
		action.GetKey().GetLocalResourceSetup(),
	)
	label := labelForConfiguredTarget(target)
	if label == "" {
		return false
	}
	configurationID := configurationIDForTarget(target)
	primaryOutput := actionPrimaryOutput(action)
	if primaryOutput == "" {
		primaryOutput = firstNonEmpty(action.GetKey().GetKey(), label)
	}

	now := event.GetTimestamp()
	if now == nil {
		now = timestamppb.Now()
	}
	startTime := startTimeFromEndEvent(event.GetSpanEnd(), now)
	lastCommand := lastCommandExecution(action.GetCommands())
	commandLine := commandLine(lastCommand)
	exitCode := actionExitCode(action, lastCommand)
	stdout, stderr := commandOutput(lastCommand)

	out.Reset()
	out.Id = &bspb.BuildEventId{
		Id: &bspb.BuildEventId_ActionCompleted{
			ActionCompleted: &bspb.BuildEventId_ActionCompletedId{
				PrimaryOutput: primaryOutput,
				Label:         label,
				Configuration: &bspb.BuildEventId_ConfigurationId{
					Id: configurationID,
				},
			},
		},
	}
	out.Payload = &bspb.BuildEvent_Action{
		Action: &bspb.ActionExecuted{
			Success:  !action.GetFailed(),
			Type:     actionMnemonic(action),
			ExitCode: exitCode,
			Stdout:   fileWithContents("stdout", stdout),
			Stderr:   fileWithContents("stderr", stderr),
			Label:    label,
			Configuration: &bspb.BuildEventId_ConfigurationId{
				Id: configurationID,
			},
			CommandLine: commandLine,
			StartTime:   timestamppb.New(startTime),
			EndTime:     now,
		},
	}
	return true
}

func populateTestSummaryEvent(out *bspb.BuildEvent, event *bdpb.BuckEvent) bool {
	testEnd := event.GetSpanEnd().GetTestEnd()
	if testEnd == nil {
		return false
	}

	suite := testEnd.GetSuite()
	target := suite.GetTargetLabel()
	label := labelForConfiguredTarget(target)
	if label == "" {
		return false
	}
	configurationID := configurationIDForTarget(target)
	overallStatus := testStatusFromCommandReport(testEnd.GetCommandReport())

	now := event.GetTimestamp()
	if now == nil {
		now = timestamppb.Now()
	}
	startTime := startTimeFromEndEvent(event.GetSpanEnd(), now)
	totalRunCount := int32(1)
	if n := len(suite.GetTestNames()); n > 0 {
		totalRunCount = saturatingInt32(n)
	}

	passed, failed := testSummaryFiles(overallStatus, testEnd.GetCommandReport())
	out.Reset()
	out.Id = &bspb.BuildEventId{
		Id: &bspb.BuildEventId_TestSummary{
			TestSummary: &bspb.BuildEventId_TestSummaryId{
				Label: label,
				Configuration: &bspb.BuildEventId_ConfigurationId{
					Id: configurationID,
				},
			},
		},
	}
	out.Payload = &bspb.BuildEvent_TestSummary{
		TestSummary: &bspb.TestSummary{
			OverallStatus:    overallStatus,
			TotalRunCount:    totalRunCount,
			RunCount:         1,
			AttemptCount:     1,
			ShardCount:       1,
			Passed:           passed,
			Failed:           failed,
			FirstStartTime:   timestamppb.New(startTime),
			LastStopTime:     now,
			TotalRunDuration: event.GetSpanEnd().GetDuration(),
		},
	}
	return true
}

func populateTestResultEvent(out *bspb.BuildEvent, event *bdpb.BuckEvent) bool {
	testResult := event.GetInstant().GetTestResult()
	if testResult == nil {
		return false
	}
	label := labelForConfiguredTarget(testResult.GetTargetLabel())
	if label == "" {
		label = testResult.GetName()
	}
	if label == "" {
		return false
	}

	configurationID := configurationIDForTarget(testResult.GetTargetLabel())
	statusDetails := strings.TrimSpace(testResult.GetDetails())
	if message := strings.TrimSpace(testResult.GetMsg().GetMsg()); message != "" {
		statusDetails = firstNonEmpty(statusDetails, message)
	}
	timestamp := event.GetTimestamp()
	if timestamp == nil {
		timestamp = timestamppb.Now()
	}
	startTime := timestamp.AsTime()
	if duration := testResult.GetDuration(); duration != nil {
		d := duration.AsDuration()
		if d > 0 {
			startTime = startTime.Add(-d)
		}
	}

	out.Reset()
	out.Id = &bspb.BuildEventId{
		Id: &bspb.BuildEventId_TestResult{
			TestResult: &bspb.BuildEventId_TestResultId{
				Label: label,
				Configuration: &bspb.BuildEventId_ConfigurationId{
					Id: configurationID,
				},
			},
		},
	}
	out.Payload = &bspb.BuildEvent_TestResult{
		TestResult: &bspb.TestResult{
			Status:              testStatus(testResult.GetStatus()),
			StatusDetails:       statusDetails,
			TestAttemptStart:    timestamppb.New(startTime),
			TestAttemptDuration: testResult.GetDuration(),
		},
	}
	return true
}

func populateProgressEvent(out *bspb.BuildEvent, sequenceNumber int64, event *bdpb.BuckEvent) {
	out.Reset()
	stdout := ""
	stderr := ""

	instant := event.GetInstant()
	if streamingOutput := instant.GetStreamingOutput(); streamingOutput != nil {
		stdout = streamingOutput.GetMessage()
	}
	if consoleMessage := instant.GetConsoleMessage(); consoleMessage != nil {
		stderr = consoleMessage.GetMessage()
	}
	if consoleWarning := instant.GetConsoleWarning(); consoleWarning != nil {
		stderr = consoleWarning.GetMessage()
	}
	if structuredError := instant.GetStructuredError(); structuredError != nil {
		stderr = structuredError.GetPayload()
	}
	if actionError := instant.GetActionError(); actionError != nil {
		stderr = actionErrorMessage(actionError)
	}
	if testSession := instant.GetTestDiscovery().GetSession(); testSession != nil {
		stdout = testSession.GetInfo()
	}

	out.Id = &bspb.BuildEventId{
		Id: &bspb.BuildEventId_Progress{
			Progress: &bspb.BuildEventId_ProgressId{
				OpaqueCount: sequenceToOpaqueCount(sequenceNumber),
			},
		},
	}
	out.Payload = &bspb.BuildEvent_Progress{
		Progress: &bspb.Progress{
			Stdout: stdout,
			Stderr: stderr,
		},
	}
}

func actionErrorMessage(actionError *bdpb.ActionError) string {
	if actionError == nil {
		return ""
	}
	if message := strings.TrimSpace(actionError.GetUnknown()); message != "" {
		return message
	}
	if message := strings.TrimSpace(actionError.GetMissingOutputs().GetMessage()); message != "" {
		return message
	}
	if details := actionError.GetLastCommand().GetDetails(); details != nil {
		if stderr := strings.TrimSpace(details.GetCmdStderr()); stderr != "" {
			return stderr
		}
		if stdout := strings.TrimSpace(details.GetCmdStdout()); stdout != "" {
			return stdout
		}
	}
	return "Action failed"
}

func firstConfiguredTarget(labels ...*bdpb.ConfiguredTargetLabel) *bdpb.ConfiguredTargetLabel {
	for _, label := range labels {
		if label != nil && label.GetLabel() != nil {
			return label
		}
	}
	return nil
}

func startTimeFromEndEvent(spanEnd *bdpb.SpanEndEvent, endTime *timestamppb.Timestamp) time.Time {
	startTime := endTime.AsTime()
	if spanEnd == nil {
		return startTime
	}
	if duration := spanEnd.GetDuration(); duration != nil {
		d := duration.AsDuration()
		if d > 0 {
			startTime = startTime.Add(-d)
		}
	}
	return startTime
}

func saturatingInt32(v int) int32 {
	if v <= 0 {
		return 0
	}
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(v)
}

func saturatingInt32FromUint32(v uint32) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(v)
}

func actionPrimaryOutput(action *bdpb.ActionExecutionEnd) string {
	if action == nil {
		return ""
	}
	if output := action.GetOutputs(); len(output) > 0 {
		if digest := strings.TrimSpace(output[0].GetTinyDigest()); digest != "" {
			return digest
		}
	}
	name := action.GetName()
	category := strings.TrimSpace(name.GetCategory())
	identifier := strings.TrimSpace(name.GetIdentifier())
	switch {
	case category == "":
		return identifier
	case identifier == "":
		return category
	default:
		return category + ":" + identifier
	}
}

func actionMnemonic(action *bdpb.ActionExecutionEnd) string {
	if action == nil {
		return ""
	}
	if category := strings.TrimSpace(action.GetName().GetCategory()); category != "" {
		return category
	}
	kind := strings.TrimSpace(action.GetKind().String())
	if kind == "" || kind == "NOT_SET" {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(kind, "ACTION_KIND_"))
}

func lastCommandExecution(commands []*bdpb.CommandExecution) *bdpb.CommandExecution {
	for i := len(commands) - 1; i >= 0; i-- {
		if commands[i] != nil {
			return commands[i]
		}
	}
	return nil
}

func actionExitCode(action *bdpb.ActionExecutionEnd, command *bdpb.CommandExecution) int32 {
	if command == nil {
		if action.GetFailed() {
			return 1
		}
		return 0
	}
	if details := command.GetDetails(); details != nil {
		exitCode := details.GetSignedExitCode()
		if action.GetFailed() && exitCode == 0 {
			return 1
		}
		return exitCode
	}
	if action.GetFailed() {
		return 1
	}
	return 0
}

func commandOutput(command *bdpb.CommandExecution) (stdout, stderr string) {
	if command == nil {
		return "", ""
	}
	details := command.GetDetails()
	if details == nil {
		return "", ""
	}
	return details.GetCmdStdout(), details.GetCmdStderr()
}

func commandLine(command *bdpb.CommandExecution) []string {
	if command == nil {
		return nil
	}
	details := command.GetDetails()
	if details == nil {
		return nil
	}
	commandKind := details.GetCommandKind()
	switch {
	case commandKind.GetLocalCommand() != nil:
		return append([]string{}, commandKind.GetLocalCommand().GetArgv()...)
	case commandKind.GetWorkerCommand() != nil:
		return append([]string{}, commandKind.GetWorkerCommand().GetArgv()...)
	case commandKind.GetWorkerInitCommand() != nil:
		return append([]string{}, commandKind.GetWorkerInitCommand().GetArgv()...)
	default:
		return nil
	}
}

func fileWithContents(name, content string) *bspb.File {
	if content == "" {
		return nil
	}
	contents := []byte(content)
	if len(contents) > maxInlineFileBytes {
		contents = append(contents[:maxInlineFileBytes], []byte("... (truncated)")...)
	}
	return &bspb.File{
		Name: name,
		File: &bspb.File_Contents{
			Contents: contents,
		},
	}
}

func testStatusFromCommandReport(command *bdpb.CommandExecution) bspb.TestStatus {
	if command == nil {
		return bspb.TestStatus_NO_STATUS
	}
	switch command.GetStatus().(type) {
	case *bdpb.CommandExecution_Success_:
		return bspb.TestStatus_PASSED
	case *bdpb.CommandExecution_Timeout_:
		return bspb.TestStatus_TIMEOUT
	case *bdpb.CommandExecution_Failure_, *bdpb.CommandExecution_WorkerFailure_:
		return bspb.TestStatus_FAILED
	case *bdpb.CommandExecution_Error_:
		return bspb.TestStatus_REMOTE_FAILURE
	case *bdpb.CommandExecution_Cancelled_:
		return bspb.TestStatus_INCOMPLETE
	default:
		return bspb.TestStatus_NO_STATUS
	}
}

func testSummaryFiles(overallStatus bspb.TestStatus, command *bdpb.CommandExecution) (passed []*bspb.File, failed []*bspb.File) {
	if command == nil {
		return nil, nil
	}
	stdout, stderr := commandOutput(command)
	files := make([]*bspb.File, 0, 2)
	if f := fileWithContents("stdout", stdout); f != nil {
		files = append(files, f)
	}
	if f := fileWithContents("stderr", stderr); f != nil {
		files = append(files, f)
	}
	if len(files) == 0 {
		return nil, nil
	}

	switch overallStatus {
	case bspb.TestStatus_PASSED, bspb.TestStatus_FLAKY:
		return files, nil
	default:
		return nil, files
	}
}

func mergeMetadata(dst, src map[string]string) {
	for k, v := range src {
		if strings.TrimSpace(k) == "" {
			continue
		}
		dst[k] = v
	}
}

func targetPatternValues(patterns []*bdpb.TargetPattern) []string {
	values := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		if value := strings.TrimSpace(pattern.GetValue()); value != "" {
			values = append(values, value)
		}
	}
	return values
}

func targetPatternsFromCLIArgs(cliArgs []string) []string {
	if len(cliArgs) <= 2 {
		return nil
	}
	patterns := make([]string, 0, len(cliArgs)-2)
	for _, arg := range cliArgs[2:] {
		arg = strings.TrimSpace(arg)
		if arg == "" || arg == "--" || strings.HasPrefix(arg, "-") {
			continue
		}
		if strings.HasPrefix(arg, "//") || strings.HasPrefix(arg, ":") || strings.Contains(arg, "//") {
			patterns = append(patterns, arg)
		}
	}
	return patterns
}

func commandName(commandStart *bdpb.CommandStart) string {
	if commandStart == nil {
		return buildToolVersion
	}
	switch {
	case commandStart.GetBuild() != nil:
		return "build"
	case commandStart.GetTargets() != nil:
		return "targets"
	case commandStart.GetQuery() != nil:
		return "query"
	case commandStart.GetCquery() != nil:
		return "cquery"
	case commandStart.GetAquery() != nil:
		return "aquery"
	case commandStart.GetTest() != nil:
		return "test"
	case commandStart.GetInstall() != nil:
		return "install"
	case commandStart.GetAudit() != nil:
		return "audit"
	case commandStart.GetBxl() != nil:
		return "bxl"
	case commandStart.GetProfile() != nil:
		return "profile"
	case commandStart.GetStarlark() != nil:
		return "starlark"
	case commandStart.GetLsp() != nil:
		return "lsp"
	case commandStart.GetDocs() != nil:
		return "docs"
	case commandStart.GetClean() != nil:
		return "clean"
	case commandStart.GetMaterialize() != nil:
		return "materialize"
	case commandStart.GetFileStatus() != nil:
		return "file-status"
	case commandStart.GetSubscribe() != nil:
		return "subscribe"
	case commandStart.GetTrace() != nil:
		return "trace"
	case commandStart.GetCtargets() != nil:
		return "ctargets"
	case commandStart.GetStarlarkDebugAttach() != nil:
		return "starlark-debug-attach"
	case commandStart.GetExplain() != nil:
		return "explain"
	case commandStart.GetExpandExternalCell() != nil:
		return "expand-external-cell"
	case commandStart.GetComplete() != nil:
		return "complete"
	default:
		return buildToolVersion
	}
}

func testStatus(status bdpb.TestStatus) bspb.TestStatus {
	switch status {
	case bdpb.TestStatus_PASS:
		return bspb.TestStatus_PASSED
	case bdpb.TestStatus_FAIL, bdpb.TestStatus_FATAL:
		return bspb.TestStatus_FAILED
	case bdpb.TestStatus_TIMEOUT:
		return bspb.TestStatus_TIMEOUT
	case bdpb.TestStatus_INFRA_FAILURE:
		return bspb.TestStatus_REMOTE_FAILURE
	case bdpb.TestStatus_LISTING_FAILED:
		return bspb.TestStatus_FAILED_TO_BUILD
	case bdpb.TestStatus_RERUN:
		return bspb.TestStatus_FLAKY
	case bdpb.TestStatus_SKIP, bdpb.TestStatus_OMITTED:
		return bspb.TestStatus_INCOMPLETE
	case bdpb.TestStatus_LISTING_SUCCESS:
		return bspb.TestStatus_PASSED
	default:
		return bspb.TestStatus_NO_STATUS
	}
}

func sequenceToOpaqueCount(sequenceNumber int64) int32 {
	if sequenceNumber <= 0 {
		return 0
	}
	if sequenceNumber > int64(math.MaxInt32) {
		return math.MaxInt32
	}
	return int32(sequenceNumber)
}

func labelForConfiguredTarget(configuredTarget *bdpb.ConfiguredTargetLabel) string {
	return labelForTarget(configuredTarget.GetLabel())
}

func labelForTarget(label *bdpb.TargetLabel) string {
	pkg := strings.TrimSpace(label.GetPackage())
	name := strings.TrimSpace(label.GetName())
	switch {
	case pkg == "":
		return name
	case strings.Contains(pkg, "//"):
		cell, packagePath, _ := strings.Cut(pkg, "//")
		base := "//" + strings.TrimPrefix(packagePath, "/")
		if cell != "" && cell != "root" {
			base = "@" + cell + "//" + strings.TrimPrefix(packagePath, "/")
		}
		if packagePath == "" {
			if cell != "" && cell != "root" {
				base = "@" + cell + "//"
			} else {
				base = "//"
			}
		}
		if name == "" {
			return base
		}
		return base + ":" + strings.TrimPrefix(name, ":")
	default:
		if name == "" {
			return "//" + pkg
		}
		return "//" + pkg + ":" + strings.TrimPrefix(name, ":")
	}
}

func configurationIDForTarget(label *bdpb.ConfiguredTargetLabel) string {
	if id := strings.TrimSpace(label.GetConfiguration().GetFullName()); id != "" {
		return id
	}
	return defaultConfigurationID
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

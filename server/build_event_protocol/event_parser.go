package event_parser

import (
	"bytes"
	"strings"
	"time"

	"golang.org/x/crypto/ssh/terminal"

	"proto/build_event_stream"
	"proto/command_line"
	inpb "proto/invocation"
)

const (
	envVarPrefix              = "--"
	envVarOptionName          = "client_env"
	envVarSeparator           = "="
	envVarRedactedPlaceholder = "<REDACTED>"
)

func filterCommandLine(in *command_line.CommandLine) *command_line.CommandLine {
	if in == nil {
		return nil
	}
	var out command_line.CommandLine
	out = *in
	for _, section := range out.Sections {
		switch p := section.SectionType.(type) {
		case *command_line.CommandLineSection_OptionList:
			{
				for _, option := range p.OptionList.Option {
					if option.OptionName == envVarOptionName {
						parts := strings.Split(option.OptionValue, envVarSeparator)
						option.OptionValue = strings.Join([]string{parts[0], envVarRedactedPlaceholder}, envVarSeparator)
						option.CombinedForm = envVarPrefix + envVarOptionName + envVarSeparator + parts[0] + envVarSeparator + envVarRedactedPlaceholder
					}
				}
			}
		default:
			continue
		}
	}
	return &out
}

func filterUnstructuredCommandLine(in *build_event_stream.UnstructuredCommandLine) *build_event_stream.UnstructuredCommandLine {
	if in == nil {
		return nil
	}
	var out build_event_stream.UnstructuredCommandLine
	out = *in
	for i, arg := range out.Args {
		if strings.HasPrefix(arg, envVarPrefix+envVarOptionName) {
			parts := strings.Split(arg, envVarSeparator)
			if len(parts) < 2 {
				continue;
			}
			out.Args[i] = envVarPrefix + envVarOptionName + envVarSeparator + parts[1] + envVarSeparator + envVarRedactedPlaceholder
		}
	}
	return &out
}

func FillInvocationFromEvents(buildEvents []*inpb.InvocationEvent, invocation *inpb.Invocation) {
	startTimeMillis := int64(-1)
	endTimeMillis := int64(-1)

	var rwBuf bytes.Buffer
	t := terminal.NewTerminal(&rwBuf, "")

	for _, event := range buildEvents {
		invocation.Event = append(invocation.Event, event)

		switch p := event.BuildEvent.Payload.(type) {
		case *build_event_stream.BuildEvent_Progress:
			{
				t.Write([]byte(p.Progress.Stderr))
				t.Write([]byte(p.Progress.Stdout))
			}
		case *build_event_stream.BuildEvent_Aborted:
			{
			}
		case *build_event_stream.BuildEvent_Started:
			{
				startTimeMillis = p.Started.StartTimeMillis
				invocation.Command = p.Started.Command
				for _, child := range event.BuildEvent.Children {
					// Here we are then. Knee-deep.
					switch c := child.Id.(type) {
					case *build_event_stream.BuildEventId_Pattern:
						{
							invocation.Pattern = c.Pattern.Pattern
						}
					}
				}
			}
		case *build_event_stream.BuildEvent_UnstructuredCommandLine:
			{
				p.UnstructuredCommandLine = filterUnstructuredCommandLine(p.UnstructuredCommandLine)
			}
		case *build_event_stream.BuildEvent_StructuredCommandLine:
			{
				filteredCL := filterCommandLine(p.StructuredCommandLine)
				if filteredCL != nil {
					invocation.StructuredCommandLine = append(invocation.StructuredCommandLine, filteredCL)
				}
			}
		case *build_event_stream.BuildEvent_OptionsParsed:
			{
			}
		case *build_event_stream.BuildEvent_WorkspaceStatus:
			{
				for _, item := range p.WorkspaceStatus.Item {
					switch item.Key {
					case "BUILD_USER":
						invocation.User = item.Value
					case "BUILD_HOST":
						invocation.Host = item.Value
					}
				}
			}
		case *build_event_stream.BuildEvent_Fetch:
			{
			}
		case *build_event_stream.BuildEvent_Configuration:
			{
			}
		case *build_event_stream.BuildEvent_Expanded:
			{
			}
		case *build_event_stream.BuildEvent_Configured:
			{
			}
		case *build_event_stream.BuildEvent_Action:
			{
			}
		case *build_event_stream.BuildEvent_NamedSetOfFiles:
			{
			}
		case *build_event_stream.BuildEvent_Completed:
			{
				invocation.Success = p.Completed.Success
			}
		case *build_event_stream.BuildEvent_TestResult:
			{
			}
		case *build_event_stream.BuildEvent_TestSummary:
			{
			}
		case *build_event_stream.BuildEvent_Finished:
			{
				endTimeMillis = p.Finished.FinishTimeMillis
			}
		case *build_event_stream.BuildEvent_BuildToolLogs:
			{
			}
		case *build_event_stream.BuildEvent_BuildMetrics:
			{
				invocation.ActionCount = p.BuildMetrics.ActionSummary.ActionsExecuted
			}
		case *build_event_stream.BuildEvent_WorkspaceInfo:
			{
			}
		case *build_event_stream.BuildEvent_BuildMetadata:
			{
			}
		case *build_event_stream.BuildEvent_ConvenienceSymlinksIdentified:
			{
			}
		}
	}

	buildDuration := time.Duration((endTimeMillis - startTimeMillis) * int64(time.Millisecond))
	invocation.DurationUsec = buildDuration.Microseconds()
	invocation.ConsoleBuffer = string(rwBuf.Bytes())
}

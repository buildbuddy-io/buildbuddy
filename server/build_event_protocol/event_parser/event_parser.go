package event_parser

import (
	"bytes"
	"regexp"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
	"github.com/buildbuddy-io/buildbuddy/server/terminal"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

const (
	envVarPrefix              = "--"
	envVarOptionName          = "client_env"
	buildMetadataOptionName   = "build_metadata"
	envVarSeparator           = "="
	envVarRedactedPlaceholder = "<REDACTED>"
	urlSecretRegexString      = `\:\/\/.*\@`
)

func parseAndFilterCommandLine(in *command_line.CommandLine) (*command_line.CommandLine, map[string]string, map[string]string) {
	envVarMap := make(map[string]string)
	buildMetadataMap := make(map[string]string)
	if in == nil {
		return nil, envVarMap, buildMetadataMap
	}
	urlSecretRegex := regexp.MustCompile(urlSecretRegexString)
	var out command_line.CommandLine
	out = *in
	for _, section := range out.Sections {
		switch p := section.SectionType.(type) {
		case *command_line.CommandLineSection_OptionList:
			{
				for _, option := range p.OptionList.Option {
					if strings.Contains(option.OptionValue, "@") {
						option.OptionValue = urlSecretRegex.ReplaceAllString(option.OptionValue, "://"+envVarRedactedPlaceholder+"@")
						option.CombinedForm = urlSecretRegex.ReplaceAllString(option.CombinedForm, "://"+envVarRedactedPlaceholder+"@")
					}
					if option.OptionName == "remote_header" ||  option.OptionName == "remote_cache_header" {
						option.OptionValue = envVarRedactedPlaceholder
						option.CombinedForm = envVarPrefix + option.OptionName + envVarSeparator + envVarRedactedPlaceholder
					}
					if option.OptionName == envVarOptionName {
						parts := strings.Split(option.OptionValue, envVarSeparator)
						if len(parts) == 2 {
							envVarMap[parts[0]]=parts[1]
						}
						option.OptionValue = strings.Join([]string{parts[0], envVarRedactedPlaceholder}, envVarSeparator)
						option.CombinedForm = envVarPrefix + envVarOptionName + envVarSeparator + parts[0] + envVarSeparator + envVarRedactedPlaceholder
					}
					if option.OptionName == buildMetadataOptionName {
						parts := strings.Split(option.OptionValue, envVarSeparator)
						if len(parts) == 2 {
							buildMetadataMap[parts[0]]=parts[1]
						}
					}
				}
			}
		default:
			continue
		}
	}
	return &out, envVarMap, buildMetadataMap
}

func filterUnstructuredCommandLine(in *build_event_stream.UnstructuredCommandLine) *build_event_stream.UnstructuredCommandLine {
	if in == nil {
		return nil
	}
	urlSecretRegex := regexp.MustCompile(urlSecretRegexString)
	var out build_event_stream.UnstructuredCommandLine
	out = *in
	for i, arg := range out.Args {
		if strings.Contains(arg, "@") {
			out.Args[i] = urlSecretRegex.ReplaceAllString(arg, "://"+envVarRedactedPlaceholder+"@")
		}
		if strings.HasPrefix(arg, envVarPrefix+"remote_header") {
			out.Args[i] = envVarPrefix + "remote_header" + envVarSeparator + envVarRedactedPlaceholder
		}
		if strings.HasPrefix(arg, envVarPrefix+"remote_cache_header") {
			out.Args[i] = envVarPrefix + "remote_cache_header" + envVarSeparator + envVarRedactedPlaceholder
		}
		if strings.HasPrefix(arg, envVarPrefix+envVarOptionName) {
			parts := strings.Split(arg, envVarSeparator)
			if len(parts) < 2 {
				continue
			}
			out.Args[i] = envVarPrefix + envVarOptionName + envVarSeparator + parts[1] + envVarSeparator + envVarRedactedPlaceholder
		}
	}
	return &out
}

func FillInvocationFromEvents(buildEvents []*inpb.InvocationEvent, invocation *inpb.Invocation) {
	startTimeMillis := int64(-1)
	endTimeMillis := int64(-1)

	var consoleBuffer bytes.Buffer

	for _, event := range buildEvents {
		invocation.Event = append(invocation.Event, event)

		switch p := event.BuildEvent.Payload.(type) {
		case *build_event_stream.BuildEvent_Progress:
			{
				consoleBuffer.Write([]byte(p.Progress.Stderr))
				consoleBuffer.Write([]byte(p.Progress.Stdout))
				// Clear progress event values as we've got them via ConsoleBuffer and they take up a lot of space.
				p.Progress.Stderr = ""
				p.Progress.Stdout = ""
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
				filteredCL, envVarMap, buildMetadataMap := parseAndFilterCommandLine(p.StructuredCommandLine)
				if filteredCL != nil {
					invocation.StructuredCommandLine = append(invocation.StructuredCommandLine, filteredCL)
				}

				invocation.RepoUrl = envVarMap["GITHUB_REPOSITORY"]
				if url, ok := buildMetadataMap["REPO_URL"]; ok {
					invocation.RepoUrl = url
				}

				invocation.CommitSha = envVarMap["GITHUB_SHA"]
				if sha, ok := buildMetadataMap["COMMIT_SHA"]; ok {
					invocation.CommitSha = sha
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
					case "REPO_URL":
						invocation.RepoUrl = item.Value
					case "COMMIT_SHA":
						invocation.CommitSha = item.Value
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
				invocation.Success = p.Finished.ExitCode.Code == 0
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
	// TODO(siggisim): Do this rendering once on write, rather than on every read.
	invocation.ConsoleBuffer = string(terminal.RenderAsANSI(consoleBuffer.Bytes()))
}

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
	envVarSeparator           = "="
	envVarRedactedPlaceholder = "<REDACTED>"
	urlSecretRegexString      = `\:\/\/.*\@`
)

func parseAndFilterCommandLine(in *command_line.CommandLine, allowedEnvVars []string) (*command_line.CommandLine, map[string]string) {
	envVarMap := make(map[string]string)
	if in == nil {
		return nil, envVarMap
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
					if option.OptionName == "remote_header" || option.OptionName == "remote_cache_header" {
						option.OptionValue = envVarRedactedPlaceholder
						option.CombinedForm = envVarPrefix + option.OptionName + envVarSeparator + envVarRedactedPlaceholder
					}
					if option.OptionName == envVarOptionName {
						parts := strings.Split(option.OptionValue, envVarSeparator)
						if len(parts) == 2 {
							envVarMap[parts[0]] = parts[1]
						}
						if isAllowedEnvVar(parts[0], allowedEnvVars) {
							continue
						}

						option.OptionValue = strings.Join([]string{parts[0], envVarRedactedPlaceholder}, envVarSeparator)
						option.CombinedForm = envVarPrefix + envVarOptionName + envVarSeparator + parts[0] + envVarSeparator + envVarRedactedPlaceholder
					}
				}
			}
		default:
			continue
		}
	}
	return &out, envVarMap
}

func isAllowedEnvVar(variableName string, allowedEnvVars []string) bool {
	lowercaseVariableName := strings.ToLower(variableName)
	for _, allowed := range allowedEnvVars {
		lowercaseAllowed := strings.ToLower(allowed)
		if allowed == "*" || lowercaseVariableName == lowercaseAllowed {
			return true
		}
		isWildCard := strings.HasSuffix(allowed, "*")
		allowedPrefix := strings.ReplaceAll(lowercaseAllowed, "*", "")
		if isWildCard && strings.HasPrefix(lowercaseVariableName, allowedPrefix) {
			return true
		}
	}
	return false
}

func FillInvocationFromEvents(buildEvents []*inpb.InvocationEvent, invocation *inpb.Invocation) {
	startTimeMillis := int64(-1)
	endTimeMillis := int64(-1)

	var consoleBuffer bytes.Buffer
	var allowedEnvVars = []string{"USER", "GITHUB_ACTOR", "GITHUB_REPOSITORY", "GITHUB_SHA", "GITHUB_RUN_ID"}

	structuredCommandLines := make([]*command_line.CommandLine, 0)
	workspaceStatuses := make([]*build_event_stream.WorkspaceStatus, 0)
	buildMetadata := make([]map[string]string, 0)

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
				// Clear the unstructured command line so we don't have to redact it.
				p.UnstructuredCommandLine = &build_event_stream.UnstructuredCommandLine{}
			}
		case *build_event_stream.BuildEvent_StructuredCommandLine:
			{
				structuredCommandLines = append(structuredCommandLines, p.StructuredCommandLine)
			}
		case *build_event_stream.BuildEvent_OptionsParsed:
			{
			}
		case *build_event_stream.BuildEvent_WorkspaceStatus:
			{
				workspaceStatuses = append(workspaceStatuses, p.WorkspaceStatus)
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
				metadata := p.BuildMetadata.Metadata
				if metadata == nil {
					continue
				}
				buildMetadata = append(buildMetadata, metadata)
				if allowed, ok := metadata["ALLOW_ENV"]; ok && allowed != "" {
					allowedEnvVars = append(allowedEnvVars, strings.Split(allowed, ",")...)
				}
			}
		case *build_event_stream.BuildEvent_ConvenienceSymlinksIdentified:
			{
			}
		}
	}

	// Fill invocation in a deterministic order:
	// - Environment variables
	// - Workspace status
	// - Build metadata

	for _, commandLine := range structuredCommandLines {
		fillInvocationFromStructuredCommandLine(commandLine, invocation, allowedEnvVars)
	}
	for _, workspaceStatus := range workspaceStatuses {
		fillInvocationFromWorkspaceStatus(workspaceStatus, invocation)
	}
	for _, buildMetadatum := range buildMetadata {
		fillInvocationFromBuildMetadata(buildMetadatum, invocation)
	}

	buildDuration := time.Duration((endTimeMillis - startTimeMillis) * int64(time.Millisecond))
	invocation.DurationUsec = buildDuration.Microseconds()
	// TODO(siggisim): Do this rendering once on write, rather than on every read.
	invocation.ConsoleBuffer = string(terminal.RenderAsANSI(consoleBuffer.Bytes()))
}

func fillInvocationFromStructuredCommandLine(commandLine *command_line.CommandLine, invocation *inpb.Invocation, allowedEnvVars []string) {
	filteredCL, envVarMap := parseAndFilterCommandLine(commandLine, allowedEnvVars)
	if filteredCL != nil {
		invocation.StructuredCommandLine = append(invocation.StructuredCommandLine, filteredCL)
	}
	if user, ok := envVarMap["USER"]; ok && user != "" {
		invocation.User = user
	}
	if url, ok := envVarMap["TRAVIS_REPO_SLUG"]; ok && url != "" {
		invocation.RepoUrl = url
	}
	if url, ok := envVarMap["BUILDKITE_REPO"]; ok && url != "" {
		invocation.RepoUrl = url
	}
	if url, ok := envVarMap["CIRCLE_REPOSITORY_URL"]; ok && url != "" {
		invocation.RepoUrl = url
	}
	if url, ok := envVarMap["GITHUB_REPOSITORY"]; ok && url != "" {
		invocation.RepoUrl = url
	}
	if sha, ok := envVarMap["TRAVIS_COMMIT"]; ok && sha != "" {
		invocation.CommitSha = sha
	}
	if sha, ok := envVarMap["BUILDKITE_COMMIT"]; ok && sha != "" {
		invocation.CommitSha = sha
	}
	if sha, ok := envVarMap["CIRCLE_SHA1"]; ok && sha != "" {
		invocation.CommitSha = sha
	}
	if sha, ok := envVarMap["GITHUB_SHA"]; ok && sha != "" {
		invocation.CommitSha = sha
	}
}

func fillInvocationFromWorkspaceStatus(workspaceStatus *build_event_stream.WorkspaceStatus, invocation *inpb.Invocation) {
	for _, item := range workspaceStatus.Item {
		if item.Value == "" {
			continue
		}
		switch item.Key {
		case "BUILD_USER":
			invocation.User = item.Value
		case "USER":
			invocation.User = item.Value
		case "BUILD_HOST":
			invocation.Host = item.Value
		case "HOST":
			invocation.Host = item.Value
		case "REPO_URL":
			invocation.RepoUrl = item.Value
		case "COMMIT_SHA":
			invocation.CommitSha = item.Value
		}
	}
}

func fillInvocationFromBuildMetadata(metadata map[string]string, invocation *inpb.Invocation) {
	if sha, ok := metadata["COMMIT_SHA"]; ok && sha != "" {
		invocation.CommitSha = sha
	}
	if url, ok := metadata["REPO_URL"]; ok && url != "" {
		invocation.RepoUrl = url
	}
	if user, ok := metadata["USER"]; ok && user != "" {
		invocation.User = user
	}
	if host, ok := metadata["HOST"]; ok && host != "" {
		invocation.Host = host
	}
	if visibility, ok := metadata["VISIBILITY"]; ok && visibility == "PUBLIC" {
		invocation.ReadPermission = inpb.InvocationPermission_PUBLIC
	}
}

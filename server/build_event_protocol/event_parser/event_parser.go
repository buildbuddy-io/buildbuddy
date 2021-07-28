package event_parser

import (
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
	"github.com/buildbuddy-io/buildbuddy/server/terminal"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

const (
	envVarOptionName   = "client_env"
	envVarSeparator    = "="
	undefinedTimestamp = int64(-1)
)

func parseEnv(commandLine *command_line.CommandLine) map[string]string {
	envVarMap := make(map[string]string)
	if commandLine == nil {
		return envVarMap
	}
	for _, section := range commandLine.Sections {
		if p, ok := section.SectionType.(*command_line.CommandLineSection_OptionList); ok {
			for _, option := range p.OptionList.Option {
				if option.OptionName == envVarOptionName {
					parts := strings.Split(option.OptionValue, envVarSeparator)
					if len(parts) == 2 {
						envVarMap[parts[0]] = parts[1]
					}
				}
			}
		}
	}
	return envVarMap
}

type StreamingEventParser struct {
	screenWriter           *terminal.ScreenWriter
	command                string
	buildMetadata          []map[string]string
	events                 []*inpb.InvocationEvent
	structuredCommandLines []*command_line.CommandLine
	workspaceStatuses      []*build_event_stream.WorkspaceStatus
	workflowConfigurations []*build_event_stream.WorkflowConfigured
	pattern                []string
	startTimeMillis        int64
	endTimeMillis          int64
	actionCount            int64
	success                bool
}

func NewStreamingEventParser() *StreamingEventParser {
	return &StreamingEventParser{
		startTimeMillis:        undefinedTimestamp,
		endTimeMillis:          undefinedTimestamp,
		screenWriter:           terminal.NewScreenWriter(),
		structuredCommandLines: make([]*command_line.CommandLine, 0),
		workspaceStatuses:      make([]*build_event_stream.WorkspaceStatus, 0),
		workflowConfigurations: make([]*build_event_stream.WorkflowConfigured, 0),
		buildMetadata:          make([]map[string]string, 0),
		events:                 make([]*inpb.InvocationEvent, 0),
	}
}

func (sep *StreamingEventParser) ParseEvent(event *inpb.InvocationEvent) {
	sep.events = append(sep.events, event)
	switch p := event.BuildEvent.Payload.(type) {
	case *build_event_stream.BuildEvent_Progress:
		{
			sep.screenWriter.Write([]byte(p.Progress.Stderr))
			sep.screenWriter.Write([]byte(p.Progress.Stdout))
			// Now that we've updated our screenwriter, zero out
			// progress output in the event so they don't eat up
			// memory.
			p.Progress.Stderr = ""
			p.Progress.Stdout = ""
		}
	case *build_event_stream.BuildEvent_Aborted:
		{
		}
	case *build_event_stream.BuildEvent_Started:
		{
			sep.startTimeMillis = p.Started.StartTimeMillis
			sep.command = p.Started.Command
			for _, child := range event.BuildEvent.Children {
				// Here we are then. Knee-deep.
				switch c := child.Id.(type) {
				case *build_event_stream.BuildEventId_Pattern:
					{
						sep.pattern = c.Pattern.Pattern
					}
				}
			}
		}
	case *build_event_stream.BuildEvent_UnstructuredCommandLine:
		{
		}
	case *build_event_stream.BuildEvent_StructuredCommandLine:
		{
			sep.structuredCommandLines = append(sep.structuredCommandLines, p.StructuredCommandLine)
		}
	case *build_event_stream.BuildEvent_OptionsParsed:
		{
		}
	case *build_event_stream.BuildEvent_WorkspaceStatus:
		{
			sep.workspaceStatuses = append(sep.workspaceStatuses, p.WorkspaceStatus)
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
			sep.endTimeMillis = p.Finished.FinishTimeMillis
			sep.success = p.Finished.ExitCode.Code == 0
		}
	case *build_event_stream.BuildEvent_BuildToolLogs:
		{
		}
	case *build_event_stream.BuildEvent_BuildMetrics:
		{
			sep.actionCount = p.BuildMetrics.ActionSummary.ActionsExecuted
		}
	case *build_event_stream.BuildEvent_WorkspaceInfo:
		{
		}
	case *build_event_stream.BuildEvent_BuildMetadata:
		{
			metadata := p.BuildMetadata.Metadata
			if metadata == nil {
				return
			}
			sep.buildMetadata = append(sep.buildMetadata, metadata)
		}
	case *build_event_stream.BuildEvent_ConvenienceSymlinksIdentified:
		{
		}
	case *build_event_stream.BuildEvent_WorkflowConfigured:
		{
			wfc := p.WorkflowConfigured
			if wfc == nil {
				return
			}
			sep.workflowConfigurations = append(sep.workflowConfigurations, wfc)
		}
	}
}

func (sep *StreamingEventParser) FillInvocation(invocation *inpb.Invocation) {
	invocation.Command = sep.command
	invocation.Pattern = sep.pattern
	invocation.Event = sep.events
	invocation.Success = sep.success
	invocation.ActionCount = sep.actionCount

	// Fill invocation in a deterministic order:
	// - Environment variables
	// - Workspace status
	// - Build metadata

	for _, commandLine := range sep.structuredCommandLines {
		fillInvocationFromStructuredCommandLine(commandLine, invocation)
	}
	for _, workspaceStatus := range sep.workspaceStatuses {
		fillInvocationFromWorkspaceStatus(workspaceStatus, invocation)
	}
	for _, buildMetadatum := range sep.buildMetadata {
		fillInvocationFromBuildMetadata(buildMetadatum, invocation)
	}
	for _, workflowConfigured := range sep.workflowConfigurations {
		fillInvocationFromWorkflowConfigured(workflowConfigured, invocation)
	}

	buildDuration := time.Duration(int64(0))
	if sep.endTimeMillis != undefinedTimestamp && sep.startTimeMillis != undefinedTimestamp {
		buildDuration = time.Duration((sep.endTimeMillis - sep.startTimeMillis) * int64(time.Millisecond))
	}
	invocation.DurationUsec = buildDuration.Microseconds()
	// TODO(siggisim): Do this rendering once on write, rather than on every read.
	invocation.ConsoleBuffer = string(sep.screenWriter.RenderAsANSI())
}

func fillInvocationFromStructuredCommandLine(commandLine *command_line.CommandLine, invocation *inpb.Invocation) {
	envVarMap := parseEnv(commandLine)
	if commandLine != nil {
		invocation.StructuredCommandLine = append(invocation.StructuredCommandLine, commandLine)
	}
	if user, ok := envVarMap["USER"]; ok && user != "" {
		invocation.User = user
	}
	if url, ok := envVarMap["TRAVIS_REPO_SLUG"]; ok && url != "" {
		invocation.RepoUrl = url
	}
	if url, ok := envVarMap["GIT_URL"]; ok && url != "" {
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
	if sha, ok := envVarMap["GIT_COMMIT"]; ok && sha != "" {
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
	if ci, ok := envVarMap["CI"]; ok && ci != "" {
		invocation.Role = "CI"
	}
	if ciRunner, ok := envVarMap["CI_RUNNER"]; ok && ciRunner != "" {
		invocation.Role = "CI_RUNNER"
	}

	// Gitlab CI Environment Variables
	// https://docs.gitlab.com/ee/ci/variables/predefined_variables.html
	if url, ok := envVarMap["CI_REPOSITORY_URL"]; ok && url != "" {
		invocation.RepoUrl = url
	}
	if sha, ok := envVarMap["CI_COMMIT_SHA"]; ok && sha != "" {
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
		case "ROLE":
			invocation.Role = item.Value
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
	if role, ok := metadata["ROLE"]; ok && role != "" {
		invocation.Role = role
	}
	if visibility, ok := metadata["VISIBILITY"]; ok && visibility == "PUBLIC" {
		invocation.ReadPermission = inpb.InvocationPermission_PUBLIC
	}
}

func fillInvocationFromWorkflowConfigured(workflowConfigured *build_event_stream.WorkflowConfigured, invocation *inpb.Invocation) {
	invocation.Command = "workflow run"
	invocation.Pattern = []string{workflowConfigured.ActionName}
}

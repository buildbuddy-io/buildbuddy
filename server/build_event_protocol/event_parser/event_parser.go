package event_parser

import (
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

const (
	envVarOptionName = "client_env"
	envVarSeparator  = "="
)

// eventPriority represents the priority of an invocation event
// when setting invocation fields. For example, if the build USER is set in
// build metadata, that always overrides the USER value from workspace status,
// even if the workspace status event came later in the stream.
type eventPriority int

const (
	envPriority                eventPriority = 1
	workspaceStatusPriority    eventPriority = 2
	buildMetadataPriority      eventPriority = 3
	workflowConfiguredPriority eventPriority = 4
)

func parseEnv(commandLine *command_line.CommandLine) map[string]string {
	envVarMap := make(map[string]string)
	if commandLine == nil {
		return envVarMap
	}
	for _, section := range commandLine.Sections {
		p, ok := section.SectionType.(*command_line.CommandLineSection_OptionList)
		if !ok {
			continue
		}
		for _, option := range p.OptionList.Option {
			if option.OptionName != envVarOptionName {
				continue
			}
			parts := strings.Split(option.OptionValue, envVarSeparator)
			if len(parts) == 2 {
				envVarMap[parts[0]] = parts[1]
			}
		}
	}
	return envVarMap
}

// StreamingEventParser consumes a stream of build events and populates an
// invocation proto as it does so.
//
// To save memory, only the "summary" fields (like success, duration, etc.) in
// the invocation are recorded by default, and any variable-length lists such as
// events, console buffer etc. are not saved.
type StreamingEventParser struct {
	invocation *inpb.Invocation
	startTime  *time.Time

	// fieldPriority maps invocation field pointers to the priority of the event
	// which set the field. This allows us to set invocation fields only if a
	// previous event of higher priority hasn't already set the field.
	fieldPriority map[any]eventPriority
}

func NewStreamingEventParser(invocation *inpb.Invocation) *StreamingEventParser {
	return &StreamingEventParser{
		invocation:    invocation,
		fieldPriority: make(map[any]eventPriority, 0),
	}
}

func (sep *StreamingEventParser) GetInvocation() *inpb.Invocation {
	return sep.invocation
}

func (sep *StreamingEventParser) ParseEvent(event *inpb.InvocationEvent) {
	switch p := event.BuildEvent.Payload.(type) {
	case *build_event_stream.BuildEvent_Progress:
		{
		}
	case *build_event_stream.BuildEvent_Aborted:
		{
		}
	case *build_event_stream.BuildEvent_Started:
		{
			startTime := timeutil.GetTimeWithFallback(p.Started.StartTime, p.Started.StartTimeMillis)
			sep.startTime = &startTime
			sep.invocation.Command = p.Started.Command
			for _, child := range event.BuildEvent.Children {
				// Here we are then. Knee-deep.
				switch c := child.Id.(type) {
				case *build_event_stream.BuildEventId_Pattern:
					{
						sep.invocation.Pattern = c.Pattern.Pattern
					}
				}
			}
		}
	case *build_event_stream.BuildEvent_UnstructuredCommandLine:
		{
		}
	case *build_event_stream.BuildEvent_StructuredCommandLine:
		{
			sep.fillInvocationFromStructuredCommandLine(p.StructuredCommandLine)
		}
	case *build_event_stream.BuildEvent_OptionsParsed:
		{
		}
	case *build_event_stream.BuildEvent_WorkspaceStatus:
		{
			sep.fillInvocationFromWorkspaceStatus(p.WorkspaceStatus)
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
			endTime := timeutil.GetTimeWithFallback(p.Finished.FinishTime, p.Finished.FinishTimeMillis)
			if sep.startTime != nil {
				duration := endTime.Sub(*sep.startTime)
				sep.invocation.DurationUsec = duration.Microseconds()
			}
			sep.invocation.Success = p.Finished.ExitCode.Code == 0
		}
	case *build_event_stream.BuildEvent_BuildToolLogs:
		{
		}
	case *build_event_stream.BuildEvent_BuildMetrics:
		{
			sep.invocation.ActionCount = p.BuildMetrics.ActionSummary.ActionsExecuted
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
			sep.fillInvocationFromBuildMetadata(metadata)
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
			sep.fillInvocationFromWorkflowConfigured(wfc)
		}
	}
}

// setField sets an invocation field from a build event, but only if the current
// field value was not already set by an event of higher priority (note, if the
// field was already set by an event with the *same* priority, then the field is
// overridden).
func setField[T any](sep *StreamingEventParser, priority eventPriority, field *T, value T) {
	if sep.fieldPriority[field] > priority {
		return
	}
	*field = value
	sep.fieldPriority[field] = priority
}

func (sep *StreamingEventParser) fillInvocationFromStructuredCommandLine(commandLine *command_line.CommandLine) {
	envVarMap := parseEnv(commandLine)
	invocation := sep.invocation
	if user, ok := envVarMap["USER"]; ok && user != "" {
		setField(sep, envPriority, &invocation.User, user)
	}
	if url, ok := envVarMap["TRAVIS_REPO_SLUG"]; ok && url != "" {
		setField(sep, envPriority, &invocation.RepoUrl, url)
	}
	if url, ok := envVarMap["GIT_URL"]; ok && url != "" {
		setField(sep, envPriority, &invocation.RepoUrl, url)
	}
	if url, ok := envVarMap["BUILDKITE_REPO"]; ok && url != "" {
		setField(sep, envPriority, &invocation.RepoUrl, url)
	}
	if url, ok := envVarMap["REPO_URL"]; ok && url != "" {
		setField(sep, envPriority, &invocation.RepoUrl, url)
	}
	if url, ok := envVarMap["CIRCLE_REPOSITORY_URL"]; ok && url != "" {
		setField(sep, envPriority, &invocation.RepoUrl, url)
	}
	if url, ok := envVarMap["GITHUB_REPOSITORY"]; ok && url != "" {
		setField(sep, envPriority, &invocation.RepoUrl, url)
	}
	if branch, ok := envVarMap["TRAVIS_BRANCH"]; ok && branch != "" {
		setField(sep, envPriority, &invocation.BranchName, branch)
	}
	if branch, ok := envVarMap["GIT_BRANCH"]; ok && branch != "" {
		setField(sep, envPriority, &invocation.BranchName, branch)
	}
	if branch, ok := envVarMap["BUILDKITE_BRANCH"]; ok && branch != "" {
		setField(sep, envPriority, &invocation.BranchName, branch)
	}
	if branch, ok := envVarMap["CIRCLE_BRANCH"]; ok && branch != "" {
		setField(sep, envPriority, &invocation.BranchName, branch)
	}
	if branch, ok := envVarMap["GITHUB_REF"]; ok && strings.HasPrefix(branch, "refs/heads/") {
		setField(sep, envPriority, &invocation.BranchName, strings.TrimPrefix(branch, "refs/heads/"))
	}
	if branch, ok := envVarMap["GITHUB_HEAD_REF"]; ok && branch != "" {
		setField(sep, envPriority, &invocation.BranchName, branch)
	}
	if sha, ok := envVarMap["TRAVIS_COMMIT"]; ok && sha != "" {
		setField(sep, envPriority, &invocation.CommitSha, sha)
	}
	if sha, ok := envVarMap["GIT_COMMIT"]; ok && sha != "" {
		setField(sep, envPriority, &invocation.CommitSha, sha)
	}
	if sha, ok := envVarMap["BUILDKITE_COMMIT"]; ok && sha != "" {
		setField(sep, envPriority, &invocation.CommitSha, sha)
	}
	if sha, ok := envVarMap["CIRCLE_SHA1"]; ok && sha != "" {
		setField(sep, envPriority, &invocation.CommitSha, sha)
	}
	if sha, ok := envVarMap["GITHUB_SHA"]; ok && sha != "" {
		setField(sep, envPriority, &invocation.CommitSha, sha)
	}
	if sha, ok := envVarMap["COMMIT_SHA"]; ok && sha != "" {
		setField(sep, envPriority, &invocation.CommitSha, sha)
	}
	if sha, ok := envVarMap["VOLATILE_GIT_COMMIT"]; ok && sha != "" {
		setField(sep, envPriority, &invocation.CommitSha, sha)
	}
	if ci, ok := envVarMap["CI"]; ok && ci != "" {
		setField(sep, envPriority, &invocation.Role, "CI")
	}
	if ciRunner, ok := envVarMap["CI_RUNNER"]; ok && ciRunner != "" {
		setField(sep, envPriority, &invocation.Role, "CI_RUNNER")
	}

	// Gitlab CI Environment Variables
	// https://docs.gitlab.com/ee/ci/variables/predefined_variables.html
	if url, ok := envVarMap["CI_REPOSITORY_URL"]; ok && url != "" {
		setField(sep, envPriority, &invocation.RepoUrl, url)
	}
	if branch, ok := envVarMap["CI_COMMIT_BRANCH"]; ok && branch != "" {
		setField(sep, envPriority, &invocation.BranchName, branch)
	}
	if sha, ok := envVarMap["CI_COMMIT_SHA"]; ok && sha != "" {
		setField(sep, envPriority, &invocation.CommitSha, sha)
	}
}

func (sep *StreamingEventParser) fillInvocationFromWorkspaceStatus(workspaceStatus *build_event_stream.WorkspaceStatus) {
	invocation := sep.invocation
	for _, item := range workspaceStatus.Item {
		if item.Value == "" {
			continue
		}
		switch item.Key {
		case "BUILD_USER":
			setField(sep, workspaceStatusPriority, &invocation.User, item.Value)
		case "USER":
			setField(sep, workspaceStatusPriority, &invocation.User, item.Value)
		case "BUILD_HOST":
			setField(sep, workspaceStatusPriority, &invocation.Host, item.Value)
		case "HOST":
			setField(sep, workspaceStatusPriority, &invocation.Host, item.Value)
		case "PATTERN":
			setField(sep, workspaceStatusPriority, &invocation.Pattern, strings.Split(item.Value, " "))
		case "ROLE":
			setField(sep, workspaceStatusPriority, &invocation.Role, item.Value)
		case "REPO_URL":
			setField(sep, workspaceStatusPriority, &invocation.RepoUrl, item.Value)
		case "GIT_BRANCH":
			setField(sep, workspaceStatusPriority, &invocation.BranchName, item.Value)
		case "COMMIT_SHA":
			setField(sep, workspaceStatusPriority, &invocation.CommitSha, item.Value)
		}
	}
}

func (sep *StreamingEventParser) fillInvocationFromBuildMetadata(metadata map[string]string) {
	invocation := sep.invocation
	if sha, ok := metadata["COMMIT_SHA"]; ok && sha != "" {
		setField(sep, buildMetadataPriority, &invocation.CommitSha, sha)
	}
	if branch, ok := metadata["BRANCH_NAME"]; ok && branch != "" {
		setField(sep, buildMetadataPriority, &invocation.BranchName, branch)
	}
	if url, ok := metadata["REPO_URL"]; ok && url != "" {
		setField(sep, buildMetadataPriority, &invocation.RepoUrl, url)
	}
	if user, ok := metadata["USER"]; ok && user != "" {
		setField(sep, buildMetadataPriority, &invocation.User, user)
	}
	if host, ok := metadata["HOST"]; ok && host != "" {
		setField(sep, buildMetadataPriority, &invocation.Host, host)
	}
	if pattern, ok := metadata["PATTERN"]; ok && pattern != "" {
		setField(sep, buildMetadataPriority, &invocation.Pattern, strings.Split(pattern, " "))
	}
	if role, ok := metadata["ROLE"]; ok && role != "" {
		setField(sep, buildMetadataPriority, &invocation.Role, role)
	}
	if visibility, ok := metadata["VISIBILITY"]; ok && visibility == "PUBLIC" {
		setField(sep, buildMetadataPriority, &invocation.ReadPermission, inpb.InvocationPermission_PUBLIC)
	}
}

func (sep *StreamingEventParser) fillInvocationFromWorkflowConfigured(workflowConfigured *build_event_stream.WorkflowConfigured) {
	invocation := sep.invocation
	setField(sep, workflowConfiguredPriority, &invocation.Command, "workflow run")
	setField(sep, workflowConfiguredPriority, &invocation.Pattern, []string{workflowConfigured.ActionName})
}

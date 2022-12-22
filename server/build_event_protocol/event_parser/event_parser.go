package event_parser

import (
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

const (
	envVarOptionName = "client_env"
	envVarSeparator  = "="
)

const (
	// Priorities determine the precedence of different events as they apply to
	// invocation fields.
	//
	// For example, a RepoUrl setting in BuildMetadata takes priority over a
	// repo URL set via WorkspaceStatus, even if the workspace status event came
	// after the build metadata event in the stream.

	startedPriority            = 1
	envPriority                = 2
	workspaceStatusPriority    = 3
	buildMetadataPriority      = 4
	workflowConfiguredPriority = 5
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

	priority fieldPriorities
}

// fieldPriorities keeps track of all the priorities currently assigned to each
// field. For consistency, the field names here are named exactly after the
// invocation proto fields.
type fieldPriorities struct {
	Host,
	User,
	Role,
	ReadPermission,
	RepoUrl,
	BranchName,
	CommitSha,
	Command,
	Pattern int
}

func NewStreamingEventParser(invocation *inpb.Invocation) *StreamingEventParser {
	return &StreamingEventParser{
		invocation: invocation,
	}
}

func (sep *StreamingEventParser) GetInvocation() *inpb.Invocation {
	return sep.invocation
}

func (sep *StreamingEventParser) ParseEvent(event *build_event_stream.BuildEvent) {
	switch p := event.Payload.(type) {
	case *build_event_stream.BuildEvent_Progress:
		{
		}
	case *build_event_stream.BuildEvent_Aborted:
		{
		}
	case *build_event_stream.BuildEvent_Started:
		{
			priority := startedPriority
			startTime := timeutil.GetTimeWithFallback(p.Started.StartTime, p.Started.StartTimeMillis)
			sep.startTime = &startTime
			sep.setCommand(p.Started.Command, priority)
			for _, child := range event.Children {
				// Here we are then. Knee-deep.
				switch c := child.Id.(type) {
				case *build_event_stream.BuildEventId_Pattern:
					{
						sep.setPattern(c.Pattern.Pattern, priority)
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
			sep.invocation.Success = p.Finished.ExitCode.GetCode() == 0
			sep.invocation.BazelExitCode = p.Finished.ExitCode.GetName()
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

func (sep *StreamingEventParser) fillInvocationFromStructuredCommandLine(commandLine *command_line.CommandLine) {
	priority := envPriority
	envVarMap := parseEnv(commandLine)
	if user, ok := envVarMap["USER"]; ok && user != "" {
		sep.setUser(user, priority)
	}
	if url, ok := envVarMap["TRAVIS_REPO_SLUG"]; ok && url != "" {
		sep.setRepoUrl(url, priority)
	}
	if url, ok := envVarMap["GIT_URL"]; ok && url != "" {
		sep.setRepoUrl(url, priority)
	}
	if url, ok := envVarMap["BUILDKITE_REPO"]; ok && url != "" {
		sep.setRepoUrl(url, priority)
	}
	if url, ok := envVarMap["REPO_URL"]; ok && url != "" {
		sep.setRepoUrl(url, priority)
	}
	if url, ok := envVarMap["CIRCLE_REPOSITORY_URL"]; ok && url != "" {
		sep.setRepoUrl(url, priority)
	}
	if url, ok := envVarMap["GITHUB_REPOSITORY"]; ok && url != "" {
		sep.setRepoUrl(url, priority)
	}
	if branch, ok := envVarMap["TRAVIS_BRANCH"]; ok && branch != "" {
		sep.setBranchName(branch, priority)
	}
	if branch, ok := envVarMap["GIT_BRANCH"]; ok && branch != "" {
		sep.setBranchName(branch, priority)
	}
	if branch, ok := envVarMap["BUILDKITE_BRANCH"]; ok && branch != "" {
		sep.setBranchName(branch, priority)
	}
	if branch, ok := envVarMap["CIRCLE_BRANCH"]; ok && branch != "" {
		sep.setBranchName(branch, priority)
	}
	if branch, ok := envVarMap["GITHUB_REF"]; ok && strings.HasPrefix(branch, "refs/heads/") {
		sep.setBranchName(strings.TrimPrefix(branch, "refs/heads/"), priority)
	}
	if branch, ok := envVarMap["GITHUB_HEAD_REF"]; ok && branch != "" {
		sep.setBranchName(branch, priority)
	}
	if sha, ok := envVarMap["TRAVIS_COMMIT"]; ok && sha != "" {
		sep.setCommitSha(sha, priority)
	}
	if sha, ok := envVarMap["GIT_COMMIT"]; ok && sha != "" {
		sep.setCommitSha(sha, priority)
	}
	if sha, ok := envVarMap["BUILDKITE_COMMIT"]; ok && sha != "" {
		sep.setCommitSha(sha, priority)
	}
	if sha, ok := envVarMap["CIRCLE_SHA1"]; ok && sha != "" {
		sep.setCommitSha(sha, priority)
	}
	if sha, ok := envVarMap["GITHUB_SHA"]; ok && sha != "" {
		sep.setCommitSha(sha, priority)
	}
	if sha, ok := envVarMap["COMMIT_SHA"]; ok && sha != "" {
		sep.setCommitSha(sha, priority)
	}
	if sha, ok := envVarMap["VOLATILE_GIT_COMMIT"]; ok && sha != "" {
		sep.setCommitSha(sha, priority)
	}
	if ci, ok := envVarMap["CI"]; ok && ci != "" {
		sep.setRole("CI", priority)
	}
	if ciRunner, ok := envVarMap["CI_RUNNER"]; ok && ciRunner != "" {
		sep.setRole("CI_RUNNER", priority)
	}

	// Gitlab CI Environment Variables
	// https://docs.gitlab.com/ee/ci/variables/predefined_variables.html
	if url, ok := envVarMap["CI_REPOSITORY_URL"]; ok && url != "" {
		sep.setRepoUrl(url, priority)
	}
	if branch, ok := envVarMap["CI_COMMIT_BRANCH"]; ok && branch != "" {
		sep.setBranchName(branch, priority)
	}
	if sha, ok := envVarMap["CI_COMMIT_SHA"]; ok && sha != "" {
		sep.setCommitSha(sha, priority)
	}
}

func (sep *StreamingEventParser) fillInvocationFromWorkspaceStatus(workspaceStatus *build_event_stream.WorkspaceStatus) {
	priority := workspaceStatusPriority
	for _, item := range workspaceStatus.Item {
		if item.Value == "" {
			continue
		}
		switch item.Key {
		case "BUILD_USER":
			sep.setUser(item.Value, priority)
		case "USER":
			sep.setUser(item.Value, priority)
		case "BUILD_HOST":
			sep.setHost(item.Value, priority)
		case "HOST":
			sep.setHost(item.Value, priority)
		case "PATTERN":
			sep.setPattern(strings.Split(item.Value, " "), priority)
		case "ROLE":
			sep.setRole(item.Value, priority)
		case "REPO_URL":
			sep.setRepoUrl(item.Value, priority)
		case "GIT_BRANCH":
			sep.setBranchName(item.Value, priority)
		case "COMMIT_SHA":
			sep.setCommitSha(item.Value, priority)
		}
	}
}

func (sep *StreamingEventParser) fillInvocationFromBuildMetadata(metadata map[string]string) {
	priority := buildMetadataPriority
	if sha, ok := metadata["COMMIT_SHA"]; ok && sha != "" {
		sep.setCommitSha(sha, priority)
	}
	if branch, ok := metadata["BRANCH_NAME"]; ok && branch != "" {
		sep.setBranchName(branch, priority)
	}
	if url, ok := metadata["REPO_URL"]; ok && url != "" {
		sep.setRepoUrl(url, priority)
	}
	if user, ok := metadata["USER"]; ok && user != "" {
		sep.setUser(user, priority)
	}
	if host, ok := metadata["HOST"]; ok && host != "" {
		sep.setHost(host, priority)
	}
	if pattern, ok := metadata["PATTERN"]; ok && pattern != "" {
		sep.setPattern(strings.Split(pattern, " "), priority)
	}
	if role, ok := metadata["ROLE"]; ok && role != "" {
		sep.setRole(role, priority)
	}
	if visibility, ok := metadata["VISIBILITY"]; ok && visibility == "PUBLIC" {
		sep.setReadPermission(inpb.InvocationPermission_PUBLIC, priority)
	}
}

func (sep *StreamingEventParser) fillInvocationFromWorkflowConfigured(workflowConfigured *build_event_stream.WorkflowConfigured) {
	priority := workflowConfiguredPriority
	sep.setCommand("workflow run", priority)
	sep.setPattern([]string{workflowConfigured.ActionName}, priority)
}

// All the funcs below set invocation fields only if they haven't already been
// set by an event with higher priority.

func (sep *StreamingEventParser) setHost(value string, priority int) {
	if sep.priority.Host <= priority {
		sep.priority.Host = priority
		sep.invocation.Host = value
	}
}
func (sep *StreamingEventParser) setUser(value string, priority int) {
	if sep.priority.User <= priority {
		sep.priority.User = priority
		sep.invocation.User = value
	}
}
func (sep *StreamingEventParser) setRole(value string, priority int) {
	if sep.priority.Role <= priority {
		sep.priority.Role = priority
		sep.invocation.Role = value
	}
}
func (sep *StreamingEventParser) setReadPermission(value inpb.InvocationPermission, priority int) {
	if sep.priority.ReadPermission <= priority {
		sep.priority.ReadPermission = priority
		sep.invocation.ReadPermission = value
	}
}
func (sep *StreamingEventParser) setRepoUrl(value string, priority int) {
	if norm, _ := git.NormalizeRepoURL(value); norm != nil {
		value = norm.String()
	}
	if sep.priority.RepoUrl <= priority {
		sep.priority.RepoUrl = priority
		sep.invocation.RepoUrl = value
	}
}
func (sep *StreamingEventParser) setBranchName(value string, priority int) {
	if sep.priority.BranchName <= priority {
		sep.priority.BranchName = priority
		sep.invocation.BranchName = value
	}
}
func (sep *StreamingEventParser) setCommitSha(value string, priority int) {
	if sep.priority.CommitSha <= priority {
		sep.priority.CommitSha = priority
		sep.invocation.CommitSha = value
	}
}
func (sep *StreamingEventParser) setCommand(value string, priority int) {
	if sep.priority.Command <= priority {
		sep.priority.Command = priority
		sep.invocation.Command = value
	}
}
func (sep *StreamingEventParser) setPattern(value []string, priority int) {
	if sep.priority.Pattern <= priority {
		sep.priority.Pattern = priority
		sep.invocation.Pattern = value
	}
}

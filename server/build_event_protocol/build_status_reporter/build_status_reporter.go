package build_status_reporter

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
)

type BuildStatusReporter struct {
	env          environment.Env
	githubClient *github.GithubClient
	command      string
	pattern      string
	repoURL      string
	commitSHA    string
	role         string
}

func NewBuildStatusReporter(env environment.Env) *BuildStatusReporter {
	return &BuildStatusReporter{
		env:          env,
		githubClient: github.NewGithubClient(env),
	}
}

func (r *BuildStatusReporter) ReportStatusForEvent(ctx context.Context, invocationId string, event *build_event_stream.BuildEvent) {
	githubPayload := &github.GithubStatusPayload{}

	switch p := event.Payload.(type) {
	case *build_event_stream.BuildEvent_Started:
		r.populateWorkspaceInfoFromStartedEvent(event)

	case *build_event_stream.BuildEvent_StructuredCommandLine:
		r.populateWorkspaceInfoFromStructuredCommandLine(p.StructuredCommandLine)

	case *build_event_stream.BuildEvent_BuildMetadata:
		r.populateWorkspaceInfoFromBuildMetadata(p.BuildMetadata)

	case *build_event_stream.BuildEvent_WorkspaceStatus:
		r.populateWorkspaceInfoFromWorkspaceStatus(p.WorkspaceStatus)
		githubPayload = r.githubPayloadFromWorkspaceStatusEvent(event, invocationId)

	case *build_event_stream.BuildEvent_TestSummary:
		githubPayload = r.githubPayloadFromTestSummaryEvent(event, invocationId)

	case *build_event_stream.BuildEvent_Finished:
		githubPayload = r.githubPayloadFromFinishedEvent(event, invocationId)
	}

	if githubPayload.State != "" && r.role == "CI" {
		// TODO(siggisim): Kick these into a queue or something (but maintain order).
		go r.githubClient.CreateStatus(ctx, extractUserRepoFromRepoUrl(r.repoURL), r.commitSHA, githubPayload)
	}
}

func (r *BuildStatusReporter) populateWorkspaceInfoFromStartedEvent(event *build_event_stream.BuildEvent) {
	r.command = event.GetStarted().Command
	r.pattern = patternFromEvent(event)
}

func (r *BuildStatusReporter) populateWorkspaceInfoFromStructuredCommandLine(commandLine *command_line.CommandLine) {
	for _, section := range commandLine.Sections {
		if list := section.GetOptionList(); list == nil {
			continue
		}
		for _, option := range section.GetOptionList().Option {
			if option.OptionName != "ENV" {
				continue
			}
			parts := strings.Split(option.OptionValue, "=")
			if len(parts) == 2 && (parts[0] == "CIRCLE_REPOSITORY_URL" || parts[0] == "GITHUB_REPOSITORY") {
				r.repoURL = parts[1]
			}
			if len(parts) == 2 && (parts[0] == "CIRCLE_SHA1" || parts[0] == "GITHUB_SHA") {
				r.commitSHA = parts[1]
			}
		}
	}
}

func (r *BuildStatusReporter) populateWorkspaceInfoFromBuildMetadata(metadata *build_event_stream.BuildMetadata) {
	if url, ok := metadata.Metadata["REPO_URL"]; ok && url != "" {
		r.repoURL = url
	}

	if sha, ok := metadata.Metadata["COMMIT_SHA"]; ok && sha != "" {
		r.commitSHA = sha
	}

	if role, ok := metadata.Metadata["ROLE"]; ok && role != "" {
		r.role = role
	}
}

func (r *BuildStatusReporter) populateWorkspaceInfoFromWorkspaceStatus(workspace *build_event_stream.WorkspaceStatus) {
	for _, item := range workspace.Item {
		if item.Key == "REPO_URL" && item.Value != "" {
			r.repoURL = item.Value
		}
		if item.Key == "COMMIT_SHA" && item.Value != "" {
			r.commitSHA = item.Value
		}
	}
}

func (r *BuildStatusReporter) githubPayloadFromWorkspaceStatusEvent(event *build_event_stream.BuildEvent, invocationId string) *github.GithubStatusPayload {
	appURL := r.env.GetConfigurator().GetAppBuildBuddyURL()
	return &github.GithubStatusPayload{
		Context:     fmt.Sprintf("bazel %s %s", r.command, r.pattern),
		TargetURL:   fmt.Sprintf(appURL+"/invocation/%s", invocationId),
		Description: "Running...",
		State:       "pending",
	}
}

func (r *BuildStatusReporter) githubPayloadFromTestSummaryEvent(event *build_event_stream.BuildEvent, invocationId string) *github.GithubStatusPayload {
	appURL := r.env.GetConfigurator().GetAppBuildBuddyURL()
	githubPayload := &github.GithubStatusPayload{
		Context:     event.Id.GetTestSummary().Label,
		TargetURL:   fmt.Sprintf(appURL+"/invocation/%s?target=%s", invocationId, event.Id.GetTestSummary().Label),
		Description: descriptionFromOverallStatus(event.GetTestSummary().OverallStatus),
		State:       "failure",
	}
	if event.GetTestSummary().OverallStatus == build_event_stream.TestStatus_PASSED {
		githubPayload.State = "success"
	}
	return githubPayload
}

func (r *BuildStatusReporter) githubPayloadFromFinishedEvent(event *build_event_stream.BuildEvent, invocationId string) *github.GithubStatusPayload {
	appURL := r.env.GetConfigurator().GetAppBuildBuddyURL()
	githubPayload := &github.GithubStatusPayload{
		Context:     fmt.Sprintf("bazel %s %s", r.command, r.pattern),
		TargetURL:   fmt.Sprintf(appURL+"/invocation/%s", invocationId),
		Description: descriptionFromExitCodeName(event.GetFinished().ExitCode.Name),
		State:       "failure",
	}
	if event.GetFinished().OverallSuccess {
		githubPayload.State = "success"
	}
	return githubPayload
}

func patternFromEvent(event *build_event_stream.BuildEvent) string {
	for _, child := range event.Children {
		switch c := child.Id.(type) {
		case *build_event_stream.BuildEventId_Pattern:
			{
				return strings.Join(c.Pattern.Pattern, " ")
			}
		}
	}
	return ""
}

func descriptionFromOverallStatus(overallStatus build_event_stream.TestStatus) string {
	switch overallStatus {
	case build_event_stream.TestStatus_PASSED:
		return "Passed"
	case build_event_stream.TestStatus_FLAKY:
		return "Flaky"
	case build_event_stream.TestStatus_TIMEOUT:
		return "Timeout"
	case build_event_stream.TestStatus_FAILED:
		return "Failed"
	case build_event_stream.TestStatus_INCOMPLETE:
		return "Incomplete"
	case build_event_stream.TestStatus_REMOTE_FAILURE:
		return "Remote failure"
	case build_event_stream.TestStatus_FAILED_TO_BUILD:
		return "Failed to build"
	case build_event_stream.TestStatus_TOOL_HALTED_BEFORE_TESTING:
		return "Cancelled"
	default:
		return "Unknown"
	}
}

func descriptionFromExitCodeName(exitCodeName string) string {
	return strings.Title(strings.ToLower(strings.ReplaceAll(exitCodeName, "_", " ")))
}

func extractUserRepoFromRepoUrl(repoURL string) string {
	// TODO(siggisim): Come up with a regex here.
	repoURL = strings.ReplaceAll(repoURL, "ssh://", "")
	repoURL = strings.ReplaceAll(repoURL, "http://", "")
	repoURL = strings.ReplaceAll(repoURL, "https://", "")
	repoURL = strings.ReplaceAll(repoURL, "git@", "")
	repoURL = strings.ReplaceAll(repoURL, ".git", "")
	repoURL = strings.ReplaceAll(repoURL, "github.com/", "")
	repoURL = strings.ReplaceAll(repoURL, "github.com:", "")
	return repoURL
}

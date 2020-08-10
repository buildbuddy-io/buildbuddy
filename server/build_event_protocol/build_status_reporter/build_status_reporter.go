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
	env             environment.Env
	githubClient    *github.GithubClient
	invocationID    string
	command         string
	pattern         string
	role            string
	repoURL         string
	commitSHA       string
	workspaceLoaded bool
	payloads        []*github.GithubStatusPayload
	groups          map[string]*GroupStatus
	inFlight        map[string]bool
}

type GroupStatus struct {
	name       string
	numTargets int
	numPassed  int
	numFailed  int
	numAborted int
}

func NewBuildStatusReporter(env environment.Env, invocationID string) *BuildStatusReporter {
	return &BuildStatusReporter{
		env:          env,
		githubClient: github.NewGithubClient(env),
		invocationID: invocationID,
		payloads:     make([]*github.GithubStatusPayload, 0),
		inFlight:     make(map[string]bool),
	}
}

func (r *BuildStatusReporter) ReportStatusForEvent(ctx context.Context, event *build_event_stream.BuildEvent) {
	var githubPayload *github.GithubStatusPayload

	switch p := event.Payload.(type) {
	case *build_event_stream.BuildEvent_Started:
		r.populateWorkspaceInfoFromStartedEvent(event)

	case *build_event_stream.BuildEvent_StructuredCommandLine:
		r.populateWorkspaceInfoFromStructuredCommandLine(p.StructuredCommandLine)

	case *build_event_stream.BuildEvent_BuildMetadata:
		r.populateWorkspaceInfoFromBuildMetadata(p.BuildMetadata)

	case *build_event_stream.BuildEvent_WorkspaceStatus:
		r.populateWorkspaceInfoFromWorkspaceStatus(p.WorkspaceStatus)
		githubPayload = r.githubPayloadFromWorkspaceStatusEvent(event)

	case *build_event_stream.BuildEvent_Configured:
		githubPayload = r.githubPayloadFromConfiguredEvent(event)

	case *build_event_stream.BuildEvent_TestSummary:
		githubPayload = r.githubPayloadFromTestSummaryEvent(event)

	case *build_event_stream.BuildEvent_Aborted:
		githubPayload = r.githubPayloadFromAbortedEvent(event)

	case *build_event_stream.BuildEvent_Finished:
		githubPayload = r.githubPayloadFromFinishedEvent(event)
	}

	if githubPayload != nil && r.role == "CI" {
		r.payloads = append(r.payloads, githubPayload)
		r.flushPayloadsIfWorkspaceLoaded(ctx)
	}
}

func (r *BuildStatusReporter) ReportDisconnect(ctx context.Context) {
	for label := range r.inFlight {
		r.payloads = append(r.payloads, github.NewGithubStatusPayload(label, r.invocationURL(), "Disconnected", "error"))
	}
	r.flushPayloadsIfWorkspaceLoaded(ctx)
}

func (r *BuildStatusReporter) flushPayloadsIfWorkspaceLoaded(ctx context.Context) {
	if !r.workspaceLoaded {
		return // If we haven't loaded the workspace, we can't flush payloads yet.
	}

	for _, payload := range r.payloads {
		if payload.State == "pending" {
			r.inFlight[payload.Context] = true
		} else {
			delete(r.inFlight, payload.Context)
		}

		// TODO(siggisim): Kick these into a queue or something (but maintain order).
		r.githubClient.CreateStatus(ctx, extractUserRepoFromRepoUrl(r.repoURL), r.commitSHA, payload)
	}

	r.payloads = make([]*github.GithubStatusPayload, 0)
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
			if len(parts) == 2 && (parts[0] == "CIRCLE_REPOSITORY_URL" || parts[0] == "GITHUB_REPOSITORY" || parts[0] == "BUILDKITE_REPO" || parts[0] == "TRAVIS_REPO_SLUG") {
				r.repoURL = parts[1]
			}
			if len(parts) == 2 && (parts[0] == "CIRCLE_SHA1" || parts[0] == "GITHUB_SHA" || parts[0] == "BUILDKITE_COMMIT" || parts[0] == "TRAVIS_COMMIT") {
				r.commitSHA = parts[1]
			}
			if len(parts) == 2 && parts[0] == "CI" && parts[1] != "" {
				r.role = "CI"
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
	if testGroups, ok := metadata.Metadata["TEST_GROUPS"]; ok && testGroups != "" {
		r.initializeGroups(testGroups)
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
	r.workspaceLoaded = true
}

func (r *BuildStatusReporter) githubPayloadFromWorkspaceStatusEvent(event *build_event_stream.BuildEvent) *github.GithubStatusPayload {
	return github.NewGithubStatusPayload(r.invocationLabel(), r.invocationURL(), "Running...", "pending")
}

func (r *BuildStatusReporter) githubPayloadFromConfiguredEvent(event *build_event_stream.BuildEvent) *github.GithubStatusPayload {
	if event.GetConfigured().TestSize == build_event_stream.TestSize_UNKNOWN {
		return nil // We only report pending for test targets.
	}

	label := r.labelFromEvent(event)
	groupStatus := r.groupStatusFromLabel(label)
	if groupStatus != nil {
		groupStatus.numTargets++
	}

	if groupStatus != nil && groupStatus.numTargets == 1 {
		return github.NewGithubStatusPayload(groupStatus.name, r.groupURL(groupStatus.name), "Running...", "pending")
	}

	return github.NewGithubStatusPayload(label, r.targetURL(label), "Running...", "pending")
}

func (r *BuildStatusReporter) githubPayloadFromTestSummaryEvent(event *build_event_stream.BuildEvent) *github.GithubStatusPayload {
	passed := event.GetTestSummary().OverallStatus == build_event_stream.TestStatus_PASSED
	label := r.labelFromEvent(event)
	groupStatus := r.groupStatusFromLabel(label)
	if groupStatus != nil {
		if passed {
			groupStatus.numPassed++
		} else {
			groupStatus.numFailed++
		}
	}

	description := descriptionFromOverallStatus(event.GetTestSummary().OverallStatus)

	if groupStatus != nil && groupStatus.numFailed == 1 {
		return github.NewGithubStatusPayload(groupStatus.name, r.groupURL(label), description, "failure")
	}

	if groupStatus != nil && groupStatus.numPassed == groupStatus.numTargets {
		return github.NewGithubStatusPayload(groupStatus.name, r.groupURL(label), description, "success")
	}

	if passed {
		return github.NewGithubStatusPayload(label, r.targetURL(label), description, "success")
	}

	return github.NewGithubStatusPayload(label, r.targetURL(label), description, "failure")
}

func (r *BuildStatusReporter) githubPayloadFromFinishedEvent(event *build_event_stream.BuildEvent) *github.GithubStatusPayload {
	description := descriptionFromExitCodeName(event.GetFinished().ExitCode.Name)
	if event.GetFinished().OverallSuccess {
		return github.NewGithubStatusPayload(r.invocationLabel(), r.invocationURL(), description, "success")
	}

	return github.NewGithubStatusPayload(r.invocationLabel(), r.invocationURL(), description, "failure")
}

func (r *BuildStatusReporter) githubPayloadFromAbortedEvent(event *build_event_stream.BuildEvent) *github.GithubStatusPayload {
	label := r.labelFromEvent(event)
	if label != "" && r.inFlight[label] {
		return nil // We only report cancellations for in-flight targets/groups.
	}

	groupStatus := r.groupStatusFromLabel(label)
	if groupStatus != nil {
		groupStatus.numAborted++
	}

	if groupStatus != nil && groupStatus.numAborted == 1 {
		return github.NewGithubStatusPayload(groupStatus.name, r.groupURL(groupStatus.name), "Cancelled", "error")
	}

	return github.NewGithubStatusPayload(label, r.targetURL(label), "Cancelled", "error")
}

func (r *BuildStatusReporter) invocationLabel() string {
	return fmt.Sprintf("bazel %s %s", r.command, r.pattern)
}

func (r *BuildStatusReporter) invocationURL() string {
	return fmt.Sprintf("%s/invocation/%s", r.appURL(), r.invocationID)
}

func (r *BuildStatusReporter) groupURL(label string) string {
	return fmt.Sprintf("%s?targetFilter=%s", r.invocationURL(), label)
}

func (r *BuildStatusReporter) targetURL(label string) string {
	return fmt.Sprintf("%s?target=%s", r.invocationURL(), label)
}

func (r *BuildStatusReporter) appURL() string {
	return r.env.GetConfigurator().GetAppBuildBuddyURL()
}

func (r *BuildStatusReporter) initializeGroups(testGroups string) {
	r.groups = make(map[string]*GroupStatus)
	for _, group := range strings.Split(testGroups, ",") {
		r.groups[group] = &GroupStatus{
			name: group,
		}
	}
}

func (r *BuildStatusReporter) labelFromEvent(event *build_event_stream.BuildEvent) string {
	switch id := event.Id.Id.(type) {
	case *build_event_stream.BuildEventId_TargetConfigured:
		return id.TargetConfigured.Label
	case *build_event_stream.BuildEventId_TargetCompleted:
		return id.TargetCompleted.Label
	case *build_event_stream.BuildEventId_TestResult:
		return id.TestResult.Label
	case *build_event_stream.BuildEventId_TestSummary:
		return id.TestSummary.Label
	case *build_event_stream.BuildEventId_ActionCompleted:
		return id.ActionCompleted.Label
	case *build_event_stream.BuildEventId_ConfiguredLabel:
		return id.ConfiguredLabel.Label
	case *build_event_stream.BuildEventId_UnconfiguredLabel:
		return id.UnconfiguredLabel.Label
	}
	return ""
}

func (r *BuildStatusReporter) groupStatusFromLabel(label string) *GroupStatus {
	if label == "" {
		return nil
	}

	for group, status := range r.groups {
		if strings.HasPrefix(label, group) {
			return status
		}
	}
	return nil
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

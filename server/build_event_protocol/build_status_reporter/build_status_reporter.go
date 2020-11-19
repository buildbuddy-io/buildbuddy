package build_status_reporter

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_parser"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
)

type BuildStatusReporter struct {
	env                       environment.Env
	githubClient              *github.GithubClient
	shouldReportStatusPerTest bool
	buildEventAccumulator     *accumulator.BEValues
	payloads                  []*github.GithubStatusPayload
	groups                    map[string]*GroupStatus
	inFlight                  map[string]bool
}

type GroupStatus struct {
	name       string
	numTargets int
	numPassed  int
	numFailed  int
	numAborted int
}

func NewBuildStatusReporter(env environment.Env, buildEventAccumulator *accumulator.BEValues) *BuildStatusReporter {
	githubConfig := env.GetConfigurator().GetGithubConfig()
	shouldReportStatusPerTest := false

	if githubConfig != nil {
		shouldReportStatusPerTest = githubConfig.StatusPerTestTarget
	}

	return &BuildStatusReporter{
		env:                       env,
		githubClient:              github.NewGithubClient(env),
		shouldReportStatusPerTest: shouldReportStatusPerTest,
		buildEventAccumulator:     buildEventAccumulator,
		payloads:                  make([]*github.GithubStatusPayload, 0),
		inFlight:                  make(map[string]bool),
	}
}

func (r *BuildStatusReporter) ReportStatusForEvent(ctx context.Context, event *build_event_stream.BuildEvent) {
	var githubPayload *github.GithubStatusPayload

	switch event.Payload.(type) {
	case *build_event_stream.BuildEvent_WorkspaceStatus:
		githubPayload = r.githubPayloadFromWorkspaceStatusEvent(event)

	case *build_event_stream.BuildEvent_Configured:
		if r.shouldReportStatusPerTest {
			githubPayload = r.githubPayloadFromConfiguredEvent(event)
		}
	case *build_event_stream.BuildEvent_TestSummary:
		if r.shouldReportStatusPerTest {
			githubPayload = r.githubPayloadFromTestSummaryEvent(event)
		}
	case *build_event_stream.BuildEvent_Aborted:
		githubPayload = r.githubPayloadFromAbortedEvent(event)

	case *build_event_stream.BuildEvent_Finished:
		githubPayload = r.githubPayloadFromFinishedEvent(event)
	}

	role := r.buildEventAccumulator.Role()
	if githubPayload != nil && role == "CI" {
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
	if !r.buildEventAccumulator.WorkspaceIsLoaded() {
		return // If we haven't loaded the workspace, we can't flush payloads yet.
	}

	for _, payload := range r.payloads {
		if payload.State == "pending" {
			r.inFlight[payload.Context] = true
		} else {
			delete(r.inFlight, payload.Context)
		}

		// TODO(siggisim): Kick these into a queue or something (but maintain order).
		repoURL := r.buildEventAccumulator.RepoURL()
		commitSHA := r.buildEventAccumulator.CommitSHA()
		r.githubClient.CreateStatus(ctx, event_parser.ExtractUserRepoFromRepoUrl(repoURL), commitSHA, payload)
	}

	r.payloads = make([]*github.GithubStatusPayload, 0)
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
	if label != "" || !r.inFlight[label] {
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
	command := r.buildEventAccumulator.Command()
	pattern := r.buildEventAccumulator.Pattern()
	return fmt.Sprintf("bazel %s %s", command, pattern)
}

func (r *BuildStatusReporter) invocationURL() string {
	return fmt.Sprintf("%s/invocation/%s", r.appURL(), r.buildEventAccumulator.InvocationID())
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

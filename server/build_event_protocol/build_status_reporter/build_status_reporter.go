package build_status_reporter

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
)

type BuildStatusReporter struct {
	env                       environment.Env
	githubClient              *github.GithubClient
	shouldReportStatusPerTest bool
	statusNameSuffix          string
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
	statusNameSuffix := ""
	if githubConfig != nil {
		shouldReportStatusPerTest = githubConfig.StatusPerTestTarget
		statusNameSuffix = githubConfig.StatusNameSuffix
	}

	return &BuildStatusReporter{
		env:                       env,
		shouldReportStatusPerTest: shouldReportStatusPerTest,
		statusNameSuffix:          statusNameSuffix,
		buildEventAccumulator:     buildEventAccumulator,
		payloads:                  make([]*github.GithubStatusPayload, 0),
		inFlight:                  make(map[string]bool),
	}
}

func (r *BuildStatusReporter) initGHClient(ctx context.Context) *github.GithubClient {
	if workflowID := r.buildEventAccumulator.WorkflowID(); workflowID != "" {
		if db := r.env.GetDBHandle(); db != nil {
			workflow := &tables.Workflow{}
			if err := db.Raw(`SELECT * from Workflows WHERE workflow_id = ?`, workflowID).Take(workflow).Error; err == nil {
				return github.NewGithubClient(r.env, workflow.AccessToken)
			}
		}
	}
	return github.NewGithubClient(r.env, "")
}

func (r *BuildStatusReporter) ReportStatusForEvent(ctx context.Context, event *build_event_stream.BuildEvent) {
	if role := r.buildEventAccumulator.Role(); !(role == "CI" || role == "CI_RUNNER") {
		return
	}

	// TODO: support other providers than just GitHub
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

	if githubPayload != nil {
		r.payloads = append(r.payloads, githubPayload)
		r.flushPayloadsIfWorkspaceLoaded(ctx)
	}
}

func (r *BuildStatusReporter) ReportDisconnect(ctx context.Context) {
	for label := range r.inFlight {
		r.payloads = append(r.payloads, r.newGithubStatusPayload(label, r.invocationURL(), "Disconnected", github.ErrorState))
	}
	r.flushPayloadsIfWorkspaceLoaded(ctx)
}

func (r *BuildStatusReporter) flushPayloadsIfWorkspaceLoaded(ctx context.Context) {
	if !r.buildEventAccumulator.WorkspaceIsLoaded() {
		return // If we haven't loaded the workspace, we can't flush payloads yet.
	}
	if r.githubClient == nil {
		r.githubClient = r.initGHClient(ctx)
	}

	for _, payload := range r.payloads {
		if payload.State == github.PendingState {
			r.inFlight[payload.Context] = true
		} else {
			delete(r.inFlight, payload.Context)
		}

		// TODO(siggisim): Kick these into a queue or something (but maintain order).
		repoURL := r.buildEventAccumulator.RepoURL()
		ownerRepo, err := gitutil.OwnerRepoFromRepoURL(repoURL)
		if err != nil {
			log.Warningf("Failed to report GitHub status: %s", err)
			break
		}
		commitSHA := r.buildEventAccumulator.CommitSHA()
		r.githubClient.CreateStatus(ctx, ownerRepo, commitSHA, payload)
	}

	r.payloads = make([]*github.GithubStatusPayload, 0)
}

func (r *BuildStatusReporter) newGithubStatusPayload(name, URL, description string, state github.State) *github.GithubStatusPayload {
	if r.statusNameSuffix != "" {
		name = fmt.Sprintf("%s %s", name, r.statusNameSuffix)
	}
	return r.newGithubStatusPayload(name, URL, description, state)
}

func (r *BuildStatusReporter) githubPayloadFromWorkspaceStatusEvent(event *build_event_stream.BuildEvent) *github.GithubStatusPayload {
	return r.newGithubStatusPayload(r.invocationLabel(), r.invocationURL(), "Running...", github.PendingState)
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
		return r.newGithubStatusPayload(groupStatus.name, r.groupURL(groupStatus.name), "Running...", github.PendingState)
	}

	return r.newGithubStatusPayload(label, r.targetURL(label), "Running...", github.PendingState)
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
		return r.newGithubStatusPayload(groupStatus.name, r.groupURL(label), description, github.FailureState)
	}

	if groupStatus != nil && groupStatus.numPassed == groupStatus.numTargets {
		return r.newGithubStatusPayload(groupStatus.name, r.groupURL(label), description, github.SuccessState)
	}

	if passed {
		return r.newGithubStatusPayload(label, r.targetURL(label), description, github.SuccessState)
	}

	return r.newGithubStatusPayload(label, r.targetURL(label), description, github.FailureState)
}

func (r *BuildStatusReporter) githubPayloadFromFinishedEvent(event *build_event_stream.BuildEvent) *github.GithubStatusPayload {
	description := descriptionFromExitCodeName(event.GetFinished().ExitCode.Name)
	if event.GetFinished().OverallSuccess {
		return r.newGithubStatusPayload(r.invocationLabel(), r.invocationURL(), description, github.SuccessState)
	}

	return r.newGithubStatusPayload(r.invocationLabel(), r.invocationURL(), description, github.FailureState)
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
		return r.newGithubStatusPayload(groupStatus.name, r.groupURL(groupStatus.name), "Cancelled", github.ErrorState)
	}

	return r.newGithubStatusPayload(label, r.targetURL(label), "Cancelled", github.ErrorState)
}

func (r *BuildStatusReporter) invocationLabel() string {
	// If this is a synthetic action invocation as part of a workflow, return the
	// action name configured in /buildbuddy.yaml
	if r.buildEventAccumulator.ActionName() != "" {
		return r.buildEventAccumulator.ActionName()
	}

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
	if exitCodeName == "OK" {
		return exitCodeName
	}
	return strings.Title(strings.ToLower(strings.ReplaceAll(exitCodeName, "_", " ")))
}

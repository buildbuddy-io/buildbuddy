package build_status_reporter

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"

	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
)

var (
	statusPerTestTarget = flag.Bool("github.status_per_test_target", false, "If true, report status per test target. ** Enterprise only **")
)

type BuildStatusReporter struct {
	baseBBURL                  string
	env                        environment.Env
	githubClient               interfaces.GitHubStatusClient
	buildEventAccumulator      accumulator.Accumulator
	groups                     map[string]*GroupStatus
	inFlight                   map[string]bool
	payloads                   []*github.GithubStatusPayload
	shouldReportStatusPerTest  bool
	shouldReportCommitStatuses bool
	once                       sync.Once
}

type GroupStatus struct {
	name       string
	numTargets int
	numPassed  int
	numFailed  int
	numAborted int
}

func NewBuildStatusReporter(env environment.Env, buildEventAccumulator accumulator.Accumulator) *BuildStatusReporter {
	return &BuildStatusReporter{
		baseBBURL:                 build_buddy_url.String(),
		env:                       env,
		shouldReportStatusPerTest: *statusPerTestTarget,
		buildEventAccumulator:     buildEventAccumulator,
		payloads:                  make([]*github.GithubStatusPayload, 0),
		inFlight:                  make(map[string]bool),
	}
}

func (r *BuildStatusReporter) SetBaseBuildBuddyURL(url string) {
	r.baseBBURL = url
}

func (r *BuildStatusReporter) initGHClient(ctx context.Context) interfaces.GitHubStatusClient {
	accessToken := ""
	if workflowID := r.buildEventAccumulator.WorkflowID(); workflowID != "" {
		if dbh := r.env.GetDBHandle(); dbh != nil {
			workflow := &tables.Workflow{}
			if err := dbh.NewQuery(ctx, "build_status_reporter_get_workflow").Raw(
				`SELECT * from "Workflows" WHERE workflow_id = ?`, workflowID).Take(workflow); err == nil {
				accessToken = workflow.AccessToken
			}
		}
	}
	return r.env.GetGitHubStatusService().GetStatusClient(accessToken)
}

func (r *BuildStatusReporter) isStatusReportingEnabled(ctx context.Context, repoURL string) bool {
	r.once.Do(func() {
		dbh := r.env.GetDBHandle()
		if dbh == nil {
			return
		}

		userInfo, err := r.env.GetAuthenticator().AuthenticatedUser(ctx)
		if err != nil {
			log.CtxInfof(ctx, "Failed to report GitHub status, no authenticated user: %s", err)
			return
		}

		parsedRepo, err := gitutil.ParseGitHubRepoURL(repoURL)
		if err != nil {
			log.CtxInfof(ctx, "Failed to report GitHub status, invalid repo url %s: %s", repoURL, err)
			return
		}

		installation := &tables.GitHubAppInstallation{}
		err = dbh.NewQuery(ctx, "build_status_reporter_get_app_installation").Raw(
			`SELECT * from "GitHubAppInstallations" WHERE group_id = ? AND owner = ?`, userInfo.GetGroupID(), parsedRepo.Owner).Take(installation)
		if err == nil {
			r.shouldReportCommitStatuses = installation.ReportCommitStatusesForCIBuilds
			return
		} else if !db.IsRecordNotFound(err) {
			log.CtxWarningf(ctx, "Failed to report GitHub status for %s, failed to query GitHubAppInstallations: %s", repoURL, err)
			return
		}

		// If the user hasn't installed our GH app, check legacy methods for
		// enabling status reporting. Always report statuses for users that
		// onboarded through a legacy method, because status reporting was
		// automatically enabled for them.
		legacyWorkflow := &struct{ Count int64 }{}
		err = dbh.NewQuery(ctx, "build_status_reporter_get_workflow").Raw(
			`SELECT COUNT(*) as count from "Workflows" WHERE repo_url = ?`, repoURL).Take(legacyWorkflow)
		if err == nil && legacyWorkflow.Count > 0 {
			r.shouldReportCommitStatuses = true
			return
		} else if err != nil {
			log.CtxWarningf(ctx, "Failed to report GitHub status for %s, failed to query Workflows: %s", repoURL, err)
			return
		}

		groupWithLegacyToken := &struct{ Count int64 }{}
		err = dbh.NewQuery(ctx, "build_status_reporter_get_group").Raw(
			`SELECT COUNT(*) as count from "Groups" WHERE group_id = ? AND github_token <> "" AND github_token IS NOT NULL`, userInfo.GetGroupID()).Take(groupWithLegacyToken)
		if err == nil && groupWithLegacyToken.Count > 0 {
			r.shouldReportCommitStatuses = true
			return
		} else if err != nil {
			log.CtxWarningf(ctx, "Failed to report GitHub status for %s, failed to query Groups: %s", repoURL, err)
			return
		}
	})

	return r.shouldReportCommitStatuses
}

// ReportStatusForEvent reports a status to GitHub for the event if applicable.
// This function must be called after the accumulator has been updated with
// the given event data.
func (r *BuildStatusReporter) ReportStatusForEvent(ctx context.Context, event *build_event_stream.BuildEvent) {
	githubPayload := r.getStatusPayloadForEvent(event)
	if githubPayload != nil {
		r.payloads = append(r.payloads, githubPayload)
		r.flushPayloadsIfMetadataLoaded(ctx)
	}
}

func (r *BuildStatusReporter) getStatusPayloadForEvent(event *build_event_stream.BuildEvent) *github.GithubStatusPayload {
	// Don't attempt to create a status payload if build metadata is not loaded.
	if !r.buildEventAccumulator.MetadataIsLoaded() {
		return nil
	}

	// Only report GitHub statuses for CI or CI_RUNNER roles.
	if role := r.buildEventAccumulator.Invocation().GetRole(); !(role == "CI" || role == "CI_RUNNER") {
		return nil
	}

	// If this is a metadata event, then it must be the last metadata event,
	// because we just checked that metadata is fully loaded, which only becomes
	// true once the last metadata event has been handled.
	//
	// Once we observe the last metadata event, we can report the initial
	// status with basic metadata.
	if accumulator.IsMetadataEvent(event.GetId()) {
		return r.githubPayloadForBuildMetadata()
	}

	switch event.Payload.(type) {
	case *build_event_stream.BuildEvent_Configured:
		if r.shouldReportStatusPerTest {
			return r.githubPayloadFromConfiguredEvent(event)
		}
	case *build_event_stream.BuildEvent_TestSummary:
		if r.shouldReportStatusPerTest {
			return r.githubPayloadFromTestSummaryEvent(event)
		}
	case *build_event_stream.BuildEvent_Aborted:
		return r.githubPayloadFromAbortedEvent(event)
	case *build_event_stream.BuildEvent_Finished:
		return r.githubPayloadFromFinishedEvent(event)
	}

	return nil
}

func (r *BuildStatusReporter) ReportDisconnect(ctx context.Context) {
	for label := range r.inFlight {
		r.payloads = append(r.payloads, github.NewGithubStatusPayload(label, r.invocationURL(), "Disconnected", github.ErrorState))
	}
	r.flushPayloadsIfMetadataLoaded(ctx)
}

func (r *BuildStatusReporter) flushPayloadsIfMetadataLoaded(ctx context.Context) {
	// TODO: support other providers than just GitHub
	if r.env.GetGitHubStatusService() == nil {
		return
	}

	// Don't report statuses if we don't yet have the metadata, it's explicitly
	// disabled in build metadata, or it's not enabled for this repo.
	if !r.buildEventAccumulator.MetadataIsLoaded() ||
		r.buildEventAccumulator.DisableCommitStatusReporting() ||
		!r.isStatusReportingEnabled(ctx, r.buildEventAccumulator.Invocation().GetRepoUrl()) {
		return
	}

	if r.githubClient == nil {
		r.githubClient = r.initGHClient(ctx)
	}

	for _, payload := range r.payloads {
		if github.State(payload.GetState()) == github.PendingState {
			r.inFlight[payload.GetContext()] = true
		} else {
			delete(r.inFlight, payload.GetContext())
		}

		// TODO(siggisim): Kick these into a queue or something (but maintain order).
		repoURL := r.buildEventAccumulator.Invocation().GetRepoUrl()
		ownerRepo, err := gitutil.OwnerRepoFromRepoURL(repoURL)
		if err != nil {
			log.CtxWarningf(ctx, "Failed to report GitHub status: %s", err)
			break
		}
		commitSHA := r.buildEventAccumulator.Invocation().GetCommitSha()
		if ownerRepo != "" && commitSHA != "" {
			err = r.githubClient.CreateStatus(ctx, ownerRepo, commitSHA, payload)
			if err != nil {
				// Note: using info-level log since this is often due to client
				// misconfiguration (e.g. user doesn't have BB GitHub app
				// installed).
				log.CtxInfof(ctx, "Failed to report GitHub status for %q @ %q: %s", ownerRepo, commitSHA, err)
				continue
			}
		} else {
			log.CtxDebugf(ctx, "Not reporting GitHub status (missing REPO_URL or COMMIT_SHA metadata)")
		}
	}

	r.payloads = make([]*github.GithubStatusPayload, 0)
}

func (r *BuildStatusReporter) githubPayloadForBuildMetadata() *github.GithubStatusPayload {
	return github.NewGithubStatusPayload(r.invocationLabel(), r.invocationURL(), "Running...", github.PendingState)
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
		return github.NewGithubStatusPayload(groupStatus.name, r.groupURL(groupStatus.name), "Running...", github.PendingState)
	}

	return github.NewGithubStatusPayload(label, r.targetURL(label), "Running...", github.PendingState)
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
		return github.NewGithubStatusPayload(groupStatus.name, r.groupURL(label), description, github.FailureState)
	}

	if groupStatus != nil && groupStatus.numPassed == groupStatus.numTargets {
		return github.NewGithubStatusPayload(groupStatus.name, r.groupURL(label), description, github.SuccessState)
	}

	if passed {
		return github.NewGithubStatusPayload(label, r.targetURL(label), description, github.SuccessState)
	}

	return github.NewGithubStatusPayload(label, r.targetURL(label), description, github.FailureState)
}

func (r *BuildStatusReporter) githubPayloadFromFinishedEvent(event *build_event_stream.BuildEvent) *github.GithubStatusPayload {
	finished := event.GetFinished()
	description := descriptionFromExitCodeName(finished.GetExitCode().GetName())
	startTime := r.buildEventAccumulator.StartTime()
	endTime := timeutil.GetTimeWithFallback(finished.GetFinishTime(), finished.GetFinishTimeMillis())
	if !startTime.IsZero() && endTime.After(startTime) {
		description = fmt.Sprintf("%s in %s", description, timeutil.ShortFormatDuration(endTime.Sub(startTime)))
	}
	if finished.GetExitCode().GetCode() == 0 || finished.GetExitCode().GetName() == "NO_TESTS_FOUND" {
		return github.NewGithubStatusPayload(r.invocationLabel(), r.invocationURL(), description, github.SuccessState)
	}

	return github.NewGithubStatusPayload(r.invocationLabel(), r.invocationURL(), description, github.FailureState)
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
		return github.NewGithubStatusPayload(groupStatus.name, r.groupURL(groupStatus.name), "Cancelled", github.ErrorState)
	}

	return github.NewGithubStatusPayload(label, r.targetURL(label), "Cancelled", github.ErrorState)
}

func (r *BuildStatusReporter) invocationLabel() string {
	// If this is a synthetic action invocation as part of a workflow, return the
	// action name configured in /buildbuddy.yaml
	if r.buildEventAccumulator.ActionName() != "" {
		return r.buildEventAccumulator.ActionName()
	}

	command := r.buildEventAccumulator.Invocation().GetCommand()
	pattern := r.buildEventAccumulator.Pattern()
	return fmt.Sprintf("bazel %s %s", command, pattern)
}

func (r *BuildStatusReporter) invocationID() string {
	return r.buildEventAccumulator.Invocation().GetInvocationId()
}

func (r *BuildStatusReporter) invocationURL() string {
	return r.baseBBURL + "/invocation/" + r.invocationID()
}

func (r *BuildStatusReporter) groupURL(label string) string {
	return r.baseBBURL + "/invocation/" + r.invocationID() + "?targetFilter=" + label
}

func (r *BuildStatusReporter) targetURL(label string) string {
	return r.baseBBURL + "/invocation/" + r.invocationID() + "?target=" + label
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
		return "Successful"
	}
	return strings.Title(strings.ToLower(strings.ReplaceAll(exitCodeName, "_", " ")))
}

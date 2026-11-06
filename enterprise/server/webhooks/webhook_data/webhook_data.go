package webhook_data

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

var (
	// EventName holds canonical webhook event name constants.
	EventName struct {
		Push               string
		PullRequest        string
		ManualDispatch     string
		ScheduledDispatch  string
		PullRequestComment string
	}
)

func init() {
	EventName.Push = "push"
	EventName.PullRequest = "pull_request"
	EventName.ManualDispatch = "manual_dispatch"
	EventName.ScheduledDispatch = "scheduled"
	EventName.PullRequestComment = "pull_request_comment"
}

func DebugString(wd *interfaces.WebhookData) string {
	pushedRef := fmt.Sprintf("%s@%s:%s", wd.PushedRepoURL, wd.PushedBranch, wd.SHA)
	if wd.PushedTag != "" {
		pushedRef = fmt.Sprintf("%s@%s", wd.PushedRepoURL, wd.PushedTag)
	}
	debugString := fmt.Sprintf(
		"event=%s, pushed=%s, target=%s@%s (public=%t, default_branch=%s), pr #%d (author=%s, approver=%s)",
		wd.EventName,
		pushedRef,
		wd.TargetRepoURL, wd.TargetBranch, wd.IsTargetRepoPublic, wd.TargetRepoDefaultBranch,
		wd.PullRequestNumber, wd.PullRequestAuthor, wd.PullRequestApprover)
	if wd.EventName == EventName.PullRequest {
		debugString += fmt.Sprintf(
			", action=%q, base_sha=%q, before=%q, after=%q, previous_base_ref=%q, previous_base_sha=%q",
			wd.PullRequestAction, wd.PullRequestBaseSHA, wd.PullRequestBeforeSHA, wd.PullRequestAfterSHA,
			wd.PullRequestPreviousBaseRef, wd.PullRequestPreviousBaseSHA)
	}
	return debugString
}

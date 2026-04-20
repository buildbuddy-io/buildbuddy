package webhook_data

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

var (
	// EventName holds canonical webhook event name constants.
	EventName struct {
		Push           string
		PullRequest    string
		ManualDispatch string
	}
)

func init() {
	EventName.Push = "push"
	EventName.PullRequest = "pull_request"
	EventName.ManualDispatch = "manual_dispatch"
}

func DebugString(wd *interfaces.WebhookData) string {
	pushedRef := fmt.Sprintf("%s@%s:%s", wd.PushedRepoURL, wd.PushedBranch, wd.SHA)
	if wd.PushedTag != "" {
		pushedRef = fmt.Sprintf("%s@%s", wd.PushedRepoURL, wd.PushedTag)
	}
	return fmt.Sprintf(
		"event=%s, pushed=%s, target=%s@%s (public=%t, default_branch=%s), pr #%d (author=%s, approver=%s)",
		wd.EventName,
		pushedRef,
		wd.TargetRepoURL, wd.TargetBranch, wd.IsTargetRepoPublic, wd.TargetRepoDefaultBranch,
		wd.PullRequestNumber, wd.PullRequestAuthor, wd.PullRequestApprover)
}

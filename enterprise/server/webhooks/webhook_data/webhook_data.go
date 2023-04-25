package webhook_data

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

var (
	// EventName holds canonical webhook event name constants.
	EventName struct {
		Push        string
		PullRequest string
	}
)

func init() {
	EventName.Push = "push"
	EventName.PullRequest = "pull_request"
}

func DebugString(wd *interfaces.WebhookData) string {
	return fmt.Sprintf(
		"event=%s, pushed=%s@%s:%s, target=%s@%s (public=%t, default_branch=%s), pr_author=%s, pr_approver=%s",
		wd.EventName,
		wd.PushedRepoURL, wd.PushedBranch, wd.SHA,
		wd.TargetRepoURL, wd.TargetBranch, wd.IsTargetRepoPublic, wd.TargetRepoDefaultBranch,
		wd.PullRequestAuthor, wd.PullRequestApprover)
}

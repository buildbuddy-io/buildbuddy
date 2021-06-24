package webhook_data

import (
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

// IsTrusted returns whether the event came from a trusted actor.
func IsTrusted(wd *interfaces.WebhookData) bool {
	return wd.IsRepoPrivate
}

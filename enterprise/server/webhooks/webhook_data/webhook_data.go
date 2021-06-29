package webhook_data

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

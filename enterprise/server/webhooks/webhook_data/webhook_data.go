package webhook_data

// EventSource identifies the source associated with the webhook event.
type EventSource string

const (
	GitHub EventSource = "github.com"
)

// WebhookData represents the data extracted from a Webhook event.
type WebhookData struct {
	// Source identifies the source of the event.
	Source EventSource

	// RepoURL points to the canonical repo URL containing the sources needed for the
	// workflow.
	//
	// This will be different from the workflow repo if the workflow is run on a forked
	// repo as part of a pull request.
	RepoURL string
	// SHA of the commit to be checked out.
	SHA string

	// Event-specific fields. Only one of the following are set, depending on the
	// event that was received:

	PullRequest *PullRequestEvent
	Push        *PushEvent
}

type PullRequestEvent struct {
	// BaseBranch is the branch in the repo into which the PR is being merged.
	BaseBranch string
}

type PushEvent struct {
	// Branch that was pushed.
	Branch string
}

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

// WebhookData represents the data extracted from a Webhook event.
type WebhookData struct {
	// EventName is the canonical event name that this data was created from.
	EventName string
	// TargetBranch is the branch associated with the event that determines whether
	// actions should be triggered. For push events this is the branch that was
	// pushed to. For pull_request events this is the base branch into which the PR
	// branch is being merged.
	TargetBranch string
	// RepoURL points to the canonical repo URL containing the sources needed for the
	// workflow.
	//
	// This will be different from the workflow repo if the workflow is run on a forked
	// repo as part of a pull request.
	RepoURL string
	// IsRepoPrivate returns whether the repo is private.
	IsRepoPrivate bool
	// SHA of the commit to be checked out.
	SHA string
}

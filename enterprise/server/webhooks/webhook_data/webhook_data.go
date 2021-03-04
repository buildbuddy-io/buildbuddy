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
	// PushedBranch is the name of the branch in the source repo that triggered the
	// event when pushed. Note that for forks, the branch here references the branch
	// name in the forked repository, and the TargetBranch references the branch in
	// the main repository into which the PushedBranch will be merged.
	//
	// Some examples:
	//
	// Push main branch (e.g. `git push main` or merge a PR into main):
	// - RepoURL: "https://github.com/example/example.git"
	// - PushedBranch: "main" // in "example/example" repo
	// - TargetBranch: "main" // in "example/example" repo
	//
	// Push to a PR branch within the mainline repo:
	// - RepoURL: "https://github.com/example/example.git"
	// - PushedBranch: "foo-feature" // in "example/example" repo
	// - TargetBranch: "main"        // in "example/example" repo
	//
	// Push to a PR branch within a forked repo:
	// - RepoURL: "https://github.com/some-user/example-fork.git"
	// - PushedBranch: "bar-feature" // in "some-user/example-fork" repo
	// - TargetBranch: "main"        // in "example/example" repo
	PushedBranch string
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

// IsTrusted returns whether the event came from a trusted actor.
func (wd *WebhookData) IsTrusted() bool {
	return wd.IsRepoPrivate
}

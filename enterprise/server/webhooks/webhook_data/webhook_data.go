package webhook_data

// WebhookData represents the data required to run a workflow.
type WebhookData struct {
	// Repo contains the repo that needs to be checked out to run the workflow.
	// This may not be the same as the repo associated with the workflow in the
	// case where the repo is a fork.
	Repo string
	// SHA of the commit to be checked out.
	SHA string
}

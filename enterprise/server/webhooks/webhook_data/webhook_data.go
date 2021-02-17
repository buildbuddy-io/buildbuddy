package webhook_data

// WebhookData represents the data required to run a workflow.
type WebhookData struct {
	// SHA of the commit to be checked out.
	SHA string
}

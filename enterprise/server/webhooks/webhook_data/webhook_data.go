package webhook_data

var (
	// EventName holds canonical webhook event name constants.
	EventName = struct {
		Push, PullRequest, CheckRun     string
		CheckSuite, CheckRunRerequested string
	}{
		Push:        "push",
		PullRequest: "pull_request",
		CheckRun:    "check_run",

		// Note: These are not handled by the user. The app creates a check run
		// in response to a check suite, and the user handles those events by
		// creating the run. The same thing (roughly) happens for check run
		// re-requested, except only a single check run is executed instead of
		// the configured check runs.
		CheckSuite:          "check_suite",
		CheckRunRerequested: "check_run_rerequested",
	}
)

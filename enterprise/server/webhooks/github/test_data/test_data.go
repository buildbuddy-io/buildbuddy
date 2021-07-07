package test_data

import _ "embed"

//go:embed push_event.json
var PushEvent []byte

//go:embed pull_request_event.json
var PullRequestEvent []byte

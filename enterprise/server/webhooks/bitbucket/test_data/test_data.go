package test_data

import _ "embed"

//go:embed push_event.txt
var PushEvent []byte

//go:embed pull_request_event.txt
var PullRequestEvent []byte

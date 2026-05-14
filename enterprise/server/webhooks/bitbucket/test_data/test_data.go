package test_data

import _ "embed"

//go:embed push_event.json
var PushEvent []byte

//go:embed push_tag_event.json
var PushTagEvent []byte

//go:embed delete_branch_event.json
var DeleteBranchEvent []byte

//go:embed pull_request_event.json
var PullRequestEvent []byte

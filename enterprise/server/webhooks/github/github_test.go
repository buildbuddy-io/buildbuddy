package github_test

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github/test_data"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/stretchr/testify/assert"
)

func webhookRequest(t *testing.T, eventType string, payload []byte) *http.Request {
	req, err := http.NewRequest("POST", "https://buildbuddy.io/webhooks/foo", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("X-Github-Event", eventType)
	req.Header.Add("Content-Type", "application/json")
	return req
}

func TestParseRequest_ValidPushEvent_Success(t *testing.T) {
	req := webhookRequest(t, "push", test_data.PushEvent)

	data, err := github.NewProvider().ParseWebhookData(req)

	assert.NoError(t, err)
	assert.Equal(t, &interfaces.WebhookData{
		EventName:     "push",
		PushedRepoURL: "https://github.com/test/hello_bb_ci.git",
		PushedBranch:  "main",
		SHA:           "258044d28288d5f6f1c5928b0e22580296fec666",
		TargetRepoURL: "https://github.com/test/hello_bb_ci.git",
		TargetBranch:  "main",
	}, data)
}

func TestParseRequest_ValidPullRequestEvent_Success(t *testing.T) {
	req := webhookRequest(t, "pull_request", test_data.PullRequestEvent)

	data, err := github.NewProvider().ParseWebhookData(req)

	assert.NoError(t, err)
	assert.Equal(t, &interfaces.WebhookData{
		EventName:         "pull_request",
		PushedRepoURL:     "https://github.com/test/hello_bb_ci.git",
		PushedBranch:      "pr-1613157046",
		SHA:               "21006e203e433034cd4d82859d28d3bc1dbdf9f7",
		TargetRepoURL:     "https://github.com/test/hello_bb_ci.git",
		TargetBranch:      "main",
		PullRequestAuthor: "test",
	}, data)
}

func TestParseRequest_ValidPullRequestReviewEvent_Success(t *testing.T) {
	req := webhookRequest(t, "pull_request_review", test_data.PullRequestApprovedReviewEvent)

	data, err := github.NewProvider().ParseWebhookData(req)

	assert.NoError(t, err)
	assert.Equal(t, &interfaces.WebhookData{
		EventName:           "pull_request",
		PushedRepoURL:       "https://github.com/test2/bb-workflows-test.git",
		PushedBranch:        "pr-test",
		SHA:                 "7d27db5443e48541a49693422c3b30fe6e8e3e9f",
		TargetRepoURL:       "https://github.com/test/bb-workflows-test.git",
		IsTargetRepoPublic:  true,
		TargetBranch:        "main",
		PullRequestAuthor:   "test2",
		PullRequestApprover: "test",
	}, data)
}

func TestParseRequest_InvalidEvent_Error(t *testing.T) {
	req := webhookRequest(t, "push", []byte{})

	data, err := github.NewProvider().ParseWebhookData(req)

	assert.Error(t, err)
	assert.Nil(t, data)
}

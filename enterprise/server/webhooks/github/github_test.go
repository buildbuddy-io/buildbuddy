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
		EventName:    "push",
		PushedBranch: "main",
		TargetBranch: "main",
		RepoURL:      "https://github.com/test/hello_bb_ci.git",
		IsTrusted:    true,
		SHA:          "258044d28288d5f6f1c5928b0e22580296fec666",
	}, data)
}

func TestParseRequest_ValidPullRequestEvent_Success(t *testing.T) {
	req := webhookRequest(t, "pull_request", test_data.PullRequestEvent)

	data, err := github.NewProvider().ParseWebhookData(req)

	assert.NoError(t, err)
	assert.Equal(t, &interfaces.WebhookData{
		EventName:    "pull_request",
		PushedBranch: "pr-1613157046",
		TargetBranch: "main",
		RepoURL:      "https://github.com/test/hello_bb_ci.git",
		IsTrusted:    true,
		SHA:          "21006e203e433034cd4d82859d28d3bc1dbdf9f7",
	}, data)
}

func TestParseRequest_InvalidEvent_Error(t *testing.T) {
	req := webhookRequest(t, "push", []byte{})

	data, err := github.NewProvider().ParseWebhookData(req)

	assert.Error(t, err)
	assert.Nil(t, data)
}

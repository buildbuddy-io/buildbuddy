package github_test

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github/test_data"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"github.com/stretchr/testify/assert"
)

func webhookRequest(t *testing.T, eventType string, payload []byte) *http.Request {
	req, err := http.NewRequest("POST", "https://buildbuddy.io/webhooks/foo", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("X-Github-Event", eventType)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	return req
}

func TestParseRequest_ValidPushEvent_Success(t *testing.T) {
	req := webhookRequest(t, "push", test_data.PushEvent)

	data, err := github.ParseRequest(req)

	assert.NoError(t, err)
	assert.Equal(t, &webhook_data.WebhookData{
		EventName:     "push",
		PushedBranch:  "main",
		TargetBranch:  "main",
		RepoURL:       "https://github.com/test/hello_bb_ci.git",
		IsRepoPrivate: true,
		SHA:           "258044d28288d5f6f1c5928b0e22580296fec666",
	}, data)
}

func TestParseRequest_ValidPullRequestEvent_Success(t *testing.T) {
	req := webhookRequest(t, "pull_request", test_data.PullRequestEvent)

	data, err := github.ParseRequest(req)

	assert.NoError(t, err)
	assert.Equal(t, &webhook_data.WebhookData{
		EventName:     "pull_request",
		PushedBranch:  "pr-1613157046",
		TargetBranch:  "main",
		RepoURL:       "https://github.com/test/hello_bb_ci.git",
		IsRepoPrivate: true,
		SHA:           "21006e203e433034cd4d82859d28d3bc1dbdf9f7",
	}, data)
}

func TestParseRequest_InvalidEvent_Error(t *testing.T) {
	req := webhookRequest(t, "push", []byte{})

	data, err := github.ParseRequest(req)

	assert.Error(t, err)
	assert.Nil(t, data)
}

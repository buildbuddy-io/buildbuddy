package bitbucket_test

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/bitbucket"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/bitbucket/test_data"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/stretchr/testify/assert"
)

func webhookRequest(t *testing.T, eventType string, payload []byte) *http.Request {
	req, err := http.NewRequest("POST", "https://buildbuddy.io/webhooks/foo", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Add("X-Event-Key", eventType)
	req.Header.Add("User-Agent", "Bitbucket-Webhooks/2.0")
	req.Header.Add("Content-Type", "application/json")
	return req
}

func TestParseRequest_ValidPushEvent_Success(t *testing.T) {
	req := webhookRequest(t, "repo:push", test_data.PushEvent)

	data, err := bitbucket.NewProvider().ParseWebhookData(req)

	assert.NoError(t, err)
	assert.Equal(t, &interfaces.WebhookData{
		EventName:     "push",
		PushedRepoURL: "https://bitbucket.org/buildbuddy/buildbuddy-ci-playground",
		PushedBranch:  "main",
		SHA:           "f3307f36e35d1820c78b642cc8dfec6bf28a6230",
		TargetRepoURL: "https://bitbucket.org/buildbuddy/buildbuddy-ci-playground",
		TargetBranch:  "main",
	}, data)
}

func TestParseRequest_ValidPullRequestEvent_Success(t *testing.T) {
	req := webhookRequest(t, "pullrequest:updated", test_data.PullRequestEvent)

	data, err := bitbucket.NewProvider().ParseWebhookData(req)

	assert.NoError(t, err)
	assert.Equal(t, &interfaces.WebhookData{
		EventName:     "pull_request",
		PushedRepoURL: "https://bitbucket.org/buildbuddy/buildbuddy-ci-playground",
		PushedBranch:  "test-1614450472",
		SHA:           "a4822151d5d2",
		TargetRepoURL: "https://bitbucket.org/buildbuddy/buildbuddy-ci-playground",
		TargetBranch:  "main",
	}, data)
}

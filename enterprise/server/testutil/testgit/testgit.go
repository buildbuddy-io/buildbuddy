package testgit

import (
	"context"
	"net/http"
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	// FakeWebhookID is the fake webhook ID returned by the provider when
	// creating a webhook.
	FakeWebhookID = "fake-webhook-id"
)

// FakeProvider implements the git provider interface for tests.
type FakeProvider struct {
	RegisteredWebhookURL  string
	UnregisteredWebhookID string
}

func NewFakeProvider() *FakeProvider {
	return &FakeProvider{}
}

func (p *FakeProvider) MatchRepoURL(u *url.URL) bool {
	return true
}
func (p *FakeProvider) MatchWebhookRequest(req *http.Request) bool {
	return true
}
func (p *FakeProvider) IsRepoPrivate(ctx context.Context, accessToken, repoURL string) (bool, error) {
	return false, nil
}
func (p *FakeProvider) ParseWebhookData(req *http.Request) (*interfaces.WebhookData, error) {
	return nil, status.UnimplementedError("Not implemented")
}
func (p *FakeProvider) RegisterWebhook(ctx context.Context, accessToken, repoURL, webhookURL string) (string, error) {
	p.RegisteredWebhookURL = webhookURL
	return FakeWebhookID, nil
}
func (p *FakeProvider) UnregisterWebhook(ctx context.Context, accessToken, repoURL, webhookID string) error {
	p.UnregisteredWebhookID = webhookID
	return nil
}

package slack

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateWebhookURL(t *testing.T) {
	testCases := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{name: "Slack", url: "https://hooks.slack.com/services/T000/B000/XXX"},
		{name: "Slack host is case insensitive", url: "https://HOOKS.SLACK.COM/services/T000/B000/XXX"},
		{name: "HTTP", url: "http://hooks.slack.com/services/T000/B000/XXX", wantErr: true},
		{name: "Slack subdomain", url: "https://example.hooks.slack.com/services/T000/B000/XXX", wantErr: true},
		{name: "Slack suffix", url: "https://hooks.slack.com.example.com/services/T000/B000/XXX", wantErr: true},
		{name: "Custom port", url: "https://hooks.slack.com:8443/services/T000/B000/XXX", wantErr: true},
		{name: "Private address", url: "https://127.0.0.1/services/T000/B000/XXX", wantErr: true},
		{name: "User info", url: "https://user@hooks.slack.com/services/T000/B000/XXX", wantErr: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateWebhookURL(testCase.url)
			if testCase.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewClientDisablesRedirects(t *testing.T) {
	client := NewClient()
	req, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)
	require.ErrorIs(t, client.httpClient.CheckRedirect(req, nil), http.ErrUseLastResponse)
}

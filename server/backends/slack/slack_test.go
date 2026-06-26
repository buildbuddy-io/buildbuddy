package slack

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestRegister_ConfiguresFeedbackReporter(t *testing.T) {
	var payload Payload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	flags.Set(t, "integrations.slack.feedback_webhook_url", srv.URL)

	env := real_environment.NewRealEnv(nil)
	authenticator := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"developer-key": &testauth.TestUser{
			APIKeyID:           "AK1",
			UserID:             "US1",
			UserEmail:          "developer@example.com",
			GroupID:            "GR1",
			GroupURLIdentifier: "acme",
			GroupName:          "Acme",
			AllowedGroups:      []string{"GR1"},
		},
	})
	env.SetAuthenticator(authenticator)

	// Register installs a SlackWebhook as the generic feedback reporter when
	// the feedback webhook URL is configured.
	require.NoError(t, Register(env))
	_, ok := env.GetFeedbackReporter().(*SlackWebhook)
	require.True(t, ok)

	ctx := authenticator.AuthContextFromAPIKey(context.Background(), "developer-key")
	require.NoError(t, env.GetFeedbackReporter().ReportFeedback(ctx, "The MCP server should explain auth failures."))

	require.Equal(t, "[MCP server] [acme] [developer@example.com]", payload.Text)
	require.Len(t, payload.Attachments, 1)
	require.Equal(t, "The MCP server should explain auth failures.", *payload.Attachments[0].Text)
	fields := map[string]Field{}
	for _, field := range payload.Attachments[0].Fields {
		fields[field.Title] = *field
	}
	require.Equal(t, Field{Title: "Group ID", Value: "GR1", Short: true}, fields["Group ID"])
	require.Equal(t, Field{Title: "User ID", Value: "US1", Short: true}, fields["User ID"])
	require.Equal(t, Field{Title: "API key ID", Value: "AK1", Short: true}, fields["API key ID"])
}

func TestFeedbackMessagePrefix(t *testing.T) {
	for _, testCase := range []struct {
		name string
		user interfaces.UserInfo
		want string
	}{
		{
			name: "group URL identifier and email",
			user: &testauth.TestUser{
				GroupID:            "GR1",
				GroupURLIdentifier: "acme",
				GroupName:          "Acme",
				UserEmail:          "developer@example.com",
			},
			want: "[MCP server] [acme] [developer@example.com]",
		},
		{
			name: "group name and unknown email",
			user: &testauth.TestUser{
				GroupID:   "GR1",
				GroupName: "Acme",
			},
			want: "[MCP server] [Acme]",
		},
		{
			name: "group ID fallback",
			user: &testauth.TestUser{
				GroupID: "GR1",
			},
			want: "[MCP server] [GR1]",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.want, feedbackMessagePrefix(testCase.user))
		})
	}
}

package email_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/email"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	sg "github.com/sendgrid/sendgrid-go"
)

type capturedRequest struct {
	method      string
	path        string
	auth        string
	contentType string
	payload     sendgridPayload
}

type requestRecorder struct {
	t            testing.TB
	requests     []capturedRequest
	statusCode   int
	responseBody string
}

type payloadAddress struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type sendgridPayload struct {
	From             payloadAddress           `json:"from"`
	Subject          string                   `json:"subject"`
	Personalizations []payloadPersonalization `json:"personalizations"`
	Content          []payloadContent         `json:"content"`
}

type payloadPersonalization struct {
	To []payloadAddress `json:"to"`
}

type payloadContent struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func TestSend(t *testing.T) {
	validMessage := &email.Message{
		To:      email.Address{Name: "Dev", Email: "dev@example.com"},
		Subject: "Build complete",
		Body:    "<p>The build completed.</p>",
	}
	validPayload := sendgridPayload{
		From:    payloadAddress{Name: "BuildBuddy", Email: "notifications@buildbuddy.io"},
		Subject: "Build complete",
		Personalizations: []payloadPersonalization{
			{
				To: []payloadAddress{
					{Name: "Dev", Email: "dev@example.com"},
				},
			},
		},
		Content: []payloadContent{
			{Type: "text/html", Value: "<p>The build completed.</p>"},
		},
	}
	validRequest := capturedRequest{
		method:      http.MethodPost,
		path:        "/v3/mail/send",
		auth:        "Bearer test-api-key",
		contentType: "application/json",
		payload:     validPayload,
	}
	messageSenderPayload := validPayload
	messageSenderPayload.From = payloadAddress{Name: "Usage Alerts", Email: "usage@buildbuddy.io"}
	messageSenderRequest := validRequest
	messageSenderRequest.payload = messageSenderPayload

	for _, testCase := range []struct {
		name         string
		message      *email.Message
		flags        map[string]any
		statusCode   int
		responseBody string
		wantErr      string
		wantRequests []capturedRequest
	}{
		{
			name:         "posts basic payload",
			message:      validMessage,
			statusCode:   http.StatusAccepted,
			wantRequests: []capturedRequest{validRequest},
		},
		{
			name: "uses message sender",
			message: &email.Message{
				From:    email.Address{Name: "Usage Alerts", Email: "usage@buildbuddy.io"},
				To:      email.Address{Name: "Dev", Email: "dev@example.com"},
				Subject: "Build complete",
				Body:    "<p>The build completed.</p>",
			},
			statusCode:   http.StatusAccepted,
			wantRequests: []capturedRequest{messageSenderRequest},
		},
		{
			name:         "returns SendGrid failure",
			message:      validMessage,
			statusCode:   http.StatusInternalServerError,
			responseBody: "nope",
			wantErr:      "SendGrid returned HTTP 500: nope",
			wantRequests: []capturedRequest{validRequest},
		},
		{
			name:       "requires message",
			statusCode: http.StatusAccepted,
			wantErr:    "message is required",
		},
		{
			name:    "requires API key",
			message: validMessage,
			flags: map[string]any{
				"notifications.email.sendgrid.api_key": "",
			},
			statusCode: http.StatusAccepted,
			wantErr:    "SendGrid API key is required",
		},
		{
			name:    "requires sender",
			message: validMessage,
			flags: map[string]any{
				"notifications.email.default_from_address": email.Address{Name: "BuildBuddy"},
			},
			statusCode: http.StatusAccepted,
			wantErr:    "SendGrid sender email address is required",
		},
		{
			name: "requires recipient",
			message: &email.Message{
				Subject: "Build complete",
				Body:    "<p>The build completed.</p>",
			},
			statusCode: http.StatusAccepted,
			wantErr:    "recipient is required",
		},
		{
			name: "requires subject",
			message: &email.Message{
				To:   email.Address{Name: "Dev", Email: "dev@example.com"},
				Body: "<p>The build completed.</p>",
			},
			statusCode: http.StatusAccepted,
			wantErr:    "subject is required",
		},
		{
			name: "requires body",
			message: &email.Message{
				To:      email.Address{Name: "Dev", Email: "dev@example.com"},
				Subject: "Build complete",
			},
			statusCode: http.StatusAccepted,
			wantErr:    "body is required",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			configureSendGridFlags(t)
			for name, value := range testCase.flags {
				flags.Set(t, name, value)
			}
			recorder := recordSendGridRequests(t, testCase.statusCode, testCase.responseBody)

			// Send the email for this scenario.
			err := email.Send(context.Background(), testCase.message)

			// Verify whether the scenario succeeds or returns the expected validation/provider failure.
			if testCase.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, testCase.wantErr)
			}
			require.Equal(t, testCase.wantRequests, recorder.requests)
		})
	}
}

func configureSendGridFlags(t testing.TB) {
	flags.Set(t, "notifications.email.sendgrid.api_key", "test-api-key")
	flags.Set(t, "notifications.email.default_from_address", email.Address{
		Name:  "BuildBuddy",
		Email: "notifications@buildbuddy.io",
	})
}

func recordSendGridRequests(t testing.TB, statusCode int, responseBody string) *requestRecorder {
	recorder := &requestRecorder{
		t:            t,
		statusCode:   statusCode,
		responseBody: responseBody,
	}
	client := httpclient.New(nil, "sendgrid_test")
	client.Transport = recorder

	oldHTTPClient := sg.DefaultClient.HTTPClient
	sg.DefaultClient.HTTPClient = client
	t.Cleanup(func() {
		sg.DefaultClient.HTTPClient = oldHTTPClient
	})
	return recorder
}

func (r *requestRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	require.NotNil(r.t, req.Body)
	defer req.Body.Close()

	body, err := io.ReadAll(req.Body)
	require.NoError(r.t, err)
	var payload sendgridPayload
	require.NoError(r.t, json.Unmarshal(body, &payload))
	r.requests = append(r.requests, capturedRequest{
		method:      req.Method,
		path:        req.URL.Path,
		auth:        req.Header.Get("Authorization"),
		contentType: req.Header.Get("Content-Type"),
		payload:     payload,
	})
	return &http.Response{
		StatusCode: r.statusCode,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(r.responseBody)),
	}, nil
}

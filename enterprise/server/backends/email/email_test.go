package email_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/email"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestSend(t *testing.T) {
	validMessage := &email.Message{
		ToAddresses: []email.Address{{Name: "Dev", Email: "dev@example.com"}},
		Subject:     "Build complete",
		Body:        "<p>The build completed.</p>",
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
		request:     "POST /v3/mail/send",
		auth:        "Bearer test-api-key",
		contentType: "application/json",
		payload:     validPayload,
	}
	messageSenderPayload := validPayload
	messageSenderPayload.From = payloadAddress{Name: "Usage Alerts", Email: "usage@buildbuddy.io"}
	messageSenderRequest := validRequest
	messageSenderRequest.payload = messageSenderPayload
	multiRecipientPayload := sendgridPayload{
		From:    payloadAddress{Name: "BuildBuddy", Email: "notifications@buildbuddy.io"},
		Subject: "Build complete",
		Personalizations: []payloadPersonalization{
			{
				To: []payloadAddress{
					{Name: "Dev", Email: "dev@example.com"},
				},
			},
			{
				To: []payloadAddress{
					{Name: "Admin", Email: "admin@example.com"},
				},
			},
		},
		Content: []payloadContent{
			{Type: "text/html", Value: "<p>The build completed.</p>"},
		},
	}
	multiRecipientRequest := validRequest
	multiRecipientRequest.payload = multiRecipientPayload

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
				From:        email.Address{Name: "Usage Alerts", Email: "usage@buildbuddy.io"},
				ToAddresses: []email.Address{{Name: "Dev", Email: "dev@example.com"}},
				Subject:     "Build complete",
				Body:        "<p>The build completed.</p>",
			},
			statusCode:   http.StatusAccepted,
			wantRequests: []capturedRequest{messageSenderRequest},
		},
		{
			name: "posts multiple individually addressed recipients",
			message: &email.Message{
				ToAddresses: []email.Address{
					{Name: "Dev", Email: "dev@example.com"},
					{Name: "Admin", Email: "admin@example.com"},
				},
				Subject: "Build complete",
				Body:    "<p>The build completed.</p>",
			},
			statusCode:   http.StatusAccepted,
			wantRequests: []capturedRequest{multiRecipientRequest},
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
			name: "requires non-empty recipient address",
			message: &email.Message{
				ToAddresses: []email.Address{{Name: "Admin"}},
				Subject:     "Build complete",
				Body:        "<p>The build completed.</p>",
			},
			statusCode: http.StatusAccepted,
			wantErr:    "recipient email address is required",
		},
		{
			name: "requires subject",
			message: &email.Message{
				ToAddresses: []email.Address{{Name: "Dev", Email: "dev@example.com"}},
				Body:        "<p>The build completed.</p>",
			},
			statusCode: http.StatusAccepted,
			wantErr:    "subject is required",
		},
		{
			name: "requires body",
			message: &email.Message{
				ToAddresses: []email.Address{{Name: "Dev", Email: "dev@example.com"}},
				Subject:     "Build complete",
			},
			statusCode: http.StatusAccepted,
			wantErr:    "body is required",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			flags.Set(t, "notifications.email.sendgrid.api_key", "test-api-key")
			flags.Set(t, "notifications.email.default_from_address", email.Address{
				Name:  "BuildBuddy",
				Email: "notifications@buildbuddy.io",
			})
			for name, value := range testCase.flags {
				flags.Set(t, name, value)
			}
			server := runFakeSendgridServer(t, testCase.statusCode, testCase.responseBody)
			client := email.NewClient(email.ClientConfig{
				HTTPClient:   server.httpClient,
				SendGridHost: server.url,
			})

			err := client.Send(context.Background(), testCase.message)
			if testCase.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, testCase.wantErr)
			}
			require.Equal(t, testCase.wantRequests, server.capturedRequests)
		})
	}
}

type capturedRequest struct {
	request     string
	auth        string
	contentType string
	payload     sendgridPayload
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

type fakeSendgridServer struct {
	t                testing.TB
	httpClient       *http.Client
	url              string
	capturedRequests []capturedRequest
	statusCode       int
	responseBody     string
}

func runFakeSendgridServer(t testing.TB, statusCode int, responseBody string) *fakeSendgridServer {
	t.Helper()
	server := &fakeSendgridServer{
		t:            t,
		statusCode:   statusCode,
		responseBody: responseBody,
	}
	httpServer := httptest.NewServer(server)
	server.httpClient = httpServer.Client()
	server.url = httpServer.URL
	t.Cleanup(func() {
		httpServer.Close()
	})
	return server
}

func (s *fakeSendgridServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	body, err := io.ReadAll(req.Body)
	require.NoError(s.t, err)
	var payload sendgridPayload
	require.NoError(s.t, json.Unmarshal(body, &payload))
	s.capturedRequests = append(s.capturedRequests, capturedRequest{
		request:     req.Method + " " + req.URL.Path,
		auth:        req.Header.Get("Authorization"),
		contentType: req.Header.Get("Content-Type"),
		payload:     payload,
	})
	w.WriteHeader(s.statusCode)
	_, err = w.Write([]byte(s.responseBody))
	require.NoError(s.t, err)
}

package sendgrid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	sg "github.com/sendgrid/sendgrid-go/helpers/mail"
)

var (
	apiKey = flag.String("notifications.email.sendgrid.api_key", "", "API key for delivering email notifications via SendGrid.", flag.Secret)
)

const v3BaseURL = "https://api.sendgrid.com/v3"

// Enabled returns whether the SendGrid API is configured.
func Enabled() bool {
	return *apiKey != ""
}

type sendgridClient struct {
	// HTTP client used for pooling connections to sendgrid.
	client http.Client
}

func NewClient() (*sendgridClient, error) {
	if !Enabled() {
		return nil, status.FailedPreconditionError("SendGrid API key is not configured")
	}
	return &sendgridClient{}, nil
}

// SendEmail sends an email using the SendGrid API.
//
// The msgID parameter is ignored - SendGrid does not support idempotency.
func (s *sendgridClient) SendEmail(ctx context.Context, msgID, fromName, fromAddress string, toAddresses []string, subject, contentType, content string) error {
	slices.Sort(toAddresses)
	toAddresses = slices.Compact(toAddresses)
	toEmails := make([]*sg.Email, 0, len(toAddresses))
	for _, to := range toAddresses {
		toEmails = append(toEmails, &sg.Email{Address: to})
	}
	payload := sg.SGMailV3{
		Subject: subject,
		Personalizations: []*sg.Personalization{
			{To: toEmails},
		},
		From: &sg.Email{
			Name:    fromName,
			Address: fromAddress,
		},
		Content: []*sg.Content{
			{Type: contentType, Value: content},
		},
	}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", v3BaseURL+"/mail/send", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+*apiKey)

	log.CtxDebugf(ctx, "Sending email with SendGrid")
	rsp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer rsp.Body.Close()

	// Read body unconditionally to allow reusing the TCP connection.
	responseBytes, _ := io.ReadAll(rsp.Body)
	if rsp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %q", rsp.StatusCode, string(responseBytes))
	}

	log.CtxDebugf(ctx, "SendGrid /mail/send: HTTP %d", rsp.StatusCode)

	return nil
}

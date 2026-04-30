package email

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	sg "github.com/sendgrid/sendgrid-go"
	sgmail "github.com/sendgrid/sendgrid-go/helpers/mail"
)

var (
	// Generic email settings - if we switch providers, these flags would still
	// apply.
	defaultFromAddress = flag.Struct("notifications.email.default_from_address", Address{}, "Default sender address for email notifications.")

	// Provider-specific flags
	sendgridAPIKey = flag.String("notifications.email.sendgrid.api_key", "", "SendGrid API key used to send email notifications.", flag.Secret)
)

func init() {
	sg.DefaultClient.HTTPClient = httpclient.New(nil, "sendgrid")
}

// Address identifies an email sender or recipient.
type Address struct {
	// Name is the display name associated with Email.
	Name string `json:"name" yaml:"name"`
	// Email is the email address.
	Email string `json:"email" yaml:"email"`
}

// Message describes an email notification.
type Message struct {
	// From is the sender address. If empty, the configured default sender is
	// used.
	From Address
	// To is the primary recipient.
	To Address
	// Subject is the email subject.
	Subject string
	// Body is the HTML email body.
	Body string
}

// IsConfigured returns whether an email backend is configured.
func IsConfigured() bool {
	return *sendgridAPIKey != ""
}

// Send sends msg through the configured backend using the configured
// notification flags.
func Send(ctx context.Context, msg *Message) error {
	if !IsConfigured() {
		return status.FailedPreconditionError("SendGrid API key is required")
	}
	email, err := buildEmail(msg)
	if err != nil {
		return err
	}
	return sendWithClient(ctx, newClient(), email)
}

func newClient() *sg.Client {
	return sg.NewSendClient(*sendgridAPIKey)
}

func sendWithClient(ctx context.Context, client *sg.Client, email *sgmail.SGMailV3) error {
	resp, err := client.SendWithContext(ctx, email)
	if err != nil {
		return status.UnavailableErrorf("send email through SendGrid: %s", err)
	}
	if resp == nil {
		return status.UnavailableError("send email through SendGrid: empty response")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return status.UnavailableErrorf("SendGrid returned HTTP %d: %s", resp.StatusCode, resp.Body)
	}
	return nil
}

func buildEmail(msg *Message) (*sgmail.SGMailV3, error) {
	if msg == nil {
		return nil, status.InvalidArgumentError("message is required")
	}
	from := msg.From
	if from.Email == "" {
		from = defaultFrom()
	}
	if from.Email == "" {
		return nil, status.FailedPreconditionError("SendGrid sender email address is required")
	}
	if msg.To.Email == "" {
		return nil, status.InvalidArgumentError("recipient is required")
	}
	if msg.Subject == "" {
		return nil, status.InvalidArgumentError("subject is required")
	}
	if msg.Body == "" {
		return nil, status.InvalidArgumentError("body is required")
	}

	email := sgmail.NewV3Mail()
	email.SetFrom(toSendGridAddress(from))
	email.Subject = msg.Subject
	email.AddContent(sgmail.NewContent("text/html", msg.Body))

	personalization := sgmail.NewPersonalization()
	personalization.AddTos(toSendGridAddress(msg.To))
	email.AddPersonalizations(personalization)

	return email, nil
}

func defaultFrom() Address {
	return *defaultFromAddress
}

func toSendGridAddress(address Address) *sgmail.Email {
	return sgmail.NewEmail(address.Name, address.Email)
}

package email

import (
	"context"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	sendgridrest "github.com/sendgrid/rest"
	sendgrid "github.com/sendgrid/sendgrid-go"
	sendgridmail "github.com/sendgrid/sendgrid-go/helpers/mail"
)

var (
	// Generic email settings - if we switch providers, these flags would still
	// apply.
	defaultFromAddress = flag.Struct("notifications.email.default_from_address", Address{}, "Default sender address for email notifications.")

	// Provider-specific flags
	sendgridAPIKey = flag.String("notifications.email.sendgrid.api_key", "", "SendGrid API key used to send email notifications.", flag.Secret)
)

// IsConfigured returns whether email provider credentials are configured.
func IsConfigured() bool {
	return *sendgridAPIKey != ""
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
	// ToAddresses are recipients who each receive their own addressed email.
	ToAddresses []Address
	// Subject is the email subject.
	Subject string
	// Body is the HTML email body.
	Body string
}

// Client sends email notifications.
type Client struct {
	client       *sendgridrest.Client
	sendgridHost string
}

// ClientConfig configures a Client.
type ClientConfig struct {
	// HTTPClient is the HTTP client used to call SendGrid. If nil, a default
	// BuildBuddy HTTP client is used.
	HTTPClient *http.Client
	// SendGridHost overrides the SendGrid API host. If empty, SendGrid's
	// default host is used.
	SendGridHost string
}

// NewClient returns an email notification client.
func NewClient(config ClientConfig) *Client {
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = httpclient.New(nil, "sendgrid")
	}
	return &Client{
		client:       &sendgridrest.Client{HTTPClient: httpClient},
		sendgridHost: config.SendGridHost,
	}
}

// Send sends msg through the configured backend using the configured
// notification flags.
func (c *Client) Send(ctx context.Context, msg *Message) error {
	if *sendgridAPIKey == "" {
		return status.FailedPreconditionError("SendGrid API key is required")
	}
	email, err := buildEmail(msg)
	if err != nil {
		return err
	}
	return sendWithClient(ctx, c.client, newRequest(email, c.sendgridHost))
}

func newRequest(email *sendgridmail.SGMailV3, host string) sendgridrest.Request {
	request := sendgrid.GetRequest(*sendgridAPIKey, "/v3/mail/send", host)
	request.Method = sendgridrest.Post
	request.Body = sendgridmail.GetRequestBody(email)
	return request
}

func sendWithClient(ctx context.Context, client *sendgridrest.Client, request sendgridrest.Request) error {
	resp, err := client.SendWithContext(ctx, request)
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

func buildEmail(msg *Message) (*sendgridmail.SGMailV3, error) {
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
	recipients := msg.ToAddresses
	if len(recipients) == 0 {
		return nil, status.InvalidArgumentError("recipient is required")
	}
	for _, recipient := range recipients {
		if recipient.Email == "" {
			return nil, status.InvalidArgumentError("recipient email address is required")
		}
	}
	if msg.Subject == "" {
		return nil, status.InvalidArgumentError("subject is required")
	}
	if msg.Body == "" {
		return nil, status.InvalidArgumentError("body is required")
	}

	email := sendgridmail.NewV3Mail()
	email.SetFrom(toSendGridAddress(from))
	email.Subject = msg.Subject
	email.AddContent(sendgridmail.NewContent("text/html", msg.Body))

	for _, recipient := range recipients {
		personalization := sendgridmail.NewPersonalization()
		personalization.AddTos(toSendGridAddress(recipient))
		email.AddPersonalizations(personalization)
	}

	return email, nil
}

func defaultFrom() Address {
	return *defaultFromAddress
}

func toSendGridAddress(address Address) *sendgridmail.Email {
	return sendgridmail.NewEmail(address.Name, address.Email)
}

// Command sendmail sends an email using the configured email backend.
// This can be used for manually testing emails, e.g. to validate that the HTML
// displays as expected.
//
// Example:
//
//	# config.yaml:
//	# notifications:
//	#   email:
//	#     sendgrid:
//	#       api_key: ${SECRET:BB_TEST_SENDGRID_API_KEY}
//	#     default_from_address:
//	#       name: BuildBuddy
//	#       email: notifications@buildbuddy.io
//
//	bb run -- //enterprise/server/backends/email/sendmail:sendmail \
//	  --config_file=/path/to/config.yaml \
//	  --config_secrets.provider=gcp \
//	  --config_secrets.gcp.project_id=my-project \
//	  --to=you@example.com \
//	  --subject="BuildBuddy email" \
//	  --body="<p>Hello from BuildBuddy.</p>"
package main

import (
	"context"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/email"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	to      = flag.String("to", "", "Recipient email address, optionally with a display name like 'BuildBuddy <hello@example.com>'.")
	subject = flag.String("subject", "BuildBuddy email", "Subject for the email.")
	body    = flag.String("body", "<p>This is an email from BuildBuddy's email notification backend.</p>", "Rich text body for the email.")
)

const (
	sendTimeout = 1 * time.Minute
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	if err := configsecrets.Configure(); err != nil {
		return fmt.Errorf("prepare config secrets provider: %w", err)
	}
	if err := config.Load(); err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := log.Configure(); err != nil {
		return fmt.Errorf("configure log: %w", err)
	}

	recipient, err := parseRecipient(*to)
	if err != nil {
		return err
	}

	client := email.NewClient(email.ClientConfig{})

	msg := &email.Message{
		To:      recipient,
		Subject: *subject,
		Body:    *body,
	}

	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	if err := client.Send(ctx, msg); err != nil {
		return fmt.Errorf("send email: %w", err)
	}

	log.Infof("Sent email to %s", *to)
	return nil
}

func parseRecipient(value string) (email.Address, error) {
	if strings.TrimSpace(value) == "" {
		return email.Address{}, fmt.Errorf("--to is required")
	}
	address, err := mail.ParseAddress(value)
	if err != nil {
		return email.Address{}, fmt.Errorf("parse recipient: %w", err)
	}
	return email.Address{
		Name:  address.Name,
		Email: address.Address,
	}, nil
}

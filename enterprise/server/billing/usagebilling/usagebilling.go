// Package usagebilling sets a group up for usage based billing. Stripe holds
// the card and collects payment, and Metronome meters the group's usage and
// generates the invoices.
package usagebilling

import (
	"context"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/metronome"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/stripe"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type service struct {
	metronome *metronome.Client
}

func Register(env *real_environment.RealEnv) error {
	s := &service{}
	if metronome.Configured() {
		client, err := metronome.NewClient(nil, nil)
		if err != nil {
			return err
		}
		s.metronome = client
	}
	env.SetBillingService(s)
	return nil
}

func (s *service) Configured() bool {
	return stripe.Configured() && s.metronome != nil && metronome.PackageConfigured()
}

func (s *service) CreateUsageBasedBillingSetupSession(ctx context.Context, group *tables.Group, userID, existingCustomerID, successURL, cancelURL string) (*interfaces.UsageBasedBillingSetupSession, error) {
	customerID := strings.TrimSpace(existingCustomerID)
	if customerID == "" {
		customer, err := stripe.CreateCustomer(ctx, group, userID)
		if err != nil {
			return nil, err
		}
		customerID = customer.ID
	}
	session, err := stripe.CreateCheckoutSetupSession(ctx, group.GroupID, userID, customerID, successURL, cancelURL)
	if err != nil {
		return nil, err
	}
	return &interfaces.UsageBasedBillingSetupSession{
		CustomerID:     customerID,
		SetupSessionID: session.ID,
		PaymentSetupID: session.SetupIntent.ID,
		SetupURL:       session.URL,
	}, nil
}

func (s *service) CompleteUsageBasedBillingSetup(ctx context.Context, group *tables.Group, expectedSetupSessionID, expectedCustomerID, setupSessionID string) (*interfaces.UsageBasedBillingSetupCompletion, error) {
	session, err := stripe.RetrieveCheckoutSession(ctx, setupSessionID)
	if err != nil {
		return nil, err
	}
	if session.Mode != stripe.CheckoutSetupMode {
		return nil, status.FailedPreconditionErrorf("billing setup session %q is not a setup session", setupSessionID)
	}
	if session.Status != stripe.CheckoutComplete {
		return nil, status.FailedPreconditionErrorf("billing setup session %q is not complete", setupSessionID)
	}
	if session.ClientReferenceID != group.GroupID {
		return nil, status.PermissionDeniedError("billing setup session does not belong to the selected organization")
	}
	if session.ID != expectedSetupSessionID {
		return nil, status.PermissionDeniedError("billing setup session does not match the latest setup session")
	}
	if expectedCustomerID != "" && expectedCustomerID != session.Customer {
		return nil, status.PermissionDeniedError("billing setup session customer does not match the selected organization")
	}
	if session.Customer == "" || session.SetupIntent.PaymentMethod == "" {
		return nil, status.FailedPreconditionError("billing setup session has no saved payment method")
	}

	// Metronome's invoices are charged to the customer's default payment method.
	if err := stripe.SetDefaultPaymentMethod(ctx, session.Customer, session.SetupIntent.PaymentMethod); err != nil {
		return nil, err
	}

	// The usage exporter normally creates the group's Metronome customer and
	// contract the first time it reports usage. A group that sets up billing
	// before that gets them here, the same way: the group ID is the ingest
	// alias and the contract's uniqueness key.
	metronomeCustomerID, err := s.metronome.FindCustomerID(ctx, group.GroupID)
	if err != nil {
		return nil, err
	}
	if metronomeCustomerID == "" {
		metronomeCustomerID, err = s.metronome.CreateCustomer(ctx, group.GroupID, group.GroupID)
		if err != nil {
			return nil, err
		}
	}
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	if err := s.metronome.CreateContract(ctx, metronomeCustomerID, monthStart, group.GroupID); err != nil {
		return nil, err
	}
	if err := s.metronome.BillThroughStripe(ctx, metronomeCustomerID, group.GroupID, session.Customer); err != nil {
		return nil, err
	}
	return &interfaces.UsageBasedBillingSetupCompletion{
		CustomerID:     session.Customer,
		SetupSessionID: session.ID,
		PaymentSetupID: session.SetupIntent.ID,
	}, nil
}

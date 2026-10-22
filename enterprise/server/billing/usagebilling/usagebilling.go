// Package usagebilling sets a group up for usage based billing: Stripe holds
// the payment method and collects, Metronome meters and invoices.
package usagebilling

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/metronome"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/stripe"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
)

type service struct {
	env       *real_environment.RealEnv
	metronome *metronome.Client
}

func Register(env *real_environment.RealEnv) error {
	if enabled, err := stripe.Enabled(); !enabled {
		return err
	}
	if !metronome.PackageConfigured() {
		return status.FailedPreconditionError("billing.metronome.package_alias is required")
	}
	client, err := metronome.NewClient(nil, nil)
	if err != nil {
		return err
	}
	env.SetBillingService(&service{env: env, metronome: client})
	return nil
}

func (s *service) CreateSetupSession(ctx context.Context, group *tables.Group) (string, error) {
	customerID, err := metronome.EnsureCustomer(ctx, s.metronome, group.GroupID, time.Now())
	if err != nil {
		return "", err
	}
	// Reuse the group's Stripe customer so that retries don't create more.
	link, err := s.metronome.FindStripeLink(ctx, customerID)
	if err != nil {
		return "", err
	}
	if link == nil {
		stripeCustomerID, err := stripe.CreateCustomer(ctx, group.Name, group.GroupID)
		if err != nil {
			return "", err
		}
		if link, err = s.metronome.LinkStripeCustomer(ctx, customerID, stripeCustomerID); err != nil {
			return "", err
		}
	}
	// Stripe fills in {CHECKOUT_SESSION_ID} when it redirects back.
	settingsURL := build_buddy_url.WithPath("/settings/org/details").String()
	return stripe.CreateCheckoutSetupSession(ctx, link.StripeCustomerID, group.GroupID, settingsURL+"?setup_session_id={CHECKOUT_SESSION_ID}", settingsURL)
}

func (s *service) CompleteSetup(ctx context.Context, group *tables.Group, setupSessionID string) error {
	session, err := stripe.RetrieveCheckoutSession(ctx, setupSessionID)
	if err != nil {
		return err
	}
	if session.ClientReferenceID != group.GroupID {
		return status.PermissionDeniedError("billing setup session does not belong to the selected organization")
	}
	if session.Mode != stripe.SetupMode || session.Status != stripe.CheckoutComplete || session.SetupIntent.PaymentMethod == "" {
		return status.FailedPreconditionErrorf("billing setup session %q has no saved payment method", setupSessionID)
	}
	customerID, err := metronome.EnsureCustomer(ctx, s.metronome, group.GroupID, time.Now())
	if err != nil {
		return err
	}
	link, err := s.metronome.FindStripeLink(ctx, customerID)
	if err != nil {
		return err
	}
	if link == nil || session.Customer != link.StripeCustomerID {
		return status.FailedPreconditionErrorf("billing setup session %q is not for the organization's Stripe customer", setupSessionID)
	}
	// Metronome's invoices are charged to the customer's default payment method.
	if err := stripe.SetDefaultPaymentMethod(ctx, link.StripeCustomerID, session.SetupIntent.PaymentMethod); err != nil {
		return err
	}
	if err := s.metronome.AddBillingProviderToContract(ctx, customerID, group.GroupID, link.ID); err != nil {
		return err
	}

	result := s.env.GetDBHandle().NewQuery(ctx, "usagebilling_set_usage_based").Raw(
		`UPDATE "Groups" SET status = ? WHERE group_id = ? AND status = ?`,
		int32(grpb.Group_USAGE_BASED_GROUP_STATUS), group.GroupID, int32(grpb.Group_FREE_TIER_GROUP_STATUS),
	).Exec()
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return status.FailedPreconditionError("usage-based billing setup is only available for free tier organizations")
	}
	if qm := s.env.GetQuotaManager(); qm != nil {
		if err := qm.ReloadBucketsAndNotify(ctx); err != nil {
			log.Warningf("Error reloading quota buckets: %s", err)
		}
	}
	return nil
}

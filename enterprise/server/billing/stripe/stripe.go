package stripe

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	enabled = flag.Bool("billing.stripe.enabled", false, "If true, usage-based billing setup with Stripe is enabled.")
	apiKey  = flag.String("billing.stripe.api_key", "", "Stripe API key used to create checkout setup sessions.", flag.Secret)
	apiURL  = flag.String("billing.stripe.api_url", "https://api.stripe.com", "Stripe API base URL.", flag.Internal)

	httpClient = &http.Client{Timeout: 15 * time.Second}
)

const (
	checkoutSetupMode = "setup"
	checkoutComplete  = "complete"
	apiVersion        = "2026-04-22.dahlia"
	idempotencyPrefix = "buildbuddy-usage-billing-"
)

type customer struct {
	ID string `json:"id"`
}

type checkoutSession struct {
	ID                string `json:"id"`
	URL               string `json:"url"`
	Customer          string `json:"customer"`
	SetupIntent       string `json:"setup_intent"`
	Mode              string `json:"mode"`
	Status            string `json:"status"`
	ClientReferenceID string `json:"client_reference_id"`
}

type service struct{}

func Register(env *real_environment.RealEnv) error {
	env.SetBillingService(&service{})
	return nil
}

func configured() bool {
	return *enabled && *apiKey != "" && strings.TrimSpace(*apiURL) != ""
}

func (s *service) Configured() bool {
	return configured()
}

func (s *service) CreateUsageBasedBillingSetupSession(ctx context.Context, group *tables.Group, existingCustomerID, successURL, cancelURL string) (*interfaces.UsageBasedBillingSetupSession, error) {
	customerID := strings.TrimSpace(existingCustomerID)
	if customerID == "" {
		customer, err := createCustomer(ctx, group)
		if err != nil {
			return nil, err
		}
		customerID = customer.ID
	}

	session, err := createCheckoutSetupSession(ctx, group.GroupID, customerID, successURL, cancelURL)
	if err != nil {
		return nil, err
	}
	return &interfaces.UsageBasedBillingSetupSession{
		CustomerID:     customerID,
		SetupSessionID: session.ID,
		PaymentSetupID: session.SetupIntent,
		SetupURL:       session.URL,
	}, nil
}

func (s *service) CompleteUsageBasedBillingSetup(ctx context.Context, groupID, expectedSetupSessionID, expectedCustomerID, setupSessionID string) (*interfaces.UsageBasedBillingSetupCompletion, error) {
	session, err := retrieveCheckoutSession(ctx, setupSessionID)
	if err != nil {
		return nil, err
	}
	if session.Mode != checkoutSetupMode {
		return nil, status.FailedPreconditionErrorf("billing setup session %q is not a setup session", setupSessionID)
	}
	if session.Status != checkoutComplete {
		return nil, status.FailedPreconditionErrorf("billing setup session %q is not complete", setupSessionID)
	}
	if session.ClientReferenceID != groupID {
		return nil, status.PermissionDeniedError("billing setup session does not belong to the selected organization")
	}
	if session.ID != expectedSetupSessionID {
		return nil, status.PermissionDeniedError("billing setup session does not match the latest setup session")
	}
	if expectedCustomerID != "" && expectedCustomerID != session.Customer {
		return nil, status.PermissionDeniedError("billing setup session customer does not match the selected organization")
	}
	if session.Customer == "" || session.SetupIntent == "" {
		return nil, status.FailedPreconditionError("billing setup session is missing saved payment method setup state")
	}
	return &interfaces.UsageBasedBillingSetupCompletion{
		CustomerID:     session.Customer,
		SetupSessionID: session.ID,
		PaymentSetupID: session.SetupIntent,
	}, nil
}

func createCustomer(ctx context.Context, group *tables.Group) (*customer, error) {
	form := url.Values{}
	form.Set("name", group.Name)
	form.Set("metadata[group_id]", group.GroupID)

	customer := &customer{}
	if err := postForm(ctx, "/v1/customers", form, idempotencyPrefix+"customer-"+group.GroupID, customer); err != nil {
		return nil, err
	}
	if customer.ID == "" {
		return nil, status.UnavailableError("Stripe customer creation returned an empty customer ID")
	}
	return customer, nil
}

func createCheckoutSetupSession(ctx context.Context, groupID, customerID, successURL, cancelURL string) (*checkoutSession, error) {
	form := url.Values{}
	form.Set("mode", checkoutSetupMode)
	form.Set("currency", "usd")
	form.Set("customer", customerID)
	form.Set("success_url", successURL)
	form.Set("cancel_url", cancelURL)
	form.Set("client_reference_id", groupID)
	form.Set("metadata[group_id]", groupID)
	form.Set("setup_intent_data[metadata][group_id]", groupID)

	session := &checkoutSession{}
	if err := postForm(ctx, "/v1/checkout/sessions", form, "", session); err != nil {
		return nil, err
	}
	if session.ID == "" || session.URL == "" {
		return nil, status.UnavailableError("Stripe checkout session creation returned an incomplete session")
	}
	return session, nil
}

func retrieveCheckoutSession(ctx context.Context, sessionID string) (*checkoutSession, error) {
	session := &checkoutSession{}
	if err := get(ctx, "/v1/checkout/sessions/"+url.PathEscape(sessionID), session); err != nil {
		return nil, err
	}
	if session.ID == "" {
		return nil, status.UnavailableError("Stripe checkout session retrieval returned an empty session ID")
	}
	return session, nil
}

type errorResponse struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

func postForm(ctx context.Context, path string, form url.Values, idempotencyKey string, out any) error {
	req, err := newRequest(ctx, http.MethodPost, path, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	return do(req, out)
}

func get(ctx context.Context, path string, out any) error {
	req, err := newRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	return do(req, out)
}

func newRequest(ctx context.Context, method, requestPath string, body io.Reader) (*http.Request, error) {
	if !*enabled {
		return nil, status.FailedPreconditionError("billing.stripe.enabled is required")
	}
	if *apiKey == "" {
		return nil, status.FailedPreconditionError("billing.stripe.api_key is required")
	}
	baseURL := strings.TrimRight(*apiURL, "/")
	if baseURL == "" {
		return nil, status.FailedPreconditionError("billing.stripe.api_url is required")
	}
	req, err := http.NewRequestWithContext(ctx, method, baseURL+requestPath, body)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(*apiKey, "")
	req.Header.Set("Stripe-Version", apiVersion)
	return req, nil
}

func do(req *http.Request, out any) error {
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		stripeErr := &errorResponse{}
		if err := json.Unmarshal(body, stripeErr); err == nil && stripeErr.Error.Message != "" {
			return status.UnavailableErrorf("Stripe API error: %s", stripeErr.Error.Message)
		}
		return status.UnavailableErrorf("Stripe API error: status %d", resp.StatusCode)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return status.UnavailableErrorf("parse Stripe API response: %s", err)
	}
	return nil
}

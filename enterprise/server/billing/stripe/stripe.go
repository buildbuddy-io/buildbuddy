// Package stripe is the client for the parts of Stripe used by usage based
// billing: creating a customer, saving a card through Checkout, and setting
// the saved card as the customer's default payment method.
package stripe

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

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
	CheckoutSetupMode = "setup"
	CheckoutComplete  = "complete"
	apiVersion        = "2026-04-22.dahlia"
	idempotencyPrefix = "buildbuddy-usage-billing-"
)

func Configured() bool {
	return *enabled && *apiKey != "" && strings.TrimSpace(*apiURL) != ""
}

type Customer struct {
	ID string `json:"id"`
}

// SetupIntent is an ID string unless the session was retrieved with
// expand[]=setup_intent, in which case it is an object.
type SetupIntent struct {
	ID            string `json:"id"`
	PaymentMethod string `json:"payment_method"`
}

func (s *SetupIntent) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		return json.Unmarshal(b, &s.ID)
	}
	if string(b) == "null" {
		return nil
	}
	type setupIntent SetupIntent
	return json.Unmarshal(b, (*setupIntent)(s))
}

type CheckoutSession struct {
	ID                string      `json:"id"`
	URL               string      `json:"url"`
	Customer          string      `json:"customer"`
	SetupIntent       SetupIntent `json:"setup_intent"`
	Mode              string      `json:"mode"`
	Status            string      `json:"status"`
	ClientReferenceID string      `json:"client_reference_id"`
}

// CreateCustomer creates a Stripe customer for the group. The group ID and the
// admin who set up billing are attached as metadata so the customer can be
// traced back.
func CreateCustomer(ctx context.Context, group *tables.Group, userID string) (*Customer, error) {
	form := url.Values{}
	form.Set("name", group.Name)
	form.Set("metadata[group_id]", group.GroupID)
	form.Set("metadata[user_id]", userID)

	customer := &Customer{}
	if err := postForm(ctx, "/v1/customers", form, idempotencyPrefix+"customer-"+group.GroupID, customer); err != nil {
		return nil, err
	}
	if customer.ID == "" {
		return nil, status.UnavailableError("Stripe customer creation returned an empty customer ID")
	}
	return customer, nil
}

// CreateCheckoutSetupSession creates a hosted Checkout page that saves a card
// on the customer without charging it.
func CreateCheckoutSetupSession(ctx context.Context, groupID, userID, customerID, successURL, cancelURL string) (*CheckoutSession, error) {
	form := url.Values{}
	form.Set("mode", CheckoutSetupMode)
	form.Set("currency", "usd")
	form.Set("customer", customerID)
	form.Set("success_url", successURL)
	form.Set("cancel_url", cancelURL)
	form.Set("client_reference_id", groupID)
	form.Set("metadata[group_id]", groupID)
	form.Set("metadata[user_id]", userID)
	form.Set("setup_intent_data[metadata][group_id]", groupID)

	session := &CheckoutSession{}
	if err := postForm(ctx, "/v1/checkout/sessions", form, "", session); err != nil {
		return nil, err
	}
	if session.ID == "" || session.URL == "" {
		return nil, status.UnavailableError("Stripe checkout session creation returned an incomplete session")
	}
	return session, nil
}

// RetrieveCheckoutSession returns the session with its setup intent expanded,
// so the saved payment method is available once the session is complete.
func RetrieveCheckoutSession(ctx context.Context, sessionID string) (*CheckoutSession, error) {
	session := &CheckoutSession{}
	if err := get(ctx, "/v1/checkout/sessions/"+url.PathEscape(sessionID)+"?expand[]=setup_intent", session); err != nil {
		return nil, err
	}
	if session.ID == "" {
		return nil, status.UnavailableError("Stripe checkout session retrieval returned an empty session ID")
	}
	return session, nil
}

// SetDefaultPaymentMethod makes the payment method the one invoices are
// charged to.
func SetDefaultPaymentMethod(ctx context.Context, customerID, paymentMethodID string) error {
	form := url.Values{}
	form.Set("invoice_settings[default_payment_method]", paymentMethodID)
	return postForm(ctx, "/v1/customers/"+url.PathEscape(customerID), form, "", &Customer{})
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
	if !Configured() {
		return nil, status.FailedPreconditionError("billing.stripe.enabled and billing.stripe.api_key are required")
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(*apiURL, "/")+requestPath, body)
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

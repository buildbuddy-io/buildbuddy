// Package stripe is the client for the parts of Stripe used by usage based
// billing.
package stripe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/metronome"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	enabled = flag.Bool("billing.stripe.enabled", false, "If true, usage-based billing setup with Stripe is enabled. Requires billing.stripe.api_key, billing.metronome.api_key, and billing.metronome.package_alias.")
	apiKey  = flag.String("billing.stripe.api_key", "", "Stripe API key used to create checkout setup sessions.", flag.Secret)
	apiURL  = flag.String("billing.stripe.api_url", "https://api.stripe.com", "Stripe API base URL.", flag.Internal)

	httpClient = &http.Client{Transport: httpclient.New(nil, "stripe").Transport, Timeout: 15 * time.Second}
)

const (
	CheckoutComplete = "complete"
	SetupMode        = "setup"
	apiVersion       = "2026-04-22.dahlia"
)

// Enabled returns whether Stripe is enabled, which requires an API key.
func Enabled() (bool, error) {
	if !*enabled {
		return false, nil
	}
	if *apiKey == "" {
		return false, status.FailedPreconditionError("billing.stripe.api_key is required")
	}
	return true, nil
}

type CheckoutSession struct {
	Customer          string `json:"customer"`
	Mode              string `json:"mode"`
	Status            string `json:"status"`
	ClientReferenceID string `json:"client_reference_id"`
	SetupIntent       struct {
		PaymentMethod string `json:"payment_method"`
	} `json:"setup_intent"`
}

func CreateCustomer(ctx context.Context, name, groupID string) (string, error) {
	form := url.Values{}
	form.Set("name", name)
	form.Set("metadata[group_id]", groupID)
	var customer struct {
		ID string `json:"id"`
	}
	err := do(ctx, http.MethodPost, "/v1/customers", form, &customer)
	return customer.ID, err
}

// CreateCheckoutSetupSession returns the URL of a hosted page that saves a
// payment method on the customer.
func CreateCheckoutSetupSession(ctx context.Context, customerID, groupID, successURL, cancelURL string) (string, error) {
	form := url.Values{}
	form.Set("mode", SetupMode)
	form.Set("currency", "usd")
	form.Set("customer", customerID)
	form.Set("client_reference_id", groupID)
	form.Set("success_url", successURL)
	form.Set("cancel_url", cancelURL)
	var session struct {
		URL string `json:"url"`
	}
	err := do(ctx, http.MethodPost, "/v1/checkout/sessions", form, &session)
	return session.URL, err
}

func RetrieveCheckoutSession(ctx context.Context, sessionID string) (*CheckoutSession, error) {
	session := &CheckoutSession{}
	if err := do(ctx, http.MethodGet, "/v1/checkout/sessions/"+url.PathEscape(sessionID)+"?expand[]=setup_intent", nil, session); err != nil {
		return nil, err
	}
	return session, nil
}

func SetDefaultPaymentMethod(ctx context.Context, customerID, paymentMethodID string) error {
	form := url.Values{}
	form.Set("invoice_settings[default_payment_method]", paymentMethodID)
	return do(ctx, http.MethodPost, "/v1/customers/"+url.PathEscape(customerID), form, nil)
}

func do(ctx context.Context, method, path string, form url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, *apiURL+path, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.SetBasicAuth(*apiKey, "")
	req.Header.Set("Stripe-Version", apiVersion)
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		var stripeErr struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(body, &stripeErr)
		return metronome.ErrorForStatusCode(resp.StatusCode, fmt.Sprintf("Stripe API error (HTTP %d): %s", resp.StatusCode, stripeErr.Error.Message))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(body, out)
}

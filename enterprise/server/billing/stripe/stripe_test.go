package stripe_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/stripe"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T, handler http.HandlerFunc) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, _, _ := r.BasicAuth()
		assert.Equal(t, "sk_test_123", key)
		assert.NotEmpty(t, r.Header.Get("Stripe-Version"))
		require.NoError(t, r.ParseForm())
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	flags.Set(t, "billing.stripe.enabled", true)
	flags.Set(t, "billing.stripe.api_key", "sk_test_123")
	flags.Set(t, "billing.stripe.api_url", server.URL)
}

func TestCreateCustomer(t *testing.T) {
	setup(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST /v1/customers", r.Method+" "+r.URL.Path)
		assert.Equal(t, "Acme", r.PostForm.Get("name"))
		assert.Equal(t, "GR1", r.PostForm.Get("metadata[group_id]"))
		assert.Equal(t, "US1", r.PostForm.Get("metadata[user_id]"))
		assert.Contains(t, r.Header.Get("Idempotency-Key"), "GR1")
		fmt.Fprint(w, `{"id":"cus_1"}`)
	})

	customer, err := stripe.CreateCustomer(t.Context(), &tables.Group{GroupID: "GR1", Name: "Acme"}, "US1")
	require.NoError(t, err)
	assert.Equal(t, "cus_1", customer.ID)
}

func TestCheckoutSetupSession(t *testing.T) {
	setup(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/checkout/sessions":
			assert.Equal(t, "setup", r.PostForm.Get("mode"))
			assert.Equal(t, "cus_1", r.PostForm.Get("customer"))
			assert.Equal(t, "GR1", r.PostForm.Get("client_reference_id"))
			assert.Equal(t, "https://app.example.com/ok", r.PostForm.Get("success_url"))
			assert.Equal(t, "https://app.example.com/cancel", r.PostForm.Get("cancel_url"))
			fmt.Fprint(w, `{"id":"cs_1","url":"https://checkout.example.com/cs_1","customer":"cus_1","setup_intent":"seti_1","mode":"setup","status":"open","client_reference_id":"GR1"}`)
		case "GET /v1/checkout/sessions/cs_1":
			assert.Equal(t, "setup_intent", r.URL.Query().Get("expand[]"))
			fmt.Fprint(w, `{"id":"cs_1","customer":"cus_1","setup_intent":{"id":"seti_1","payment_method":"pm_1"},"mode":"setup","status":"complete","client_reference_id":"GR1"}`)
		default:
			http.NotFound(w, r)
		}
	})

	created, err := stripe.CreateCheckoutSetupSession(t.Context(), "GR1", "US1", "cus_1", "https://app.example.com/ok", "https://app.example.com/cancel")
	require.NoError(t, err)
	assert.Equal(t, "https://checkout.example.com/cs_1", created.URL)
	assert.Equal(t, "seti_1", created.SetupIntent.ID)

	completed, err := stripe.RetrieveCheckoutSession(t.Context(), "cs_1")
	require.NoError(t, err)
	assert.Equal(t, stripe.CheckoutComplete, completed.Status)
	assert.Equal(t, "GR1", completed.ClientReferenceID)
	assert.Equal(t, "pm_1", completed.SetupIntent.PaymentMethod)
}

func TestSetDefaultPaymentMethod(t *testing.T) {
	setup(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST /v1/customers/cus_1", r.Method+" "+r.URL.Path)
		assert.Equal(t, "pm_1", r.PostForm.Get("invoice_settings[default_payment_method]"))
		fmt.Fprint(w, `{"id":"cus_1"}`)
	})

	require.NoError(t, stripe.SetDefaultPaymentMethod(t.Context(), "cus_1", "pm_1"))
}

func TestErrors(t *testing.T) {
	setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
		fmt.Fprint(w, `{"error":{"message":"Your card was declined.","type":"card_error"}}`)
	})

	err := stripe.SetDefaultPaymentMethod(t.Context(), "cus_1", "pm_1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Your card was declined.")

	flags.Set(t, "billing.stripe.enabled", false)
	err = stripe.SetDefaultPaymentMethod(t.Context(), "cus_1", "pm_1")
	require.True(t, status.IsFailedPreconditionError(err), "unexpected error: %v", err)
}

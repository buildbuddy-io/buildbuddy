package csp

import (
	"io"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// Nonce is the context key type for the per-request CSP nonce.
// The associated value is always of type template.HTMLAttr.
type Nonce struct{}

const ReportingEndpoint = "/csp-report"

var ReportingHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	report, err := io.ReadAll(r.Body)
	if err != nil {
		log.CtxInfof(r.Context(), "CSP report failure: %s", err)
		return
	}
	log.CtxInfof(r.Context(), "CSP violation: %s", report)
})

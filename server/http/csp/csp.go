package csp

import (
	"io"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// Nonce is the context key type for the per-request CSP nonce.
// The associated value is always of type string and should be inserted
// between the double quotes of a nonce = "..." attribute.
type Nonce struct{}

const ReportingEndpoint = "/csp-report"

var ReportingHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	report, err := io.ReadAll(r.Body)
	if err != nil {
		log.CtxDebugf(r.Context(), "CSP report failure: %s", err)
		return
	}
	log.CtxDebugf(r.Context(), "CSP violation: %s", report)
})

package csp

import (
	"io"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
)

// Nonce is the context key type for the per-request CSP nonce.
// The associated value is always of type template.HTMLAttr.
type Nonce struct{}

const ReportingEndpoint = "/csp-report"

var ReportingHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	report, err := io.ReadAll(r.Body)
	if err != nil {
		alert.UnexpectedEvent("csp_violation_report_failure", err.Error())
		return
	}
	alert.UnexpectedEvent("csp_violation", string(report))
})

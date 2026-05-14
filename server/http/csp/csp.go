package csp

import (
	"encoding/json"
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
	for _, report := range extractReports(r.Body) {
		log.CtxWarning(r.Context(), report)
	}
})

// Extract the individual valid CSP reports and return them as JSON strings.
// This function performs a very basic normalization of the reports across
// different browsers and attempts to filter out reports caused by browser
// extensions.
func extractReports(r io.Reader) []string {
	var rawReport any
	if err := json.NewDecoder(r).Decode(&rawReport); err != nil {
		return nil
	}
	// Wrap single reports in an array to handle them uniformly.
	all, ok := rawReport.([]any)
	if !ok {
		all = []any{rawReport}
	}

	var filtered []string
	for _, anyReport := range all {
		var actualReport map[string]any
		report, ok := anyReport.(map[string]any)
		if !ok {
			// All individual reports must be JSON objects.
			continue
		}
		if actualReport, _ = report["csp-report"].(map[string]any); actualReport != nil {
			if actualReport["source-file"] == "moz-extension" {
				continue
			}
		} else if actualReport, _ = report["body"].(map[string]any); actualReport != nil {
			if actualReport["sourceFile"] == "chrome-extension" {
				continue
			}
		} else {
			actualReport = report
		}
		// Normalize the report to a JSON string.
		if b, err := json.Marshal(actualReport); err == nil {
			filtered = append(filtered, string(b))
		}
	}
	return filtered
}

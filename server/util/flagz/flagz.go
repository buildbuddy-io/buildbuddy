package flagz

import (
	"fmt"
	"net/http"

	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

// ServeHTTP renders the current flag state to YAML and writes it to the
// http.ResponseWriter. The endpoint is read-only: requests with query
// parameters are rejected rather than ignored, so that callers expecting to set
// a flag don't silently get a no-op.
func ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")

	if len(r.URL.Query()) > 0 {
		http.Error(w, "/flagz is read-only and does not support setting flags.", http.StatusBadRequest)
		return
	}
	b, err := flagyaml.SplitDocumentedYAMLFromFlags(flagyaml.RedactSecrets)
	if err != nil {
		errorText := fmt.Sprintf("Encountered error when attempting to generate YAML: %s", err)
		http.Error(w, errorText, http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

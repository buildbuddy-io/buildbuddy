package flagz

import (
	"fmt"
	"net/http"

	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

// ServeHTTP renders the current flag state to YAML and writes it to the
// http.ResponseWriter. The endpoint is read-only: only GET and HEAD are
// allowed, and query parameters are ignored.
func ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "/flagz is read-only; only GET and HEAD are supported.", http.StatusMethodNotAllowed)
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

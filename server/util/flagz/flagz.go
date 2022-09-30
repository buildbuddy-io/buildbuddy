package flagz

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"

	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

// ServeHTTP takes an http.Request, sets flags based on the query parameters,
// if any, renders the current flag state to YAML, and writes that YAML output
// to the http.ResponseWriter.
func ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")

	// Keep track of which flag.Values have been set in case one is set multiple
	// times in the query via, for example, FlagAlias.
	set := make(map[flag.Value]struct{})
	for key, values := range r.URL.Query() {
		newValueString := ""
		if len(values) == 1 {
			newValueString = values[0]
		} else if len(values) > 1 {
			// Flagz endpoint does not support setting flag to multiple values.
			errorText := fmt.Sprintf("Flag %s specifies more than one value.", key)
			http.Error(w, errorText, http.StatusBadRequest)
			return
		}
		flg := flag.Lookup(key)
		if flg == nil {
			errorText := fmt.Sprintf("Flag %s does not exist.", key)
			http.Error(w, errorText, http.StatusBadRequest)
			return
		}

		// Unwrap the value to ensure we have the real flag.Value, not a wrapper
		// like, for example, DeprecatedFlag or FlagAlias.
		unwrappedValue := flagtypes.UnwrapFlagValue(flg.Value)
		if _, ok := set[unwrappedValue]; !ok {
			// Override the value only if we have not set it before
			if err := flagutil.SetWithOverride(flg.Name, newValueString); err != nil {
				errorText := fmt.Sprintf("Error setting flag %s: %s", key, err)
				http.Error(w, errorText, http.StatusInternalServerError)
				return
			}
			set[unwrappedValue] = struct{}{}
			continue
		}
		if err := unwrappedValue.Set(newValueString); err != nil {
			errorText := fmt.Sprintf("Error setting flag %s: %s", key, err)
			http.Error(w, errorText, http.StatusInternalServerError)
			return
		}
	}
	b, err := flagyaml.SplitDocumentedYAMLFromFlags(flagyaml.RedactSecrets)
	if err != nil {
		errorText := fmt.Sprintf("Encountered error when attempting to generate YAML: %s", err)
		http.Error(w, errorText, http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

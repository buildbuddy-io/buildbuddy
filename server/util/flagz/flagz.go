package flagz

import (
	"flag"
	"fmt"
	"net/http"
	"reflect"

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
		if len(values) > 1 {
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
		addr := reflect.ValueOf(unwrappedValue)

		// Make a new empty flag.value of the appropriate type so it can be set
		// fresh. This allows us to override the flag while still using the standard
		// flag.Value interface's Set method, so it can be set with a string like we
		// would use on the command line.
		blankValue := reflect.New(addr.Type().Elem()).Interface().(flag.Value)
		if len(values) > 0 {
			if err := blankValue.Set(values[0]); err != nil {
				errorText := fmt.Sprintf("Encountered error setting flag %s to %s: %s", key, values[0], err)
				http.Error(w, errorText, http.StatusInternalServerError)
				return
			}
		}

		// Take the previously blank value and convert it to the underlying type
		// (the type which would be returned when defining the flag initially).
		newValue, err := flagtypes.ConvertFlagValue(blankValue)
		if err != nil {
			errorText := fmt.Sprintf("Error converting flag %s when setting flag: %s", key, err)
			http.Error(w, errorText, http.StatusInternalServerError)
			return
		}
		appendSlice := false
		if _, ok := set[unwrappedValue]; ok {
			// Override the value only if we have not set it before
			appendSlice = true
		}
		if err := flagutil.SetValueForFlagName(key, reflect.ValueOf(newValue).Elem().Interface(), map[string]struct{}{}, appendSlice); err != nil {
			errorText := fmt.Sprintf("Error setting flag %s: %s", key, err)
			http.Error(w, errorText, http.StatusInternalServerError)
			return
		}
		set[unwrappedValue] = struct{}{}
	}
	b, err := flagyaml.SplitDocumentedYAMLFromFlags()
	if err != nil {
		errorText := fmt.Sprintf("Encountered error when attempting to generate YAML: %s", err)
		http.Error(w, errorText, http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

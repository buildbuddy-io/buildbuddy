package flagz

import (
	"flag"
	"net/http"
	"reflect"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

// ServeHTTP takes an http.Request, sets flags based on the query parameters,
// if any, renders the current flag state to YAML, and writes that YAML output
// to the http.ResponseWriter.
func ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Keep track of which flag.Values have been set in case one is set multiple
	// times in the query via, for example, FlagAlias.
	set := make(map[flag.Value]struct{})
	for key, values := range r.URL.Query() {
		flg := flag.Lookup(key)
		if flg == nil {
			log.Errorf("Attempted to change non-existent flag %s via flagz interface.", key)
			http.Error(w, "Flag " + key + " does not exist.", http.StatusBadRequest)
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
		var err error
		for _, value := range values {
			if err = blankValue.Set(value); err != nil {
				log.Errorf("Encountered error setting flag %s to %s via flagz interface: %v", key, value, err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		if err != nil {
			continue
		}

		// Take the previously blank value and convert it to the underlying type
		// (the type which would be returned when defining the flag initially).
		newValue, err := flagtypes.ConvertFlagValue(blankValue)
		if err != nil {
			log.Errorf("Error converting flag %s when setting flag via flagz interface: %v", key, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		appendSlice := false
		if _, ok := set[unwrappedValue]; ok {
			// Override the value only if we have not set it before
			appendSlice = true
		}
		if err := flagutil.SetValueForFlagName(key, reflect.ValueOf(newValue).Elem().Interface(), map[string]struct{}{}, appendSlice); err != nil {
			log.Errorf("Error setting flag %s when setting flag via flagz interface: %v", key, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		set[unwrappedValue] = struct{}{}
	}
	b, err := flagyaml.SplitDocumentedYAMLFromFlags()
	if err != nil {
		log.Errorf("Encountered error when attempting to generate YAML for flagz endpoint: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Write(b)
}

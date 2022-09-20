package flagz

import (
	"flag"
	"net/http"
	"reflect"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

func ServeHTTP(w http.ResponseWriter, r *http.Request) {
	set := make(map[flag.Value]struct{})
	for key, values := range r.URL.Query() {
		flg := flag.Lookup(key)
		if flg == nil {
			log.Warningf("Attempted to change non-existent flag %s via flagz interface.", key)
			continue
		}
		unwrappedValue := flagtypes.UnwrapFlagValue(flg.Value)
		addr := reflect.ValueOf(unwrappedValue)
		blankValue := reflect.New(addr.Type().Elem()).Interface().(flag.Value)
		var err error
		for _, value := range values {
			if err = blankValue.Set(value); err != nil {
				log.Errorf("Encountered error setting flag %s to %s via flagz interface: %v", key, value, err)
			}
		}
		if err != nil {
			continue
		}
		newValue, err := flagtypes.ConvertFlagValue(blankValue)
		if err != nil {
			log.Errorf("Error converting flag %s when setting flag via flagz interface: %v", key, err)
			continue
		}
		appendSlice := false
		if _, ok := set[unwrappedValue]; ok {
			// Override the value only if we have not set it before
			appendSlice = true
		}
		if err := flagutil.SetValueForFlagName(key, reflect.ValueOf(newValue).Elem().Interface(), map[string]struct{}{}, appendSlice); err != nil {
			log.Errorf("Error setting flag %s when setting flag via flagz interface: %v", key, err)
			continue
		}
		set[unwrappedValue] = struct{}{}
	}
	b, err := flagyaml.SplitDocumentedYAMLFromFlags()
	if err != nil {
		log.Errorf("Encountered error when attempting to generate YAML for flagz endpoint: %v", err)
		w.WriteHeader(500)
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Write(b)
}

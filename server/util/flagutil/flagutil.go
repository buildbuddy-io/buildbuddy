package flagutil

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
)

// SetValueForFlagName sets the value for a flag by name. setFlags is the set of
// flags that have already been set on the command line; those flags will not be
// set again except to accumulate into them, in the case of collection flags. To
// force the setting of a flag, pass a nil map. If accumulate is true, a slice
// value will be appended to the current slice value and a map value will be
// merged into the current map value; otherwise, the value will replace the
// current value. accumulate has no effect if the flag is not Accumulable.
func SetValueForFlagName(name string, newValue any, setFlags map[string]struct{}, accumulate bool) error {
	return common.SetValueForFlagName(common.DefaultFlagSet, name, newValue, setFlags, accumulate)
}

func SetValueForFlagSet(flagset *flag.FlagSet, name string, newValue any, setFlags map[string]struct{}, accumulate bool) error {
	return common.SetValueForFlagName(flagset, name, newValue, setFlags, accumulate)
}

// SetWithOverride sets the flag's value by creating a new, empty flag.Value of
// the same type as the flag Value specified by name, calling
// `Set.(newValueString)` on the new flag.Value, and then explicitly setting the
// data pointed to by flagValue to the data pointed to by the new flag value.
func SetWithOverride(name, newValueString string) error {
	return common.SetWithOverride(common.DefaultFlagSet, name, newValueString)
}

// ResetFlags resets all flags to their default values, as specified by
// the string stored in the corresponding flag.DefValue.
func ResetFlags() error {
	return common.ResetFlags(common.DefaultFlagSet)
}

// GetDereferencedValue retypes and returns the dereferenced Value for
// a given flag name.
func GetDereferencedValue[T any](name string) (T, error) {
	return common.GetDereferencedValue[T](common.DefaultFlagSet, name)
}

// Expand updates the flag value to replace any placeholders in format ${FOO}
// with the content of calling the mapper function with the placeholder name.
var Expand = common.Expand

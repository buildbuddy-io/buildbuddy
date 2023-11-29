package flagutil

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
)

// SetValueForFlagName sets the value for a flag by name. setFlags is the set of
// flags that have already been set on the command line; those flags will not be
// set again except to append to them, in the case of slices. To force the
// setting of a flag, pass a nil map. If appendSlice is true, a slice value will
// be appended to the current slice value; otherwise, a slice value will replace
// the current slice value. appendSlice has no effect if the values in question
// are not slices.
func SetValueForFlagName(name string, newValue any, setFlags map[string]struct{}, appendSlice bool) error {
	return common.SetValueForFlagName(common.DefaultFlagSet, name, newValue, setFlags, appendSlice)
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

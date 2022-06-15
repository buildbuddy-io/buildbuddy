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
var SetValueForFlagName = common.SetValueForFlagName

// GetDereferencedValue retypes and returns the dereferenced Value for
// a given flag name.
func GetDereferencedValue[T any](name string) (T, error) {
	return common.GetDereferencedValue[T](name)
}

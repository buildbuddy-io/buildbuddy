package flagutil

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
)

// SetValueForFlagName sets the value for a flag by name.
var SetValueForFlagName = common.SetValueForFlagName

// GetDereferencedValue retypes and returns the dereferenced Value for
// a given flag name.
func GetDereferencedValue[T any](name string) (T, error) {
	return common.GetDereferencedValue[T](name)
}

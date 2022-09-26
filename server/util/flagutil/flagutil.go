package flagutil

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types/autoflags"
)

// SetValueForFlagName sets the value for a flag by name. setFlags is the set of
// flags that have already been set on the command line; those flags will not be
// set again except to append to them, in the case of slices. To force the
// setting of a flag, pass a nil map. If appendSlice is true, a slice value will
// be appended to the current slice value; otherwise, a slice value will replace
// the current slice value. appendSlice has no effect if the values in question
// are not slices.
var SetValueForFlagName = common.SetValueForFlagName

// SetWithOverride sets the flag's value by creating a new, empty flag.Value of
// the same type as the flag Value specified by name, calling
// `Set.(newValueString)` on the new flag.Value, and then explicitly setting the
// data pointed to by flagValue to the data pointed to by the new flag value.
var SetWithOverride = common.SetWithOverride

// ResetFlags resets all flags to their default values, as specified by
// the string stored in the corresponding flag.DefValue.
var ResetFlags = common.ResetFlags

// GetDereferencedValue retypes and returns the dereferenced Value for
// a given flag name.
func GetDereferencedValue[T any](name string) (T, error) {
	return common.GetDereferencedValue[T](name)
}

// New declares a new flag named `name` with the specified value `defaultValue`
// of type `T` and the help text `usage`. It returns a pointer to where the
// value is stored.
func New[T any](name string, defaultValue T, usage string) *T {
	return autoflags.New(name, defaultValue, usage)
}

// Var declares a new flag named `name` with the specified value `defaultValue`
// of type `T` stored at the pointer `value` and the help text `usage`.
func Var[T any](value *T, name string, defaultValue T, usage string) {
	autoflags.Var(value, name, defaultValue, usage)
}

// Deprecated declares a new deprecated flag named `name` with the specified
// value `defaultValue` of type `T`, the help text `usage`, and a
// `migrationPlan` explaining to the user how to migrate from the deprecated
// functionality. It returns a pointer to where the value is stored.
func Deprecated[T any](name string, defaultValue T, usage string, migrationPlan string) *T {
	return autoflags.Deprecated(name, defaultValue, usage, migrationPlan)
}

// DeprecatedVar declares a new deprecated flag named `name` with the specified
// value `defaultValue` of type `T` stored at the pointer `value`, the help text
// `usage`, and a `migrationPlan` explaining to the user how to migrate from the
// deprecated functionality.
func DeprecatedVar[T any](value *T, name string, defaultValue T, usage string, migrationPlan string) {
	autoflags.DeprecatedVar(value, name, defaultValue, usage, migrationPlan)
}

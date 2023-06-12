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
// value is stored. Tags may be used to mark flags; for example, use
// `SecretTag` to mark a flag that contains a secret that should be redacted in
// output, or use `DeprecatedTag(migrationPlan)` to mark a flag that has been
// deprecated and provide its migration plan.
func New[T any](name string, defaultValue T, usage string, tags ...autoflags.Taggable) *T {
	return autoflags.New(name, defaultValue, usage, tags...)
}

// Var declares a new flag named `name` with the specified value `defaultValue`
// of type `T` stored at the pointer `value` and the help text `usage`. Tags may
// be used to mark flags; for example, use `SecretTag` to mark a flag that
// contains a secret that should be redacted in output, or use
// `DeprecatedTag(migrationPlan)` to mark a flag that has been deprecated and
// provide its migration plan.
func Var[T any](value *T, name string, defaultValue T, usage string, tags ...autoflags.Taggable) {
	autoflags.Var(value, name, defaultValue, usage, tags...)
}

// Use to mark a flag secret.
var SecretTag = autoflags.SecretTag

// Use to mark a flag deprecated.
var DeprecatedTag = autoflags.DeprecatedTag

// Use to indicate that a flag should ignored by the YAML config parser.
var YAMLIgnoreTag = autoflags.YAMLIgnoreTag

package autoflags

import (
	"log"
	"net/url"
	"reflect"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"

	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
)

// New declares a new flag named `name` with the specified value `defaultValue`
// of type `T` and the help text `usage`. It returns a pointer to where the
// value is stored.
func New[T any](name string, defaultValue T, usage string) *T {
	value := reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Interface().(*T)
	Var(value, name, defaultValue, usage)
	return value
}

// Var declares a new flag named `name` with the specified value `defaultValue`
// of type `T` stored at the pointer `value` and the help text `usage`.
func Var[T any](value *T, name string, defaultValue T, usage string) {
	switch v := any(value).(type) {
	case *bool:
		common.DefaultFlagSet.BoolVar(v, name, any(defaultValue).(bool), usage)
	case *time.Duration:
		common.DefaultFlagSet.DurationVar(v, name, any(defaultValue).(time.Duration), usage)
	case *float64:
		common.DefaultFlagSet.Float64Var(v, name, any(defaultValue).(float64), usage)
	case *int:
		common.DefaultFlagSet.IntVar(v, name, any(defaultValue).(int), usage)
	case *int64:
		common.DefaultFlagSet.Int64Var(v, name, any(defaultValue).(int64), usage)
	case *uint:
		common.DefaultFlagSet.UintVar(v, name, any(defaultValue).(uint), usage)
	case *uint64:
		common.DefaultFlagSet.Uint64Var(v, name, any(defaultValue).(uint64), usage)
	case *string:
		common.DefaultFlagSet.StringVar(v, name, any(defaultValue).(string), usage)
	case *[]string:
		flagtypes.StringSliceVar(v, name, any(defaultValue).([]string), usage)
	case *url.URL:
		flagtypes.URLVar(v, name, any(defaultValue).(url.URL), usage)
	default:
		if reflect.TypeOf(value).Elem().Kind() == reflect.Slice {
			flagtypes.JSONSliceVar(value, name, defaultValue, usage)
			return
		}
		if reflect.TypeOf(value).Elem().Kind() == reflect.Struct {
			flagtypes.JSONStructVar(value, name, defaultValue, usage)
			return
		}
		log.Fatalf("Var was called from flag registry for flag %s with value %v of unrecognized type %T.", name, defaultValue, defaultValue)
	}
}

// Deprecated declares a new deprecated flag named `name` with the specified
// value `defaultValue` of type `T`, the help text `usage`, and a
// `migrationPlan` explaining to the user how to migrate from the deprecated
// functionality. It returns a pointer to where the value is stored.
func Deprecated[T any](name string, defaultValue T, usage string, migrationPlan string) *T {
	value := New(name, defaultValue, usage)
	flagtypes.Deprecate[T](name, migrationPlan)
	return value
}

// DeprecatedVar declares a new deprecated flag named `name` with the specified
// value `defaultValue` of type `T` stored at the pointer `value`, the help text
// `usage`, and a `migrationPlan` explaining to the user how to migrate from the
// deprecated functionality.
func DeprecatedVar[T any](value *T, name string, defaultValue T, usage string, migrationPlan string) {
	Var(value, name, defaultValue, usage)
	flagtypes.Deprecate[T](name, migrationPlan)
}

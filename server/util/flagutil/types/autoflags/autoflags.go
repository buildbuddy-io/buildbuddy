package autoflags

import (
	"flag"
	"net/url"
	"reflect"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
	flagtags "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types/autoflags/tags"
)

// New declares a new flag named `name` with the specified value `defaultValue`
// of type `T` and the help text `usage`. It returns a pointer to where the
// value is stored. Tags may be used to mark flags; for example, use
// `SecretTag` to mark a flag that contains a secret that should be redacted in
// output, or use `DeprecatedTag(migrationPlan)` to mark a flag that has been
// deprecated and provide its migration plan.
func New[T any](flagset *flag.FlagSet, name string, defaultValue T, usage string, tags ...flagtags.Taggable) *T {
	value := reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Interface().(*T)
	Var(flagset, value, name, defaultValue, usage, tags...)
	return value
}

// Var declares a new flag named `name` with the specified value `defaultValue`
// of type `T` stored at the pointer `value` and the help text `usage`. Tags may
// be used to mark flags; for example, use `SecretTag` to mark a flag that
// contains a secret that should be redacted in output, or use
// `DeprecatedTag(migrationPlan)` to mark a flag that has been deprecated and
// provide its migration plan.
func Var[T any](flagset *flag.FlagSet, value *T, name string, defaultValue T, usage string, tags ...flagtags.Taggable) {
	switch v := any(value).(type) {
	case *bool:
		flagset.BoolVar(v, name, any(defaultValue).(bool), usage)
		Tag[T, flag.Value](flagset, name, tags...)
	case *time.Duration:
		flagset.DurationVar(v, name, any(defaultValue).(time.Duration), usage)
		Tag[T, flag.Value](flagset, name, tags...)
	case *float64:
		flagset.Float64Var(v, name, any(defaultValue).(float64), usage)
		Tag[T, flag.Value](flagset, name, tags...)
	case *int:
		flagset.IntVar(v, name, any(defaultValue).(int), usage)
		Tag[T, flag.Value](flagset, name, tags...)
	case *int64:
		flagset.Int64Var(v, name, any(defaultValue).(int64), usage)
		Tag[T, flag.Value](flagset, name, tags...)
	case *uint:
		flagset.UintVar(v, name, any(defaultValue).(uint), usage)
		Tag[T, flag.Value](flagset, name, tags...)
	case *uint64:
		flagset.Uint64Var(v, name, any(defaultValue).(uint64), usage)
		Tag[T, flag.Value](flagset, name, tags...)
	case *string:
		flagset.StringVar(v, name, any(defaultValue).(string), usage)
		Tag[T, flag.Value](flagset, name, tags...)
	case *[]string:
		flagtypes.StringSliceVar(flagset, v, name, any(defaultValue).([]string), usage)
		Tag[T, *flagtypes.StringSliceFlag](flagset, name, tags...)
	case *url.URL:
		flagtypes.URLVar(flagset, v, name, any(defaultValue).(url.URL), usage)
		Tag[T, *flagtypes.URLFlag](flagset, name, tags...)
	default:
		if reflect.TypeOf(value).Elem().Kind() == reflect.Slice {
			flagtypes.JSONSliceVar(flagset, value, name, defaultValue, usage)
			Tag[T, *flagtypes.JSONSliceFlag[T]](flagset, name, tags...)
			break
		}
		if reflect.TypeOf(value).Elem().Kind() == reflect.Struct {
			flagtypes.JSONStructVar(flagset, value, name, defaultValue, usage)
			Tag[T, *flagtypes.JSONStructFlag[T]](flagset, name, tags...)
			break
		}
		log.Fatalf("Var was called from flag registry for flag %s with value %v of unrecognized type %T.", name, defaultValue, defaultValue)
	}
}

func Tag[T any, FV flag.Value](flagset *flag.FlagSet, name string, tags ...flagtags.Taggable) {
	for _, tg := range tags {
		flagtags.Tag[T, FV](flagset, name, tg)
	}
}

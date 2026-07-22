package flag

import (
	"flag"
	"log"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types/autoflags"

	flagtags "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types/autoflags/tags"
)

var Secret = flagtags.SecretTag
var Deprecated = flagtags.DeprecatedTag
var YAMLIgnore = flagtags.YAMLIgnoreTag
var Internal = flagtags.InternalTag

type Meta = flagtags.MetaTag

var NewFlagSet = flag.NewFlagSet
var ContinueOnError = flag.ContinueOnError
var ExitOnError = flag.ExitOnError
var PanicOnError = flag.PanicOnError
var ErrHelp = flag.ErrHelp
var CommandLine = flag.CommandLine
var Args = flag.Args

type Flag = flag.Flag
type FlagSet = flag.FlagSet

// IsSecret returns whether the given flag's value may contain secrets,
// meaning it should never be displayed or logged: either the flag was
// declared with the Secret tag, or its type contains a struct field tagged
// config:"secret" (at any depth). Note that for the latter, YAML config
// export redacts just the secret fields (see flagyaml.RedactSecrets);
// callers of this coarser check should redact the whole value.
func IsSecret(flg *flag.Flag) bool {
	if s, ok := flg.Value.(common.Secretable); ok && s.IsSecret() {
		return true
	}
	t, err := common.GetTypeForFlagValue(flg.Value)
	if err != nil {
		// Unrecognized flag value type; assume it may contain secrets.
		return true
	}
	return typeContainsSecrets(t, map[reflect.Type]bool{})
}

func typeContainsSecrets(t reflect.Type, seen map[reflect.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return typeContainsSecrets(t.Elem(), seen)
	case reflect.Map:
		return typeContainsSecrets(t.Key(), seen) || typeContainsSecrets(t.Elem(), seen)
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if slices.Contains(strings.Split(field.Tag.Get("config"), ","), "secret") {
				return true
			}
			if typeContainsSecrets(field.Type, seen) {
				return true
			}
		}
	}
	return false
}

func New[T any](flagset *flag.FlagSet, name string, defaultValue T, usage string, tags ...flagtags.Taggable) *T {
	return autoflags.New[T](flagset, name, defaultValue, usage, tags...)
}

func Parse() {
	flag.Parse()
}

func String(name string, value string, usage string, tags ...flagtags.Taggable) *string {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Bool(name string, value bool, usage string, tags ...flagtags.Taggable) *bool {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Int(name string, value int, usage string, tags ...flagtags.Taggable) *int {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Int64(name string, value int64, usage string, tags ...flagtags.Taggable) *int64 {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Uint(name string, value uint, usage string, tags ...flagtags.Taggable) *uint {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Uint64(name string, value uint64, usage string, tags ...flagtags.Taggable) *uint64 {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Float64(name string, value float64, usage string, tags ...flagtags.Taggable) *float64 {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Duration(name string, value time.Duration, usage string, tags ...flagtags.Taggable) *time.Duration {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func URL(name string, value string, usage string, tags ...flagtags.Taggable) *url.URL {
	u, err := url.Parse(value)
	if err != nil {
		log.Fatalf("Error parsing default URL value '%s' for flag: %v", value, err)
		return nil
	}
	return autoflags.New(common.DefaultFlagSet, name, *u, usage, tags...)
}

func Slice[T any](name string, value []T, usage string, tags ...flagtags.Taggable) *[]T {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Map[K comparable, V any](name string, value map[K]V, usage string, tags ...flagtags.Taggable) *map[K]V {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Struct[T any](name string, value T, usage string, tags ...flagtags.Taggable) *T {
	return autoflags.New(common.DefaultFlagSet, name, value, usage, tags...)
}

func Alias[T any](oldname string, newNames ...string) *T {
	return types.Alias[T](common.DefaultFlagSet, oldname, newNames...)
}

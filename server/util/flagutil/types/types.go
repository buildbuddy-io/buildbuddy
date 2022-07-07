package types

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"gopkg.in/yaml.v3"
)

// NewPrimitiveFlagVar returns a flag.Value derived from the given primitive pointer.
func NewPrimitiveFlagVar[T bool | time.Duration | float64 | int | int64 | uint | uint64 | string](value *T) flag.Value {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	switch v := any(value).(type) {
	case *bool:
		fs.BoolVar(v, "", *v, "")
	case *time.Duration:
		fs.DurationVar(v, "", *v, "")
	case *float64:
		fs.Float64Var(v, "", *v, "")
	case *int:
		fs.IntVar(v, "", *v, "")
	case *int64:
		fs.Int64Var(v, "", *v, "")
	case *uint:
		fs.UintVar(v, "", *v, "")
	case *uint64:
		fs.Uint64Var(v, "", *v, "")
	case *string:
		fs.StringVar(v, "", *v, "")
	}
	return fs.Lookup("").Value
}

// NewPrimitiveFlagVar returns a flag.Value derived from the given primitive.
func NewPrimitiveFlag[T bool | time.Duration | float64 | int | int64 | uint | uint64 | string](value T) flag.Value {
	return NewPrimitiveFlagVar(&value)
}

type SliceFlag[T any] []T

func NewSliceFlag[T any](slice *[]T) *SliceFlag[T] {
	return (*SliceFlag[T])(slice)
}

func Slice[T any](name string, defaultValue []T, usage string) *[]T {
	slice := make([]T, len(defaultValue))
	copy(slice, defaultValue)
	common.DefaultFlagSet.Var(NewSliceFlag(&slice), name, usage)
	return &slice
}

func SliceVar[T any](slice *[]T, name, usage string) {
	common.DefaultFlagSet.Var(NewSliceFlag(slice), name, usage)
}

func (f *SliceFlag[T]) String() string {
	switch v := any((*[]T)(f)).(type) {
	case *[]string:
		return strings.Join(*v, ",")
	default:
		b, err := json.Marshal(f)
		if err != nil {
			alert.UnexpectedEvent("config_cannot_marshal_struct", "err: %s", err)
			return "[]"
		}
		return string(b)
	}
}

func (f *SliceFlag[T]) Set(values string) error {
	if v, ok := any((*[]T)(f)).(*[]string); ok {
		for _, val := range strings.Split(values, ",") {
			*v = append(*v, val)
		}
		return nil
	}
	v := (*[]T)(f)
	var a any
	if err := json.Unmarshal([]byte(values), &a); err != nil {
		return err
	}
	if _, ok := a.([]any); ok {
		var dst []T
		if err := json.Unmarshal([]byte(values), &dst); err != nil {
			return err
		}
		*v = append(*v, dst...)
		return nil
	}
	if _, ok := a.(map[string]any); ok {
		var dst T
		if err := json.Unmarshal([]byte(values), &dst); err != nil {
			return err
		}
		*v = append(*v, dst)
		return nil
	}
	return fmt.Errorf("Default Set for SliceFlag can only accept JSON objects or arrays, but type was %T", a)
}

func (f *SliceFlag[T]) AppendSlice(slice any) error {
	s, ok := slice.([]T)
	if !ok {
		return status.FailedPreconditionErrorf("Cannot append value %v of type %T to flag of type %T.", slice, slice, ([]T)(nil))
	}
	v := (*[]T)(f)
	*v = append(*v, s...)
	return nil
}

func (f *SliceFlag[T]) AliasedType() reflect.Type {
	return reflect.TypeOf((*[]T)(nil))
}

func (f *SliceFlag[T]) YAMLTypeAlias() reflect.Type {
	return f.AliasedType()
}

type URLFlag url.URL

func URL(name string, value url.URL, usage string) *url.URL {
	u := &value
	common.DefaultFlagSet.Var((*URLFlag)(u), name, usage)
	return u
}

func URLVar(value *url.URL, name string, usage string) {
	common.DefaultFlagSet.Var((*URLFlag)(value), name, usage)
}

func URLFromString(name, value, usage string) *url.URL {
	u, err := url.Parse(value)
	if err != nil {
		log.Fatalf("Error parsing default URL value '%s' for flag: %v", value, err)
		return nil
	}
	return URL(name, *u, usage)
}

func (f *URLFlag) Set(value string) error {
	u, err := url.Parse(value)
	if err != nil {
		return err
	}
	*(*url.URL)(f) = *u
	return nil
}

func (f *URLFlag) String() string {
	return (*url.URL)(f).String()
}

func (f *URLFlag) UnmarshalYAML(value *yaml.Node) error {
	u, err := url.Parse(value.Value)
	if err != nil {
		return &yaml.TypeError{Errors: []string{err.Error()}}
	}
	*(*url.URL)(f) = *u
	return nil
}

func (f *URLFlag) MarshalYAML() (any, error) {
	return f.String(), nil
}

func (f *URLFlag) AliasedType() reflect.Type {
	return reflect.TypeOf((*url.URL)(nil))
}

func (f *URLFlag) YAMLTypeAlias() reflect.Type {
	return reflect.TypeOf((*URLFlag)(nil))
}

func (f *URLFlag) YAMLTypeString() string {
	return "URL"
}

type FlagAlias struct {
	name string
}

func Alias[T any](newName, name string) *T {
	f := &FlagAlias{name: name}
	var flg *flag.Flag
	for aliaser, ok := common.IsNameAliasing(f), true; ok; aliaser, ok = flg.Value.(common.IsNameAliasing) {
		if flg = common.DefaultFlagSet.Lookup(aliaser.AliasedName()); flg == nil {
			log.Fatalf("Error aliasing flag %s as %s: flag %s does not exist.", name, newName, aliaser.AliasedName())
		}
	}
	addr := reflect.ValueOf(flg.Value)
	if t, err := common.GetTypeForFlagValue(flg.Value); err == nil {
		if !addr.CanConvert(t) {
			log.Fatalf("Error aliasing flag %s as %s: Flag %s of type %T could not be converted to %s.", name, newName, flg.Name, flg.Value, t)
		}
		addr = addr.Convert(t)
	}
	value, ok := addr.Interface().(*T)
	if !ok {
		log.Fatalf("Error aliasing flag %s as %s: Failed to assert flag %s of type %T as type %T.", name, newName, flg.Name, flg.Value, (*T)(nil))
	}
	common.DefaultFlagSet.Var(f, newName, "Alias for "+name)
	return value
}

func (f *FlagAlias) Set(value string) error {
	return common.DefaultFlagSet.Set(f.name, value)
}

func (f *FlagAlias) String() string {
	return f.WrappedValue().String()
}

func (f *FlagAlias) AliasedName() string {
	return f.name
}

func (f *FlagAlias) WrappedValue() flag.Value {
	return common.DefaultFlagSet.Lookup(f.name).Value
}

type DeprecatedFlag struct {
	flag.Value
	name          string
	migrationPlan string
}

// DeprecatedVar takes a flag.Value (which can be obtained for primitive types
// via the NewPrimitiveFlag or NewPrimitiveFlagVar functions), the customary
// name and usage parameters, and a migration plan, and defines a flag that will
// notify users that it is deprecated when it is set.
//
// For example, if you wanted to deprecate a flag like this:
// var foo = flag.String("foo", "foo default value", "Use the specified foo.")
//
// You would redefine the flag as deprecated like this:
// var foo = DeprecatedVar[string](
//   NewPrimitiveFlag("foo default value"),
//   "foo",
//   "help text for foo",
//   "All of our foos were destroyed in a fire, please specify a bar instead.",
// )
func DeprecatedVar[T any](value flag.Value, name string, usage, migrationPlan string) *T {
	common.DefaultFlagSet.Var(&DeprecatedFlag{value, name, migrationPlan}, name, usage+" **DEPRECATED** "+migrationPlan)
	return reflect.ValueOf(value).Convert(reflect.TypeOf((*T)(nil))).Interface().(*T)
}

func (d *DeprecatedFlag) Set(value string) error {
	log.Warningf("Flag \"%s\" was set on the command line but has been deprecated: %s", d.name, d.migrationPlan)
	return d.Value.Set(value)
}

func (d *DeprecatedFlag) WrappedValue() flag.Value {
	return d.Value
}

func (d *DeprecatedFlag) SetValueForFlagNameHook() {
	log.Warningf("Flag \"%s\" was set programmatically by name but has been deprecated: %s", d.name, d.migrationPlan)
}

func (d *DeprecatedFlag) YAMLSetValueHook() {
	log.Warningf("Flag \"%s\" was set through the YAML config but has been deprecated: %s", d.name, d.migrationPlan)
}

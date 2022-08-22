package types

// For user-defined flag value types. New flag value types should be declared
// either as a type definition of the type they contain (see `StringSliceFlag`
// for an example) or a type definition of a `reflect.Value` which will itself
// wrap the desired value (see `JSONSliceFlag` for an example). Any new type
// should also be added to the `Var` function in the `autoflags` subpackage of
// this package.
//
// New flag value types should implement `common.TypeAliased` and `flag.Value`.

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

type JSONSliceFlag[T any] reflect.Value

func NewJSONSliceFlag[T any](slice *T) *JSONSliceFlag[T] {
	v := (JSONSliceFlag[T])(reflect.ValueOf(slice))
	return &v
}

func JSONSlice[T any](name string, defaultValue T, usage string) *T {
	value := reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Interface().(*T)
	JSONSliceVar(value, name, defaultValue, usage)
	return value
}

func JSONSliceVar[T any](value *T, name string, defaultValue T, usage string) {
	src := reflect.ValueOf(defaultValue)
	if src.Kind() != reflect.Slice {
		log.Fatalf("JSONSliceVar called for flag %s with non-slice value %v of type %T.", name, defaultValue, defaultValue)
	}
	v := reflect.ValueOf(value)
	if src.IsNil() && !v.Elem().IsNil() {
		v.Elem().Set(reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Elem())
	} else if v.Elem().Len() != src.Len() || v.Elem().IsNil() {
		v.Elem().Set(reflect.MakeSlice(reflect.TypeOf((*T)(nil)).Elem(), src.Len(), src.Len()))
	}
	reflect.Copy(v.Elem(), src)
	common.DefaultFlagSet.Var((*JSONSliceFlag[T])(&v), name, usage)
}

func (f *JSONSliceFlag[T]) String() string {
	b, err := json.Marshal((*reflect.Value)(f).Interface())
	if err != nil {
		alert.UnexpectedEvent("config_cannot_marshal_struct", "err: %s", err)
		return "[]"
	}
	return string(b)
}

func (f *JSONSliceFlag[T]) Set(values string) error {
	var a any
	if err := json.Unmarshal([]byte(values), &a); err != nil {
		return err
	}
	v := (reflect.Value)(*f).Elem()
	if _, ok := a.([]any); ok {
		dst := reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Interface()
		if err := json.Unmarshal([]byte(values), dst); err != nil {
			return err
		}
		v.Set(reflect.AppendSlice(v, reflect.ValueOf(dst).Elem()))
		return nil
	}
	if _, ok := a.(map[string]any); ok {
		dst := reflect.New(reflect.TypeOf((*T)(nil)).Elem().Elem()).Interface()
		if err := json.Unmarshal([]byte(values), dst); err != nil {
			return err
		}
		v.Set(reflect.Append(v, reflect.ValueOf(dst).Elem()))
		return nil
	}
	return fmt.Errorf("Default Set for SliceFlag can only accept JSON objects or arrays, but type was %T", a)
}

func (f *JSONSliceFlag[T]) AppendSlice(slice any) error {
	v := (reflect.Value)(*f)
	if _, ok := slice.(T); !ok {
		return status.FailedPreconditionErrorf("Cannot append value %v of type %T to flag of type %s.", slice, slice, v.Type().Elem())
	}
	v.Elem().Set(reflect.AppendSlice(v.Elem(), reflect.ValueOf(slice)))
	return nil
}

func (f *JSONSliceFlag[T]) AliasedType() reflect.Type {
	return reflect.TypeOf((*T)(nil))
}

func (f *JSONSliceFlag[T]) Slice() T {
	return *(reflect.Value)(*f).Interface().(*T)
}

type StringSliceFlag []string

func NewStringSliceFlag(slice *[]string) *StringSliceFlag {
	return (*StringSliceFlag)(slice)
}

func StringSlice(name string, defaultValue []string, usage string) *[]string {
	value := &[]string{}
	StringSliceVar(value, name, defaultValue, usage)
	return value
}

func StringSliceVar(value *[]string, name string, defaultValue []string, usage string) {
	if defaultValue == nil && *value != nil {
		*value = nil
	} else if len(*value) != len(defaultValue) || *value == nil {
		*value = make([]string, len(defaultValue))
	}
	copy(*value, defaultValue)
	common.DefaultFlagSet.Var((*StringSliceFlag)(value), name, usage)
}

func (f *StringSliceFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *StringSliceFlag) Set(values string) error {
	for _, val := range strings.Split(values, ",") {
		*f = append(*f, val)
	}
	return nil
}

func (f *StringSliceFlag) AppendSlice(slice any) error {
	s, ok := slice.([]string)
	if !ok {
		return status.FailedPreconditionErrorf("Cannot append value %v of type %T to flag of type []string.", slice, slice)
	}
	*f = append(*f, s...)
	return nil
}

func (f *StringSliceFlag) AliasedType() reflect.Type {
	return reflect.TypeOf((*[]string)(nil))
}

type URLFlag url.URL

func URL(name string, defaultValue url.URL, usage string) *url.URL {
	value := &url.URL{}
	URLVar(value, name, defaultValue, usage)
	return value
}

func URLVar(value *url.URL, name string, defaultValue url.URL, usage string) {
	*value = *defaultValue.ResolveReference(&url.URL{})
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

type FlagAlias[T any] struct {
	name string
}

// Alias defines a new name or names for the existing flag at the passed name
// and returns a pointer to the data backing it. If no new names are passed,
// Alias simply returns said pointer without creating any new alias flags.
func Alias[T any](name string, newNames ...string) *T {
	f := &FlagAlias[T]{name: name}
	var flg *flag.Flag
	for aliaser, ok := common.IsNameAliasing(f), true; ok; aliaser, ok = flg.Value.(common.IsNameAliasing) {
		if flg = common.DefaultFlagSet.Lookup(aliaser.AliasedName()); flg == nil {
			log.Fatalf("Error aliasing flag %s as %s: flag %s does not exist.", name, strings.Join(newNames, ", "), aliaser.AliasedName())
		}
	}
	converted, err := common.ConvertFlagValue(flg.Value)
	if err != nil {
		log.Fatalf("Error aliasing flag %s as %s: %v", name, strings.Join(newNames, ", "), err)
	}
	value, ok := converted.(*T)
	if !ok {
		log.Fatalf("Error aliasing flag %s as %s: Failed to assert flag %s of type %T as type %T.", name, strings.Join(newNames, ", "), flg.Name, flg.Value, (*T)(nil))
	}
	for _, newName := range newNames {
		common.DefaultFlagSet.Var(f, newName, "Alias for "+name)
	}
	return value
}

func (f *FlagAlias[T]) Set(value string) error {
	return common.DefaultFlagSet.Set(f.name, value)
}

func (f *FlagAlias[T]) String() string {
	if f == nil || (f.name == "" && f.WrappedValue() == nil) {
		return fmt.Sprint(common.Zero[T]())
	}
	return f.WrappedValue().String()
}

func (f *FlagAlias[T]) AliasedName() string {
	return f.name
}

func (f *FlagAlias[T]) WrappedValue() flag.Value {
	flg := common.DefaultFlagSet.Lookup(f.name)
	if flg == nil {
		return nil
	}
	return flg.Value
}

type DeprecatedFlag[T any] struct {
	flag.Value
	name          string
	MigrationPlan string
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
	common.DefaultFlagSet.Var(value, name, usage)
	Deprecate[T](name, migrationPlan)
	converted, err := common.ConvertFlagValue(value)
	if err != nil {
		log.Fatalf("Error creating deprecated flag %s: %v", name, err)
	}
	c, ok := converted.(*T)
	if !ok {
		log.Fatalf("Error creating deprecated flag %s: could not coerce flag of type %T to type %T.", name, converted, (*T)(nil))
	}
	return c
}

// Deprecate deprecates an existing flag by name; generally this should be
// called in an init func. While simpler to use than DeprecatedVar, it does
// decouple the flag declaration from the flag deprecation.
func Deprecate[T any](name, migrationPlan string) {
	flg := common.DefaultFlagSet.Lookup(name)
	converted, err := common.ConvertFlagValue(flg.Value)
	if err != nil {
		log.Fatalf("Error creating deprecated flag %s: %v", name, err)
	} else if _, ok := converted.(*T); !ok {
		log.Fatalf("Error creating deprecated flag %s: could not coerce flag of type %T to type %T.", name, converted, (*T)(nil))
	}
	flg.Value = &DeprecatedFlag[T]{
		Value:         flg.Value,
		name:          flg.Name,
		MigrationPlan: migrationPlan,
	}
	flg.Usage = flg.Usage + " **DEPRECATED** " + migrationPlan
}

func (d *DeprecatedFlag[T]) Set(value string) error {
	log.Warningf("Flag \"%s\" was set on the command line but has been deprecated: %s", d.name, d.MigrationPlan)
	return d.Value.Set(value)
}

func (d *DeprecatedFlag[T]) WrappedValue() flag.Value {
	return d.Value
}

func (d *DeprecatedFlag[T]) SetValueForFlagNameHook() {
	log.Warningf("Flag \"%s\" was set programmatically by name but has been deprecated: %s", d.name, d.MigrationPlan)
}

func (d *DeprecatedFlag[T]) YAMLSetValueHook() {
	log.Warningf("Flag \"%s\" was set through the YAML config but has been deprecated: %s", d.name, d.MigrationPlan)
}

func (d *DeprecatedFlag[T]) String() string {
	if d == nil || d.Value == nil {
		return fmt.Sprint(common.Zero[T]())
	}
	return d.Value.String()
}

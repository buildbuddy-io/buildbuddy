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
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types/autoflags/tags"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"gopkg.in/yaml.v3"
)

var UnwrapFlagValue = common.UnwrapFlagValue
var ConvertFlagValue = common.ConvertFlagValue

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

func JSONSlice[T any](flagset *flag.FlagSet, name string, defaultValue T, usage string) *T {
	value := reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Interface().(*T)
	JSONSliceVar(flagset, value, name, defaultValue, usage)
	return value
}

func JSONSliceVar[T any](flagset *flag.FlagSet, value *T, name string, defaultValue T, usage string) {
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
	flagset.Var((*JSONSliceFlag[T])(&v), name, usage)
}

func (f *JSONSliceFlag[T]) String() string {
	if !(*reflect.Value)(f).IsValid() || (*reflect.Value)(f).IsNil() {
		return "[]"
	}
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

func expandValue(value any, mapping func(string) (string, error)) (any, error) {
	switch cv := value.(type) {
	case map[string]any:
		if err := expandMapValues(cv, mapping); err != nil {
			return nil, err
		}
		return value, nil
	case []any:
		if err := expandSliceValues(cv, mapping); err != nil {
			return nil, err
		}
		return value, nil
	case string:
		ev, err := mapping(cv)
		if err != nil {
			return nil, err
		}
		return ev, nil
	default:
		return value, nil
	}
}

func expandSliceValues(slice []any, mapping func(string) (string, error)) error {
	for i, v := range slice {
		ev, err := expandValue(v, mapping)
		if err != nil {
			return err
		}
		slice[i] = ev
	}
	return nil
}

func expandMapValues(yamlMap map[string]any, mapping func(string) (string, error)) error {
	for k, v := range yamlMap {
		ev, err := expandValue(v, mapping)
		if err != nil {
			return err
		}
		yamlMap[k] = ev
	}
	return nil
}

func (f *JSONSliceFlag[T]) Expand(mapping func(string) (string, error)) error {
	var dst []any
	if err := json.Unmarshal([]byte(f.String()), &dst); err != nil {
		return err
	}
	if err := expandSliceValues(dst, mapping); err != nil {
		return err
	}
	exp, err := json.Marshal(dst)
	if err != nil {
		return err
	}
	sl := reflect.MakeSlice(reflect.TypeOf((*T)(nil)).Elem(), 0, 0)
	v := (reflect.Value)(*f)
	v.Elem().Set(sl)
	return f.Set(string(exp))
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

type JSONStructFlag[T any] reflect.Value

func NewJSONStructFlag[T any](value *T) *JSONStructFlag[T] {
	v := (JSONStructFlag[T])(reflect.ValueOf(value))
	return &v
}

func JSONStruct[T any](flagset *flag.FlagSet, name string, defaultValue T, usage string) *T {
	value := reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Interface().(*T)
	JSONStructVar(flagset, value, name, defaultValue, usage)
	return value
}

func JSONStructVar[T any](flagset *flag.FlagSet, value *T, name string, defaultValue T, usage string) {
	src := reflect.ValueOf(defaultValue)
	if src.Kind() != reflect.Struct {
		log.Fatalf("JSONStructVar called for flag %s with non-struct value %v of type %T.", name, defaultValue, defaultValue)
	}
	v := reflect.ValueOf(value)
	v.Elem().Set(reflect.ValueOf(defaultValue))
	flagset.Var((*JSONStructFlag[T])(&v), name, usage)
}

func (f *JSONStructFlag[T]) String() string {
	if !(*reflect.Value)(f).IsValid() || (*reflect.Value)(f).IsNil() {
		return "{}"
	}
	b, err := json.Marshal((*reflect.Value)(f).Interface())
	if err != nil {
		alert.UnexpectedEvent("config_cannot_marshal_struct", "err: %s", err)
		return "{}"
	}
	return string(b)
}

func (f *JSONStructFlag[T]) Set(values string) error {
	v := (reflect.Value)(*f).Elem()
	dst := reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Interface().(*T)
	if err := json.Unmarshal([]byte(values), dst); err != nil {
		return err
	}
	v.Set(reflect.ValueOf(*dst))
	return nil
}

func (f *JSONStructFlag[T]) Expand(mapping func(string) (string, error)) error {
	var ov any
	if err := json.Unmarshal([]byte(f.String()), &ov); err != nil {
		return err
	}
	nv, err := expandValue(ov, mapping)
	if err != nil {
		return err
	}
	exp, err := json.Marshal(nv)
	if err != nil {
		return err
	}
	return f.Set(string(exp))
}

func (f *JSONStructFlag[T]) AliasedType() reflect.Type {
	return reflect.TypeOf((*T)(nil))
}

func (f *JSONStructFlag[T]) Struct() T {
	return *(reflect.Value)(*f).Interface().(*T)
}

type StringSliceFlag []string

func NewStringSliceFlag(slice *[]string) *StringSliceFlag {
	return (*StringSliceFlag)(slice)
}

func StringSlice(flagset *flag.FlagSet, name string, defaultValue []string, usage string) *[]string {
	value := &[]string{}
	StringSliceVar(flagset, value, name, defaultValue, usage)
	return value
}

func StringSliceVar(flagset *flag.FlagSet, value *[]string, name string, defaultValue []string, usage string) {
	if defaultValue == nil && *value != nil {
		*value = nil
	} else if len(*value) != len(defaultValue) || *value == nil {
		*value = make([]string, len(defaultValue))
	}
	copy(*value, defaultValue)
	flagset.Var((*StringSliceFlag)(value), name, usage)
}

func (f *StringSliceFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *StringSliceFlag) Set(values string) error {
	if values == "" {
		return nil
	}
	for _, val := range strings.Split(values, ",") {
		*f = append(*f, val)
	}
	return nil
}

func (f *StringSliceFlag) Expand(mapping func(string) (string, error)) error {
	for i, v := range *f {
		val, err := mapping(v)
		if err != nil {
			return err
		}
		(*f)[i] = val
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

func URL(flagset *flag.FlagSet, name string, defaultValue url.URL, usage string) *url.URL {
	value := &url.URL{}
	URLVar(flagset, value, name, defaultValue, usage)
	return value
}

func URLVar(flagset *flag.FlagSet, value *url.URL, name string, defaultValue url.URL, usage string) {
	*value = *defaultValue.ResolveReference(&url.URL{})
	flagset.Var((*URLFlag)(value), name, usage)
}

func URLFromString(flagset *flag.FlagSet, name, value, usage string) *url.URL {
	u, err := url.Parse(value)
	if err != nil {
		log.Fatalf("Error parsing default URL value '%s' for flag: %v", value, err)
		return nil
	}
	return URL(flagset, name, *u, usage)
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

// Alias defines a new name or names for the existing flag at the passed name
// and returns a pointer to the data backing it. If no new names are passed,
// Alias simply returns said pointer without creating any new alias flags.
func Alias[T any](flagset *flag.FlagSet, name string, newNames ...string) *T {
	switch v := any((*T)(nil)).(type) {
	case *bool:
		return tags.Tag[T, flag.Value](flagset, name, tags.AliasTag(newNames...))
	case *time.Duration:
		return tags.Tag[T, flag.Value](flagset, name, tags.AliasTag(newNames...))
	case *float64:
		return tags.Tag[T, flag.Value](flagset, name, tags.AliasTag(newNames...))
	case *int:
		return tags.Tag[T, flag.Value](flagset, name, tags.AliasTag(newNames...))
	case *int64:
		return tags.Tag[T, flag.Value](flagset, name, tags.AliasTag(newNames...))
	case *uint:
		return tags.Tag[T, flag.Value](flagset, name, tags.AliasTag(newNames...))
	case *uint64:
		return tags.Tag[T, flag.Value](flagset, name, tags.AliasTag(newNames...))
	case *string:
		return tags.Tag[T, flag.Value](flagset, name, tags.AliasTag(newNames...))
	case *[]string:
		return tags.Tag[T, *StringSliceFlag](flagset, name, tags.AliasTag(newNames...))
	case *url.URL:
		return tags.Tag[T, *URLFlag](flagset, name, tags.AliasTag(newNames...))
	default:
		if reflect.TypeOf(v).Elem().Kind() == reflect.Slice {
			return tags.Tag[T, *JSONSliceFlag[T]](flagset, name, tags.AliasTag(newNames...))
		}
		if reflect.TypeOf(v).Elem().Kind() == reflect.Struct {
			return tags.Tag[T, *JSONStructFlag[T]](flagset, name, tags.AliasTag(newNames...))
		}
		log.Fatalf("Alias was called from flag registry for flag %s with unrecognized parameterized type %T.", name, common.Zero[T]())
	}
	return nil
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
// var foo = DeprecatedVar[string, flag.Value](
//
//	NewPrimitiveFlag("foo default value"),
//	"foo",
//	"help text for foo",
//	"All of our foos were destroyed in a fire, please specify a bar instead.",
//
// )
func DeprecatedVar[T any, FV flag.Value](flagset *flag.FlagSet, value FV, name string, usage, migrationPlan string) *T {
	flagset.Var(value, name, usage)
	Deprecate[T, FV](flagset, name, migrationPlan)
	converted, err := common.ConvertFlagValue(flagset.Lookup(name).Value)
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
func Deprecate[T any, FV flag.Value](flagset *flag.FlagSet, name, migrationPlan string) {
	tags.Tag[T, FV](flagset, name, tags.DeprecatedTag(migrationPlan))
}

func Secret[T any, FV flag.Value](flagset *flag.FlagSet, name string) {
	tags.Tag[T, FV](flagset, name, tags.SecretTag)
}

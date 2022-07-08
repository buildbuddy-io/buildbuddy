package common

import (
	"flag"
	"reflect"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"gopkg.in/yaml.v3"
)

var (
	// Used for type conversions between flags and normal go types
	flagTypeMap = map[reflect.Type]reflect.Type{
		flagTypeFromFlagFuncName("Bool"):     reflect.TypeOf((*bool)(nil)),
		flagTypeFromFlagFuncName("Duration"): reflect.TypeOf((*time.Duration)(nil)),
		flagTypeFromFlagFuncName("Float64"):  reflect.TypeOf((*float64)(nil)),
		flagTypeFromFlagFuncName("Int"):      reflect.TypeOf((*int)(nil)),
		flagTypeFromFlagFuncName("Int64"):    reflect.TypeOf((*int64)(nil)),
		flagTypeFromFlagFuncName("Uint"):     reflect.TypeOf((*uint)(nil)),
		flagTypeFromFlagFuncName("Uint64"):   reflect.TypeOf((*uint64)(nil)),
		flagTypeFromFlagFuncName("String"):   reflect.TypeOf((*string)(nil)),
	}

	// Change only for testing purposes
	DefaultFlagSet = flag.CommandLine
)

func flagTypeFromFlagFuncName(name string) reflect.Type {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	ff := reflect.ValueOf(fs).MethodByName(name)
	in := make([]reflect.Value, ff.Type().NumIn())
	for i := range in {
		in[i] = reflect.New(ff.Type().In(i)).Elem()
	}
	ff.Call(in)
	return reflect.TypeOf(fs.Lookup("").Value)
}

type TypeAliased interface {
	// AliasedType returns the type this flag.Value aliases.
	AliasedType() reflect.Type
}

type IsNameAliasing interface {
	// AliasedName returns the flag name this flag.Value aliases.
	AliasedName() string
}

type WrappingValue interface {
	// WrappedValue returns the value this flag.Value wraps.
	WrappedValue() flag.Value
}

type Appendable interface {
	// AppendSlice appends the passed slice to this flag.Value.
	AppendSlice(any) error
}

type DocumentNodeOption interface {
	// Transform transforms the passed yaml.Node in place.
	Transform(in any, n *yaml.Node)
	// Passthrough returns whether this option should be passed to child nodes.
	Passthrough() bool
}

type SetValueForFlagNameHooked interface {
	// SetValueForFlagNameHooked is the hook for flags that is called when the
	// flag.Value is set by name.
	SetValueForFlagNameHook()
}

// GetTypeForFlagValue returns the (pointer) Type this flag Value aliases; this
// is the same type returned when defining the flag initially.
func GetTypeForFlagValue(value flag.Value) (reflect.Type, error) {
	if v, ok := value.(WrappingValue); ok {
		return GetTypeForFlagValue(v.WrappedValue())
	}
	if t, ok := flagTypeMap[reflect.TypeOf(value)]; ok {
		return t, nil
	} else if v, ok := value.(TypeAliased); ok {
		return v.AliasedType(), nil
	}
	return nil, status.UnimplementedErrorf("Unsupported flag type : %T", value)
}

// ConvertFlagValue returns the data this flag Value contains, converted to the
// same type as is returned when defining the flag initially.
func ConvertFlagValue(value flag.Value) (any, error) {
	addr := reflect.ValueOf(value)
	for v, ok := value.(WrappingValue); ok; v, ok = v.WrappedValue().(WrappingValue) {
		addr = reflect.ValueOf(v.WrappedValue())
	}
	t, err := GetTypeForFlagValue(value)
	if err != nil {
		return nil, err
	}
	if !addr.CanConvert(t) {
		return nil, status.InternalErrorf("Flag of type %T could not be converted to %s.", value, t)
	}
	addr = addr.Convert(t)
	return addr.Interface(), nil
}

// SetValueForFlagName sets the value for a flag by name. setFlags is the set of
// flags that have already been set on the command line; those flags will not be
// set again except to append to them, in the case of slices. To force the
// setting of a flag, pass a nil map. If appendSlice is true, a slice value will
// be appended to the current slice value; otherwise, a slice value will replace
// the current slice value. appendSlice has no effect if the values in question
// are not slices.
func SetValueForFlagName(name string, newValue any, setFlags map[string]struct{}, appendSlice bool) error {
	flg := DefaultFlagSet.Lookup(name)
	if flg == nil {
		return status.NotFoundErrorf("Undefined flag: %s", name)
	}
	return setValueFromFlagName(flg.Value, name, newValue, setFlags, appendSlice)
}

func setValueFromFlagName(flagValue flag.Value, name string, newValue any, setFlags map[string]struct{}, appendSlice bool, setHooks ...func()) error {
	if v, ok := flagValue.(SetValueForFlagNameHooked); ok {
		setHooks = append(setHooks, v.SetValueForFlagNameHook)
	}
	return SetValueWithCustomIndirectBehavior(flagValue, name, newValue, setFlags, appendSlice, setValueFromFlagName, setHooks...)
}

type SetValueForIndirectFxn func(flagValue flag.Value, name string, newValue any, setFlags map[string]struct{}, appendSlice bool, setHooks ...func()) error

// SetValueWithCustomIndirectBehavior sets the value for a flag, but if the flag
// passed is an alias for another flag or wraps another flag.Value, it instead
// calls setValueForIndirect with the new flag.Value. setFlags is the set of
// flags that have already been set on the command line; those flags will not be
// set again except to append to them, in the case of slices. To force the
// setting of a flag, pass a nil map. If appendSlice is true, a slice value will
// be appended to the current slice value; otherwise, a slice value will replace
// the current slice value. appendSlice has no effect if the values in question
// are not slices. setHooks is a slice of functions to call in order if the
// flag.Value will be set.
func SetValueWithCustomIndirectBehavior(flagValue flag.Value, name string, newValue any, setFlags map[string]struct{}, appendSlice bool, setValueForIndirect SetValueForIndirectFxn, setHooks ...func()) error {
	if v, ok := flagValue.(IsNameAliasing); ok {
		aliasedFlag := DefaultFlagSet.Lookup(v.AliasedName())
		if aliasedFlag == nil {
			return status.NotFoundErrorf("Flag %s aliases undefined flag: %s", name, v.AliasedName())
		}
		return setValueForIndirect(aliasedFlag.Value, v.AliasedName(), newValue, setFlags, appendSlice, setHooks...)
	}
	// Unwrap any wrapper values (e.g. DeprecatedFlag)
	if v, ok := flagValue.(WrappingValue); ok {
		return setValueForIndirect(v.WrappedValue(), name, newValue, setFlags, appendSlice, setHooks...)
	}
	var appendFlag Appendable
	// For slice flags, append the values to the existing values if appendSlice is true
	if v, ok := flagValue.(Appendable); ok && appendSlice {
		appendFlag = v
	}
	// For non-append flags, skip the value if it has already been set
	if _, ok := setFlags[name]; appendFlag == nil && ok {
		return nil
	}
	for _, setHook := range setHooks {
		setHook()
	}
	if appendFlag != nil {
		if err := appendFlag.AppendSlice(newValue); err != nil {
			return status.InternalErrorf("Error encountered appending to flag %s: %s", name, err)
		}
		return nil
	}
	t, err := GetTypeForFlagValue(flagValue)
	if err != nil {
		return status.UnimplementedErrorf("Error encountered setting flag %s: %s", name, err)
	}
	if !reflect.ValueOf(newValue).CanConvert(t.Elem()) {
		return status.FailedPreconditionErrorf("Cannot convert value %v of type %T into type %v for flag %s.", newValue, newValue, t.Elem(), name)
	}
	reflect.ValueOf(flagValue).Convert(t).Elem().Set(reflect.ValueOf(newValue).Convert(t.Elem()))
	return nil
}

// GetDereferencedValue retypes and returns the dereferenced Value for
// a given flag name.
func GetDereferencedValue[T any](name string) (T, error) {
	flg := DefaultFlagSet.Lookup(name)
	if flg == nil {
		return *Zero[T](), status.NotFoundErrorf("Undefined flag: %s", name)
	}
	return getDereferencedValueFrom[T](flg.Value, flg.Name)
}

func getDereferencedValueFrom[T any](value flag.Value, name string) (T, error) {
	if v, ok := value.(IsNameAliasing); ok {
		return GetDereferencedValue[T](v.AliasedName())
	}
	converted, err := ConvertFlagValue(value)
	if err != nil {
		return *Zero[T](), status.InternalErrorf("Error dereferencing flag %s: %v", name, err)
	}
	t := reflect.TypeOf((*T)(nil))
	if t == reflect.TypeOf((*any)(nil)) {
		return reflect.ValueOf(converted).Elem().Interface().(T), nil
	}
	v, ok := converted.(*T)
	if !ok {
		return *Zero[T](), status.InternalErrorf("Failed to assert flag %s of type %T as type %s.", name, converted, t)
	}
	return *v, nil
}

// Zero returns a pointer to a zero-value of the provided type.
func Zero[T any]() *T {
	return reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Interface().(*T)
}

// AddTestFlagTypeForTesting adds a type correspondence to the internal
// flagTypeMap.
func AddTestFlagTypeForTesting(flagValue, value any) {
	flagTypeMap[reflect.TypeOf(flagValue)] = reflect.TypeOf(value)
}

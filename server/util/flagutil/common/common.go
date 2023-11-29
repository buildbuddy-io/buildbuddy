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

// Expandable may be implemented by custom flag types to indicate that they
// wish to handle placeholder expansion on their contents themselves.
type Expandable interface {
	Expand(mapping func(string) (string, error)) error
}

type Secretable interface {
	IsSecret() bool
}

type DocumentNodeOption interface {
	// Transform transforms the passed yaml.Node. It returns nil if the node
	// should not be included in the documented YAML.
	Transform(in any, n *yaml.Node, flg *flag.Flag) (*yaml.Node, error)
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

// UnwrapFlagValue resolves the given flag.Value to its underlying flag.Value
// type, iteratively unwrapping wrapper types like DeprecatedFlag or FlagAlias
// if they are present.
func UnwrapFlagValue(value flag.Value) flag.Value {
	for v, ok := value.(WrappingValue); ok; v, ok = v.WrappedValue().(WrappingValue) {
		value = v.WrappedValue()
	}
	return value
}

// ConvertFlagValue returns the data this flag Value contains, converted to the
// same type as is returned when defining the flag initially.
func ConvertFlagValue(value flag.Value) (any, error) {
	addr := reflect.ValueOf(UnwrapFlagValue(value))
	t, err := GetTypeForFlagValue(value)
	if err != nil {
		return nil, err
	}
	if addr.CanConvert(reflect.TypeOf((*reflect.Value)(nil))) {
		addr = *addr.Convert(reflect.TypeOf((*reflect.Value)(nil))).Interface().(*reflect.Value)
		if !addr.CanConvert(t) {
			return nil, status.InternalErrorf("Flag of type %T with concrete type *reflect.Value wrapping type %T could not be converted to %s.", value, addr.Interface(), t)
		}
	} else if !addr.CanConvert(t) {
		return nil, status.InternalErrorf("Flag of type %T with value %v could not be converted to %s.", value, value, t)
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
func SetValueForFlagName(flagset *flag.FlagSet, name string, newValue any, setFlags map[string]struct{}, appendSlice bool) error {
	flg := DefaultFlagSet.Lookup(name)
	if flg == nil {
		return status.NotFoundErrorf("Undefined flag: %s", name)
	}
	return setValueFromFlagName(flagset, flg.Value, name, newValue, setFlags, appendSlice)
}

func setValueFromFlagName(flagset *flag.FlagSet, flagValue flag.Value, name string, newValue any, setFlags map[string]struct{}, appendSlice bool, setHooks ...func()) error {
	if v, ok := flagValue.(SetValueForFlagNameHooked); ok {
		setHooks = append(setHooks, v.SetValueForFlagNameHook)
	}
	return SetValueWithCustomIndirectBehavior(flagset, flagValue, name, newValue, setFlags, appendSlice, setValueFromFlagName, setHooks...)
}

type SetValueForIndirectFxn func(flagset *flag.FlagSet, flagValue flag.Value, name string, newValue any, setFlags map[string]struct{}, appendSlice bool, setHooks ...func()) error

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
func SetValueWithCustomIndirectBehavior(flagset *flag.FlagSet, flagValue flag.Value, name string, newValue any, setFlags map[string]struct{}, appendSlice bool, setValueForIndirect SetValueForIndirectFxn, setHooks ...func()) error {
	if v, ok := flagValue.(IsNameAliasing); ok {
		aliasedFlag := flagset.Lookup(v.AliasedName())
		if aliasedFlag == nil {
			return status.NotFoundErrorf("Flag %s aliases undefined flag: %s", name, v.AliasedName())
		}
		return setValueForIndirect(flagset, aliasedFlag.Value, v.AliasedName(), newValue, setFlags, appendSlice, setHooks...)
	}
	// Unwrap any wrapper values (e.g. DeprecatedFlag)
	if v, ok := flagValue.(WrappingValue); ok {
		return setValueForIndirect(flagset, v.WrappedValue(), name, newValue, setFlags, appendSlice, setHooks...)
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
	converted, err := ConvertFlagValue(flagValue)
	if err != nil {
		return status.UnimplementedErrorf("Error encountered setting flag %s: %s", name, err)
	}
	v := reflect.ValueOf(converted).Elem()
	t := v.Type()
	if !reflect.ValueOf(newValue).CanConvert(t) {
		return status.FailedPreconditionErrorf("Cannot convert value %v of type %T into type %v for flag %s.", newValue, newValue, t, name)
	}
	v.Set(reflect.ValueOf(newValue).Convert(t))
	return nil
}

// GetDereferencedValue retypes and returns the dereferenced Value for
// a given flag name.
func GetDereferencedValue[T any](flagset *flag.FlagSet, name string) (T, error) {
	flg := flagset.Lookup(name)
	if flg == nil {
		return Zero[T](), status.NotFoundErrorf("Undefined flag: %s", name)
	}
	return getDereferencedValueFrom[T](flagset, flg.Value, flg.Name)
}

func getDereferencedValueFrom[T any](flagset *flag.FlagSet, value flag.Value, name string) (T, error) {
	if v, ok := value.(IsNameAliasing); ok {
		return GetDereferencedValue[T](flagset, v.AliasedName())
	}
	converted, err := ConvertFlagValue(value)
	if err != nil {
		return Zero[T](), status.InternalErrorf("Error dereferencing flag %s: %v", name, err)
	}
	t := reflect.TypeOf((*T)(nil))
	if t == reflect.TypeOf((*any)(nil)) {
		return reflect.ValueOf(converted).Elem().Interface().(T), nil
	}
	v, ok := converted.(*T)
	if !ok {
		return Zero[T](), status.InternalErrorf("Failed to assert flag %s of type %T as type %s.", name, converted, t)
	}
	return *v, nil
}

// Zero returns a zero-value of the provided type.
func Zero[T any]() T {
	return *reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Interface().(*T)
}

// ResetFlags resets all flags to their default values, as specified by
// the string stored in the corresponding flag.DefValue.
func ResetFlags(flagset *flag.FlagSet) error {
	errors := []error{}
	flagset.VisitAll(func(flg *flag.Flag) {
		if err := setWithOverride(flagset, flg.Value, flg.Name, flg.DefValue, true); err != nil {
			errors = append(errors, status.InternalErrorf("Error resetting flag %s: %s", flg.Name, err))
			return
		}
	})
	if len(errors) > 0 {
		return status.InternalErrorf("Errors encountered when resetting flags: %v", errors)
	}
	return nil
}

// SetWithOverride sets the flag's value by creating a new, empty flag.Value of
// the same type as the flag Value specified by name, calling
// `Set.(newValueString)` on the new flag.Value, and then explicitly setting the
// data pointed to by flagValue to the data pointed to by the new flag value.
func SetWithOverride(flagset *flag.FlagSet, name, newValueString string) error {
	flg := flagset.Lookup(name)
	if flg == nil {
		return status.NotFoundErrorf("Error when attempting to override flag %s: Flag does not exist.", name)
	}
	flagValue := flg.Value

	return setWithOverride(flagset, flagValue, name, newValueString, false)
}

func setWithOverride(flagset *flag.FlagSet, flagValue flag.Value, name, newValueString string, skipWrappers bool) error {
	// Unwrap the value to ensure we have the real flag.Value, not a wrapper like,
	// for example, DeprecatedFlag or FlagAlias.
	unwrapped := UnwrapFlagValue(flagValue)

	// Make a new empty flag.value of the appropriate type so it can be set
	// fresh. This allows us to override the flag while still using the standard
	// flag.Value interface's Set method, so it can be set with newValueString.
	blankFlagValue := reflect.New(reflect.TypeOf(unwrapped).Elem()).Interface().(flag.Value)

	if reflect.ValueOf(blankFlagValue).CanConvert(reflect.TypeOf((*reflect.Value)(nil))) {
		t, err := GetTypeForFlagValue(unwrapped)
		if err != nil {
			return status.InternalErrorf("Error getting type for copy of flag %s: %s", name, err)
		}

		// Set the blank flag value to the zero value for flag values which alias
		// reflect.Value instead of aliasing their value directly (such as, for
		// example, JSONSliceFlag and JSONStructFlag) in order to correctly
		// initialize them.
		blankValueAddr := reflect.ValueOf(blankFlagValue).Convert(reflect.TypeOf((*reflect.Value)(nil))).Interface().(*reflect.Value)
		*blankValueAddr = reflect.New(t.Elem())
	}

	if err := blankFlagValue.Set(newValueString); err != nil {
		return status.InternalErrorf("Error setting copy of flag %s to %s: %s", name, newValueString, err)
	}
	if blankFlagValue.String() == unwrapped.String() {
		// The values are the same, no need to set the flag.
		return nil
	}

	// Take the blank value and convert it to the underlying type
	// (the type which would be returned when defining the flag initially).
	newValue, err := ConvertFlagValue(blankFlagValue)
	if err != nil {
		return status.InternalErrorf("Error converting copy of flag %s: %s", name, err)
	}
	value := flagValue
	if skipWrappers {
		value = unwrapped
	}
	if err := setValueFromFlagName(flagset, value, name, reflect.Indirect(reflect.ValueOf(newValue)).Interface(), map[string]struct{}{}, false); err != nil {
		return err
	}
	return nil
}

// AddTestFlagTypeForTesting adds a type correspondence to the internal
// flagTypeMap.
func AddTestFlagTypeForTesting(flagValue, value any) {
	flagTypeMap[reflect.TypeOf(flagValue)] = reflect.TypeOf(value)
}

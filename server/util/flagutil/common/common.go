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
	AliasedType() reflect.Type
}

type IsNameAliasing interface {
	AliasedName() string
}

type WrappingValue interface {
	WrappedValue() flag.Value
}

type Appendable interface {
	AppendSlice(any) error
}

type DocumentNodeOption interface {
	Transform(in any, n *yaml.Node)
	Passthrough() bool
}

type SetValueHooked interface {
	SetValueHook()
}

// GetTypeForFlagValue returns the (pointer) Type this flag aliases; this is the same
// type returned when defining the flag initially.
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

// SetValueForFlagName sets the value for a flag by name.
func SetValueForFlagName(name string, i any, setFlags map[string]struct{}, appendSlice bool, strict bool) error {
	flg := DefaultFlagSet.Lookup(name)
	if flg == nil {
		if strict {
			return status.NotFoundErrorf("Undefined flag: %s", name)
		}
		return nil
	}
	return setValue(flg.Value, name, i, setFlags, appendSlice)
}

func setValue(value flag.Value, name string, i any, setFlags map[string]struct{}, appendSlice bool) error {
	var appendFlag Appendable
	// For slice flags, append the YAML values to the existing values if appendSlice is true
	if v, ok := value.(Appendable); ok && appendSlice {
		appendFlag = v
	}
	// For non-append flags, skip the YAML values if it was set on the command line
	if _, ok := setFlags[name]; appendFlag == nil && ok {
		return nil
	}
	if v, ok := value.(SetValueHooked); ok {
		v.SetValueHook()
	}
	if v, ok := value.(IsNameAliasing); ok {
		return SetValueForFlagName(v.AliasedName(), i, setFlags, appendSlice, true)
	}
	// Unwrap any wrapper values (e.g. DeprecatedFlag)
	if v, ok := value.(WrappingValue); ok {
		return setValue(v.WrappedValue(), name, i, setFlags, appendSlice)
	}
	if appendFlag != nil {
		if err := appendFlag.AppendSlice(i); err != nil {
			return status.InternalErrorf("Error encountered appending to flag %s: %s", name, err)
		}
		return nil
	}
	t, err := GetTypeForFlagValue(value)
	if err != nil {
		return status.UnimplementedErrorf("Error encountered setting flag %s: %s", name, err)
	}
	if !reflect.ValueOf(i).CanConvert(t.Elem()) {
		return status.FailedPreconditionErrorf("Cannot convert value %v of type %T into type %v for flag %s.", i, i, t.Elem(), name)
	}
	reflect.ValueOf(value).Convert(t).Elem().Set(reflect.ValueOf(i).Convert(t.Elem()))
	return nil
}

// GetDereferencedValue retypes and returns the dereferenced Value for
// a given flag name.
func GetDereferencedValue[T any](name string) (T, error) {
	flg := DefaultFlagSet.Lookup(name)
	zeroT := reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Interface().(*T)
	if flg == nil {
		return *zeroT, status.NotFoundErrorf("Undefined flag: %s", name)
	}
	return getDereferencedValueFrom[T](flg.Value, flg.Name)
}

func getDereferencedValueFrom[T any](value flag.Value, name string) (T, error) {
	zeroT := reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Interface().(*T)
	if v, ok := value.(IsNameAliasing); ok {
		return GetDereferencedValue[T](v.AliasedName())
	}
	// Unwrap any wrapper values (e.g. DeprecatedFlag)
	if v, ok := value.(WrappingValue); ok {
		return getDereferencedValueFrom[T](v.WrappedValue(), name)
	}
	t := reflect.TypeOf((*T)(nil))
	addr := reflect.ValueOf(value)
	if t == reflect.TypeOf((*any)(nil)) {
		var err error
		t, err = GetTypeForFlagValue(value)
		if err != nil {
			return *zeroT, status.InternalErrorf("Error dereferencing flag %s to unspecified type: %s.", name, err)
		}
		if !addr.CanConvert(t) {
			return *zeroT, status.InvalidArgumentErrorf("Flag %s of type %T could not be converted to %s.", name, value, t)
		}
		return addr.Convert(t).Elem().Interface().(T), nil
	}
	if !addr.CanConvert(t) {
		return *zeroT, status.InvalidArgumentErrorf("Flag %s of type %T could not be converted to %s.", name, value, t)
	}
	v, ok := addr.Convert(t).Interface().(*T)
	if !ok {
		return *zeroT, status.InternalErrorf("Failed to assert flag %s of type %T as type %s.", name, value, t)
	}
	return *v, nil
}

// AddTestFlagTypeForTesting adds a type correspondence to the internal
// flagTypeMap.
func AddTestFlagTypeForTesting(flagValue, value any) {
	flagTypeMap[reflect.TypeOf(flagValue)] = reflect.TypeOf(value)
}

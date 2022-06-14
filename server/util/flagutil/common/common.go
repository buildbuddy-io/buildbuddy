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

type Appendable interface {
	AppendSlice(any) error
}

type DocumentNodeOption interface {
	Transform(in any, n *yaml.Node)
	Passthrough() bool
}

// GetTypeForFlag returns the (pointer) Type this flag aliases; this is the same
// type returned when defining the flag initially.
func GetTypeForFlag(flg *flag.Flag) (reflect.Type, error) {
	if t, ok := flagTypeMap[reflect.TypeOf(flg.Value)]; ok {
		return t, nil
	} else if v, ok := flg.Value.(TypeAliased); ok {
		return v.AliasedType(), nil
	}
	return nil, status.UnimplementedErrorf("Unsupported flag type at %s: %T", flg.Name, flg.Value)
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
	// For slice flags, append the YAML values to the existing values if appendSlice is true
	if v, ok := flg.Value.(Appendable); ok && appendSlice {
		if err := v.AppendSlice(i); err != nil {
			return status.InternalErrorf("Error encountered appending to flag %s: %s", flg.Name, err)
		}
		return nil
	}
	if v, ok := flg.Value.(IsNameAliasing); ok {
		return SetValueForFlagName(v.AliasedName(), i, setFlags, appendSlice, strict)
	}
	// For non-append flags, skip the YAML values if it was set on the command line
	if _, ok := setFlags[name]; ok {
		return nil
	}
	t, err := GetTypeForFlag(flg)
	if err != nil {
		return status.UnimplementedErrorf("Error encountered setting flag: %s", err)
	}
	if !reflect.ValueOf(i).CanConvert(t.Elem()) {
		return status.FailedPreconditionErrorf("Cannot convert value %v of type %T into type %v for flag %s.", i, i, t.Elem(), flg.Name)
	}
	reflect.ValueOf(flg.Value).Convert(t).Elem().Set(reflect.ValueOf(i).Convert(t.Elem()))
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
	if v, ok := flg.Value.(IsNameAliasing); ok {
		return GetDereferencedValue[T](v.AliasedName())
	}
	t := reflect.TypeOf((*T)(nil))
	addr := reflect.ValueOf(flg.Value)
	if t == reflect.TypeOf((*any)(nil)) {
		var err error
		t, err = GetTypeForFlag(flg)
		if err != nil {
			return *zeroT, status.InternalErrorf("Error dereferencing flag to unspecified type: %s.", err)
		}
		if !addr.CanConvert(t) {
			return *zeroT, status.InvalidArgumentErrorf("Flag %s of type %T could not be converted to %s.", name, flg.Value, t)
		}
		return addr.Convert(t).Elem().Interface().(T), nil
	}
	if !addr.CanConvert(t) {
		return *zeroT, status.InvalidArgumentErrorf("Flag %s of type %T could not be converted to %s.", name, flg.Value, t)
	}
	v, ok := addr.Convert(t).Interface().(*T)
	if !ok {
		return *zeroT, status.InternalErrorf("Failed to assert flag %s of type %T as type %s.", name, flg.Value, t)
	}
	return *v, nil
}

// AddTestFlagTypeForTesting adds a type correspondence to the internal
// flagTypeMap.
func AddTestFlagTypeForTesting(flagValue, value any) {
	flagTypeMap[reflect.TypeOf(flagValue)] = reflect.TypeOf(value)
}

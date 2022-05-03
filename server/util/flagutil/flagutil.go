package flagutil

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	// Change only for testing purposes
	defaultFlagSet = flag.CommandLine

	// Used for type conversions between flags and YAML
	flagTypeMap = map[reflect.Type]reflect.Type{
		flagTypeFromFlagFuncName("Bool"):     reflect.TypeOf((*bool)(nil)),
		flagTypeFromFlagFuncName("Duration"): reflect.TypeOf((*time.Duration)(nil)),
		flagTypeFromFlagFuncName("Float64"):  reflect.TypeOf((*float64)(nil)),
		flagTypeFromFlagFuncName("Int"):      reflect.TypeOf((*int)(nil)),
		flagTypeFromFlagFuncName("Int64"):    reflect.TypeOf((*int64)(nil)),
		flagTypeFromFlagFuncName("Uint"):     reflect.TypeOf((*uint)(nil)),
		flagTypeFromFlagFuncName("Uint64"):   reflect.TypeOf((*uint64)(nil)),
		flagTypeFromFlagFuncName("String"):   reflect.TypeOf((*string)(nil)),

		reflect.TypeOf((*stringSliceFlag)(nil)): reflect.TypeOf((*[]string)(nil)),
		reflect.TypeOf((*URLFlag)(nil)):         reflect.TypeOf((*URLFlag)(nil)),
	}
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

// TODO: When we get generics, we can replace these function with Slice and use
// it for all types of slices (currently just strings and structs).
func StringSlice(name string, defaultValue []string, usage string) *[]string {
	sliceFlag, err := NewSliceFlag(&defaultValue)
	if err != nil {
		log.Fatalf("Encountered error creating flag for %s: %v", name, err)
	}
	defaultFlagSet.Var(sliceFlag, name, usage)
	return &defaultValue
}

func StructSliceVar(structSlicePtr interface{}, name, usage string) {
	sliceFlag, err := NewSliceFlag(structSlicePtr)
	if err != nil {
		log.Fatalf("Encountered error creating flag for %s: %v", name, err)
	}
	defaultFlagSet.Var(sliceFlag, name, usage)
}

// String flag that gets validated as a URL
// TODO: just use the URL directly (not the string) once we can generate the
// YAML map from the flags instead of the other way around.
func URLString(name, value, usage string) *string {
	u := NewURLFlag(&value)
	defaultFlagSet.Var(u, name, usage)
	return &value
}

// NOTE: slice flags are *appended* to default values and
// config values, instead of overriding them completely.
type SliceFlag interface {
	flag.Value
	UnderlyingSlice() interface{}
	SetTo(interface{})
	Slice(i, j int) interface{}
	AppendSlice(interface{}) interface{}
	Len() int
}

func NewSliceFlag(slicePtr interface{}) (SliceFlag, error) {
	if stringSlicePtr, ok := slicePtr.(*[]string); ok {
		return newStringSliceFlag(stringSlicePtr), nil
	}
	if reflect.TypeOf(slicePtr).Elem().Elem().Kind() == reflect.Struct {
		return newStructSliceFlag(slicePtr), nil
	}
	return nil, fmt.Errorf("Unrecognized slice pointer type in NewSliceFlag: %T", slicePtr)
}

type stringSliceFlag []string

func (f *stringSliceFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *stringSliceFlag) Set(values string) error {
	for _, val := range strings.Split(values, ",") {
		*f = append(*f, val)
	}
	return nil
}

func (f *stringSliceFlag) UnderlyingSlice() interface{} {
	slice := make([]string, len(*f))
	copy(slice, *f)
	return slice
}

func (f *stringSliceFlag) SetTo(stringSlice interface{}) {
	ss, ok := stringSlice.([]string)
	if !ok {
		alert.UnexpectedEvent("string_slice_flag_type_error", "SetTo accepts only []string, but was passed parameter of type %T", stringSlice)
		return
	}
	*f = make([]string, len(ss))
	copy(*f, ss)
}

func (f *stringSliceFlag) Slice(i, j int) interface{} {
	return f.UnderlyingSlice().([]string)[i:j]
}

func (f *stringSliceFlag) AppendSlice(stringSlice interface{}) interface{} {
	ss, ok := stringSlice.([]string)
	if !ok {
		alert.UnexpectedEvent("string_slice_flag_type_error", "SetTo accepts only []string, but was passed parameter of type %T", stringSlice)
		return []string{}
	}
	return append(f.UnderlyingSlice().([]string), ss...)
}

func (f *stringSliceFlag) Len() int {
	return len(*f)
}

func newStringSliceFlag(stringSlicePtr *[]string) *stringSliceFlag {
	return (*stringSliceFlag)(stringSlicePtr)
}

type structSliceFlag struct {
	dstSlice reflect.Value
}

func (f *structSliceFlag) String() string {
	if !f.dstSlice.IsValid() {
		return "[]"
	}
	b, err := json.Marshal(f.dstSlice.Interface())
	if err != nil {
		alert.UnexpectedEvent("config_cannot_marshal_struct", "err: %s", err)
		return "[]"
	}
	return string(b)
}

func (f *structSliceFlag) Set(value string) error {
	var i interface{}
	if err := json.Unmarshal([]byte(value), &i); err != nil {
		return err
	}
	if _, ok := i.([]interface{}); ok {
		dst := reflect.New(f.dstSlice.Type())
		if err := json.Unmarshal([]byte(value), dst.Interface()); err != nil {
			return err
		}
		f.dstSlice.Set(reflect.AppendSlice(f.dstSlice, dst.Elem()))
		return nil
	}
	if _, ok := i.(map[string]interface{}); ok {
		dst := reflect.New(f.dstSlice.Type().Elem())
		if err := json.Unmarshal([]byte(value), dst.Interface()); err != nil {
			return err
		}
		f.dstSlice.Set(reflect.Append(f.dstSlice, dst.Elem()))
		return nil
	}
	return fmt.Errorf("Set for structSliceFlag can only accept JSON objects or arrays, but type was %T", i)
}

func (f *structSliceFlag) UnderlyingSlice() interface{} {
	slice := reflect.MakeSlice(f.dstSlice.Type(), f.dstSlice.Len(), f.dstSlice.Len())
	reflect.Copy(slice, f.dstSlice)
	return slice.Interface()
}

func (f *structSliceFlag) SetTo(structSlice interface{}) {
	if reflect.TypeOf(structSlice).Kind() != reflect.Slice {
		alert.UnexpectedEvent("struct_slice_flag_type_error", "SetTo accepts only slices of struct types, but was passed parameter of type %T", structSlice)
		return
	}
	if reflect.TypeOf(structSlice).Elem().Kind() != reflect.Struct {
		alert.UnexpectedEvent("struct_slice_flag_type_error", "SetTo accepts only slices of struct types, but was passed parameter of type %T", structSlice)
		return
	}
	if reflect.TypeOf(structSlice) != f.dstSlice.Type() {
		alert.UnexpectedEvent("struct_slice_flag_type_error", "SetTo was passed a slice of %T, which cannot be appended to a slice of type %T", structSlice, f.UnderlyingSlice())
		return
	}
	length := reflect.ValueOf(structSlice).Len()
	f.dstSlice.Set(reflect.MakeSlice(f.dstSlice.Type(), length, length))
	reflect.Copy(f.dstSlice, reflect.ValueOf(structSlice))
}

func (f *structSliceFlag) Slice(i, j int) interface{} {
	return reflect.ValueOf(f.UnderlyingSlice()).Slice(i, j).Interface()
}

func (f *structSliceFlag) AppendSlice(structSlice interface{}) interface{} {
	if reflect.TypeOf(structSlice).Kind() != reflect.Slice {
		alert.UnexpectedEvent("struct_slice_flag_type_error", "Append accepts only slices of struct types, but was passed parameter of type %T", structSlice)
		return reflect.MakeSlice(f.dstSlice.Type(), 0, 0)
	}
	if reflect.TypeOf(structSlice).Elem().Kind() != reflect.Struct {
		alert.UnexpectedEvent("struct_slice_flag_type_error", "Append accepts only slices of struct types, but was passed parameter of type %T", structSlice)
		return reflect.MakeSlice(f.dstSlice.Type(), 0, 0)
	}
	if reflect.TypeOf(structSlice) != f.dstSlice.Type() {
		alert.UnexpectedEvent("struct_slice_flag_type_error", "Append was passed a slice of %T, which cannot be appended to a slice of type %T", structSlice, f.UnderlyingSlice())
		return reflect.MakeSlice(f.dstSlice.Type(), 0, 0)
	}
	return reflect.AppendSlice(reflect.ValueOf(f.UnderlyingSlice()), reflect.ValueOf(structSlice)).Interface()
}

func (f *structSliceFlag) Len() int {
	return f.dstSlice.Len()
}

func newStructSliceFlag(structSlicePtr interface{}) *structSliceFlag {
	return &structSliceFlag{reflect.ValueOf(structSlicePtr).Elem()}
}

type URLFlag string

func NewURLFlag(s *string) *URLFlag {
	_, err := url.Parse(*s)
	if err != nil {
		log.Fatalf("Error parsing default URL value '%s' for flag: %v", *s, err)
		return nil
	}
	return (*URLFlag)(s)
}

func (f *URLFlag) Set(value string) error {
	*f = URLFlag(value)
	_, err := url.Parse(value)
	if err != nil {
		return err
	}
	return nil
}

func (f *URLFlag) String() string {
	return string(*f)
}

func (f *URLFlag) UnmarshalYAML(unmarshal func(interface{}) error) error {
	err := unmarshal((*string)(f))
	if err != nil {
		return err
	}
	_, err = url.Parse(string(*f))
	return err
}

func GenerateYAMLMapFromFlags() (map[interface{}]interface{}, error) {
	yamlMap := make(map[interface{}]interface{})
	var errors []error
	defaultFlagSet.VisitAll(func(flg *flag.Flag) {
		keys := strings.Split(flg.Name, ".")
		m := yamlMap
		for i, k := range keys[:len(keys)-1] {
			v, ok := m[k]
			if !ok {
				v := make(map[interface{}]interface{})
				m[k], m = v, v
				continue
			}
			m, ok = v.(map[interface{}]interface{})
			if !ok {
				errors = append(errors, fmt.Errorf("When trying to create YAML map hierarchy for %s, encountered non-map value of type %T at %s", flg.Name, v, strings.Join(keys[:i+1], ".")))
				return
			}
		}
		k := keys[len(keys)-1]
		if v, ok := m[k]; ok {
			errors = append(errors, fmt.Errorf("When trying to create YAML value for %s, encountered pre-existing value of type %T.", flg.Name, v))
			return
		}
		if v, ok := flg.Value.(*structSliceFlag); ok {
			m[k] = reflect.New(v.dstSlice.Type()).Elem().Interface()
			return
		} else if t, ok := flagTypeMap[reflect.TypeOf(flg.Value)]; ok {
			m[k] = reflect.New(t.Elem()).Elem().Interface()
			return
		}
		errors = append(errors, fmt.Errorf("Unsupported flag type at %s: %T", flg.Name, flg.Value))
	})
	if errors != nil {
		return nil, fmt.Errorf("Errors encountered when converting flags to YAML map: %v", errors)
	}
	return yamlMap, nil
}

func PopulateFlagsFromYAMLMap(m map[interface{}]interface{}) error {
	setFlags := make(map[string]struct{})
	defaultFlagSet.Visit(func(flg *flag.Flag) {
		setFlags[flg.Name] = struct{}{}
	})

	return populateFlagsFromYAML(m, []string{}, setFlags)
}

func populateFlagsFromYAML(i interface{}, prefix []string, setFlags map[string]struct{}) error {
	if m, ok := i.(map[interface{}]interface{}); ok {
		for k, v := range m {
			suffix, ok := k.(string)
			if !ok {
				return fmt.Errorf("non-string key in YAML map at %s.", strings.Join(prefix, "."))
			}
			if err := populateFlagsFromYAML(v, append(prefix, suffix), setFlags); err != nil {
				return err
			}
		}
		return nil
	}
	return SetValueForFlagName(strings.Join(prefix, "."), i, setFlags, true, false)
}

func SetValueForFlagName(name string, i interface{}, setFlags map[string]struct{}, appendSlice bool, strict bool) error {
	flg := defaultFlagSet.Lookup(name)
	if flg == nil {
		if strict {
			return status.NotFoundErrorf("Undefined flag: %s", name)
		}
		return nil
	}
	// For slice flags, append the YAML values to the existing values if appendSlice is true
	if v, ok := flg.Value.(SliceFlag); ok && appendSlice {
		if reflect.TypeOf(i) != reflect.TypeOf(v.UnderlyingSlice()) {
			return status.FailedPreconditionErrorf("Cannot append value %v of type %T to flag %s of type %T.", i, i, flg.Name, v.UnderlyingSlice())
		}
		v.SetTo(v.AppendSlice(i))
		return nil
	}
	// For non-append flags, skip the YAML values if it was set on the command line
	if _, ok := setFlags[name]; ok {
		return nil
	}
	if v, ok := flg.Value.(*structSliceFlag); ok {
		if reflect.TypeOf(i) != reflect.TypeOf(v.UnderlyingSlice()) {
			return status.FailedPreconditionErrorf("Cannot append value %v of type %T to flag %s of type %T.", i, i, flg.Name, v.UnderlyingSlice())
		}
		v.SetTo(i)
		return nil
	}
	t, ok := flagTypeMap[reflect.TypeOf(flg.Value)]
	if !ok {
		return status.UnimplementedErrorf("Unsupported flag type at %s: %T", flg.Name, flg.Value)
	}
	if !reflect.ValueOf(i).CanConvert(t.Elem()) {
		return status.FailedPreconditionErrorf("Cannot convert value %v of type %T into type %v for flag %s.", i, i, t, flg.Name)
	}
	reflect.ValueOf(flg.Value).Convert(t).Elem().Set(reflect.ValueOf(i).Convert(t.Elem()))
	return nil
}

func DereferencedValueFromFlagName(name string) (interface{}, error) {
	flg := defaultFlagSet.Lookup(name)
	if flg == nil {
		return nil, status.NotFoundErrorf("Undefined flag: %s", name)
	}
	if v, ok := flg.Value.(*structSliceFlag); ok {
		return v.UnderlyingSlice(), nil
	}
	addr := reflect.ValueOf(flg.Value)
	t, ok := flagTypeMap[addr.Type()]
	if !ok {
		return nil, status.UnimplementedErrorf("Unsupported flag type at %s: %s", name, addr.Type())
	}
	return addr.Convert(t).Elem().Interface(), nil
}

// FOR TESTING PURPOSES ONLY
func AddTestFlagTypeForTesting(flagValue interface{}, value interface{}) {
	flagTypeMap[reflect.TypeOf(flagValue)] = reflect.TypeOf(value)
}

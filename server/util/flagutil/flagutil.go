package flagutil

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"gopkg.in/yaml.v3"
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
	}

	// Flag names to ignore when generating a YAML map or populating flags (e. g.,
	// the flag specifying the path to the config file)
	ignoreSet = make(map[string]struct{})
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

type YAMLTypeAliasable interface {
	YAMLTypeAlias() reflect.Type
}

type TypeAliased interface {
	AliasedType() reflect.Type
}

type isNameAliasing interface {
	AliasedName() string
}

type Appendable interface {
	AppendSlice(any) error
}

// String flag that gets validated as a URL
// TODO: just use the URL directly (not the string) once we can generate the
// YAML map from the flags instead of the other way around.
func URLString(name, value, usage string) *string {
	u := NewURLFlag(&value)
	defaultFlagSet.Var(u, name, usage)
	return &value
}

type SliceFlag[T any] []T

func NewSliceFlag[T any](slice *[]T) *SliceFlag[T] {
	return (*SliceFlag[T])(slice)
}

func Slice[T any](name string, defaultValue []T, usage string) *[]T {
	slice := make([]T, len(defaultValue))
	copy(slice, defaultValue)
	defaultFlagSet.Var(NewSliceFlag(&slice), name, usage)
	return &slice
}

func SliceVar[T any](slice *[]T, name, usage string) {
	defaultFlagSet.Var(NewSliceFlag(slice), name, usage)
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

func (f *URLFlag) UnmarshalYAML(unmarshal func(any) error) error {
	err := unmarshal((*string)(f))
	if err != nil {
		return err
	}
	_, err = url.Parse(string(*f))
	return err
}

func (f *URLFlag) AliasedType() reflect.Type {
	return reflect.TypeOf((*string)(nil))
}

func (f *URLFlag) YAMLTypeAlias() reflect.Type {
	return reflect.TypeOf((*URLFlag)(nil))
}

type FlagAlias struct {
	name string
}

func Alias[T any](newName, name string) *T {
	f := &FlagAlias{name: name}
	var flg *flag.Flag
	for aliaser, ok := isNameAliasing(f), true; ok; aliaser, ok = flg.Value.(isNameAliasing) {
		if flg = defaultFlagSet.Lookup(aliaser.AliasedName()); flg == nil {
			log.Fatalf("Error aliasing flag %s as %s: flag %s does not exist.", name, newName, aliaser.AliasedName())
		}
	}
	addr := reflect.ValueOf(flg.Value)
	if t, err := getTypeForFlag(flg); err == nil {
		if !addr.CanConvert(t) {
			log.Fatalf("Error aliasing flag %s as %s: Flag %s of type %T could not be converted to %s.", name, newName, flg.Name, flg.Value, t)
		}
		addr = addr.Convert(t)
	}
	value, ok := addr.Interface().(*T)
	if !ok {
		log.Fatalf("Error aliasing flag %s as %s: Failed to assert flag %s of type %T as type %T.", name, newName, flg.Name, flg.Value, (*T)(nil))
	}
	defaultFlagSet.Var(f, newName, "Alias for "+name)
	return value
}

func (f *FlagAlias) Set(value string) error {
	return defaultFlagSet.Set(f.name, value)
}

func (f *FlagAlias) String() string {
	return defaultFlagSet.Lookup(f.name).Value.String()
}

func (f *FlagAlias) AliasedName() string {
	return f.name
}

func (f *FlagAlias) AliasedType() reflect.Type {
	flg := defaultFlagSet.Lookup(f.name)
	t, err := getTypeForFlag(flg)
	if err != nil {
		return reflect.TypeOf(flg.Value)
	}
	return t
}

func (f *FlagAlias) YAMLTypeAlias() reflect.Type {
	flg := defaultFlagSet.Lookup(f.name)
	t, err := getYAMLTypeForFlag(flg)
	if err != nil {
		return reflect.TypeOf(flg.Value)
	}
	return t
}

// IgnoreFlagForYAML ignores the flag with this name when generating YAML and when
// populating flags from YAML input.
func IgnoreFlagForYAML(name string) {
	ignoreSet[name] = struct{}{}
}

func getTypeForFlag(flg *flag.Flag) (reflect.Type, error) {
	if t, ok := flagTypeMap[reflect.TypeOf(flg.Value)]; ok {
		return t, nil
	} else if v, ok := flg.Value.(TypeAliased); ok {
		return v.AliasedType(), nil
	}
	return nil, status.UnimplementedErrorf("Unsupported flag type at %s: %T", flg.Name, flg.Value)
}

func getYAMLTypeForFlag(flg *flag.Flag) (reflect.Type, error) {
	if t, ok := flagTypeMap[reflect.TypeOf(flg.Value)]; ok {
		return t, nil
	} else if v, ok := flg.Value.(YAMLTypeAliasable); ok {
		return v.YAMLTypeAlias(), nil
	}
	return nil, status.UnimplementedErrorf("Unsupported flag type at %s: %T", flg.Name, flg.Value)
}

// GenerateYAMLTypeMapFromFlags generates a map of the type that should be
// marshaled from YAML for each flag name at the corresponding nested map index.
func GenerateYAMLTypeMapFromFlags() (map[string]any, error) {
	yamlMap := make(map[string]any)
	var errors []error
	defaultFlagSet.VisitAll(func(flg *flag.Flag) {
		keys := strings.Split(flg.Name, ".")
		for i := range keys {
			if _, ok := ignoreSet[strings.Join(keys[:i+1], ".")]; ok {
				return
			}
		}
		m := yamlMap
		for i, k := range keys[:len(keys)-1] {
			v, ok := m[k]
			if !ok {
				v := make(map[string]any)
				m[k], m = v, v
				continue
			}
			m, ok = v.(map[string]any)
			if !ok {
				errors = append(errors, status.FailedPreconditionErrorf("When trying to create YAML type map hierarchy for %s, encountered non-map value of type %T at %s", flg.Name, v, strings.Join(keys[:i+1], ".")))
				return
			}
		}
		k := keys[len(keys)-1]
		if v, ok := m[k]; ok {
			errors = append(errors, status.FailedPreconditionErrorf("When trying to create type for %s for YAML type map, encountered pre-existing value of type %T.", flg.Name, v))
			return
		}
		t, err := getYAMLTypeForFlag(flg)
		if err != nil {
			errors = append(errors, err)
			return
		}
		m[k] = t.Elem()
	})
	if errors != nil {
		return nil, status.InternalErrorf("Errors encountered when converting flags to YAML map: %v", errors)
	}
	return yamlMap, nil
}

// RetypeAndFilterYAMLMap un-marshals yaml from the input yamlMap and then
// re-marshals it into the types specified by the type map, replacing the
// original value in the input map. Filters out any values not specified by the
// flags.
func RetypeAndFilterYAMLMap(yamlMap map[string]any, typeMap map[string]any, prefix []string) error {
	for k := range yamlMap {
		label := append(prefix, k)
		if _, ok := typeMap[k]; !ok {
			// No flag corresponds to this, warn and delete.
			log.Warningf("No flags correspond to YAML input at '%s'.", strings.Join(label, "."))
			delete(yamlMap, k)
			continue
		}
		switch t := typeMap[k].(type) {
		case reflect.Type:
			// this is a value, populate it from the YAML
			yamlData, err := yaml.Marshal(yamlMap[k])
			if err != nil {
				return status.InternalErrorf("Encountered error marshaling %v to YAML at %s: %s", yamlMap[k], strings.Join(label, "."), err)
			}
			v := reflect.New(t).Elem()
			err = yaml.Unmarshal(yamlData, v.Addr().Interface())
			if err != nil {
				return status.InternalErrorf("Encountered error marshaling %s to YAML for type %v at %s: %s", string(yamlData), v.Type(), strings.Join(label, "."), err)
			}
			if v.Type() != t {
				return status.InternalErrorf("Failed to unmarshal YAML to the specified type at %s: wanted %v, got %T", strings.Join(label, "."), t, v.Type())
			}
			yamlMap[k] = v.Interface()
		case map[string]any:
			yamlSubmap, ok := yamlMap[k].(map[string]any)
			if !ok {
				// this is a value, not a map, and there is no corresponding type
				alert.UnexpectedEvent("Input YAML contained non-map value %v of type %T at label %s", yamlMap[k], yamlMap[k], strings.Join(label, "."))
				delete(yamlMap, k)
			}
			err := RetypeAndFilterYAMLMap(yamlSubmap, t, label)
			if err != nil {
				return err
			}
		default:
			return status.InvalidArgumentErrorf("typeMap contained invalid type %T at %s.", typeMap[k], strings.Join(label, "."))
		}
	}
	return nil
}

// PopulateFlagsFromData takes some YAML input and unmarshals it, then uses the
// umnarshaled data to populate the unset flags with names corresponding to the
// keys.
func PopulateFlagsFromData(data []byte) error {
	// expand environment variables
	expandedData := []byte(os.ExpandEnv(string(data)))

	yamlMap := make(map[string]any)
	if err := yaml.Unmarshal([]byte(expandedData), yamlMap); err != nil {
		return status.InternalErrorf("Error parsing config file: %s", err)
	}
	typeMap, err := GenerateYAMLTypeMapFromFlags()
	if err != nil {
		return err
	}
	if err := RetypeAndFilterYAMLMap(yamlMap, typeMap, []string{}); err != nil {
		return status.InternalErrorf("Error encountered retyping YAML map: %s", err)
	}

	return PopulateFlagsFromYAMLMap(yamlMap)
}

// PopulateFlagsFromData takes the path to some YAML file, reads it, and
// unmarshals it, then uses the umnarshaled data to populate the unset flags
// with names corresponding to the keys.
func PopulateFlagsFromFile(configFile string) error {
	log.Infof("Reading buildbuddy config from '%s'", configFile)

	_, err := os.Stat(configFile)

	// If the file does not exist then skip it.
	if os.IsNotExist(err) {
		log.Warningf("No config file found at %s.", configFile)
		return nil
	}

	fileBytes, err := os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("Error reading config file: %s", err)
	}

	return PopulateFlagsFromData(fileBytes)
}

// PopulateFlagsFromYAMLMap takes a map populated by YAML from some YAML input
// and iterates over it, finding flags with names corresponding to the keys and
// setting the flag to the YAML value if the flag was not set on the command
// line.
func PopulateFlagsFromYAMLMap(m map[string]any) error {
	setFlags := make(map[string]struct{})
	defaultFlagSet.Visit(func(flg *flag.Flag) {
		setFlags[flg.Name] = struct{}{}
	})

	return populateFlagsFromYAML(m, []string{}, setFlags)
}

func populateFlagsFromYAML(i any, prefix []string, setFlags map[string]struct{}) error {
	if m, ok := i.(map[string]any); ok {
		for k, v := range m {
			p := append(prefix, k)
			if _, ok := ignoreSet[strings.Join(p, ".")]; ok {
				return nil
			}
			if err := populateFlagsFromYAML(v, p, setFlags); err != nil {
				return err
			}
		}
		return nil
	}
	name := strings.Join(prefix, ".")
	if _, ok := ignoreSet[name]; ok {
		return nil
	}
	return SetValueForFlagName(name, i, setFlags, true, false)
}

// SetValueForFlagName sets the value for a flag by name.
func SetValueForFlagName(name string, i any, setFlags map[string]struct{}, appendSlice bool, strict bool) error {
	flg := defaultFlagSet.Lookup(name)
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
	// For non-append flags, skip the YAML values if it was set on the command line
	if _, ok := setFlags[name]; ok {
		return nil
	}
	if v, ok := flg.Value.(isNameAliasing); ok {
		return SetValueForFlagName(v.AliasedName(), i, setFlags, appendSlice, strict)
	}
	t, err := getTypeForFlag(flg)
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
	flg := defaultFlagSet.Lookup(name)
	zeroT := reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Interface().(*T)
	if flg == nil {
		return *zeroT, status.NotFoundErrorf("Undefined flag: %s", name)
	}
	if v, ok := flg.Value.(isNameAliasing); ok {
		return GetDereferencedValue[T](v.AliasedName())
	}
	t := reflect.TypeOf((*T)(nil))
	addr := reflect.ValueOf(flg.Value)
	if t == reflect.TypeOf((*any)(nil)) {
		var err error
		t, err = getTypeForFlag(flg)
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

// FOR TESTING PURPOSES ONLY
// AddTestFlagTypeForTesting adds a type correspondence to the internal
// flagTypeMap.
func AddTestFlagTypeForTesting(flagValue, value any) {
	flagTypeMap[reflect.TypeOf(flagValue)] = reflect.TypeOf(value)
}

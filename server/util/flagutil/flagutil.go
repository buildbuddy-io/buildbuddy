package flagutil

import (
	"bytes"
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

	nilableKinds = map[reflect.Kind]struct{}{
		reflect.Chan:      {},
		reflect.Func:      {},
		reflect.Interface: {},
		reflect.Map:       {},
		reflect.Ptr:       {},
		reflect.Slice:     {},
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

func IgnoreFilter(flg *flag.Flag) bool {
	keys := strings.Split(flg.Name, ".")
	for i := range keys {
		if _, ok := ignoreSet[strings.Join(keys[:i+1], ".")]; ok {
			return false
		}
	}
	return true
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

type DocumentedMarshaler interface {
	DocumentNode(n *yaml.Node, opts ...DocumentNodeOption) error
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

type URLFlag url.URL

func URL(name string, value url.URL, usage string) *url.URL {
	u := &value
	defaultFlagSet.Var((*URLFlag)(u), name, usage)
	return u
}

func URLVar(value *url.URL, name string, usage string) {
	defaultFlagSet.Var((*URLFlag)(value), name, usage)
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

func (f *URLFlag) DocumentNode(n *yaml.Node, opts ...DocumentNodeOption) error {
	for _, opt := range opts {
		if _, ok := opt.(*addTypeToLineComment); ok {
			if n.LineComment != "" {
				n.LineComment += " "
			}
			n.LineComment += "type: URL"
			continue
		}
		opt.Transform(f, n)
	}
	return nil
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

type DocumentNodeOption interface {
	Transform(in any, n *yaml.Node)
	Passthrough() bool
}

type headComment string

func (h *headComment) Transform(in any, n *yaml.Node) { n.HeadComment = string(*h) }
func (h *headComment) Passthrough() bool              { return false }

// HeadComment sets the HeadComment of a yaml.Node to the specified string.
func HeadComment(s string) *headComment { return (*headComment)(&s) }

type lineComment string

func (l *lineComment) Transform(in any, n *yaml.Node) { n.LineComment = string(*l) }
func (l *lineComment) Passthrough() bool              { return false }

// LineComment sets the LineComment of a yaml.Node to the specified string.
func LineComment(s string) *lineComment { return (*lineComment)(&s) }

type footComment string

func (f *footComment) Transform(in any, n *yaml.Node) { n.FootComment = string(*f) }
func (f *footComment) Passthrough() bool              { return false }

// FootComment sets the FootComment of a yaml.Node to the specified string.
func FootComment(s string) *footComment { return (*footComment)(&s) }

type addTypeToLineComment struct{}

func (f *addTypeToLineComment) Transform(in any, n *yaml.Node) {
	if n.LineComment != "" {
		n.LineComment += " "
	}
	n.LineComment = fmt.Sprintf("%stype: %T", n.LineComment, in)
}

func (f *addTypeToLineComment) Passthrough() bool { return true }

// AddTypeToLineComment appends the type specification to the LineComment of the yaml.Node.
func AddTypeToLineComment() *addTypeToLineComment { return (*addTypeToLineComment)(&struct{}{}) }

func FilterPassthrough(opts []DocumentNodeOption) []DocumentNodeOption {
	ptOpts := []DocumentNodeOption{}
	for _, opt := range opts {
		if opt.Passthrough() {
			ptOpts = append(ptOpts, opt)
		}
	}
	return ptOpts
}

// DocumentedNode returns a yaml.Node representing the input value with
// documentation in the comments.
func DocumentedNode(in any, opts ...DocumentNodeOption) (*yaml.Node, error) {
	n := &yaml.Node{}
	if err := n.Encode(in); err != nil {
		return nil, err
	}
	if err := DocumentNode(in, n, opts...); err != nil {
		return nil, err
	}
	return n, nil
}

// DocumentNode fills the comments of a yaml.Node with documentation.
func DocumentNode(in any, n *yaml.Node, opts ...DocumentNodeOption) error {
	switch m := in.(type) {
	case DocumentedMarshaler:
		return m.DocumentNode(n, opts...)
	case yaml.Marshaler:
		// pass
	default:
		v := reflect.ValueOf(in)
		t := v.Type()
		switch t.Kind() {
		case reflect.Ptr:
			// document based on the value pointed to
			if !v.IsNil() {
				return DocumentNode(v.Elem().Interface(), n, opts...)
			} else {
				return DocumentNode(reflect.New(reflect.TypeOf(t).Elem()).Elem().Interface(), n, opts...)
			}
		case reflect.Struct:
			// yaml.Node stores mappings in Content as [key1, value1, key2, value2...]
			contentIndex := make(map[string]int, len(n.Content)/2)
			for i := 0; i < len(n.Content)/2; i++ {
				contentIndex[n.Content[2*i].Value] = 2*i + 1
			}
			for i := 0; i < t.NumField(); i++ {
				ft := t.FieldByIndex([]int{i})
				name := strings.Split(ft.Tag.Get("yaml"), ",")[0]
				if name == "" {
					name = strings.ToLower(ft.Name)
				}
				idx, ok := contentIndex[name]
				if !ok {
					// field is not encoded by yaml
					continue
				}
				if err := DocumentNode(
					v.FieldByIndex([]int{i}).Interface(),
					n.Content[idx],
					append(
						[]DocumentNodeOption{LineComment(ft.Tag.Get("usage"))},
						FilterPassthrough(opts)...,
					)...,
				); err != nil {
					return err
				}
			}
		case reflect.Slice:
			// yaml.Node stores sequences in Content as [element1, element2...]
			for i := range n.Content {
				var err error
				if err = DocumentNode(v.Index(i).Interface(), n.Content[i], FilterPassthrough(opts)...); err != nil {
					return err
				}
			}
			if len(n.Content) == 0 {
				exampleNode, err := DocumentedNode(reflect.MakeSlice(t, 1, 1).Interface(), FilterPassthrough(opts)...)
				if err != nil {
					return err
				}
				if exampleNode.Content[0].Kind != yaml.ScalarNode {
					example, err := yaml.Marshal(exampleNode)
					if err != nil {
						return err
					}
					n.FootComment = fmt.Sprintf("e.g.,\n%s", string(example))
				}
			}
		case reflect.Map:
			// yaml.Node stores mappings in Content as [key1, value1, key2, value2...]
			for i := 0; i < len(n.Content)/2; i++ {
				k := reflect.ValueOf(n.Content[2*i].Value)
				if err := DocumentNode(
					v.MapIndex(k).Interface(),
					n.Content[2*i+1],
					FilterPassthrough(opts)...,
				); err != nil {
					return err
				}
			}
		}
	}
	for _, opt := range opts {
		opt.Transform(in, n)
	}
	return nil
}

// GenerateDocumentedYAMLNodeFromFlag produces a documented yaml.Node which
// represents the value contained in the flag.
func GenerateDocumentedYAMLNodeFromFlag(flg *flag.Flag) (*yaml.Node, error) {
	t, err := getYAMLTypeForFlag(flg)
	if err != nil {
		return nil, status.InternalErrorf("Error encountered generating default YAML from flags: %s", err)
	}
	v, err := GetDereferencedValue[any](flg.Name)
	if err != nil {
		return nil, status.InternalErrorf("Error encountered generating default YAML from flags: %s", err)
	}
	value := reflect.New(reflect.TypeOf(v))
	value.Elem().Set(reflect.ValueOf(v))
	if !value.CanConvert(t) {
		return nil, status.FailedPreconditionErrorf("Cannot convert value %v of type %T into type %v for flag %s.", value.Interface(), value.Type(), t, flg.Name)
	}
	return DocumentedNode(value.Convert(t).Interface(), LineComment(flg.Usage), AddTypeToLineComment())
}

// SplitDocumentedYAMLFromFlags produces marshaled YAML representing the flags,
// partitioned into two groups: structured (flags containing dots), and
// unstructured (flags not containing dots).
func SplitDocumentedYAMLFromFlags() ([]byte, error) {
	b := bytes.NewBuffer([]byte{})

	if _, err := b.Write([]byte("# Unstructured settings\n\n")); err != nil {
		return nil, err
	}
	um, err := GenerateYAMLMapWithValuesFromFlags(
		GenerateDocumentedYAMLNodeFromFlag,
		func(flg *flag.Flag) bool { return !strings.Contains(flg.Name, ".") },
		IgnoreFilter,
	)
	if err != nil {
		return nil, err
	}
	ub, err := yaml.Marshal(um)
	if err != nil {
		return nil, err
	}
	if _, err := b.Write(ub); err != nil {
		return nil, err
	}

	if _, err := b.Write([]byte("\n# Structured settings\n\n")); err != nil {
		return nil, err
	}
	sm, err := GenerateYAMLMapWithValuesFromFlags(
		GenerateDocumentedYAMLNodeFromFlag,
		func(flg *flag.Flag) bool { return strings.Contains(flg.Name, ".") },
		IgnoreFilter,
	)
	if err != nil {
		return nil, err
	}
	sb, err := yaml.Marshal(sm)
	if err != nil {
		return nil, err
	}
	if _, err := b.Write(sb); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// GenerateYAMLMapWithValuesFromFlags generates a YAML map structure
// representing the flags, with values generated from the flags as per the
// generateValue function that has been passed in, and filtering out any flags
// for which any of the passed filter functions return false. Any nil generated
// values are not added to the map, and any empty maps are recursively removed
// such that the final map returned contains no empty maps at any point in its
// structure.
func GenerateYAMLMapWithValuesFromFlags[T any](generateValue func(*flag.Flag) (T, error), filters ...func(*flag.Flag) bool) (map[string]any, error) {
	yamlMap := make(map[string]any)
	var errors []error
	defaultFlagSet.VisitAll(func(flg *flag.Flag) {
		for _, f := range filters {
			if !f(flg) {
				return
			}
		}
		keys := strings.Split(flg.Name, ".")
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
				errors = append(errors, status.FailedPreconditionErrorf("When trying to create YAML map hierarchy for %s, encountered non-map value %s of type %T at %s", flg.Name, v, v, strings.Join(keys[:i+1], ".")))
				return
			}
		}
		k := keys[len(keys)-1]
		if v, ok := m[k]; ok {
			errors = append(errors, status.FailedPreconditionErrorf("When generating value for %s for YAML map, encountered pre-existing value %s of type %T.", flg.Name, v, v))
			return
		}
		v, err := generateValue(flg)
		if err != nil {
			errors = append(errors, err)
			return
		}
		value := reflect.ValueOf(v)
		if _, ok := nilableKinds[value.Kind()]; ok && value.IsNil() {
			return
		}
		m[k] = v
	})
	if errors != nil {
		return nil, status.InternalErrorf("Errors encountered when generating YAML map from flags: %v", errors)
	}

	return RemoveEmptyMapsFromYAMLMap(yamlMap), nil
}

// RemoveEmptyMapsFromYAMLMap recursively removes all empty maps, such that the
// returned map contains no empty maps at any point in its structure. The
// original map is returned unless it is empty after removal, in which case nil
// is returned.
func RemoveEmptyMapsFromYAMLMap(m map[string]any) map[string]any {
	for k, v := range m {
		mv, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if m[k] = RemoveEmptyMapsFromYAMLMap(mv); m[k] == nil {
			delete(m, k)
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
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
			v := reflect.New(t.Elem()).Elem()
			err = yaml.Unmarshal(yamlData, v.Addr().Interface())
			if err != nil {
				return status.InternalErrorf("Encountered error marshaling %s to YAML for type %v at %s: %s", string(yamlData), v.Type(), strings.Join(label, "."), err)
			}
			if v.Type() != t.Elem() {
				return status.InternalErrorf("Failed to unmarshal YAML to the specified type at %s: wanted %v, got %T", strings.Join(label, "."), t.Elem(), v.Type())
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
	node := &yaml.Node{}
	if err := yaml.Unmarshal([]byte(expandedData), node); err != nil {
		return status.InternalErrorf("Error parsing config file: %s", err)
	}
	if len(node.Content) > 0 {
		node = node.Content[0]
	} else {
		node = nil
	}
	typeMap, err := GenerateYAMLMapWithValuesFromFlags(getYAMLTypeForFlag, IgnoreFilter)
	if err != nil {
		return err
	}
	if err := RetypeAndFilterYAMLMap(yamlMap, typeMap, []string{}); err != nil {
		return status.InternalErrorf("Error encountered retyping YAML map: %s", err)
	}

	return PopulateFlagsFromYAMLMap(yamlMap, node)
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
// and a yaml.Node populated by YAML from the same input and iterates over it,
// finding flags with names corresponding to the keys and setting the flag to
// the YAML value if the flag was not set on the command line. The yaml.Node
// preserves order when setting the flag values, which is important for aliases.
// If Node is nil, the order values will be set in is random, as per go's
// implementation of map traversal.
func PopulateFlagsFromYAMLMap(m map[string]any, node *yaml.Node) error {
	setFlags := make(map[string]struct{})
	defaultFlagSet.Visit(func(flg *flag.Flag) {
		setFlags[flg.Name] = struct{}{}
	})

	return populateFlagsFromYAML(m, []string{}, node, setFlags)
}

func populateFlagsFromYAML(a any, prefix []string, node *yaml.Node, setFlags map[string]struct{}) error {
	if m, ok := a.(map[string]any); ok {
		i := 0
		for k, v := range m {
			var n *yaml.Node
			if node != nil {
				// Ensure that we populate flags in the order they are specified in the
				// YAML data if the node structure data was provided.
				for ok := false; node != nil && !ok; i++ {
					k = node.Content[2*i].Value
					n = node.Content[2*i+1]
					v, ok = m[k]
				}
			}
			p := append(prefix, k)
			if _, ok := ignoreSet[strings.Join(p, ".")]; ok {
				return nil
			}
			if err := populateFlagsFromYAML(v, p, n, setFlags); err != nil {
				return err
			}
		}
		return nil
	}
	name := strings.Join(prefix, ".")
	if _, ok := ignoreSet[name]; ok {
		return nil
	}
	return SetValueForFlagName(name, a, setFlags, true, false)
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

package yaml

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"gopkg.in/yaml.v3"
)

const spacesPerYAMLIndentLevel = 4

var (
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

	AppendTypeToLineComment = &appendTypeToLineComment{}

	WordWrapForIndentationLevel = generateWordWrapRegexByIndentationLevel(76, 3)

	StartOfLine = regexp.MustCompile("(?m)^")
)

func generateWordWrapRegexByIndentationLevel(targetLineLength, minWrapLength int) []*regexp.Regexp {
	s := make([]*regexp.Regexp, 0, (targetLineLength-minWrapLength)/spacesPerYAMLIndentLevel+1)
	for wrapLength := targetLineLength; wrapLength >= minWrapLength; wrapLength -= spacesPerYAMLIndentLevel {
		s = append(s, regexp.MustCompile("(([^\n]{"+strconv.Itoa(minWrapLength)+","+strconv.Itoa(wrapLength)+"})((\\s+)|$))"))
	}
	return s
}

// IgnoreFlagForYAML ignores the flag with this name when generating YAML and when
// populating flags from YAML input.
func IgnoreFlagForYAML(name string) {
	ignoreSet[name] = struct{}{}
}

// IgnoreFilter is a filter that checks flags against IgnoreSet.
func IgnoreFilter(flg *flag.Flag) bool {
	keys := strings.Split(flg.Name, ".")
	for i := range keys {
		if _, ok := ignoreSet[strings.Join(keys[:i+1], ".")]; ok {
			return false
		}
	}
	return true
}

// StructuredFilter is a filter that checks that flags contain dots (in other
// words, it checks that they are not top-level values in the YAML
// representation).
func StructuredFilter(flg *flag.Flag) bool {
	return strings.Contains(flg.Name, ".")
}

// UnstructuredFilter is a filter that checks that flags do not contain dots (in
// other words, it checks that they are top-level values in the YAML
// representation).
func UnstructuredFilter(flg *flag.Flag) bool {
	return !StructuredFilter(flg)
}

type YAMLTypeAliasable interface {
	// YAMLTypeAlias returns the type alias we use in YAML for this flag.Value.
	// Only necessary if the type used for YAML is not the type returned by
	// AliasedType.
	YAMLTypeAlias() reflect.Type
}

type YAMLTypeStringable interface {
	// YAMLTypeString returns the name to print for this type in YAML docs.
	YAMLTypeString() string
}

type YAMLSetValueHooked interface {
	// YAMLSetValueHook is the hook for flags that is called when the flag.Value
	// is set through the YAML config.
	YAMLSetValueHook()
}

type DocumentedMarshaler interface {
	// DocumentNode documents the yaml.Node representing this value.
	DocumentNode(n *yaml.Node, opts ...common.DocumentNodeOption) error
}

// GetYAMLTypeForFlagValue returns the type alias to use in YAML contexts for the flag.
func GetYAMLTypeForFlagValue(value flag.Value) (reflect.Type, error) {
	if v, ok := value.(common.WrappingValue); ok {
		return GetYAMLTypeForFlagValue(v.WrappedValue())
	}
	if v, ok := value.(YAMLTypeAliasable); ok {
		return v.YAMLTypeAlias(), nil
	} else if t, err := common.GetTypeForFlagValue(value); err == nil {
		return t, nil
	}
	return nil, status.UnimplementedErrorf("Unsupported flag type: %T", value)
}

// GetYAMLTypeString returns the string to use to represent the type to the
// user in the YAML documentation.
func GetYAMLTypeString(yamlValue any) string {
	if v, ok := yamlValue.(YAMLTypeStringable); ok {
		return v.YAMLTypeString()
	}
	value := reflect.ValueOf(yamlValue)
	if value.CanAddr() {
		if v, ok := value.Addr().Interface().(YAMLTypeStringable); ok {
			return v.YAMLTypeString()
		}
	}
	t := value.Type()
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.String()
}

type HeadComment string

func (h *HeadComment) Transform(in any, n *yaml.Node) { n.HeadComment = string(*h) }
func (h *HeadComment) Passthrough() bool              { return false }

// NewHeadComment returns a HeadComment for the specified string.
func NewHeadComment(s string) *HeadComment { return (*HeadComment)(&s) }

type LineComment string

func (l *LineComment) Transform(in any, n *yaml.Node) { n.LineComment = string(*l) }
func (l *LineComment) Passthrough() bool              { return false }

// NewLineComment returns a LineComment for the specified string.
func NewLineComment(s string) *LineComment { return (*LineComment)(&s) }

type FootComment string

func (f *FootComment) Transform(in any, n *yaml.Node) { n.FootComment = string(*f) }
func (f *FootComment) Passthrough() bool              { return false }

// NewFootComment returns a FootComment for the specified string.
func NewFootComment(s string) *FootComment { return (*FootComment)(&s) }

type appendTypeToLineComment struct{}

func (f *appendTypeToLineComment) Transform(in any, n *yaml.Node) {
	typeString := GetYAMLTypeString(in)
	if n.LineComment != "" {
		n.LineComment += " "
	}
	n.LineComment += "(type: " + typeString + ")"
}

func (f *appendTypeToLineComment) Passthrough() bool { return false }

func filterPassthrough(opts []common.DocumentNodeOption) []common.DocumentNodeOption {
	ptOpts := []common.DocumentNodeOption{}
	for _, opt := range opts {
		if opt.Passthrough() {
			ptOpts = append(ptOpts, opt)
		}
	}
	return ptOpts
}

type Style yaml.Style

func (f *Style) Transform(in any, n *yaml.Node) {
	if reflect.ValueOf(in).Kind() != reflect.Map {
		n.Style |= *(*yaml.Style)(f)
	}
}

func (f *Style) Passthrough() bool { return true }

// NewDocumentedMarshalerWithOptions wraps a value such that it can be encoded
// by YAML, documented with DocumentNode (applying the passed
// DocumentNodeOptions), and then marshaled into documented  YAML.
func NewDocumentedMarshalerWithOptions(in any, opts ...common.DocumentNodeOption) *DocumentedMarshalerWithOptions {
	return &DocumentedMarshalerWithOptions{in: in, opts: opts}
}

// DocumentedMarshalerWithOptions produces a documented node with predefined
// options `opts` from `in` when marshaled to YAML.
type DocumentedMarshalerWithOptions struct {
	in   any
	opts []common.DocumentNodeOption
}

func (d *DocumentedMarshalerWithOptions) MarshalYAML() (any, error) {
	return d.in, nil
}

func (d *DocumentedMarshalerWithOptions) DocumentNode(n *yaml.Node, opts ...common.DocumentNodeOption) error {
	concatOpts := []common.DocumentNodeOption{}
	concatOpts = append(concatOpts, d.opts...)
	concatOpts = append(concatOpts, opts...)
	return DocumentNode(d.in, n, concatOpts...)
}

// DocumentedNode returns a yaml.Node representing the input value with
// documentation in the comments.
func DocumentedNode(in any, opts ...common.DocumentNodeOption) (*yaml.Node, error) {
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
func DocumentNode(in any, n *yaml.Node, opts ...common.DocumentNodeOption) error {
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
				exampleNode, err := DocumentedNode(reflect.Indirect(reflect.New(t.Elem())).Interface(), append(filterPassthrough(opts), AppendTypeToLineComment)...)
				if err != nil {
					return err
				}
				if exampleNode.Kind != yaml.ScalarNode {
					example, err := yaml.Marshal(exampleNode)
					if err != nil {
						return err
					}
					replacedExample := string(StartOfLine.ReplaceAll(example, []byte("    ")))
					n.FootComment = fmt.Sprintf("For example:\n%s", string(replacedExample))
				}
				return DocumentNode(reflect.New(t.Elem()).Elem().Interface(), n, opts...)
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
						append(
							[]common.DocumentNodeOption{NewLineComment(ft.Tag.Get("usage"))},
							AppendTypeToLineComment,
						),
						filterPassthrough(opts)...,
					)...,
				); err != nil {
					return err
				}
			}
		case reflect.Slice:
			// yaml.Node stores sequences in Content as [element1, element2...]
			for i := range n.Content {
				var err error
				if err = DocumentNode(v.Index(i).Interface(), n.Content[i], filterPassthrough(opts)...); err != nil {
					return err
				}
			}
			if len(n.Content) == 0 {
				exampleNode, err := DocumentedNode(reflect.MakeSlice(t, 1, 1).Interface(), append(filterPassthrough(opts), AppendTypeToLineComment)...)
				if err != nil {
					return err
				}
				if exampleNode.Content[0].Kind != yaml.ScalarNode {
					example, err := yaml.Marshal(exampleNode)
					if err != nil {
						return err
					}
					n.FootComment = fmt.Sprintf("For example:\n%s", string(example))
				}
			}
		case reflect.Map:
			// yaml.Node stores mappings in Content as [key1, value1, key2, value2...]
			for i := 0; i < len(n.Content)/2; i++ {
				k := reflect.ValueOf(n.Content[2*i].Value)
				if err := DocumentNode(
					v.MapIndex(k).Interface(),
					n.Content[2*i+1],
					filterPassthrough(opts)...,
				); err != nil {
					return err
				}
				// TODO: For now, YAML v3 value Node Head Comments are placed below Foot
				// Comments, so we transfer them to the key Node. When this is fixed, we
				// can remove this code.
				n.Content[2*i].HeadComment = n.Content[2*i+1].HeadComment
				n.Content[2*i+1].HeadComment = ""
			}
		}
	}
	for _, opt := range opts {
		opt.Transform(in, n)
	}
	return nil
}

// GenerateDocumentedMarshalerFromFlag produces a DocumentedMarshalerWithOptions
// that represents the value contained in the flag which documents its name,
// type, and usage as a HeadComment.
func GenerateDocumentedMarshalerFromFlag(flg *flag.Flag) (*DocumentedMarshalerWithOptions, error) {
	t, err := GetYAMLTypeForFlagValue(flg.Value)
	if err != nil {
		return nil, status.InternalErrorf("Error encountered generating default YAML from flags when processing flag %s: %s", flg.Name, err)
	}
	v, err := common.GetDereferencedValue[any](flg.Name)
	if err != nil {
		return nil, status.InternalErrorf("Error encountered generating default YAML from flags: %s", err)
	}
	value := reflect.New(reflect.TypeOf(v))
	value.Elem().Set(reflect.ValueOf(v))
	if !value.CanConvert(t) {
		return nil, status.FailedPreconditionErrorf("Cannot convert value %v of type %s into type %v for flag %s.", value.Interface(), value.Type(), t, flg.Name)
	}
	yamlValue := value.Convert(t).Interface()
	headComment := flg.Name + " (" + GetYAMLTypeString(yamlValue) + ")"
	if flg.Usage != "" {
		headComment = headComment + ": " + flg.Usage
	}
	headComment = string(WordWrapForIndentationLevel[strings.Count(flg.Name, ".")].ReplaceAll([]byte(headComment), []byte("$1\n")))
	return NewDocumentedMarshalerWithOptions(yamlValue, NewHeadComment(headComment)), nil
}

// SplitDocumentedYAMLFromFlags produces marshaled YAML representing the flags,
// partitioned into two groups: structured (flags containing dots), and
// unstructured (flags not containing dots).
func SplitDocumentedYAMLFromFlags(styles ...yaml.Style) ([]byte, error) {
	style := (Style)(0)
	for s := range styles {
		style |= Style(s)
	}
	b := bytes.NewBuffer([]byte{})

	if _, err := b.Write([]byte("# Unstructured settings\n\n")); err != nil {
		return nil, err
	}
	um, err := GenerateYAMLMapWithValuesFromFlags(
		GenerateDocumentedMarshalerFromFlag,
		UnstructuredFilter,
		IgnoreFilter,
	)
	if err != nil {
		return nil, err
	}
	un, err := DocumentedNode(um, &style)
	if err != nil {
		return nil, err
	}
	ub, err := yaml.Marshal(un)
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
		GenerateDocumentedMarshalerFromFlag,
		StructuredFilter,
		IgnoreFilter,
	)
	if err != nil {
		return nil, err
	}
	sn, err := DocumentedNode(sm, &style)
	if err != nil {
		return nil, err
	}
	sb, err := yaml.Marshal(sn)
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
	common.DefaultFlagSet.VisitAll(func(flg *flag.Flag) {
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

// OverrideFlagsFromData takes some YAML input and marshals it, then uses the
// unmarshaled data to override the flags with names corresponding to the keys.
func OverrideFlagsFromData(data []byte) error {
	// expand environment variables
	expandedData := []byte(os.ExpandEnv(string(data)))

	yamlMap, node, err := getYAMLMapAndNodeFromData(expandedData)
	if err != nil {
		return err
	}
	return populateFlagsFromYAML(yamlMap, []string{}, node, nil, false)
}

// PopulateFlagsFromData takes some YAML input and unmarshals it, then uses the
// unmarshaled data to populate the unset flags with names corresponding to the
// keys.
func PopulateFlagsFromData(data []byte) error {
	// expand environment variables
	expandedData := []byte(os.ExpandEnv(string(data)))

	yamlMap, node, err := getYAMLMapAndNodeFromData(expandedData)
	if err != nil {
		return err
	}
	return PopulateFlagsFromYAMLMap(yamlMap, node)
}

func getYAMLMapAndNodeFromData(data []byte) (map[string]any, *yaml.Node, error) {
	yamlMap := make(map[string]any)
	if err := yaml.Unmarshal([]byte(data), yamlMap); err != nil {
		return nil, nil, status.InternalErrorf("Error parsing config file: %s", err)
	}
	node := &yaml.Node{}
	if err := yaml.Unmarshal([]byte(data), node); err != nil {
		return nil, nil, status.InternalErrorf("Error parsing config file: %s", err)
	}
	if len(node.Content) > 0 {
		node = node.Content[0]
	}
	typeMap, err := GenerateYAMLMapWithValuesFromFlags(
		func(flg *flag.Flag) (reflect.Type, error) {
			return GetYAMLTypeForFlagValue(flg.Value)
		},
		IgnoreFilter,
	)
	if err != nil {
		return nil, nil, err
	}
	if err := RetypeAndFilterYAMLMap(yamlMap, typeMap, []string{}); err != nil {
		return nil, nil, status.InternalErrorf("Error encountered retyping YAML map: %s", err)
	}
	return yamlMap, node, nil
}

func onSIGHUP(fn func() error) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)
	go func() {
		for range c {
			if err := fn(); err != nil {
				log.Warningf("SIGHUP handler err: %s", err)
			}
		}
	}()
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

	if err := PopulateFlagsFromData(fileBytes); err != nil {
		return err
	}

	// Setup a listener to re-read the config file on SIGHUP.
	onSIGHUP(func() error {
		log.Infof("Re-reading buildbuddy config from '%s'", configFile)
		fileBytes, err := os.ReadFile(configFile)
		if err != nil {
			return fmt.Errorf("Error reading config file: %s", err)
		}
		return PopulateFlagsFromData(fileBytes)
	})
	return nil
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
	common.DefaultFlagSet.Visit(func(flg *flag.Flag) {
		setFlags[flg.Name] = struct{}{}
	})

	return populateFlagsFromYAML(m, []string{}, node, setFlags, true)
}

func populateFlagsFromYAML(a any, prefix []string, node *yaml.Node, setFlags map[string]struct{}, appendSlice bool) error {
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
			if err := populateFlagsFromYAML(v, p, n, setFlags, appendSlice); err != nil {
				return err
			}
		}
		return nil
	}
	name := strings.Join(prefix, ".")
	if _, ok := ignoreSet[name]; ok {
		return nil
	}

	flg := common.DefaultFlagSet.Lookup(name)
	if flg == nil {
		return nil
	}
	return setValueForYAML(flg.Value, name, a, setFlags, appendSlice)
}

func setValueForYAML(flagValue flag.Value, name string, newValue any, setFlags map[string]struct{}, appendSlice bool, setHooks ...func()) error {
	if v, ok := flagValue.(YAMLSetValueHooked); ok {
		setHooks = append(setHooks, v.YAMLSetValueHook)
	}
	return common.SetValueWithCustomIndirectBehavior(flagValue, name, newValue, setFlags, appendSlice, setValueForYAML, setHooks...)
}

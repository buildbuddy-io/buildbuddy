package options

import (
	"fmt"
	"iter"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"

	bfpb "github.com/buildbuddy-io/buildbuddy/proto/bazel_flags"
)

const (
	StarlarkBuiltinPluginID = "//builtin/starlark"
	UnknownBuiltinPluginID  = "//builtin/unknown"
)

const (
	longForm = iota
	shortForm
	negativeForm
)

// These are the starlark flag prefixes
var StarlarkSkippedPrefixes = map[string]struct{}{
	"//":   {},
	"no//": {},
	"@":    {},
	"no@":  {},
}

// Before Bazel 7, the flag protos did not contain the `RequiresValue` field,
// so there is no way to identify expansion options, which must be parsed
// differently. Since there are only nineteen such options (and bazel 6 is
// currently only receiving maintenance support and thus unlikely to add new
// expansion options), we can just enumerate them here so that we can correctly
// identify them in the absence of that field.
var preBazel7ExpansionOptions = map[string]struct{}{
	"noincompatible_genquery_use_graphless_query":     {},
	"incompatible_genquery_use_graphless_query":       {},
	"persistent_android_resource_processor":           {},
	"persistent_multiplex_android_resource_processor": {},
	"persistent_android_dex_desugar":                  {},
	"persistent_multiplex_android_dex_desugar":        {},
	"persistent_multiplex_android_tools":              {},
	"start_app":                                       {},
	"debug_app":                                       {},
	"java_debug":                                      {},
	"remote_download_minimal":                         {},
	"remote_download_toplevel":                        {},
	"host_jvm_debug":                                  {},
	"long":                                            {},
	"short":                                           {},
	"expunge_async":                                   {},
	"experimental_spawn_scheduler":                    {},
	"experimental_persistent_javac":                   {},
	"null":                                            {},
	"order_results":                                   {},
	"noorder_results":                                 {},
}

type Defined interface {
	Name() string
	ShortName() string
	Multi() bool
	HasNegative() bool
	RequiresValue() bool
	HasSupportedCommands() bool
	Supports(string) bool
	PluginID() string
}

// Definition defines a single Bazel option for the parser.
//
// TODO: Allow plugins to define their own option definitions.
type Definition struct {
	// name is the long-form name of this flag. Example: "compilation_mode"
	name string

	// shortName is the short-form name of this flag, if one exists. Bazel only
	// allows single letters for these, like "c". Note that the "=" assignment
	// syntax cannot be used with short names. For example,
	// "--compilation_mode=opt" would be specified as "-c opt", not "-c=opt".
	shortName string

	// multi specifies whether the flag can be passed more than once.
	// Each occurrence of the flag value is accumulated in a list.
	multi bool

	// hasNegative specifies whether the flag allows a "no" prefix" to be used in
	// order to set the value to false.
	hasNegative bool

	// Flags that do not require a value must be parsed differently. Their name
	// and value, if any,must appear as a single token, which means the "=" syntax
	// has to be used when assigning a value. For example, "bazel build
	// --subcommands false" is actually equivalent to "bazel build
	// --subcommands=true //false:false".
	requiresValue bool

	// The list of commands that support this option.
	supportedCommands map[string]struct{}

	// pluginID is the ID of the bb cli plugin associated with this option
	// definition, if applicable (or a pseudo-plugin ID for so-called "built-in"
	// plugins).
	pluginID string
}

func (d *Definition) Name() string {
	return d.name
}

func (d *Definition) ShortName() string {
	return d.shortName
}

func (d *Definition) Multi() bool {
	return d.multi
}

func (d *Definition) HasNegative() bool {
	return d.hasNegative
}

func (d *Definition) RequiresValue() bool {
	return d.requiresValue
}

func (d *Definition) HasSupportedCommands() bool {
	return len(d.supportedCommands) != 0
}

func (d *Definition) SupportedCommands() iter.Seq[string] {
	return func(yield func(string) bool) {
		for k := range d.supportedCommands {
			if !yield(k) {
				return
			}
		}
	}
}

func (d *Definition) Supports(command string) bool {
	_, ok := d.supportedCommands[command]
	return ok
}

func (d *Definition) AddSupportedCommand(commands ...string) {
	if d.supportedCommands == nil {
		d.supportedCommands = make(map[string]struct{}, 1)
	}
	for _, command := range commands {
		d.supportedCommands[command] = struct{}{}
	}
}

func (d *Definition) PluginID() string {
	return d.pluginID
}

type DefinitionOpt func(d *Definition)

func WithShortName(shortName string) DefinitionOpt {
	return func(d *Definition) { d.shortName = shortName }
}
func WithMulti() DefinitionOpt         { return func(d *Definition) { d.multi = true } }
func WithNegative() DefinitionOpt      { return func(d *Definition) { d.hasNegative = true } }
func WithRequiresValue() DefinitionOpt { return func(d *Definition) { d.requiresValue = true } }
func WithPluginID(pluginID string) DefinitionOpt {
	return func(d *Definition) { d.pluginID = pluginID }
}

func WithSupportFor(commands ...string) DefinitionOpt {
	return func(d *Definition) {
		if len(commands) == 0 {
			return
		}
		if d.supportedCommands == nil {
			d.supportedCommands = make(map[string]struct{}, len(commands))
		}
		for _, command := range commands {
			d.supportedCommands[command] = struct{}{}
		}
	}
}

func NewDefinition(name string, opts ...DefinitionOpt) *Definition {
	d := &Definition{name: name}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// DefinitionFrom takes a FlagInfo proto message and converts it into a
// Definition.
func DefinitionFrom(info *bfpb.FlagInfo) *Definition {
	switch info.GetName() {
	case "bazelrc":
		// `bazel help flags-as-proto` incorrectly reports `bazelrc` as not
		// allowing multiple values.
		// See https://github.com/bazelbuild/bazel/issues/24730 for more info.
		v := true
		info.AllowsMultiple = &v
	case "block_for_lock":
		// `bazel help flags-as-proto` incorrectly reports `block_for_lock` as
		// supporting non-startup commands, but in actuality it only has an effect
		// as a startup option.
		// See https://github.com/bazelbuild/bazel/pull/24953 for more info.
		info.Commands = []string{"startup"}
	case "watchfs":
		// `bazel help flags-as-proto` can report `watchfs` as being supported
		// as a startup option, despite it being deprecated as a startup option
		// and moved to only be supported as a command option.
		//
		// If it is supported as a command option, we remove "startup" from its
		// list of supported commands. In newer versions of bazel (v8.0.0+), this
		// is already true and thus this step is unnecessary.
		if len(info.GetCommands()) > 1 {
			commands := []string{}
			for _, c := range info.GetCommands() {
				if c != "startup" {
					commands = append(commands, c)
				}
			}
			info.Commands = commands
		}
	case "experimental_convenience_symlinks":
		fallthrough
	case "subcommands":
		// `bazel help flags-as-proto` incorrectly reports `subcommands` as not
		// having a negative form.
		// See https://github.com/bazelbuild/bazel/issues/24882 for more info.
		v := true
		info.HasNegativeFlag = &v
	}
	if info.RequiresValue == nil {
		// If flags-as-proto does not support RequiresValue, mark flags with
		// negative forms and known expansion flags as not requiring values, and
		// mark all other flags as requiring values.
		if info.GetHasNegativeFlag() {
			v := false
			info.RequiresValue = &v
		} else if _, ok := preBazel7ExpansionOptions[info.GetName()]; ok {
			v := false
			info.RequiresValue = &v
		} else {
			v := true
			info.RequiresValue = &v
		}
	}
	d := &Definition{
		name:              info.GetName(),
		shortName:         info.GetAbbreviation(),
		multi:             info.GetAllowsMultiple(),
		hasNegative:       info.GetHasNegativeFlag(),
		requiresValue:     info.GetRequiresValue(),
		supportedCommands: make(map[string]struct{}, len(info.GetCommands())),
	}
	for _, cmd := range info.GetCommands() {
		d.supportedCommands[cmd] = struct{}{}
	}
	return d
}

// Option represents a single parsed command-line option, including any value
// that option may have, regardless of if said value was provided with an `=`,
// as a separate argument, or, in the case of boolean arguments, implicitly via
// the `--[no]option` syntax. The benefit this interface affords us is largely
// that we can implement types for specific kinds of options and thus handle
// the behavior of those options more cleanly and with greater readability by
// reducing the need for large blocks of branching conditionals.
type Option interface {
	arguments.Argument
	Defined
	ExpectsValue() bool
	ClearValue()
	GetDefinition() *Definition
	UseShortName(bool)
	Normalized() Option
}

// GeneralOption is the concrete implementation of `Option` which supports all
// types of options and their associated value, if any. It is a stop-gap measure
// before we move to more specialized `Option` types, like types that require
// values, or can use the negative/positive boolean forms, or flags that expand
// into other flags, or starlark flags.
type GeneralOption struct {
	*Definition
	Value         *string
	UsesShortName bool
	IsNegative    bool
	Joined        bool
}

func (o *GeneralOption) GetDefinition() *Definition {
	return o.Definition
}

func (o *GeneralOption) ExpectsValue() bool {
	return o.Definition.RequiresValue() && o.Value == nil
}

func (o *GeneralOption) GetValue() string {
	if o.Value != nil {
		return *o.Value
	}
	if o.Definition.HasNegative() {
		if o.IsNegative {
			return "0"
		}
		return "1"
	}
	return ""
}

func (o *GeneralOption) ClearValue() {
	o.Value = nil
	o.IsNegative = false
}

func (o *GeneralOption) SetValue(value string) {
	o.Value = &value
	o.IsNegative = false
}

func (o *GeneralOption) Format() []string {
	if o.Value != nil {
		switch {
		case o.Definition.PluginID() == StarlarkBuiltinPluginID && o.IsNegative:
			// Starlark flags can have both a "no" prefix and a value; account for
			// that here.
			return []string{"--no" + o.Definition.Name() + "=" + *o.Value}
		case o.Joined || o.Definition.HasNegative():
			return []string{"--" + o.Definition.Name() + "=" + *o.Value}
		case o.UsesShortName && o.Definition.ShortName() != "":
			return []string{"-" + o.Definition.ShortName(), *o.Value}
		default:
			return []string{"--" + o.Definition.Name(), *o.Value}
		}
	}
	if o.IsNegative {
		return []string{"--no" + o.Definition.Name()}
	}
	if o.UsesShortName && o.Definition.ShortName() != "" {
		return []string{"-" + o.Definition.ShortName()}
	}
	return []string{"--" + o.Definition.Name()}
}

func (o *GeneralOption) UseShortName(u bool) {
	o.UsesShortName = u && o.Definition.ShortName() != ""
}

// Normalized returns a copy of this option after being normalized to a form
// that will format into the canonical representation of the option.
func (o *GeneralOption) Normalized() Option {
	if o.Definition.PluginID() == UnknownBuiltinPluginID {
		// don't normalize unknown options
		return &GeneralOption{
			Definition:    o.Definition,
			Value:         o.Value,
			UsesShortName: o.UsesShortName,
			IsNegative:    o.IsNegative,
			Joined:        o.Joined,
		}
	}
	if o.Definition.PluginID() == StarlarkBuiltinPluginID {
		// Starlark flags can have both a "no" prefix and a value; account for
		// that here.
		return &GeneralOption{
			Definition: o.Definition,
			Value:      o.Value,
			IsNegative: o.IsNegative,
		}
	}
	if o.Definition.RequiresValue() {
		// standard required value
		return &GeneralOption{
			Definition: o.Definition,
			Value:      o.Value,
			Joined:     true,
		}
	}
	if o.Definition.HasNegative() {
		normalizedValue, err := o.AsBool()
		if err != nil {
			return &GeneralOption{
				Definition: o.Definition,
				Value:      o.Value,
			}
		}
		return &GeneralOption{
			Definition: o.Definition,
			IsNegative: !normalizedValue,
		}
	}
	return &GeneralOption{
		Definition: o.Definition,
	}
}

func (o *GeneralOption) AsBool() (bool, error) {
	switch o.GetValue() {
	case "yes", "true", "1", "":
		return true, nil
	case "no", "false", "0":
		return false, nil
	}
	return false, fmt.Errorf("Error converting to bool: flag '--%s' has non-boolean value '%s'.", o.Name(), o.GetValue())
}

func NewStarlarkOptionDefinition(optName string) *Definition {
	return &Definition{
		name:        strings.TrimPrefix(optName, "no"),
		multi:       true,
		hasNegative: true,
		pluginID:    StarlarkBuiltinPluginID,
	}
}

func NewOption(optName string, v *string, d *Definition) (Option, error) {
	if d == nil {
		return nil, fmt.Errorf("In NewOption: definition was nil for optname %s and value %+v", optName, v)
	}

	// validate optName
	var form int
	switch optName {
	case d.name:
		form = longForm
	case d.shortName:
		form = shortForm
	case "no" + d.name:
		if d.hasNegative {
			form = negativeForm
			break
		}
		fallthrough
	default:
		return nil, fmt.Errorf("option name '%s' cannot specify an option with definition '%#v'", optName, d)
	}

	if d.PluginID() == StarlarkBuiltinPluginID {
		return &GeneralOption{Definition: d, Value: v, IsNegative: form == negativeForm}, nil
	}
	if d.requiresValue {
		return &GeneralOption{Definition: d, Value: v, UsesShortName: form == shortForm, Joined: v != nil}, nil
	}
	if v != nil {
		// A flag that didn't require a value had one anyway; this is normally okay if this
		// isn't a startup option, but if it's an expansion option we need to emit
		// a warning, and if it's a boolean option prefixed with "no", we need to emit an
		// error.
		if d.Supports("startup") {
			// Unlike command options, startup options don't allow specifying
			// values for options that do not require values.
			return nil, fmt.Errorf("in option --%q: option %q does not take a value", optName, d.name)
		}
		if !d.hasNegative {
			// This is an expansion option with a specified value. Expansion options
			// ignore values and output a warning. Since we canonicalize the options
			// and remove the value ourselves, we should output the warning instead.
			log.Warnf("option '%s' is an expansion option. It does not accept values, and does not change its expansion based on the value provided. Value '%s' will be ignored.", d.name, v)
			v = nil
		}
		if form == negativeForm {
			// This is a negative boolean value (of the form "--noNAME") with a
			// specified value, which is unsupported.
			return nil, fmt.Errorf("Unexpected value after boolean option: %s", optName)
		}
	}
	o := &GeneralOption{Definition: d, Value: v, UsesShortName: form == shortForm, IsNegative: form == negativeForm}
	return o, nil
}

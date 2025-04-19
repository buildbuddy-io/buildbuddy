package options

import (
	"fmt"
	"iter"

	bfpb "github.com/buildbuddy-io/buildbuddy/proto/bazel_flags"
)

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

type Option struct {
	*Definition
	Value string
}

func (o *Option) AsBool() (bool, error) {
	switch o.Value {
	case "yes":
		return true, nil
	case "true":
		return true, nil
	case "1":
		return true, nil
	case "":
		return true, nil
	case "no":
		return false, nil
	case "false":
		return false, nil
	case "0":
		return false, nil
	}
	return false, fmt.Errorf("Error converting to bool: flag '--%s' has non-boolean value '%s'.", o.Name(), o.Value)
}

func (o *Option) GetDefinition() *Definition {
	return o.Definition
}

// package parser handles the logic for parsing and manipulating Bazel
// configuration (both flags and RC files).
package parser

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/google/shlex"

	bfpb "github.com/buildbuddy-io/buildbuddy/proto/bazel_flags"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	workspacePrefix                  = `%workspace%/`
	enablePlatformSpecificConfigFlag = "enable_platform_specific_config"
)

var (
	// Inheritance hierarchy: https://bazel.build/run/bazelrc#option-defaults
	// All commands inherit from "common".
	parentCommand = map[string]string{
		"test":           "build",
		"run":            "build",
		"clean":          "build",
		"mobile-install": "build",
		"info":           "build",
		"print_action":   "build",
		"config":         "build",
		"cquery":         "build",
		"aquery":         "build",

		"coverage": "test",
	}

	bazelFlagHelpPattern = regexp.MustCompile(`` +
		`^\s+--` + // Each flag help line begins with "  --"
		`(?P<no>\[no\])?` + // then the optional string "[no]"
		`(?P<name>\w+)\s*` + // then a flag name like "compilation_mode"
		`(\[-(?P<short_name>\w+)\]\s+)?` + // then an optional short name like "[-c]"
		`(\((?P<description>.*)\))?` + // then an optional description like "(some help text)"
		`$`)

	flagShortNamePattern = regexp.MustCompile(`^[a-z]$`)

	// make this a var so the test can replace it.
	bazelHelp = runBazelHelpWithCache

	optionSetsOnce = sync.OnceValue(func() *struct{options map[string]*OptionSet; error} {
		type Return = struct{options map[string]*OptionSet; error}
		protoHelp, err := bazelHelp()
		if err != nil {
			return &Return{nil, err}
		}
		flagCollection, err := DecodeHelpFlagsAsProto(protoHelp)
		if err != nil {
			return &Return{nil, err}
		}
		sets, err := GetOptionSetsfromProto(flagCollection)
		return &Return{sets, err}
	})

	bazelCommandsOnce = sync.OnceValue(func() *struct{commands map[string]struct{}; error} {
		type Return = struct{commands map[string]struct{}; error}
		sets, err := OptionSets()
		if err != nil {
			return &Return{nil, err}
		}
		commands := make(map[string]struct{}, len(sets))
		for command := range(sets) {
			if command == "startup" || command == "common" || command == "always" {
				// not a real command, just a flag classifier
				continue
			}
			commands[command] = struct{}{}
		}
		return &Return{commands, nil}
	})

)

// Before Bazel 7, the flag protos did not contain the `RequiresValue` field,
// so there is no way to identify expansion options, which must be parsed
// differently. Since there are only nineteen such options (and bazel 6 is
// currently only receiving maintenance support and thus unlikely to add new
// expansion options), we can just enumerate them here so that we can correctly
// identify them in the absence of that field.
var preBazel7ExpansionOptions = map[string]struct{}{
	"noincompatible_genquery_use_graphless_query":     struct{}{},
	"incompatible_genquery_use_graphless_query":       struct{}{},
	"persistent_android_resource_processor":           struct{}{},
	"persistent_multiplex_android_resource_processor": struct{}{},
	"persistent_android_dex_desugar":                  struct{}{},
	"persistent_multiplex_android_dex_desugar":        struct{}{},
	"persistent_multiplex_android_tools":              struct{}{},
	"start_app":                     struct{}{},
	"debug_app":                     struct{}{},
	"java_debug":                    struct{}{},
	"remote_download_minimal":       struct{}{},
	"remote_download_toplevel":      struct{}{},
	"host_jvm_debug":                struct{}{},
	"long":                          struct{}{},
	"short":                         struct{}{},
	"expunge_async":                 struct{}{},
	"experimental_spawn_scheduler":  struct{}{},
	"experimental_persistent_javac": struct{}{},
	"null":                          struct{}{},
	"order_results":                 struct{}{},
	"noorder_results":               struct{}{},
}

// These are the startup flags that bazel forbids in rc files.
var StartupFlagNoRc = map[string]struct{}{
	"ignore_all_rc_files": {},
	"home_rc": {},
	"workspace_rc": {},
	"system_rc": {},
	"bazelrc": {},
}

// These are the prefixes
var StarlarkSkippedPrefixes = map[string]struct{}{
	"--//": {},
	"--no//": {},
	"--@": {},
	"--no@": {},
}


// OptionSet contains a set of Option schemas, indexed for ease of parsing.
type OptionSet struct {
	All         []*OptionSchema
	ByName      map[string]*OptionSchema
	ByShortName map[string]*OptionSchema

	// Label is the label this option set is associated with. This can be a
	// command, "startup", "common", or "always".
	// If  it is "startup", this slightly changes parsing semantics: booleans
	// cannot be specified with "=0", or "=1".
	Label string
}

type Option struct {
	OptionSchema *OptionSchema
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
	return false, fmt.Errorf("Error converting to bool: flag '--%s' has non-boolean value '%s'.", o.OptionSchema.Name, o.Value)
}

func (o *Option) String() string {
	if o.OptionSchema.HasNegative {
		v, err := o.AsBool()
		if err != nil {
			return "--" + o.OptionSchema.Name + "=" + o.Value
		}
		if v {
			return "--" + o.OptionSchema.Name
		} else {
			return "--no" + o.OptionSchema.Name
		}
	}
	if !o.OptionSchema.RequiresValue {
		return "--" + o.OptionSchema.Name
	}
	return "--" + o.OptionSchema.Name + "=" + o.Value
}

type ParsedArgs struct {
	StartupOptions []*Option
	Command        string
	CommandOptions []*Option
	PositionalArgs []string
}

type ParsedConfig struct {
	Options        map[string][]*Option
	PositionalArgs map[string][]string
}

type ExpandedConfig struct {
	Options []*Option
	PositionalArgs []string
}

func NewOptionSet(options []*OptionSchema, label string) *OptionSet {
	s := &OptionSet{
		All:         options,
		ByName:      map[string]*OptionSchema{},
		ByShortName: map[string]*OptionSchema{},
		Label:       label,
	}
	for _, o := range options {
		s.ByName[o.Name] = o
		if o.ShortName != "" {
			s.ByShortName[o.ShortName] = o
		}
	}
	return s
}

func NewParsedConfig() *ParsedConfig {
	return &ParsedConfig{
		Options: map[string][]*Option{},
		PositionalArgs: map[string][]string{},
	}
}

// Next parses the next option in the args list, starting at the given index. It
// is intended to be called in a loop that parses an argument list against an
// OptionSet.
//
// If the start index is out of bounds or if the next argument requires a
// lookahead that is out of bounds, it returns an error.
//
// It returns the option schema (if known), the canonical argument value, and
// the next iteration index. When the args are exhausted, the next iteration
// index is returned as len(list), which the caller should handle.
//
// If args[start] corresponds to an option that is not known by the option set,
// the returned values will be (nil, "", start+1). It is up to the caller to
// decide how args[start] should be interpreted.
func (s *OptionSet) Next(args []string, start int) (option *Option, next int, err error) {
	if start > len(args) {
		return nil, -1, fmt.Errorf("arg index %d out of bounds", start)
	}
	startToken := args[start]
	var optionSchema *OptionSchema
	var optValue *string
	if strings.HasPrefix(startToken, "--") {
		longName := strings.TrimPrefix(startToken, "--")
		eqIndex := strings.Index(longName, "=")
		if eqIndex >= 0 {
			v := longName[eqIndex+1:]
			longName = longName[:eqIndex]
			optionSchema = s.ByName[longName]
			optValue = &v
			if optionSchema != nil && !optionSchema.RequiresValue {
				// Unlike command options, startup options don't allow specifying
				// values for options that do not require values.
				if s.Label == "startup" {
					return nil, -1, fmt.Errorf("in option %q: option %q does not take a value", startToken, optionSchema.Name)
				}
				// Boolean options may specify values, but expansion options ignore
				// values and output a warning. Since we canonicalize the options and
				// remove the value ourselves, we should output the warning instead.
				if !optionSchema.HasNegative {
					log.Warnf("option '--%s' is an expansion option. It does not accept values, and does not change its expansion based on the value provided. Value '%s' will be ignored.", optionSchema.Name, *optValue)
					optValue = nil
				}
			}
		} else {
			optionSchema = s.ByName[longName]
			// If the long name is unrecognized, check to see if it's actually
			// specifying a bool flag like "noremote_upload_local_results"
			if optionSchema == nil && strings.HasPrefix(longName, "no") {
				longName := strings.TrimPrefix(longName, "no")
				optionSchema = s.ByName[longName]
				if optionSchema != nil && !optionSchema.HasNegative {
					return nil, -1, fmt.Errorf("illegal use of 'no' prefix on non-boolean option: %s", startToken)
				}
				v := "0"
				optValue = &v
			}
		}
	} else if strings.HasPrefix(startToken, "-") {
		shortName := strings.TrimPrefix(startToken, "-")
		if !flagShortNamePattern.MatchString(shortName) {
			return nil, -1, fmt.Errorf("invalid options syntax: %s", startToken)
		}
		optionSchema = s.ByShortName[shortName]
	}
	if optionSchema == nil {
		// Unknown option, possibly a positional argument or plugin-specific
		// argument. Let the caller decide what to do.
		return nil, start, nil
	}
	next = start + 1
	if optValue == nil {
		if !optionSchema.RequiresValue {
			v := ""
			if optionSchema.HasNegative {
				v = "1"
			}
			optValue = &v
		} else {
			if start+1 >= len(args) {
				return nil, -1, fmt.Errorf("expected value after %s", startToken)
			}
			v := args[start+1]
			optValue = &v
			next = start + 2
		}
	}
	// Canonicalize boolean values.
	if optionSchema.HasNegative {
		if *optValue == "false" || *optValue == "no" {
			*optValue = "0"
		} else if *optValue == "true" || *optValue == "yes" {
			*optValue = "1"
		}
	}
	option = &Option{
		OptionSchema: optionSchema,
		Value: *optValue,
	}
	return option, next, nil
}

// formatoption returns a canonical representation of an option name=value
// assignment as a single token.
func (o *Option) formatOption() string {
	if o.OptionSchema.RequiresValue {
		return "--" + o.OptionSchema.Name + "=" + o.Value
	}
	if !o.OptionSchema.HasNegative {
		return "--" + o.OptionSchema.Name
	}
	// We use "--name" or "--noname" as the canonical representation for
	// bools, since these are the only formats allowed for startup options.
	// Subcommands like "build" and "run" do allow other formats like
	// "--name=true" or "--name=0", but we choose to stick with the lowest
	// common demoninator between subcommands and startup options here,
	// mainly to avoid confusion.
	if o.Value == "1" || o.Value == "true" || o.Value == "yes" || o.Value == "" {
		return "--" + o.OptionSchema.Name
	}
	if o.Value == "0" || o.Value == "false" || o.Value == "no" {
		return "--no" + o.OptionSchema.Name
	}
	// Account for flags that have negative forms, but also accept non-boolean
	// arguments, like `--subcommands=pretty_print`
	return "--" + o.OptionSchema.Name + "=" + o.Value
}

// OptionSchema describes the schema for a single Bazel option.
//
// TODO: Allow plugins to define their own option schemas.
type OptionSchema struct {
	// Name is the long-form name of this flag. Example: "compilation_mode"
	Name string

	// ShortName is the short-form name of this flag, if one exists. Bazel only
	// allows single letters for these, like "c". Note that the "=" assignment
	// syntax cannot be used with short names. For example,
	// "--compilation_mode=opt" would be specified as "-c opt", not "-c=opt".
	ShortName string

	// Multi specifies whether the flag can be passed more than once.
	// Each occurrence of the flag value is accumulated in a list.
	Multi bool

	// HasNegative specifies whether the flag allows a "no" prefix" to be used in
	// order to set the value to false.
	HasNegative bool

	// Flags that do not require a value must be parsed differently. Their name
	// and value, if any,must appear as a single token, which means the "=" syntax
	// has to be used when assigning a value. For example, "bazel build
	// --subcommands false" is actually equivalent to "bazel build
	// --subcommands=true //false:false".
	RequiresValue bool

	// The list of commands that support this option.
	SupportedCommands map[string]struct{}
}

func BazelCommands() (map[string]struct{}, error) {
	once := bazelCommandsOnce()
	return once.commands, once.error
}

// CommandLineSchema specifies the flag parsing schema for a bazel command line
// invocation.
type CommandLineSchema struct {
	// StartupOptions contains the allowed startup options. These depend only
	// on the version of Bazel being used.
	StartupOptions *OptionSet

	// Command contains the literal command parsed from the command line.
	Command string

	// CommandOptions contains the possible options for the command. These
	// depend on both the command being run as well as the version of bazel
	// being invoked.
	CommandOptions *OptionSet

	// TODO: Allow plugins to register custom StartupOptions and CommandOptions
	// so that we can properly parse those arguments. For example, plugins could
	// tell us whether a particular argument requires a bool or a string, so
	// that we know for certain whether we should parse "--plugin_arg foo" as
	// "--plugin_arg=foo" or "--plugin_arg=true //foo:foo". This would also
	// allow us to show plugin-specific help.
}

// DecodeHelpFlagsAsProto takes the output of `bazel help flags-as-proto` and
// returns the FlagCollection proto message it encodes.
func DecodeHelpFlagsAsProto(protoHelp string) (*bfpb.FlagCollection, error) {
	b, err := base64.StdEncoding.DecodeString(protoHelp)
	if err != nil {
		return nil, err
	}
	flagCollection := &bfpb.FlagCollection{}
	if err := proto.Unmarshal(b, flagCollection); err != nil {
		return nil, err
	}
	return flagCollection, nil
}

// GetOptionSetsFromProto takes a FlagCollection proto message, converts it into
// Options, places each option into OptionSets based on the commands it
// specifies (creating new OptionSets if necessary), and then returns a map
// such that those OptionSets are keyed by the associated command (or "startup"
// in the case of startup options). Additionally, any non-startup flags are also
// placed in the "common" OptionSet for ease of rc file parsing.
func GetOptionSetsfromProto(flagCollection *bfpb.FlagCollection) (map[string]*OptionSet, error) {
	sets := make(map[string]*OptionSet)
	for _, info := range flagCollection.FlagInfos {
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
		case "watch_fs":
			// `bazel help flags-as-proto` can report `watch_fs` as bring supported
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
			// `bazel help flags-as-proto` incorrectly reports
			// `experimental_convenience_symlinks` as not having a negative form.
			// See https://github.com/bazelbuild/bazel/issues/24882 for more info.
			v := true
			info.HasNegativeFlag = &v
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
		o := &OptionSchema{
			Name:              info.GetName(),
			ShortName:         info.GetAbbreviation(),
			Multi:             info.GetAllowsMultiple(),
			HasNegative:       info.GetHasNegativeFlag(),
			RequiresValue:     info.GetRequiresValue(),
			SupportedCommands: make(map[string]struct{}, len(info.GetCommands())),
		}
		for _, cmd := range info.GetCommands() {
			o.SupportedCommands[cmd] = struct{}{}
		}
		var maybeCommonAndAlways []string
		if len(info.GetCommands()) != 1 || info.GetCommands()[0] != "startup" {
			maybeCommonAndAlways = []string{"common", "always"}
		}
		for _, cmd := range append(info.GetCommands(), maybeCommonAndAlways...)  {
			var set *OptionSet
			var ok bool
			if set, ok = sets[cmd]; ok {
				set.All = append(set.All, o)
				set.ByName[o.Name] = o
				if o.ShortName != "" {
					set.ByShortName[o.ShortName] = o
				}
			} else {
				sets[cmd] = NewOptionSet([]*OptionSchema{o}, cmd)
			}
		}
	}
	return sets, nil
}

func OptionSets() (map[string]*OptionSet, error) {
	once := optionSetsOnce()
	return once.options, once.error
}

// Parse options until we encounter a positional argument, and return the
// options and the index of the positional argument that terminated parsing,
// or the length of the input arguments array if no positional argument was
// encountered.
func ParseOptions(args []string, optionSet *OptionSet) ([]*Option, int, error) {
	if optionSet == nil {
		optionSets, err := OptionSets()
		if err != nil {
			return nil, 0, err
		}
		optionSet = optionSets["startup"]
	}
	var parsedOptions []*Option
	// Iterate through the args, looking for a terminating token.
	for i := 0; i < len(args); {
		token := args[i]
		if token == "--" {
			// POSIX-specified (and bazel-supported) delimiter to end option parsing
			return parsedOptions, i, nil
		}
		option, next, err := optionSet.Next(args, i)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to parse options: %s", err)
		}
		i = next
		if option.OptionSchema != nil {
			// We parsed an option, not a positional argument
			parsedOptions = append(parsedOptions, option)
			continue
		}
		if !strings.HasPrefix(token, "-") {
			// This is a positional argument, return it.
			return parsedOptions, i, nil
		}
		// Ignore unrecognized tokens for now.
		// TODO: Return an error for unknown tokens once we have a fully
		// static schema, including plugin flags.
		//
		// return nil, nil, 0, fmt.Errorf("failed to parse options: Unrecognized option '%s'", token)
	}
	return parsedOptions, len(args), nil
}

func ParseArgs(args []string, optionSet *OptionSet) (*ParsedArgs, error) {
	optionSets, err := OptionSets()
	if err != nil {
		return nil, err
	}
	parsedArgs := &ParsedArgs{}
	if optionSet != nil {
		parsedArgs.Command = optionSet.Label
	}
	next := args
	for {
		if optionSet == nil {
			optionSet = optionSets["startup"]
		}
		options, argIndex, err := ParseOptions(next, optionSet)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s options: %s", optionSet.Label, err)
		}
		if optionSet != nil {
			parsedArgs.CommandOptions = append(parsedArgs.CommandOptions, options...)
		} else {
			parsedArgs.StartupOptions = options
		}
		next = next[argIndex:]
		if len(next) == 0 {
			break
		}
		if next[0] == "--" {
			parsedArgs.PositionalArgs = append(parsedArgs.PositionalArgs, next[1:]...)
			next = nil
		} else {
			parsedArgs.PositionalArgs = append(parsedArgs.PositionalArgs, next[0])
			next = next[1:]
		}
		if parsedArgs.Command == "" {
			parsedArgs.Command = parsedArgs.PositionalArgs[0]
			parsedArgs.PositionalArgs = parsedArgs.PositionalArgs[1:]
			if parsedArgs.Command == "" {
				// bazel treats a blank command as a help command that halts both option and
				// argument parsing and ignores all non-startup options in the rc file.
				parsedArgs.PositionalArgs = nil
				break
			}
			bazelCommands, err := BazelCommands() 
			if err != nil {
				return nil, err
			}
			if _, ok := bazelCommands[parsedArgs.Command]; !ok {
				return nil, fmt.Errorf("Command '%s' not found. Try 'bb help'.", parsedArgs.PositionalArgs[0])
			}
			optionSet = optionSets[parsedArgs.Command]
		}
	}
	if parsedArgs.Command == "" {
		// bazel treats no command as a "help" command.
		parsedArgs.Command = "help"
	}
	return parsedArgs, nil
}

func CanonicalizeOptions(options []*Option) ([]*Option, error) {
	lastOptionIndex := map[string]int{}
	for i, opt := range options {
		lastOptionIndex[opt.OptionSchema.Name] = i
	}
	// Accumulate only the last instance of a given option
	var canonical []*Option
	for i, opt := range options {
		if !opt.OptionSchema.Multi && lastOptionIndex[opt.OptionSchema.Name] > i {
			continue
		}
		canonical = append(canonical, opt)
	}
	sort.SliceStable(canonical, func(i, j int) bool {
		return canonical[i].OptionSchema.Name < canonical[j].OptionSchema.Name
	})
	return canonical, nil
}

func (p *ParsedArgs) CanonicalizeOptions() error {
	startupOptions, err := CanonicalizeOptions(p.StartupOptions)
	if err != nil {
		return err
	}
	p.StartupOptions = startupOptions
	commandOptions, err := CanonicalizeOptions(p.CommandOptions)
	if err != nil {
		return err
	}
	p.CommandOptions = commandOptions
	return nil
}

func (p *ParsedArgs) GetOption(optionName string) ([]*Option, error) {
	var matches []*Option
	var optionSchema *OptionSchema
	process := func(o *Option) error {
		if o.OptionSchema.Name == optionName {
			if optionSchema == nil {
				optionSchema = o.OptionSchema
				matches = append(matches, o)
			} else if optionSchema != o.OptionSchema {
				return fmt.Errorf("Found multiple options named '%s' with conflicting schemae.", optionName)
			} else {
				if optionSchema.Multi {
					matches = append(matches, o)
				} else {
					matches[0] = o
				}
			}
		}
		return nil
	}
	for _, o := range p.StartupOptions {
		if err := process(o); err != nil {
			return nil, err
		}
	}
	startupMatches := len(matches)
	for _, o := range p.CommandOptions {
		if err := process(o); err != nil {
			return nil, err
		}
	}
	if startupMatches != 0 && startupMatches != len(matches) {
		return nil, fmt.Errorf("Found option '%s' in both the startup and the command options; cannot resolve value.", optionName)
	}
	return matches, nil
}

func (p *ParsedArgs) RemoveOption(optionName string) {
	for i := len(p.StartupOptions) - 1; i >= 0; i-- {
		if p.StartupOptions[i].OptionSchema.Name == optionName {
			p.StartupOptions = append(p.StartupOptions[:i], p.StartupOptions[i+1:]...)
		}
	}
	for i := len(p.CommandOptions) - 1; i >= 0; i-- {
		if p.CommandOptions[i].OptionSchema.Name == optionName {
			p.CommandOptions = append(p.CommandOptions[:i], p.CommandOptions[i+1:]...)
		}
	}
}

func (p *ParsedArgs) AppendOption(optionName, value string) error {
	optionSets, err := OptionSets()
	if err != nil {
		return err
	}
	startupOptions := optionSets["startup"]
	var startupOptionSchema *OptionSchema
	if startupOptions != nil {
		startupOptionSchema = startupOptions.ByName[optionName]
	}
	commandOptions := optionSets[p.Command]
	var commandOptionSchema *OptionSchema
	if commandOptions != nil {
		commandOptionSchema = commandOptions.ByName[optionName]
	}
	if startupOptionSchema == nil && commandOptionSchema == nil {
		return fmt.Errorf("No option schema found for option '%s'.", optionName)
	}
	if startupOptionSchema != nil && commandOptionSchema != nil {
		return fmt.Errorf("Option schema found for option '%s' in both startup options and %s options, cannot resolve schema.", optionName, p.Command)
	}
	if startupOptionSchema != nil {
		p.StartupOptions = append(p.StartupOptions, &Option{OptionSchema: startupOptionSchema, Value: value})
	}
	if commandOptionSchema != nil {
		p.CommandOptions = append(p.CommandOptions, &Option{OptionSchema: commandOptionSchema, Value: value})
	}
	return nil
}

// runBazelHelpWithCache returns the `bazel help flags-as-proto` output for the
// version of bazel that will be chosen by bazelisk. The output is cached in
// ~/.cache/buildbuddy/bazel_metadata/$VERSION/help/$TOPIC.txt
func runBazelHelpWithCache() (string, error) {
	resolvedVersion, err := bazelisk.ResolveVersion()
	if err != nil {
		return "", fmt.Errorf("could not resolve effective bazel version: %s", err)
	}
	versionKey := resolvedVersion
	if filepath.IsAbs(versionKey) {
		// If using an absolute path to a bazel binary, use its digest as the
		// version key.
		f, err := os.Open(versionKey)
		if err != nil {
			return "", err
		}
		defer f.Close()
		d, err := digest.Compute(f, repb.DigestFunction_SHA256)
		if err != nil {
			return "", err
		}
		versionKey = d.GetHash()
	}
	bbCacheDir, err := storage.CacheDir()
	if err != nil {
		return "", err
	}
	helpCacheDir := filepath.Join(bbCacheDir, "bazel_metadata", versionKey, "help")
	if err := os.MkdirAll(helpCacheDir, 0755); err != nil {
		return "", fmt.Errorf("failed to initialize bazel metadata cache: %s", err)
	}
	topic := "flags-as-proto"
	helpCacheFilePath := filepath.Join(helpCacheDir, fmt.Sprintf("%s.txt", topic))
	b, err := os.ReadFile(helpCacheFilePath)
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("failed to read from bazel metadata cache: %s", err)
	}
	if err == nil {
		return string(b), nil
	}

	tmp, err := os.CreateTemp("", "bazel-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %s", err)
	}
	defer tmp.Close()
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir: %s", err)
	}
	defer os.RemoveAll(tmpDir)
	buf := &bytes.Buffer{}
	log.Printf("\x1b[90mGathering metadata for bazel %s...\x1b[m", topic)
	opts := &bazelisk.RunOpts{Stdout: io.MultiWriter(tmp, buf)}
	exitCode, err := bazelisk.Run([]string{
		"--ignore_all_rc_files",
		// Run in a temp output base to avoid messing with any running bazel
		// server in the current workspace.
		"--output_base=" + filepath.Join(tmpDir, "output_base"),
		// Make sure this server doesn't stick around for long.
		"--max_idle_secs=10",
		"help",
		topic,
	}, opts)
	if err != nil {
		return "", fmt.Errorf("failed to run bazel: %s", err)
	}
	if exitCode != 0 {
		return "", fmt.Errorf("unknown error from `bazel help %s`: exit code %d", topic, exitCode)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("failed to close temp file: %s", err)
	}
	if err := disk.MoveFile(tmp.Name(), helpCacheFilePath); err != nil {
		return "", fmt.Errorf("failed to write to bazel metadata cache: %s", err)
	}
	return buf.String(), nil
}

type Rules struct {
	All              []*RcRule
	ByPhaseAndConfig map[string]map[string][]*RcRule
}

// RcRule is a rule parsed from a bazelrc file.
type RcRule struct {
	Phase  string
	Config string
	// Tokens contains the raw (non-canonicalized) tokens in the rule.
	Tokens []string
}

func appendRcRulesFromImport(workspaceDir, path string, configs map[string]map[string][]string, optional bool, importStack []string) error {
	if strings.HasPrefix(path, workspacePrefix) {
		path = filepath.Join(workspaceDir, path[len(workspacePrefix):])
	}

	file, err := os.Open(path)
	if err != nil {
		if optional {
			return nil
		}
		return err
	}
	defer file.Close()
	return appendRcRulesFromFile(workspaceDir, file, configs, importStack)
}

// configs is a map keyed by config name where the values are maps keyed by phase name where the values are lists containing all the rules for
// that config in the order they are encountered.
func appendRcRulesFromFile(workspaceDir string, f *os.File, configs map[string]map[string][]string, importStack []string) error {
	rpath, err := realpath(f.Name())
	if err != nil {
		return fmt.Errorf("could not determine real path of bazelrc file: %s", err)
	}
	for _, path := range importStack {
		if path == rpath {
			return fmt.Errorf("circular import detected: %s -> %s", strings.Join(importStack, " -> "), rpath)
		}
	}
	importStack = append(importStack, rpath)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		// Handle line continuations (lines can end with "\" to effectively
		// continue the same line)
		for strings.HasSuffix(line, `\`) && scanner.Scan() {
			line = line[:len(line)-1] + scanner.Text()
		}

		line = stripCommentsAndWhitespace(line)

		tokens := strings.Fields(line)
		if len(tokens) == 0 {
			// blank line
			continue
		}
		if tokens[0] == "import" || tokens[0] == "try-import" {
			isOptional := tokens[0] == "try-import"
			path := strings.TrimSpace(strings.TrimPrefix(line, tokens[0]))
			if err = appendRcRulesFromImport(workspaceDir, path, configs, isOptional, importStack); err != nil {
				return err
			}
			continue
		}

		rule, err := parseRcRule(line)
		if err != nil {
			log.Debugf("Error parsing bazelrc option: %s", err.Error())
			continue
		}
		if rule == nil {
			continue
		}
		// Bazel doesn't support configs for startup options and ignores them if
		// they appear in a bazelrc: https://bazel.build/run/bazelrc#config
		if rule.Phase == "startup" && rule.Config != "" {
			continue
		}
		configs[rule.Config][rule.Phase] = append(configs[rule.Config][rule.Phase], rule.Tokens...)
	}
	log.Debugf("Added rc rules from %q; new configs: %#v", rpath, configs)
	return scanner.Err()
}

func realpath(path string) (string, error) {
	directPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return filepath.Abs(directPath)
}

func stripCommentsAndWhitespace(line string) string {
	index := strings.Index(line, "#")
	if index >= 0 {
		line = line[:index]
	}
	return strings.TrimSpace(line)
}

func parseRcRule(line string) (*RcRule, error) {
	tokens, err := shlex.Split(line)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, fmt.Errorf("unexpected empty line")
	}
	if len(tokens) == 1 {
		// bazel ignores .bazelrc lines consisting of a single shlex token
		return nil, nil
	}
	if !strings.Contains(tokens[0], ":") {
		return &RcRule{
			Phase:  tokens[0],
			Tokens: tokens[1:],
		}, nil
	}
	phaseConfig := strings.Split(tokens[0], ":")
	if len(phaseConfig) != 2 {
		return nil, fmt.Errorf("invalid bazelrc syntax: %s", phaseConfig)
	}
	return &RcRule{
		Phase:  phaseConfig[0],
		Config: phaseConfig[1],
		Tokens: tokens[1:],
	}, nil
}

func ParseRCFiles(workspaceDir string, filePaths ...string) (map[string]*ParsedConfig, error) {
	optionSets, err := OptionSets()
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	parsedConfigs := map[string]*ParsedConfig{}
	for _, filePath := range filePaths {
		configs := map[string]map[string][]string{}
		r, err := realpath(filePath)
		if err != nil {
			continue
		}
		if seen[r] {
			continue
		}
		seen[r] = true

		file, err := os.Open(filePath)
		if err != nil {
			continue
		}
		defer file.Close()
		err = appendRcRulesFromFile(workspaceDir, file, configs, nil /*=importStack*/)
		if err != nil {
			return nil, err
		}
		for name, config := range configs {
			parsedConfig, ok := parsedConfigs[name]
			if !ok {
				parsedConfig = NewParsedConfig()
				parsedConfigs[name] = parsedConfig
			}
			for phase, tokens := range config {
				parsedArgs, err := ParseArgs(tokens, optionSets[phase])
				if err != nil {
					return nil, err
				}
				parsedConfig.Options[phase] = append(parsedConfig.Options[phase], parsedArgs.CommandOptions...)
				parsedConfig.PositionalArgs[phase] = append(parsedConfig.PositionalArgs[phase], parsedArgs.PositionalArgs...)
			}
		}
	}
	return parsedConfigs, nil
}

func (p *ParsedArgs) ExpandConfigs(parsedConfigs map[string]*ParsedConfig) error {
	cliPositionalArgLen := len(p.PositionalArgs)
	if err := p.expandConfigs(parsedConfigs); err != nil {
		return err
	}
	if err := p.CanonicalizeOptions(); err != nil {
		return err
	}
	for i := 0; i < len(p.CommandOptions); {
		if p.CommandOptions[i].OptionSchema.Name != enablePlatformSpecificConfigFlag {
			i++
			continue
		}
		phases := getPhases(p.Command)
		platformConfig, err := appendArgsForConfig(nil, parsedConfigs, phases, getBazelOS(), nil, true)
		if err != nil {
			return err
		}
		for j := 0; j < len(platformConfig.Options); {
			if platformConfig.Options[j].OptionSchema.Name != enablePlatformSpecificConfigFlag {
				j++
				continue
			}
			// ignore any enable_platform_specific_config flag within the platform config expansion
			platformConfig.Options = append(platformConfig.Options[:j], platformConfig.Options[j+1:]...)
		}
		p.CommandOptions = concat(p.CommandOptions[:i], platformConfig.Options, p.CommandOptions[i+1:])
		configPositionalArgLen := len(p.PositionalArgs) - cliPositionalArgLen
		p.PositionalArgs = concat(p.PositionalArgs[:configPositionalArgLen], platformConfig.PositionalArgs, p.PositionalArgs[configPositionalArgLen:])
	}
	return nil
}

// Mirroring the behavior here:
// https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/runtime/ConfigExpander.java#L41
func getBazelOS() string {
	switch runtime.GOOS {
	case "linux":
		return "linux"
	case "darwin":
		return "macos"
	case "windows":
		return "windows"
	case "freebsd":
		return "freebsd"
	case "openbsd":
		return "openbsd"
	default:
		return runtime.GOOS
	}
}

func (p *ParsedArgs) ConsumeAndParseRCFiles() (map[string]*ParsedConfig, error) {
	ws, err := workspace.Path()
	if err != nil {
		log.Debugf("Could not determine workspace dir: %s", err)
	}
	return p.consumeAndParseRCFiles(ws)
}

func (p *ParsedArgs) consumeAndParseRCFiles(workspaceDir string) (map[string]*ParsedConfig, error) {
	rcFiles, err := consumeRCFileArgs(p, workspaceDir)
	if err != nil {
		return nil, err
	}
	log.Debugf("Parsing rc files %s", rcFiles)
	parsedConfigs, err := ParseRCFiles(workspaceDir, rcFiles...)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bazelrc file: %s", err)
	}
	return parsedConfigs, nil
}

func (p *ParsedArgs) expandConfigs(parsedConfigs map[string]*ParsedConfig) error {
	// Expand startup args first, before any other args (including explicit
	// startup args).
	startupConfig, err := appendArgsForConfig(nil, parsedConfigs, []string{"startup"}, "" /*=config*/, nil, true)
	if err != nil {
		return fmt.Errorf("failed to expand startup options: %s", err)
	}
	if len(startupConfig.PositionalArgs) != 0 {
		return fmt.Errorf("Unknown startup option: '%s'.\nFor more info, run 'bb help startup_options'.", startupConfig.PositionalArgs[0])
	}
	p.StartupOptions = append(startupConfig.Options, p.StartupOptions...)

	// Always apply bazelrc rules in order of the precedence hierarchy. For
	// example, for the "test" command, apply options in order of "always",
	// then "common", then "build", then "test".
	phases := getPhases(p.Command)
	log.Debugf("Bazel command: %q, rc rule classes: %v", p.Command, phases)

	// We'll refer to args in bazelrc which aren't expanded from a --config
	// option as "default" args, like a .bazelrc line that just says "-c dbg" or
	// "build -c dbg" as opposed to something qualified like "build:dbg -c dbg".
	//
	// These default args take lower precedence than explicit command line args
	// so we expand those first just after the command.
	log.Debugf("Args before expanding default rc rules: %#v", p)
	commandConfig, err := appendArgsForConfig(nil, parsedConfigs, phases, "" /*=config*/, nil, true)
	if err != nil {
		return fmt.Errorf("failed to evaluate bazelrc configuration: %s", err)
	}
	log.Debugf("Prepending arguments %v from default rc rules", commandConfig)
	p.CommandOptions = append(commandConfig.Options, p.CommandOptions...)
	p.PositionalArgs = append(commandConfig.PositionalArgs, p.PositionalArgs...)
	log.Debugf("Options after expanding default rc rules: %v", p.CommandOptions)
	log.Debugf("Positional arguments after expanding default rc rules: %v", p.PositionalArgs)


	configPositionalArgs := []string{}
	for i := 0; i < len(p.CommandOptions); {
		o := p.CommandOptions[i]
		if o.OptionSchema.Name != "config" {
			i++
			continue
		}
		configArgs, err := appendArgsForConfig(nil, parsedConfigs, phases, o.Value, nil, false)
		if err != nil {
			return fmt.Errorf("failed to evaluate bazelrc configuration: %s", err)
		}
		log.Debugf("Expanded config %q to %v", configArgs)
		p.CommandOptions = concat(p.CommandOptions[:i], configArgs.Options, p.CommandOptions[i+1:])
		i += len(configArgs.Options)
		configPositionalArgs = append(configPositionalArgs, configArgs.PositionalArgs...)
	}

	p.PositionalArgs = append(configPositionalArgs, p.PositionalArgs...)
	log.Debugf("Fully expanded args: %+v", p)

	return nil
}

func appendArgsForConfig(expandedConfig *ExpandedConfig, parsedConfigs map[string]*ParsedConfig, phases []string, config string, configStack []string, allowEmpty bool) (*ExpandedConfig, error) {
	parsedConfig, ok := parsedConfigs[config]
	if !ok {
		return nil, fmt.Errorf("Config value '%s' is not defined in any .rc file", config)
	}
	var err error
	for _, c := range configStack {
		if c == config {
			return nil, fmt.Errorf("circular --config reference detected: %s -> %s", strings.Join(configStack, " -> "), config)
		}
	}
	configStack = append(configStack, config)
	if expandedConfig == nil {
		expandedConfig = &ExpandedConfig{}
	}
	// empty config names do not require supported phases
	empty := true
	for _, phase := range phases {
		if _, ok := parsedConfig.Options[phase]; ok {
			empty = false
		}
		if _, ok := parsedConfig.PositionalArgs[phase]; ok {
			empty = false
		}
		for _, o := range parsedConfig.Options[phase] {
			if o.OptionSchema.Name == "config" {
				expandedConfig, err = appendArgsForConfig(expandedConfig, parsedConfigs, phases, o.Value, configStack, false)
				if err != nil {
					return nil, err
				}
			}
			// For the 'common' phase, only append the arg if it's supported by
			// the command.
			if phase == "common" {
				if _, ok := o.OptionSchema.SupportedCommands[phases[len(phases)-1]]; ok {
					expandedConfig.Options = append(expandedConfig.Options, o)
				} else {
					log.Debugf("common rc rule: opt %q is unsupported by command %q; skipping", o.OptionSchema.Name, phases[len(phases)-1])
				}
				continue
			}

			// Happy path: this is a "normal" phase like "build", "test" etc.
			// rather than a "pseudo-phase" like "common" etc., and it's a plain
			// old non-config arg that doesn't itself need to be expanded. Just
			// append the arg and continue.
			expandedConfig.Options = append(expandedConfig.Options, o)
		}
		expandedConfig.PositionalArgs = append(expandedConfig.PositionalArgs, parsedConfig.PositionalArgs[phase]...)
	}
	if empty && !allowEmpty {
		return nil, fmt.Errorf("Config value '%s' is not defined in any .rc file", config)
	}
	return expandedConfig, nil
}

func consumeRCFileArgs(parsedArgs *ParsedArgs, workspaceDir string) (rcFiles []string, err error) {
	// Before we do anything, check whether --ignore_all_rc_files is already
	// set. If so, return an empty list of RC files, since bazel will do the
	// same.
	ignoreAllRCFiles := false
	for _, o := range parsedArgs.StartupOptions {
		if o.OptionSchema.Name == "ignore_all_rc_files" {
			val, err := o.AsBool()
			if err != nil {
				return nil, err
			}
			ignoreAllRCFiles = val
		}
	}
	if ignoreAllRCFiles {
		return nil, nil
	}

	// Now do another pass through the args and parse workspace_rc, system_rc,
	// home_rc, and bazelrc args. Note that if we encounter an arg
	// --bazelrc=/dev/null, that means bazel will ignore subsequent --bazelrc
	// args, so we ignore them as well.
	workspaceRC := true
	systemRC := true
	homeRC := true
	encounteredDevNullBazelrc := false
	var newStartupArgs []*Option
	var explicitBazelrcPaths []string
	for _, o := range(parsedArgs.StartupOptions) {
		switch o.OptionSchema.Name {
		case "workspace_rc":
			val, err := o.AsBool()
			if err != nil {
				return nil, err
			}
			workspaceRC = val
			continue
		case "system_rc":
			val, err := o.AsBool()
			if err != nil {
				return nil, err
			}
			systemRC = val
			continue
		case "home_rc":
			val, err := o.AsBool()
			if err != nil {
				return nil, err
			}
			homeRC = val
			continue
		case "bazelrc":
			if o.Value != "" {
				if encounteredDevNullBazelrc {
					continue
				}
				if o.Value == "/dev/null" {
					encounteredDevNullBazelrc = true
					continue
				}
				explicitBazelrcPaths = append(explicitBazelrcPaths, o.Value)
				continue
			}
		}
		newStartupArgs = append(newStartupArgs, o)
	}
	parsedArgs.StartupOptions = newStartupArgs
	
	optionSets, err := OptionSets()
	if err != nil {
		return nil, err
	}
	// Ignore all RC files when actually running bazel, since the CLI has
	// already accounted for them.
	parsedArgs.StartupOptions = append(parsedArgs.StartupOptions, &Option{
		OptionSchema: optionSets["startup"].ByName["ignore_all_rc_files"],
		Value: "1",
	})
	// Parse rc files in the order defined here:
	// https://bazel.build/run/bazelrc#bazelrc-file-locations
	if systemRC {
		rcFiles = append(rcFiles, "/etc/bazel.bazelrc")
		rcFiles = append(rcFiles, `%ProgramData%\bazel.bazelrc`)
	}
	if workspaceRC && workspaceDir != "" {
		rcFiles = append(rcFiles, filepath.Join(workspaceDir, ".bazelrc"))
	}
	if homeRC {
		usr, err := user.Current()
		if err == nil {
			rcFiles = append(rcFiles, filepath.Join(usr.HomeDir, ".bazelrc"))
		}
	}
	rcFiles = append(rcFiles, explicitBazelrcPaths...)
	return rcFiles, nil
}

func asStartupBoolFlag(arg, name string) (value, ok bool) {
	if arg == "--"+name {
		return true, true
	}
	if arg == "--no"+name {
		return false, true
	}
	return false, false
}

// TODO: Return an empty string if the subcommand happens to come after
// a bb-specific command. For example, `bb install --path test` should
// return an empty string, not "test".
// TODO: More robust parsing of startup options. For example, this has a bug
// that passing `bazel --output_base build test ...` returns "build" as the
// bazel command, even though "build" is the argument to --output_base.
func GetBazelCommandAndIndex(args []string) (string, int) {
	commands, err := BazelCommands()
	if err != nil {
		return "", -1
	}
	for i, a := range args {
		if _, ok := commands[a]; ok {
			return a, i
		}
	}
	return "", -1
}

// GetFirstTargetPattern makes a best-attempt effort to return the first target
// pattern in a bazel command.
func GetFirstTargetPattern(args []string) string {
	_, bazelCmdIdx := GetBazelCommandAndIndex(args)
	for i := bazelCmdIdx + 1; i < len(args); i++ {
		s := args[i]
		// Skip over the shortened compilation_mode flag and its value (Ex. -c opt)
		if s == "-c" {
			i++
			continue
		}
		if !strings.HasPrefix(s, "-") {
			return s
		}
	}
	return ""
}

// getPhases returns the command's inheritance hierarchy in increasing order of
// precedence.
//
// Examples:
//
//	getPhases("run")      // {"always", "common", "build", "run"}
//	getPhases("coverage") // {"always", "common", "build", "test", "coverage"}
func getPhases(command string) (out []string) {
	for {
		if command == "" {
			out = append(out, "common", "always")
			break
		}
		out = append(out, command)
		command = parentCommand[command]
	}
	reverse(out)
	return
}

func reverse(a []string) {
	for i := 0; i < len(a)/2; i++ {
		j := len(a) - i - 1
		a[i], a[j] = a[j], a[i]
	}
}

	func concat[T any](slices ...[]T) []T {
	length := 0
	for _, s := range slices {
		length += len(s)
	}
	out := make([]T, 0, length)
	for _, s := range slices {
		out = append(out, s...)
	}
	return out
}

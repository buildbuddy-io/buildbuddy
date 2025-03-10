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

	StarlarkBuiltinPluginID = "//builtin/starlark"
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

	commonPhases = map[string]struct{}{
		"common": {},
		"always": {},
	}

	flagShortNamePattern = regexp.MustCompile(`^[a-z]$`)

	// make this a var so the test can replace it.
	bazelHelp = runBazelHelpWithCache

	generateParserOnce = sync.OnceValue(
		func() *struct {
			p *Parser
			error
		} {
			type Return = struct {
				p *Parser
				error
			}
			protoHelp, err := bazelHelp()
			if err != nil {
				return &Return{nil, err}
			}
			flagCollection, err := DecodeHelpFlagsAsProto(protoHelp)
			if err != nil {
				return &Return{nil, err}
			}
			parser, err := GenerateParser(flagCollection)
			return &Return{parser, err}
		},
	)
)

// Set the help text that encodes the bazel flags collection proto to the given
// string. Intended to be used only for testing purposes.
func SetBazelHelpForTesting(encodedProto string) {
	bazelHelp = func() (string, error) {
		return encodedProto, nil
	}
}

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
	"start_app":                                       struct{}{},
	"debug_app":                                       struct{}{},
	"java_debug":                                      struct{}{},
	"remote_download_minimal":                         struct{}{},
	"remote_download_toplevel":                        struct{}{},
	"host_jvm_debug":                                  struct{}{},
	"long":                                            struct{}{},
	"short":                                           struct{}{},
	"expunge_async":                                   struct{}{},
	"experimental_spawn_scheduler":                    struct{}{},
	"experimental_persistent_javac":                   struct{}{},
	"null":                                            struct{}{},
	"order_results":                                   struct{}{},
	"noorder_results":                                 struct{}{},
}

// These are the startup flags that bazel forbids in rc files.
var StartupFlagNoRc = map[string]struct{}{
	"ignore_all_rc_files": {},
	"home_rc":             {},
	"workspace_rc":        {},
	"system_rc":           {},
	"bazelrc":             {},
}

// These are the starlark flag prefixes
var StarlarkSkippedPrefixes = map[string]struct{}{
	"//":   {},
	"no//": {},
	"@":    {},
	"no@":  {},
}

type Option struct {
	OptionDefinition *OptionDefinition
	Value            string
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
	return false, fmt.Errorf("Error converting to bool: flag '--%s' has non-boolean value '%s'.", o.OptionDefinition.Name, o.Value)
}

// Parser contains a set of OptionDefinitions (indexed for ease of parsing) and
// the known bazel commands.
type Parser struct {
	ByName      map[string]*OptionDefinition
	ByShortName map[string]*OptionDefinition

	BazelCommands map[string]struct{}
}

func NewParser(optionDefinitions []*OptionDefinition) *Parser {
	p := &Parser{
		ByName:        map[string]*OptionDefinition{},
		ByShortName:   map[string]*OptionDefinition{},
		BazelCommands: map[string]struct{}{},
	}
	for _, o := range optionDefinitions {
		p.ByName[o.Name] = o
		if o.ShortName != "" {
			p.ByShortName[o.ShortName] = o
		}
	}
	return p
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
	Options        []*Option
	PositionalArgs []string
}

func (p *Parser) ForceAddOptionDefinition(o *OptionDefinition) {
	p.ByName[o.Name] = o
	if o.ShortName != "" {
		p.ByShortName[o.ShortName] = o
	}
}

func (p *Parser) AddOptionDefinition(o *OptionDefinition) error {
	if _, ok := p.ByName[o.Name]; ok {
		return fmt.Errorf("Naming collision adding flag %s; flag already exists with that name.", o.Name)
	}
	if _, ok := p.ByShortName[o.ShortName]; ok {
		return fmt.Errorf("Naming collision adding flag with short name %s; flag already exists with that short name.", o.ShortName)
	}
	p.ForceAddOptionDefinition(o)
	return nil
}

func NewParsedConfig() *ParsedConfig {
	return &ParsedConfig{
		Options:        map[string][]*Option{},
		PositionalArgs: map[string][]string{},
	}
}

// Next parses the next option in the args list, starting at the given index. It
// is intended to be called in a loop that parses an argument list against a
// Parser.
//
// If the start index is out of bounds or if the next argument requires a
// lookahead that is out of bounds, it returns an error.
//
// It returns the parsed option and the next iteration index. When the args are
// exhausted, the next iteration index is returned as len(list), which the
// caller should handle.
//
// If args[start] corresponds to an option definition that is not known by the
// parser, the returned values will be (nil, "", start+1). It is up to the
// caller to decide how args[start] should be interpreted.
func (p *Parser) Next(args []string, command string, start int) (option *Option, next int, err error) {
	if start > len(args) {
		return nil, -1, fmt.Errorf("arg index %d out of bounds", start)
	}
	startToken := args[start]
	option, needsValue, err := p.ParseOption(command, startToken)
	if err != nil {
		return nil, -1, err
	}
	if option == nil {
		log.Debugf("Unknown option %s for command %s", startToken, command)
		// Unknown option, possibly a positional argument or plugin-specific
		// argument. Let the caller decide what to do.
		return nil, start, nil
	}
	if !needsValue {
		return option, start + 1, nil
	}
	if start+1 >= len(args) {
		return nil, -1, fmt.Errorf("expected value after %s", startToken)
	}
	option.Value = args[start+1]
	return option, start + 2, nil
}

// formatOption returns a canonical representation of an option name=value
// assignment as a single token.
func (o *Option) formatOption() string {
	if o.OptionDefinition.RequiresValue {
		// normal `--flag=value` option
		return "--" + o.OptionDefinition.Name + "=" + o.Value
	}
	if !o.OptionDefinition.HasNegative {
		// expansion option, just return it
		return "--" + o.OptionDefinition.Name
	}
	// We use "--name" or "--noname" as the canonical representation for
	// bools, since these are the only formats allowed for startup options.
	// Subcommands like "build" and "run" do allow other formats like
	// "--name=true" or "--name=0", but we choose to stick with the lowest
	// common demoninator between subcommands and startup options here,
	// mainly to avoid confusion.
	v, err := o.AsBool()
	if err == nil {
		if v {
			return "--" + o.OptionDefinition.Name
		}
		return "--no" + o.OptionDefinition.Name
	}
	// Account for flags that have negative forms, but also accept non-boolean
	// arguments, like `--subcommands=pretty_print`
	return "--" + o.OptionDefinition.Name + "=" + o.Value
}

// OptionDefinition defines a single Bazel option for the parser.
//
// TODO: Allow plugins to define their own option definitions.
type OptionDefinition struct {
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

	// PluginID is the ID of the bb cli plugin associated with this option
	// definition, if applicable (or a pseudo-plugin ID for so-called "built-in"
	// plugins).
	PluginID string
}

func BazelCommands() (map[string]struct{}, error) {
	once := generateParserOnce()
	return once.p.BazelCommands, once.error
}

func (p *Parser) parseStarlarkOptionDefinition(optName string) *OptionDefinition {
	for prefix := range StarlarkSkippedPrefixes {
		if strings.HasPrefix(optName, prefix) {
			supportedCommands := make(map[string]struct{}, len(p.BazelCommands))
			for cmd := range p.BazelCommands {
				supportedCommands[cmd] = struct{}{}
			}
			d := &OptionDefinition{
				Name:              optName,
				Multi:             true,
				HasNegative:       true,
				SupportedCommands: supportedCommands,
				PluginID:          StarlarkBuiltinPluginID,
			}
			return d
		}
	}
	return nil
}

func (p *Parser) parseLongNameOption(command, optName string) (option *Option, needsValue bool, err error) {
	v := ""
	hasValue := false
	if eqIndex := strings.Index(optName, "="); eqIndex != -1 {
		// This option is of the form --NAME=value; split it up into the option
		// name and the option value.
		v = optName[eqIndex+1:]
		hasValue = true
		optName = optName[:eqIndex]
	}
	if d, ok := p.ByName[optName]; ok {
		if _, ok := d.SupportedCommands[command]; !ok {
			// The option exists, but does not support this command.
			return nil, false, nil
		}
		if d.PluginID == StarlarkBuiltinPluginID {
			// We don't validate or normalize starlark options
			return &Option{OptionDefinition: d, Value: v}, false, nil
		}
		if !d.RequiresValue && hasValue {
			// A flag that didn't require a value had one anyway; this is okay if this
			// isn't a startup option, but if it's an expansion option we need to emit
			// a warning.
			if command == "startup" {
				// Unlike command options, startup options don't allow specifying
				// values for options that do not require values.
				return nil, false, fmt.Errorf("in option --%q: option %q does not take a value", optName, d.Name)
			}
			if !d.HasNegative {
				// This is an expansion option with a specified value. Expansion options
				// ignore values and output a warning. Since we canonicalize the options
				// and remove the value ourselves, we should output the warning instead.
				log.Warnf("option '%s' is an expansion option. It does not accept values, and does not change its expansion based on the value provided. Value '%s' will be ignored.", d.Name, v)
				v = ""
			}
		}
		option := &Option{OptionDefinition: d, Value: v}
		if d.HasNegative {
			if b, err := option.AsBool(); err == nil {
				// Normalize this boolean value
				if b {
					option.Value = "1"
				} else {
					option.Value = "0"
				}
			}
		}
		return option, d.RequiresValue && !hasValue, nil
	}
	if boolOptName, found := strings.CutPrefix(optName, "no"); found {
		if d, ok := p.ByName[boolOptName]; ok && d.HasNegative {
			if _, ok := d.SupportedCommands[command]; !ok {
				// The option exists, but does not support this command.
				return nil, false, nil
			}
			if hasValue {
				// This is a negative boolean value (of the form "--noNAME") with a
				// specified value, which is unsupported.
				return nil, false, fmt.Errorf("Unexpected value after boolean option: %s", optName)
			}
			return &Option{OptionDefinition: d, Value: "0"}, false, nil
		}
	}
	if command != "startup" {
		// Check for starlark flags we haven't encountered yet, which won't be
		// listed in the definitions we parsed from the flags collection proto. All
		// bazel commands support starlark flags like "--@repo//path:name=value".
		// Even non-build commands like "bazel info" support these, but just ignore
		// them; however, they are not supported as startup flags. If it's a valid
		// starlark flag, we add it to the set of option definitions in the parser
		// in case we encounter it again.
		d := p.parseStarlarkOptionDefinition(optName)
		if d != nil {
			// No need to check if this option already exists since we never reach
			// this code if it does.
			p.ForceAddOptionDefinition(d)
			return &Option{OptionDefinition: d, Value: v}, false, nil
		}
	}
	// The option does not exist.
	return nil, false, nil
}

func (p *Parser) parseShortNameOption(command, optName string) *Option {
	if d, ok := p.ByShortName[optName]; ok {
		if _, ok := d.SupportedCommands[command]; !ok {
			// The option exists, but does not support this command.
			return nil
		}
		v := ""
		if d.HasNegative {
			// Normalize this boolean value
			v = "1"
		}
		return &Option{OptionDefinition: d, Value: v}
	}
	// The option does not exist.
	return nil
}

// ParseOption returns an Option with the OptionDefinition for the given command
// and opt (with a value if the flag includes an "=" or is a boolean flag with a
// "--no" prefix), or nil if there is no valid OptionDefinition with the
// provided name which supports the provided command. The opt is expected to be
// either "--NAME" or "-SHORTNAME". The boolean returned indicates whether this
// option still needs a value (which is to say, if the OptionDefinition requires
// a value but none was provided via an `=`).
func (p *Parser) ParseOption(command, opt string) (option *Option, needsValue bool, err error) {
	if optName, found := strings.CutPrefix(opt, "--"); found {
		return p.parseLongNameOption(command, optName)
	}
	if optName, found := strings.CutPrefix(opt, "-"); found {
		option = p.parseShortNameOption(command, optName)
		if option == nil {
			// Not a valid option
			return nil, false, nil
		}
		return option, option.OptionDefinition.RequiresValue, nil
	}
	// This is not an option.
	return nil, false, nil
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

// GenerateParser takes a FlagCollection proto message, converts it into
// OptionDefinitions, places each option definition into a Parser, scrapes a set
// of commands from all the supported commands listed by each option for the
// Parser, and returns the Parser.
func GenerateParser(flagCollection *bfpb.FlagCollection) (*Parser, error) {
	parser := NewParser(nil)
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
			// `bazel help flags-as-proto` can report `watch_fs` as being supported
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
		o := &OptionDefinition{
			Name:              info.GetName(),
			ShortName:         info.GetAbbreviation(),
			Multi:             info.GetAllowsMultiple(),
			HasNegative:       info.GetHasNegativeFlag(),
			RequiresValue:     info.GetRequiresValue(),
			SupportedCommands: make(map[string]struct{}, len(info.GetCommands())),
		}
		for _, cmd := range info.GetCommands() {
			o.SupportedCommands[cmd] = struct{}{}
			if cmd != "startup" {
				// not a real command, just a flag classifier
				parser.BazelCommands[cmd] = struct{}{}
			}
		}
		if err := parser.AddOptionDefinition(o); err != nil {
			return nil, err
		}
	}
	return parser, nil
}

func GetParser() (*Parser, error) {
	once := generateParserOnce()
	return once.p, once.error
}

func CanonicalizeStartupArgs(args []string) ([]string, error) {
	parsed, err := ParseArgs(args, "")
	if err != nil {
		return nil, err
	}
	startupOptions, err := CanonicalizeOptions(parsed.StartupOptions)
	if err != nil {
		return nil, err
	}
	parsed.StartupOptions = startupOptions
	return parsed.FormatOptions(), nil
}

func CanonicalizeArgs(args []string) ([]string, error) {
	parsed, err := ParseArgs(args, "")
	if err != nil {
		return nil, err
	}
	err = parsed.CanonicalizeOptions()
	if err != nil {
		return nil, err
	}
	return parsed.FormatOptions(), nil
}

// Parse options until we encounter a positional argument, and return the
// options and the index of the positional argument that terminated parsing,
// or the length of the input arguments array if no positional argument was
// encountered.
func (p *Parser) ParseOptions(args []string, command string) ([]*Option, int, error) {
	var parsedOptions []*Option
	// Iterate through the args, looking for a terminating token.
	for i := 0; i < len(args); {
		token := args[i]
		if token == "--" {
			// POSIX-specified (and bazel-supported) delimiter to end option parsing
			return parsedOptions, i, nil
		}
		option, next, err := p.Next(args, command, i)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to parse options: %s", err)
		}
		i = next
		if option != nil {
			// We parsed an option, not a positional argument
			if _, ok := option.OptionDefinition.SupportedCommands[command]; ok {
				parsedOptions = append(parsedOptions, option)
				continue
			}
			if _, ok := commonPhases[command]; ok {
				// Common phases support all non-startup options
				found := false
				for cmd := range option.OptionDefinition.SupportedCommands {
					if cmd != "startup" {
						found = true
						break
					}
				}
				if found {
					parsedOptions = append(parsedOptions, option)
					continue
				}
				return nil, 0, fmt.Errorf("option '%s' is not supported by phase '%s'.", option.OptionDefinition.Name, command)
			}
			return nil, 0, fmt.Errorf("option '%s' is not supported by command '%s'.", option.OptionDefinition.Name, command)
		}
		if !strings.HasPrefix(token, "-") {
			// This is a positional argument, return it.
			return parsedOptions, i, nil
		}
		return nil, 0, fmt.Errorf("failed to parse options: Unrecognized option '%s'", token)
	}
	return parsedOptions, len(args), nil
}

func ParseArgs(args []string, command string) (*ParsedArgs, error) {
	p, err := GetParser()
	if err != nil {
		return nil, err
	}
	parsedArgs := &ParsedArgs{
		Command: command,
	}
	next := args
	for {
		command := parsedArgs.Command
		if command == "" {
			command = "startup"
		}
		options, argIndex, err := p.ParseOptions(next, command)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s options: %s", command, err)
		}
		if command == "startup" {
			parsedArgs.StartupOptions = options
		} else {
			parsedArgs.CommandOptions = append(parsedArgs.CommandOptions, options...)
		}
		next = next[argIndex:]
		if len(next) == 0 {
			break
		}
		if next[0] == "--" {
			if len(next) > 1 {
				parsedArgs.PositionalArgs = append(parsedArgs.PositionalArgs, next[1:]...)
			}
			next = nil
		} else {
			parsedArgs.PositionalArgs = append(parsedArgs.PositionalArgs, next[0])
			next = next[1:]
		}
		if command == "startup" {
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
		}
	}
	if parsedArgs.Command == "" {
		// bazel treats no command as a "help" command.
		parsedArgs.Command = "help"
	}
	if len(parsedArgs.PositionalArgs) == 0 {
		// This makes our output more consistent, which makes testing easier.
		parsedArgs.PositionalArgs = nil
	}
	return parsedArgs, nil
}

func (p *ParsedArgs) FormatOptions() []string {
	length := len(p.StartupOptions) + 1 + len(p.CommandOptions) + len(p.PositionalArgs)
	if len(p.PositionalArgs) != 0 {
		// If there are positional args, we need to also have space for the `--`
		length++
	}
	tokens := make([]string, 0, length)
	for _, o := range p.StartupOptions {
		tokens = append(tokens, o.formatOption())
	}
	tokens = append(tokens, p.Command)
	for _, o := range p.CommandOptions {
		tokens = append(tokens, o.formatOption())
	}
	if len(p.PositionalArgs) != 0 {
		tokens = append(tokens, "--")
		tokens = append(tokens, p.PositionalArgs...)
	}
	return tokens
}

func CanonicalizeOptions(options []*Option) ([]*Option, error) {
	lastOptionIndex := map[string]int{}
	for i, opt := range options {
		lastOptionIndex[opt.OptionDefinition.Name] = i
	}
	// Accumulate only the last instance of a given option
	var canonical []*Option
	for i, opt := range options {
		if !opt.OptionDefinition.Multi && lastOptionIndex[opt.OptionDefinition.Name] > i {
			continue
		}
		if opt.OptionDefinition.HasNegative {
			// Possible boolean value; normalize the value if possible.
			// This may not be possible if this a bool/enum type flag.
			if v, err := opt.AsBool(); err == nil {
				if v {
					opt.Value = "1"
				} else {
					opt.Value = "0"
				}
			}
		}
		if !opt.OptionDefinition.HasNegative && !opt.OptionDefinition.RequiresValue {
			// Expansion argument, does not take a value
			opt.Value = ""
		}
		canonical = append(canonical, opt)
	}
	sort.SliceStable(canonical, func(i, j int) bool {
		return canonical[i].OptionDefinition.Name < canonical[j].OptionDefinition.Name
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

type IndexedOption struct {
	*Option
	index int
}

func (p *ParsedArgs) GetOption(optionName string, isStartupOption bool) ([]*IndexedOption, error) {
	var matches []*IndexedOption
	var optionDefinition *OptionDefinition
	options := p.CommandOptions
	if isStartupOption {
		options = p.StartupOptions
	}
	for i, o := range options {
		if o.OptionDefinition.Name == optionName {
			if len(matches) == 0 {
				optionDefinition = o.OptionDefinition
			} else if optionDefinition != o.OptionDefinition {
				return nil, fmt.Errorf("Found multiple options named '%s' with conflicting definitions.", optionName)
			}
			matches = append(matches, &IndexedOption{o, i})
		}
	}
	return matches, nil
}

func (p *ParsedArgs) RemoveOption(optionName string) {
	for i := len(p.StartupOptions) - 1; i >= 0; i-- {
		if p.StartupOptions[i].OptionDefinition.Name == optionName {
			p.StartupOptions = append(p.StartupOptions[:i], p.StartupOptions[i+1:]...)
		}
	}
	for i := len(p.CommandOptions) - 1; i >= 0; i-- {
		if p.CommandOptions[i].OptionDefinition.Name == optionName {
			p.CommandOptions = append(p.CommandOptions[:i], p.CommandOptions[i+1:]...)
		}
	}
}

func (p *ParsedArgs) AppendOption(optionName, value string) error {
	parser, err := GetParser()
	if err != nil {
		return err
	}
	optionDefinition := parser.ByName[optionName]
	if optionDefinition == nil {
		return fmt.Errorf("No option definition found for option '%s'.", optionName)
	}
	isStartupOption := false
	if _, ok := optionDefinition.SupportedCommands["startup"]; ok {
		isStartupOption = true
	}
	isCommandOption := false
	if _, ok := optionDefinition.SupportedCommands[p.Command]; ok {
		isCommandOption = true
	}

	if isStartupOption && isCommandOption {
		return fmt.Errorf("Option definition found for option '%s' in both startup options and %s options, cannot resolve definition.", optionName, p.Command)
	}
	if isStartupOption {
		p.StartupOptions = append(p.StartupOptions, &Option{OptionDefinition: optionDefinition, Value: value})
		return nil
	}
	if isCommandOption {
		p.CommandOptions = append(p.CommandOptions, &Option{OptionDefinition: optionDefinition, Value: value})
		return nil
	}
	return fmt.Errorf("Option definition found for option '%s', but it is not a startup option and the command '%s' does not support it.", optionName, p.Command)
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
	errBuf := &bytes.Buffer{}
	log.Debugf("\x1b[90mGathering metadata for bazel %s...\x1b[m", topic)
	opts := &bazelisk.RunOpts{Stdout: io.MultiWriter(tmp, buf), Stderr: errBuf}
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
		config, ok := configs[rule.Config]
		if !ok {
			config = make(map[string][]string)
			configs[rule.Config] = config
		}
		config[rule.Phase] = append(config[rule.Phase], rule.Tokens...)
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
				parsedArgs, err := ParseArgs(tokens, phase)
				if err != nil {
					return nil, err
				}
				if phase == "startup" {
					for _, o := range parsedArgs.StartupOptions {
						if _, ok := StartupFlagNoRc[o.OptionDefinition.Name]; ok {
							return nil, fmt.Errorf("Can't specify %s in the .bazelrc file.", o.OptionDefinition.Name)
						}
					}
					parsedConfig.Options[phase] = append(parsedConfig.Options[phase], parsedArgs.StartupOptions...)
				} else {
					parsedConfig.Options[phase] = append(parsedConfig.Options[phase], parsedArgs.CommandOptions...)
				}
				if len(parsedArgs.PositionalArgs) > 0 {
					parsedConfig.PositionalArgs[phase] = append(parsedConfig.PositionalArgs[phase], parsedArgs.PositionalArgs...)
				}
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
	indexedOptions, err := p.GetOption(enablePlatformSpecificConfigFlag, false)
	if err != nil {
		return err
	}
	if len(indexedOptions) == 0 {
		return nil
	}
	lastIndexedOption := indexedOptions[len(indexedOptions)-1]
	v, err := lastIndexedOption.AsBool()
	if err != nil {
		return err
	}
	i := lastIndexedOption.index
	if v {
		phases := getPhases(p.Command)
		platformConfig, err := appendArgsForConfig(nil, parsedConfigs, phases, getBazelOS(), nil, true)
		if err != nil {
			return err
		}
		p.CommandOptions = concat(p.CommandOptions[:i], platformConfig.Options, p.CommandOptions[i+1:])
		if len(platformConfig.PositionalArgs) > 0 {
			configPositionalArgLen := len(p.PositionalArgs) - cliPositionalArgLen
			p.PositionalArgs = concat(p.PositionalArgs[:configPositionalArgLen], platformConfig.PositionalArgs, p.PositionalArgs[configPositionalArgLen:])
		}
	}
	// We only expand the last platform-specific config flag, if it's true, and
	// then remove all others, including any that may occur within that expansion.
	p.RemoveOption(enablePlatformSpecificConfigFlag)
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
	parser, err := GetParser()
	if err != nil {
		return nil, err
	}
	return parser.consumeAndParseRCFiles(p, ws)
}

func (p *Parser) consumeAndParseRCFiles(parsed *ParsedArgs, workspaceDir string) (map[string]*ParsedConfig, error) {
	rcFiles, err := consumeRCFileArgs(parsed, workspaceDir)
	if err != nil {
		return nil, err
	}
	log.Debugf("Parsing rc files %s", rcFiles)
	parsedConfigs, err := ParseRCFiles(workspaceDir, rcFiles...)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bazelrc file: %s", err)
	}

	// Ignore all RC files when actually running bazel, since the CLI has already
	// accounted for them.
	ignoreAllRCFilesOptionDefinition, ok := p.ByName["ignore_all_rc_files"]
	if !ok {
		return nil, fmt.Errorf("`ignore_all_rc_files` was not present in the option definitions.")
	}
	defaultConfig, ok := parsedConfigs[""]
	if !ok {
		defaultConfig = NewParsedConfig()
		parsedConfigs[""] = defaultConfig
	}
	defaultConfig.Options["startup"] = append(defaultConfig.Options["startup"], &Option{
		OptionDefinition: ignoreAllRCFilesOptionDefinition,
		Value:            "1",
	})

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
	if len(commandConfig.PositionalArgs) > 0 {
		p.PositionalArgs = append(commandConfig.PositionalArgs, p.PositionalArgs...)
	}
	log.Debugf("Options after expanding default rc rules: %v", p.CommandOptions)
	log.Debugf("Positional arguments after expanding default rc rules: %v", p.PositionalArgs)

	var configPositionalArgs []string
	for i := 0; i < len(p.CommandOptions); {
		o := p.CommandOptions[i]
		if o.OptionDefinition.Name != "config" {
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
		if len(configArgs.PositionalArgs) > 0 {
			configPositionalArgs = append(configPositionalArgs, configArgs.PositionalArgs...)
		}
	}

	if len(configPositionalArgs) > 0 {
		p.PositionalArgs = append(configPositionalArgs, p.PositionalArgs...)
	}
	log.Debugf("Fully expanded args: %+v", p)

	return nil
}

func appendArgsForConfig(expandedConfig *ExpandedConfig, parsedConfigs map[string]*ParsedConfig, phases []string, config string, configStack []string, allowEmpty bool) (*ExpandedConfig, error) {
	if expandedConfig == nil {
		expandedConfig = &ExpandedConfig{}
	}
	parsedConfig, ok := parsedConfigs[config]
	if !ok {
		if config == "" {
			// Empty config name is always valid
			return expandedConfig, nil
		}
		return nil, fmt.Errorf("Config value '%s' is not defined in any .rc file", config)
	}
	var err error
	for _, c := range configStack {
		if c == config {
			return nil, fmt.Errorf("circular --config reference detected: %s -> %s", strings.Join(configStack, " -> "), config)
		}
	}
	configStack = append(configStack, config)
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
			if o.OptionDefinition.Name == "config" {
				expandedConfig, err = appendArgsForConfig(expandedConfig, parsedConfigs, phases, o.Value, configStack, false)
				if err != nil {
					return nil, err
				}
				continue
			}
			// For the common phases, only append the arg if it's supported by
			// the command.
			if _, ok := commonPhases[phase]; ok {
				if _, ok := o.OptionDefinition.SupportedCommands[phases[len(phases)-1]]; ok {
					expandedConfig.Options = append(expandedConfig.Options, o)
				} else {
					log.Debugf("common rc rule: opt %q is unsupported by command %q; skipping", o.OptionDefinition.Name, phases[len(phases)-1])
				}
				continue
			}

			// Happy path: this is a "normal" phase like "build", "test" etc.
			// rather than a "pseudo-phase" like "common" etc., and it's a plain
			// old non-config arg that doesn't itself need to be expanded. Just
			// append the arg and continue.
			expandedConfig.Options = append(expandedConfig.Options, o)
		}
		if len(parsedConfig.PositionalArgs[phase]) > 0 {
			expandedConfig.PositionalArgs = append(expandedConfig.PositionalArgs, parsedConfig.PositionalArgs[phase]...)
		}
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
	options, err := parsedArgs.GetOption("ignore_all_rc_files", true)
	if err != nil {
		return nil, err
	}
	parsedArgs.RemoveOption("ignore_all_rc_files")
	if len(options) > 0 {
		val, err := options[len(options)-1].AsBool()
		if err != nil {
			return nil, err
		}
		ignoreAllRCFiles = val
	}
	if ignoreAllRCFiles {
		parsedArgs.RemoveOption("workspace_rc")
		parsedArgs.RemoveOption("system_rc")
		parsedArgs.RemoveOption("home_rc")
		parsedArgs.RemoveOption("bazelrc")
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
	var newStartupOptions []*Option
	var explicitBazelrcPaths []string
	for _, o := range parsedArgs.StartupOptions {
		switch o.OptionDefinition.Name {
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
		newStartupOptions = append(newStartupOptions, o)
	}
	parsedArgs.StartupOptions = newStartupOptions

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

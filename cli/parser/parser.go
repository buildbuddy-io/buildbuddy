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
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
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

	flagShortNamePattern = regexp.MustCompile(`^[a-z]$`)

	// make this a var so the test can replace it.
	bazelHelp = runBazelHelpWithCache

	optionDefinitionSetsOnce = sync.OnceValue(
		func() *struct {
			optionDefinitionSets map[string]*OptionDefinitionSet
			error
		} {
			type Return = struct {
				optionDefinitionSets map[string]*OptionDefinitionSet
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
			sets, err := GetOptionDefinitionSetsfromProto(flagCollection)
			return &Return{sets, err}
		},
	)

	bazelCommandsOnce = sync.OnceValue(
		func() *struct {
			commands map[string]struct{}
			error
		} {
			type Return = struct {
				commands map[string]struct{}
				error
			}
			sets, err := OptionDefinitionSets()
			if err != nil {
				return &Return{nil, err}
			}
			commands := make(map[string]struct{}, len(sets))
			for command := range sets {
				if command == "startup" || command == "common" || command == "always" || command == "" {
					// not a real command, just a flag classifier
					continue
				}
				commands[command] = struct{}{}
			}
			return &Return{commands, nil}
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

// OptionDefinitionSet contains a set of OptionDefinitions, indexed for ease of
// parsing.
type OptionDefinitionSet struct {
	All         []*OptionDefinition
	ByName      map[string]*OptionDefinition
	ByShortName map[string]*OptionDefinition

	// IsStartupOptions represents whether this OptionDefinitionSet describes Bazel's
	// startup options. If true, this slightly changes parsing semantics:
	// booleans cannot be specified with "=0", or "=1".
	IsStartupOptions bool
}

func NewOptionDefinitionSet(optionDefinitions []*OptionDefinition, isStartupOptions bool) *OptionDefinitionSet {
	s := &OptionDefinitionSet{
		All:              optionDefinitions,
		ByName:           map[string]*OptionDefinition{},
		ByShortName:      map[string]*OptionDefinition{},
		IsStartupOptions: isStartupOptions,
	}
	for _, o := range optionDefinitions {
		s.ByName[o.Name] = o
		if o.ShortName != "" {
			s.ByShortName[o.ShortName] = o
		}
	}
	return s
}

// Next parses the next option in the args list, starting at the given index. It
// is intended to be called in a loop that parses an argument list against an
// OptionDefinitionSet.
//
// If the start index is out of bounds or if the next argument requires a
// lookahead that is out of bounds, it returns an error.
//
// It returns the option definition (if known), the canonical argument value,
// and the next iteration index. When the args are exhausted, the next iteration
// index is returned as len(list), which the caller should handle.
//
// If args[start] corresponds to an option definition that is not known by the
// option definition set, the returned values will be (nil, "", start+1). It is
// up to the caller to decide how args[start] should be interpreted.
func (s *OptionDefinitionSet) Next(args []string, start int) (optionDefinition *OptionDefinition, value string, next int, err error) {
	if start > len(args) {
		return nil, "", -1, fmt.Errorf("arg index %d out of bounds", start)
	}
	startToken := args[start]
	var optValue *string
	if strings.HasPrefix(startToken, "--") {
		longName := strings.TrimPrefix(startToken, "--")
		eqIndex := strings.Index(longName, "=")
		if eqIndex >= 0 {
			v := longName[eqIndex+1:]
			longName = longName[:eqIndex]
			optionDefinition = s.ByName[longName]
			optValue = &v
			if optionDefinition != nil && !optionDefinition.RequiresValue {
				// Unlike command options, startup options don't allow specifying
				// values for options that do not require values.
				if s.IsStartupOptions {
					return nil, "", -1, fmt.Errorf("in option %q: option %q does not take a value", startToken, optionDefinition.Name)
				}
				// Boolean options may specify values, but expansion options ignore
				// values and output a warning. Since we canonicalize the options and
				// remove the value ourselves, we should output the warning instead.
				if !optionDefinition.HasNegative {
					log.Warnf("option '--%s' is an expansion option. It does not accept values, and does not change its expansion based on the value provided. Value '%s' will be ignored.", optionDefinition.Name, *optValue)
					optValue = nil
				}
			}
		} else {
			optionDefinition = s.ByName[longName]
			// If the long name is unrecognized, check to see if it's actually
			// specifying a bool flag like "noremote_upload_local_results"
			if optionDefinition == nil && strings.HasPrefix(longName, "no") {
				longName := strings.TrimPrefix(longName, "no")
				optionDefinition = s.ByName[longName]
				if optionDefinition != nil && !optionDefinition.HasNegative {
					return nil, "", -1, fmt.Errorf("illegal use of 'no' prefix on non-boolean option: %s", startToken)
				}
				v := "0"
				optValue = &v
			}
		}
	} else if strings.HasPrefix(startToken, "-") {
		shortName := strings.TrimPrefix(startToken, "-")
		if !flagShortNamePattern.MatchString(shortName) {
			return nil, "", -1, fmt.Errorf("invalid options syntax: %s", startToken)
		}
		optionDefinition = s.ByShortName[shortName]
	}
	if optionDefinition == nil {
		// Unknown option, possibly a positional argument or plugin-specific
		// argument. Let the caller decide what to do.
		return nil, "", start + 1, nil
	}
	next = start + 1
	if optValue == nil {
		if !optionDefinition.RequiresValue {
			v := ""
			if optionDefinition.HasNegative {
				v = "1"
			}
			optValue = &v
		} else {
			if start+1 >= len(args) {
				return nil, "", -1, fmt.Errorf("expected value after %s", startToken)
			}
			v := args[start+1]
			optValue = &v
			next = start + 2
		}
	}
	// Canonicalize boolean values.
	if optionDefinition.HasNegative {
		if *optValue == "false" || *optValue == "no" {
			*optValue = "0"
		} else if *optValue == "true" || *optValue == "yes" {
			*optValue = "1"
		}
	}
	return optionDefinition, *optValue, next, nil
}

// formatOption returns a canonical representation of an option name=value
// assignment as a single token.
func formatOption(optionDefinition *OptionDefinition, value string) string {
	if optionDefinition.RequiresValue {
		return "--" + optionDefinition.Name + "=" + value
	}
	if !optionDefinition.HasNegative {
		return "--" + optionDefinition.Name
	}
	// We use "--name" or "--noname" as the canonical representation for
	// bools, since these are the only formats allowed for startup options.
	// Subcommands like "build" and "run" do allow other formats like
	// "--name=true" or "--name=0", but we choose to stick with the lowest
	// common demoninator between subcommands and startup options here,
	// mainly to avoid confusion.
	if value == "1" || value == "true" || value == "yes" || value == "" {
		return "--" + optionDefinition.Name
	}
	if value == "0" || value == "false" || value == "no" {
		return "--no" + optionDefinition.Name
	}
	// Account for flags that have negative forms, but also accept non-boolean
	// arguments, like `--subcommands=pretty_print`
	return "--" + optionDefinition.Name + "=" + value
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
}

// BazelHelpFunc returns the output of "bazel help flags-as-proto". Passing
// the help function lets us replace it for testing.
type BazelHelpFunc func() (string, error)

func BazelCommands() (map[string]struct{}, error) {
	once := bazelCommandsOnce()
	return once.commands, once.error
}

// CommandLineSchema specifies the flag parsing schema for a bazel command line
// invocation.
type CommandLineSchema struct {
	// StartupOptionDefinitions contains the allowed startup option definitions.
	// These depend only on the version of Bazel being used.
	StartupOptionDefinitions *OptionDefinitionSet

	// Command contains the literal command parsed from the command line.
	Command string

	// CommandOptionDefinitions contains definitions for the possible options for
	// the command. These depend on both the command being run as well as the
	// version of bazel being invoked.
	CommandOptionDefinitions *OptionDefinitionSet

	// TODO: Allow plugins to register custom StartupOptionDefinitions and
	// CommandOptionDefinitions so that we can properly parse those arguments. For
	// example, plugins could tell us whether a particular argument requires a
	// bool or a string, so that we know for certain whether we should parse
	// "--plugin_arg foo" as "--plugin_arg=foo" or "--plugin_arg=true //foo:foo".
	// This would also allow us to show plugin-specific help.
}

// CommandSupportsOpt returns whether the given opt is in the
// CommandOptionDefitions. The opt is expected to be either "--NAME" or
// "-SHORTNAME", without an "=".
func (s *CommandLineSchema) CommandSupportsOpt(opt string) bool {
	// TODO: this func is using heuristics, since a correct impl would require
	// us to know the schema for all bazel commands. At some point we should
	// probably just do a one-time parse of all the commands so that we can do
	// this properly, or see if Bazel can give us a dump of its option schema.
	if strings.HasPrefix(opt, "--") {
		// Long-form arg
		opt = strings.TrimPrefix(opt, "--")
		if _, ok := s.CommandOptionDefinitions.ByName[opt]; ok {
			return true
		}
		// Hack: try with trimmed 'no' prefix too, in case this is a bool opt.
		if strings.HasPrefix(opt, "no") {
			if _, ok := s.CommandOptionDefinitions.ByName[strings.TrimPrefix(opt, "no")]; ok {
				return true
			}
		}
		// Check for starlark flags, which won't be listed in the schema that we
		// parsed from "bazel help" output. All bazel commands support starlark
		// flags like "--@repo//path:name=value". Even non-build commands like
		// "bazel info" support these, but just ignore them.
		if strings.Contains(opt, ":") || strings.Contains(opt, "/") {
			return true
		}
		return false
	} else if strings.HasPrefix(opt, "-") {
		// Short-form arg
		opt = strings.TrimPrefix(opt, "-")
		_, ok := s.CommandOptionDefinitions.ByShortName[opt]
		return ok
	}
	return false
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

// GetOptionDefinitionSetsFromProto takes a FlagCollection proto message,
// converts it into OptionDefinitions, places each option definition into
// OptionDefinitionSets based on the commands it specifies (creating new
// OptionDefinitionSets if necessary), and then returns a map such that those
// OptionDefinitionSets are keyed by the associated command (or "startup" in the
// case of startup options).
func GetOptionDefinitionSetsfromProto(flagCollection *bfpb.FlagCollection) (map[string]*OptionDefinitionSet, error) {
	sets := make(map[string]*OptionDefinitionSet)
	for _, info := range flagCollection.FlagInfos {
		if info.GetName() == "bazelrc" {
			// `bazel help flags-as-proto` incorrectly reports `bazelrc` as not
			// allowing multiple values.
			// See https://github.com/bazelbuild/bazel/issues/24730 for more info.
			v := true
			info.AllowsMultiple = &v
		}
		if info.GetName() == "experimental_convenience_symlinks" || info.GetName() == "subcommands" {
			// `bazel help flags-as-proto` incorrectly reports
			// `experimental_convenience_symlinks` and `subcommands` as not
			// having negative forms.
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
			Name:          info.GetName(),
			ShortName:     info.GetAbbreviation(),
			Multi:         info.GetAllowsMultiple(),
			HasNegative:   info.GetHasNegativeFlag(),
			RequiresValue: info.GetRequiresValue(),
		}
		for _, cmd := range info.GetCommands() {
			var set *OptionDefinitionSet
			var ok bool
			if set, ok = sets[cmd]; !ok {
				set = &OptionDefinitionSet{
					All:         []*OptionDefinition{},
					ByName:      make(map[string]*OptionDefinition),
					ByShortName: make(map[string]*OptionDefinition),
				}
				sets[cmd] = set
			}
			set.All = append(set.All, o)
			set.ByName[o.Name] = o
			if o.ShortName != "" {
				set.ByShortName[o.ShortName] = o
			}
		}
	}
	return sets, nil
}

func OptionDefinitionSets() (map[string]*OptionDefinitionSet, error) {
	once := optionDefinitionSetsOnce()
	return once.optionDefinitionSets, once.error
}

// GetCommandLineSchema returns the effective CommandLineSchemas for the given
// command line.
func getCommandLineSchema(args []string, onlyStartupOptions bool) (*CommandLineSchema, error) {
	var optionDefinitionSets map[string]*OptionDefinitionSet
	if protoHelp, err := bazelHelp(); err == nil {
		flagCollection, err := DecodeHelpFlagsAsProto(protoHelp)
		if err != nil {
			return nil, err
		}
		sets, err := GetOptionDefinitionSetsfromProto(flagCollection)
		if err != nil {
			return nil, err
		}
		optionDefinitionSets = sets
	}
	schema := &CommandLineSchema{}
	if startupOptionDefinitions, ok := optionDefinitionSets["startup"]; ok {
		schema.StartupOptionDefinitions = startupOptionDefinitions
	} else {
		return nil, fmt.Errorf("flags proto did not contain startup option definitions.")
	}
	bazelCommands, err := BazelCommands()
	if err != nil {
		return nil, fmt.Errorf("failed to list bazel commands: %s", err)
	}
	if onlyStartupOptions {
		return schema, nil
	}
	// Iterate through the args, looking for the bazel command. Note, we don't
	// use "arg.GetCommand()" here since it may be ambiguous whether a token not
	// starting with "-" is the bazel command or a flag argument.
	i := 0
	for i < len(args) {
		token := args[i]
		optionDefinition, _, next, err := schema.StartupOptionDefinitions.Next(args, i)
		if err != nil {
			return nil, fmt.Errorf("failed to parse startup options: %s", err)
		}
		i = next
		if optionDefinition != nil {
			// We parsed a startup option, not the bazel command. Skip.
			continue
		}
		if _, ok := bazelCommands[token]; ok {
			schema.Command = token
			break
		}

		// Ignore unrecognized tokens for now.
		// TODO: Return an error for unknown tokens once we have a fully
		// static schema, including plugin flags.
	}
	if schema.Command == "" {
		return schema, nil
	}
	if commandOptionDefinitions, ok := optionDefinitionSets[schema.Command]; ok {
		schema.CommandOptionDefinitions = commandOptionDefinitions
	} else {
		return nil, fmt.Errorf("Flags proto did not contain option definitions for command '%s'.", schema.Command)
	}
	return schema, nil
}

func CanonicalizeStartupArgs(args []string) ([]string, error) {
	return canonicalizeArgs(args, true)
}

func CanonicalizeArgs(args []string) ([]string, error) {
	return canonicalizeArgs(args, false)
}

func canonicalizeArgs(args []string, onlyStartupOptions bool) ([]string, error) {
	bazelCommand, _ := GetBazelCommandAndIndex(args)
	if bazelCommand == "" {
		// Not a bazel command; no startup args to canonicalize.
		return args, nil
	}

	args, execArgs := arg.SplitExecutableArgs(args)
	schema, err := getCommandLineSchema(args, onlyStartupOptions)
	if err != nil {
		return nil, err
	}
	// First pass: go through args, expanding short names, converting bool
	// values to 0 or 1, and converting "--name value" args to "--name=value"
	// form.
	var out []string
	var optionDefinitions []*OptionDefinition
	lastOptionIndex := map[string]int{}
	i := 0
	optionDefinitionSet := schema.StartupOptionDefinitions
	for i < len(args) {
		token := args[i]
		optionDefinition, value, next, err := optionDefinitionSet.Next(args, i)
		if err != nil {
			return nil, fmt.Errorf("failed to parse startup options: %s", err)
		}
		i = next
		if optionDefinition == nil {
			out = append(out, token)
		} else {
			lastOptionIndex[optionDefinition.Name] = len(out)
			out = append(out, formatOption(optionDefinition, value))
		}
		optionDefinitions = append(optionDefinitions, optionDefinition)
		if token == schema.Command {
			if onlyStartupOptions {
				return append(out, args[i:]...), nil
			}
			// When we see the bazel command token, switch to parsing command
			// options instead of startup options.
			optionDefinitionSet = schema.CommandOptionDefinitions
		}
	}
	// Second pass: loop through the canonical args so far, and remove any args
	// which are overridden by a later arg. Note that multi-args cannot be
	// overriden.
	var canonical []string
	for i, opt := range optionDefinitions {
		if opt != nil && !opt.Multi && lastOptionIndex[opt.Name] > i {
			continue
		}
		canonical = append(canonical, out[i])
	}
	return arg.JoinExecutableArgs(canonical, execArgs), nil
}

// runBazelHelpWithCache returns the `bazel help <topic>` output for the version
// of bazel that will be chosen by bazelisk. The output is cached in
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

func StructureRules(rules []*RcRule) *Rules {
	r := &Rules{
		All:              rules,
		ByPhaseAndConfig: map[string]map[string][]*RcRule{},
	}
	for _, rule := range rules {
		byConfig := r.ByPhaseAndConfig[rule.Phase]
		if byConfig == nil {
			byConfig = map[string][]*RcRule{}
			r.ByPhaseAndConfig[rule.Phase] = byConfig
		}
		byConfig[rule.Config] = append(byConfig[rule.Config], rule)
	}
	return r
}

func (r *Rules) ForPhaseAndConfig(phase, config string) []*RcRule {
	byConfig := r.ByPhaseAndConfig[phase]
	if byConfig == nil {
		return nil
	}
	return byConfig[config]
}

// RcRule is a rule parsed from a bazelrc file.
type RcRule struct {
	Phase  string
	Config string
	// Tokens contains the raw (non-canonicalized) tokens in the rule.
	Tokens []string
}

func (r *RcRule) String() string {
	return fmt.Sprintf("phase=%q,config=%q,tokens=%v", r.Phase, r.Config, r.Tokens)
}

func appendRcRulesFromImport(workspaceDir, path string, opts []*RcRule, optional bool, importStack []string) ([]*RcRule, error) {
	if strings.HasPrefix(path, workspacePrefix) {
		path = filepath.Join(workspaceDir, path[len(workspacePrefix):])
	}

	file, err := os.Open(path)
	if err != nil {
		if optional {
			return opts, nil
		}
		return nil, err
	}
	defer file.Close()
	return appendRcRulesFromFile(workspaceDir, file, opts, importStack)
}

func appendRcRulesFromFile(workspaceDir string, f *os.File, rules []*RcRule, importStack []string) ([]*RcRule, error) {
	rpath, err := realpath(f.Name())
	if err != nil {
		return nil, fmt.Errorf("could not determine real path of bazelrc file: %s", err)
	}
	for _, path := range importStack {
		if path == rpath {
			return nil, fmt.Errorf("circular import detected: %s -> %s", strings.Join(importStack, " -> "), rpath)
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
			rules, err = appendRcRulesFromImport(workspaceDir, path, rules, isOptional, importStack)
			if err != nil {
				return nil, err
			}
			continue
		}

		rule, err := parseRcRule(line)
		if err != nil {
			log.Debugf("Error parsing bazelrc option: %s", err.Error())
			continue
		}
		// Bazel doesn't support configs for startup options and ignores them if
		// they appear in a bazelrc: https://bazel.build/run/bazelrc#config
		if rule.Phase == "startup" && rule.Config != "" {
			continue
		}
		rules = append(rules, rule)
	}
	log.Debugf("Adding rc rules from %q: %v", rpath, rules)
	return rules, scanner.Err()
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
	if strings.HasPrefix(tokens[0], "-") {
		return &RcRule{
			Phase:  "common",
			Tokens: tokens,
		}, nil
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

func ParseRCFiles(workspaceDir string, filePaths ...string) ([]*RcRule, error) {
	options := make([]*RcRule, 0)
	seen := map[string]bool{}
	for _, filePath := range filePaths {
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
		options, err = appendRcRulesFromFile(workspaceDir, file, options, nil /*=importStack*/)
		if err != nil {
			return nil, err
		}
	}
	return options, nil
}

func ExpandConfigs(args []string) ([]string, error) {
	ws, err := workspace.Path()
	if err != nil {
		log.Debugf("Could not determine workspace dir: %s", err)
	}
	args, err = expandConfigs(ws, args)
	if err != nil {
		return nil, err
	}
	return CanonicalizeArgs(args)
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

func expandConfigs(workspaceDir string, args []string) ([]string, error) {
	_, idx := GetBazelCommandAndIndex(args)
	if idx == -1 {
		// Not a bazel command; don't expand configs.
		return args, nil
	}

	var schema *CommandLineSchema
	{
		args, _ := arg.SplitExecutableArgs(args)
		s, err := getCommandLineSchema(args, false /*=onlyStartupOptions*/)
		if err != nil {
			return nil, fmt.Errorf("failed to get command line schema: %w", err)
		}
		schema = s
	}

	args, rcFiles, err := consumeRCFileArgs(args, workspaceDir)
	if err != nil {
		return nil, err
	}
	log.Debugf("Parsing rc files %s", rcFiles)
	r, err := ParseRCFiles(workspaceDir, rcFiles...)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bazelrc file: %s", err)
	}
	rules := StructureRules(r)

	// Expand startup args first, before any other args (including explicit
	// startup args).
	startupArgs, err := appendArgsForConfig(schema, rules, nil, "startup", nil, "" /*=config*/, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to expand startup options: %s", err)
	}
	args = append(startupArgs, args...)

	command, commandIndex := GetBazelCommandAndIndex(args)
	if commandIndex == -1 {
		return args, nil
	}

	// Always apply bazelrc rules in order of the precedence hierarchy. For
	// example, for the "test" command, apply options in order of "common", then
	// "build", then "test".
	phases := getPhases(command)
	log.Debugf("Bazel command: %q, rc rule classes: %v", command, phases)

	// We'll refer to args in bazelrc which aren't expanded from a --config
	// option as "default" args, like a .bazelrc line that just says "-c dbg" or
	// "build -c dbg" as opposed to something qualified like "build:dbg -c dbg".
	//
	// These default args take lower precedence than explicit command line args
	// so we expand those first just after the command.
	log.Debugf("Args before expanding default rc rules: %v", args)
	var defaultArgs []string
	for _, phase := range phases {
		defaultArgs, err = appendArgsForConfig(schema, rules, defaultArgs, phase, phases, "" /*=config*/, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate bazelrc configuration: %s", err)
		}
	}
	log.Debugf("Prepending arguments %v from default rc rules", defaultArgs)
	args = concat(args[:commandIndex+1], defaultArgs, args[commandIndex+1:])
	log.Debugf("Args after expanding default rc rules: %v", args)

	enable, enableIndex, enableLength := arg.FindLast(args, enablePlatformSpecificConfigFlag)
	_, noEnableIndex, _ := arg.FindLast(args, "no"+enablePlatformSpecificConfigFlag)
	if enableIndex > noEnableIndex {
		if enable == "true" || enable == "yes" || enable == "1" || enable == "" {
			args = concat(args[:enableIndex], []string{"--config", getBazelOS()}, args[enableIndex+enableLength:])
			log.Debugf("Args after inserting artificial platform-specific --config argument: %s", args)
		}
	}

	offset := 0
	for offset < len(args) {
		// Find the next --config arg, starting from just after we expanded
		// the last config arg.
		config, configIndex, length := arg.Find(args[offset:], "config")
		if configIndex < 0 {
			break
		}
		configIndex = offset + configIndex

		// If the config isn't defined, leave it as-is, and let bazel return
		// an error.
		if !isConfigDefined(rules, config, phases) {
			offset = configIndex + length
			continue
		}

		var configArgs []string
		for _, phase := range phases {
			configArgs, err = appendArgsForConfig(schema, rules, configArgs, phase, phases, config, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate bazelrc configuration: %s", err)
			}
		}
		log.Debugf("Expanded config %q to args %v", configArgs)

		args = concat(args[:configIndex], configArgs, args[configIndex+length:])
		offset = configIndex
	}

	log.Debugf("Fully expanded args: %+v", args)

	return args, nil
}

func appendArgsForConfig(schema *CommandLineSchema, rules *Rules, args []string, phase string, phases []string, config string, configStack []string) ([]string, error) {
	var err error
	for _, c := range configStack {
		if c == config {
			return nil, fmt.Errorf("circular --config reference detected: %s -> %s", strings.Join(configStack, " -> "), config)
		}
	}
	configStack = append(configStack, config)
	for _, rule := range rules.ForPhaseAndConfig(phase, config) {
		// Note: at each loop iteration, we skip either 1 or 2 args, so no
		// "i++" here.
		for i := 0; i < len(rule.Tokens); {
			tok := rule.Tokens[i]

			configArg := ""
			configArgCount := 0
			if strings.HasPrefix(tok, "--config=") {
				configArg = strings.TrimPrefix(tok, "--config=")
				configArgCount = 1
			} else if tok == "--config" && i+1 < len(rule.Tokens) {
				configArg = rule.Tokens[i+1]
				// Consume the following argument in this iteration too
				configArgCount = 2
			}

			// If we have a --config arg, expand it if it is defined in any rc
			// file. If it is not defined, let bazel show an error saying that
			// it is not defined.
			if configArg != "" && isConfigDefined(rules, config, phases) {
				for _, phase := range phases {
					args, err = appendArgsForConfig(schema, rules, args, phase, phases, configArg, configStack)
					if err != nil {
						return nil, err
					}
				}
				i += configArgCount
				continue
			}

			// For the 'common' phase, only append the arg if it's supported by
			// the command.
			//
			// Note: Bazel throws an error here if the arg is not supported by
			// at least one other command. We don't implement this for now since
			// we lazily parse help per-command, and this behavior would require
			// eagerly parsing help for all commands.
			if phase == "common" {
				opt, _ := arg.SplitOptionValue(tok)
				if schema.CommandSupportsOpt(opt) {
					// Since the opt is supported, we can do a proper parse to
					// determine how many args to consume in this iteration.
					// e.g., need to skip 2 args for "-c opt", 1 arg for
					// "--nocache_test_results", and 1 arg for "--curses=yes".
					_, _, next, err := schema.CommandOptionDefinitions.Next(rule.Tokens, i)
					if err != nil {
						return nil, err
					}
					for j := i; j < next; j++ {
						args = append(args, rule.Tokens[j])
					}
					i = next
				} else {
					log.Debugf("common rc rule: opt %q is unsupported by command %q; skipping", opt, schema.Command)
					// If the opt isn't supported, apply a rough heuristic
					// to figure out whether to skip just this arg, or the
					// next arg too.
					if strings.HasPrefix(tok, "--") {
						nextArgIsOption := (i+1 < len(rule.Tokens) && strings.HasPrefix(rule.Tokens[i+1], "-"))
						if strings.Contains(tok, "=") || strings.HasPrefix(tok, "--no") || nextArgIsOption {
							i++
						} else {
							i += 2
						}
					} else if strings.HasPrefix(tok, "-") {
						i += 2
					}
				}
				continue
			}

			// Happy path: this is a "normal" phase like "build", "test" etc.
			// rather than a "pseudo-phase" like "common" etc., and it's a plain
			// old non-config arg that doesn't itself need to be expanded. Just
			// append the arg and continue.
			args = append(args, tok)
			i++
		}
	}
	return args, nil
}

func isConfigDefined(rules *Rules, config string, phases []string) bool {
	for _, phase := range phases {
		if len(rules.ForPhaseAndConfig(phase, config)) > 0 {
			return true
		}
	}
	return false
}

func consumeRCFileArgs(args []string, workspaceDir string) (newArgs []string, rcFiles []string, err error) {
	_, idx := GetBazelCommandAndIndex(args)
	if idx == -1 {
		return nil, nil, fmt.Errorf(`no command provided (run "%s help" to see available commands)`, os.Args[0])
	}
	startupArgs, cmdArgs := args[:idx], args[idx:]

	// Before we do anything, check whether --ignore_all_rc_files is already
	// set. If so, return an empty list of RC files, since bazel will do the
	// same.
	ignoreAllRCFiles := false
	for _, a := range startupArgs {
		if val, ok := asStartupBoolFlag(a, "ignore_all_rc_files"); ok {
			ignoreAllRCFiles = val
		}
	}
	if ignoreAllRCFiles {
		return args, nil, nil
	}

	// Now do another pass through the args and parse workspace_rc, system_rc,
	// home_rc, and bazelrc args. Note that if we encounter an arg
	// --bazelrc=/dev/null, that means bazel will ignore subsequent --bazelrc
	// args, so we ignore them as well.
	workspaceRC := true
	systemRC := true
	homeRC := true
	encounteredDevNullBazelrc := false
	var newStartupArgs []string
	var explicitBazelrcPaths []string
	for i := 0; i < len(startupArgs); i++ {
		a := startupArgs[i]
		if val, ok := asStartupBoolFlag(a, "workspace_rc"); ok {
			workspaceRC = val
			continue
		}
		if val, ok := asStartupBoolFlag(a, "system_rc"); ok {
			systemRC = val
			continue
		}
		if val, ok := asStartupBoolFlag(a, "home_rc"); ok {
			homeRC = val
			continue
		}
		bazelrcArg := ""
		if strings.HasPrefix(a, "--bazelrc=") {
			bazelrcArg = strings.TrimPrefix(a, "--bazelrc=")
		} else if a == "--bazelrc" && i+1 < len(args) {
			bazelrcArg = args[i+1]
			i++
		}
		if bazelrcArg != "" {
			if encounteredDevNullBazelrc {
				continue
			}
			if bazelrcArg == "/dev/null" {
				encounteredDevNullBazelrc = true
				continue
			}
			explicitBazelrcPaths = append(explicitBazelrcPaths, bazelrcArg)
			continue
		}

		newStartupArgs = append(newStartupArgs, a)
	}
	// Ignore all RC files when actually running bazel, since the CLI has
	// already accounted for them.
	newStartupArgs = append(newStartupArgs, "--ignore_all_rc_files")
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
	newArgs = append(newStartupArgs, cmdArgs...)
	return newArgs, rcFiles, nil
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
//	getPhases("run")      // {"common", "build", "run"}
//	getPhases("coverage") // {"common", "build", "test", "coverage"}
func getPhases(command string) (out []string) {
	for {
		if command == "" {
			out = append(out, "common")
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

func concat(slices ...[]string) []string {
	length := 0
	for _, s := range slices {
		length += len(s)
	}
	out := make([]string, 0, length)
	for _, s := range slices {
		out = append(out, s...)
	}
	return out
}

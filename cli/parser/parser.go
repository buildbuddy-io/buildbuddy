// package parser handles the logic for parsing and manipulating Bazel
// configuration (both flags and RC files).
package parser

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"maps"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/bazelrc"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/parsed"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"

	bfpb "github.com/buildbuddy-io/buildbuddy/proto/bazel_flags"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
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

// Parser contains a set of OptionDefinitions (indexed for ease of parsing) and
// the known bazel commands.
type Parser struct {
	ByName      map[string]*options.Definition
	ByShortName map[string]*options.Definition

	BazelCommands map[string]struct{}
}

func NewParser(optionDefinitions []*options.Definition) *Parser {
	p := &Parser{
		ByName:        map[string]*options.Definition{},
		ByShortName:   map[string]*options.Definition{},
		BazelCommands: map[string]struct{}{},
	}
	for _, o := range optionDefinitions {
		p.ByName[o.Name()] = o
		if o.ShortName() != "" {
			p.ByShortName[o.ShortName()] = o
		}
	}
	return p
}

func (p *Parser) ForceAddOptionDefinition(o *options.Definition) {
	p.ByName[o.Name()] = o
	if o.ShortName() != "" {
		p.ByShortName[o.ShortName()] = o
	}
}

func (p *Parser) AddOptionDefinition(o *options.Definition) error {
	if _, ok := p.ByName[o.Name()]; ok {
		return fmt.Errorf("Naming collision adding flag %s; flag already exists with that name.", o.Name())
	}
	if _, ok := p.ByShortName[o.ShortName()]; ok {
		return fmt.Errorf("Naming collision adding flag with short name %s; flag already exists with that short name.", o.ShortName())
	}
	p.ForceAddOptionDefinition(o)
	return nil
}

func ParseArgs(args []string) (*parsed.OrderedArgs, error) {
	p, err := GetParser()
	if err != nil {
		return nil, err
	}
	return p.ParseArgs(args)
}

func (p *Parser) ParseArgs(args []string) (*parsed.OrderedArgs, error) {
	return p.ParseArgsForCommand(args, "startup")
}

func (p *Parser) ParseArgsForCommand(args []string, command string) (*parsed.OrderedArgs, error) {
	parsedArgs := &parsed.OrderedArgs{}
	next := args
	for {
		opts, argIndex, err := p.ParseOptions(next, command)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s options: %s", command, err)
		}
		parsedArgs.Args = append(parsedArgs.Args, arguments.FromConcrete(opts)...)
		next = next[argIndex:]
		if len(next) == 0 {
			break
		}
		if next[0] == "--" {
			if command == "startup" {
				// Bazel does not recognize `--` until the command has been encountered.
				return nil, fmt.Errorf("unknown startup option '--'.")
			}
			parsedArgs.Args = append(parsedArgs.Args, &arguments.DoubleDash{})
			parsedArgs.Args = append(parsedArgs.Args, arguments.ToPositionalArguments(next[1:])...)
			break
		}
		if command == "startup" {
			command = next[0]
			if command == "" {
				// bazel treats a blank command as a help command that halts both option and
				// argument parsing and ignores all non-startup options in the rc file.
				break
			}
			if _, ok := p.BazelCommands[command]; !ok {
				return nil, fmt.Errorf("Command '%s' not found. Try 'bb help'", command)
			}
		}
		parsedArgs.Args = append(parsedArgs.Args, &arguments.PositionalArgument{Value: next[0]})
		next = next[1:]
	}
	return parsedArgs, nil
}

// Parse options until we encounter a positional argument, and return the
// options and the index of the positional argument that terminated parsing, or
// the length of the input arguments array if no positional argument was
// encountered. If no command is provided, options will not be filtered by
// command.
func (p *Parser) ParseOptions(args []string, command string) ([]options.Option, int, error) {
	var parsedOptions []options.Option
	// Iterate through the args, looking for a terminating token.
	for i := 0; i < len(args); {
		token := args[i]
		if token == "--" {
			// POSIX-specified (and bazel-supported) delimiter to end option parsing
			return parsedOptions, i, nil
		}
		option, next, err := p.Next(args, i, command == "startup")
		if err != nil {
			return nil, 0, fmt.Errorf("failed to parse options: %s", err)
		}
		if option == nil {
			// This is a positional argument, return it.
			return parsedOptions, i, nil
		}
		if command != "" {
			if option.PluginID() == options.UnknownBuiltinPluginID {
				// If this is an unknown option, assume it's supported by this command.
				option.GetDefinition().AddSupportedCommand(command)
			} else if !option.Supports(command) {
				return nil, 0, fmt.Errorf("failed to parse options: Option '%s' does not support command '%s'", token, command)
			}
		}
		parsedOptions = append(parsedOptions, option)
		i = next
	}
	return parsedOptions, len(args), nil
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
// parser, the returned values will be (nil, start+1). It is up to the caller to
// decide how args[start] should be interpreted.
//
// TODO(zoey): when we have plugin option defintions, remove the "startup"
// parameter since it only exists to aid in the guessing heuristic for unknown
// options.
func (p *Parser) Next(args []string, start int, startup bool) (option options.Option, next int, err error) {
	if start > len(args) {
		return nil, -1, fmt.Errorf("arg index %d out of bounds", start)
	}
	startToken := args[start]
	option, err = p.ParseOption(startToken)
	if err != nil {
		return nil, -1, err
	}
	if option == nil {
		// positional argument
		return nil, start + 1, nil
	}
	if unknownOption, ok := option.(*options.UnknownOption); ok {
		log.Debugf("Unknown option %s", startToken)
		// Unknown option, possibly a plugin-specific argument. Apply a rough
		// heuristic to determine whether or not to have it consume the next
		// argument.
		if b, ok := unknownOption.Option.(options.BoolLike); ok && !option.HasValue() && !b.Negated() {
			// This could actually be a required-value-type option rather than a
			// boolean option; if the next argument doesn't look like an option or a
			// bazel command, let's assume that it is.
			if start+1 < len(args) {
				if nextArg := args[start+1]; !strings.HasPrefix(nextArg, "-") {
					if _, ok := p.BazelCommands[nextArg]; !startup || !ok {
						option, err = options.NewOption(
							strings.TrimLeft(startToken, "-"),
							nil,
							options.NewDefinition(
								option.Name(),
								options.WithMulti(),
								options.WithRequiresValue(),
								options.WithShortName(option.ShortName()),
								options.WithPluginID(options.UnknownBuiltinPluginID),
							),
						)
						if err != nil {
							return nil, -1, err
						}
					}
				}
			}
		}
	}

	if !option.ExpectsValue() {
		return option, start + 1, nil
	}
	if start+1 >= len(args) || args[start+1] == "--" {
		return nil, -1, fmt.Errorf("expected value after %s", startToken)
	}
	option.SetValue(args[start+1])
	return option, start + 2, nil
}

// BazelHelpFunc returns the output of "bazel help flags-as-proto". Passing
// the help function lets us replace it for testing.
type BazelHelpFunc func() (string, error)

func (p *Parser) parseLongNameOption(optName string) (options.Option, error) {
	var v *string
	if eqIndex := strings.Index(optName, "="); eqIndex != -1 {
		// This option is of the form --NAME=value; split it up into the option
		// name and the option value.
		value := optName[eqIndex+1:]
		v = &value
		optName = optName[:eqIndex]
	}
	if d, ok := p.ByName[optName]; ok {
		return options.NewOption(optName, v, d)
	}
	if boolOptName, ok := strings.CutPrefix(optName, "no"); ok {
		if d, ok := p.ByName[boolOptName]; ok && d.HasNegative() {
			return options.NewOption(optName, v, d)
		}
	}

	for prefix := range options.StarlarkSkippedPrefixes {
		if strings.HasPrefix(optName, prefix) {
			// This is a new starlark definition; let's hang on to it.
			d := options.NewStarlarkOptionDefinition(optName)
			d.AddSupportedCommand(slices.Collect(maps.Keys(p.BazelCommands))...)
			// No need to check if this option already exists since we never reach
			// this code if it does.
			p.ForceAddOptionDefinition(d)
			return options.NewOption(optName, v, d)
		}
	}

	opts := []options.DefinitionOpt{
		options.WithMulti(),
		options.WithPluginID(options.UnknownBuiltinPluginID),
	}
	if v != nil {
		opts = append(opts, options.WithRequiresValue())
	} else {
		opts = append(opts, options.WithNegative())
	}
	return options.NewOption(
		optName,
		v,
		options.NewDefinition(optName, opts...),
	)
}

func (p *Parser) parseShortNameOption(optName string) (options.Option, error) {
	if len(optName) != 1 {
		return nil, fmt.Errorf("Invalid options syntax: '-%s'", optName)
	}
	if d, ok := p.ByShortName[optName]; ok {
		return options.NewOption(optName, nil, d)
	}
	o, err := options.NewOption(
		optName,
		nil,
		options.NewDefinition(
			// We don't know the long name for this, so just use the shortname
			// prefixed by "-", since that is guaranteed not to collide.
			"-"+optName,
			options.WithMulti(),
			options.WithNegative(),
			options.WithShortName(optName),
			options.WithPluginID(options.UnknownBuiltinPluginID),
		),
	)
	if err != nil {
		return nil, err
	}
	o.UseShortName(true)
	return o, err
}

// ParseOption returns an Option with the OptionDefinition for the given opt
// (with a value if the flag includes an "=" or is a boolean flag with a
// "--no" prefix), nil if there is a valid OptionDefinition with the provided
// name but which does not support the provided command, and an UnknownOption if
// the parser does not recognize the option at all. The opt is expected to be
// either "--NAME" or "-SHORTNAME". The boolean returned indicates whether this
// option still needs a value (which is to say, if the OptionDefinition requires
// a value but none was provided via an `=`).
func (p *Parser) ParseOption(opt string) (option options.Option, err error) {
	if optName, found := strings.CutPrefix(opt, "--"); found {
		return p.parseLongNameOption(optName)
	}
	if optName, found := strings.CutPrefix(opt, "-"); found {
		return p.parseShortNameOption(optName)
	}
	// This is not an option.
	return nil, nil
}

// DecodeHelpFlagsAsProto takes the output of `bazel help flags-as-proto` and
// returns the FlagCollection proto message it encodes.
func DecodeHelpFlagsAsProto(protoHelp string) (*bfpb.FlagCollection, error) {
	b, err := base64.StdEncoding.DecodeString(protoHelp)
	if err != nil {
		truncHelp := protoHelp
		if len(truncHelp) > 100 {
			truncHelp = truncHelp[:100] + "..."
		}
		return nil, fmt.Errorf("failed to decode 'bazel help flags-as-proto' output as base64: %s (attempted to decode %q)", err, truncHelp)
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
		d := options.DefinitionFrom(info)
		if !d.Supports("startup") {
			// only add commands from non-startup flags to the bazel commands
			for cmd := range d.SupportedCommands() {
				parser.BazelCommands[cmd] = struct{}{}
			}
			// non-startup flags support the "common" and "always" bazelrc classifiers
			d.AddSupportedCommand("common")
			d.AddSupportedCommand("always")
		}
		if err := parser.AddOptionDefinition(d); err != nil {
			return nil, err
		}
	}
	return parser, nil
}

func GetParser() (*Parser, error) {
	once := generateParserOnce()
	return once.p, once.error
}

func CanonicalizeArgs(args []string) ([]string, error) {
	// Check for bazel command prior to running the parser to avoid the
	// performance cost of generating the parser, which runs bazel.
	bazelCommand, _ := GetBazelCommandAndIndex(args)
	if bazelCommand == "" {
		// Not a bazel command; no args to canonicalize.
		return args, nil
	}

	p, err := GetParser()
	if err != nil {
		return nil, err
	}
	return p.canonicalizeArgs(args)
}

func (p *Parser) canonicalizeArgs(args []string) ([]string, error) {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		if _, ok := p.BazelCommands[args[0]]; !ok {
			// Not a bazel command; no startup args to canonicalize.
			return args, nil
		}
	}
	parsedArgs, err := ParseArgs(args)
	if err != nil {
		return nil, err
	}
	return parsedArgs.Canonicalized().Format(), nil
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
	opts := &bazelisk.RunOpts{
		Stdout:      io.MultiWriter(tmp, buf),
		Stderr:      errBuf,
		SkipWrapper: true,
	}
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

// ResolveArgs removes all rc-file options from the args, appends an
// `ignore_all_rc_files` option to the startup options, parses those rc-files
// into Configs using the default parser, and expands all config options (as
// well as any `enable_platform_specific_config` option, if one exists) using
// those coonfigs, and returns the result.
func ResolveArgs(parsedArgs *parsed.OrderedArgs) (*parsed.OrderedArgs, error) {
	ws, err := workspace.Path()
	if err != nil {
		log.Debugf("Could not determine workspace dir: %s", err)
	}
	return resolveArgs(parsedArgs, ws)
}

// resolveArgs removes all rc-file options from the args, appends an
// `ignore_all_rc_files` option to the startup options, parses those rc-files
// into Configs using the default parser, and expands all config options (as
// well as any `enable_platform_specific_config` option, if one exists) using
// those coonfigs, and returns the result.
func resolveArgs(parsedArgs *parsed.OrderedArgs, ws string) (*parsed.OrderedArgs, error) {
	p, err := GetParser()
	if err != nil {
		return nil, err
	}
	configs, defaultConfig, err := p.consumeAndParseRCFiles(parsedArgs, ws)
	if err != nil {
		return nil, err
	}
	return ExpandConfigs(parsedArgs, configs, defaultConfig)
}

// ParseRCFiles parses the provided rc files in the given workspace into Configs
// and returns a map of the named configs as well as the default (unnamed)
// Config.
func (p *Parser) ParseRCFiles(workspaceDir string, filePaths ...string) (map[string]*parsed.Config, *parsed.Config, error) {
	seen := make(map[string]struct{}, len(filePaths))
	namedConfigs := map[string]*parsed.Config{}
	defaultConfig := parsed.NewConfig()
	for _, filePath := range filePaths {
		namedRcRules := map[string]map[string][]string{}
		defaultRcRules := map[string][]string{}
		r, err := bazelrc.Realpath(filePath)
		if err != nil {
			continue
		}
		if _, ok := seen[r]; ok {
			continue
		}
		seen[r] = struct{}{}

		err = bazelrc.AppendRcRulesFromFile(workspaceDir, r, namedRcRules, defaultRcRules, nil /*=importStack*/, true)
		if err != nil {
			return nil, nil, err
		}
		for phase, tokens := range defaultRcRules {
			if !bazelrc.IsPhase(phase) {
				log.Warnf("invalid command name '%s'", phase)
				continue
			}
			args, err := p.ParseConfig(phase, tokens)
			if err != nil {
				return nil, nil, err
			}
			defaultConfig.ByPhase[phase] = append(defaultConfig.ByPhase[phase], args...)
		}
		for name, config := range namedRcRules {
			parsedConfig, ok := namedConfigs[name]
			if !ok {
				parsedConfig = parsed.NewConfig()
				namedConfigs[name] = parsedConfig
			}
			for phase, tokens := range config {
				if !bazelrc.IsPhase(phase) {
					log.Warnf("invalid command name '%s:%s'", phase, config)
					continue
				}
				args, err := p.ParseConfig(phase, tokens)
				if err != nil {
					return nil, nil, err
				}
				parsedConfig.ByPhase[phase] = append(parsedConfig.ByPhase[phase], args...)
			}
		}
	}
	return namedConfigs, defaultConfig, nil
}

// ParseConfig takes an rc-line that has been lexed and then split into the
// "phase" (for example, `common` or `build:foo`) and the remaining lexed
// tokens and returns those tokens parsed into a slice of Arguments.
func (p *Parser) ParseConfig(phase string, tokens []string) ([]arguments.Argument, error) {
	parsedArgs, err := p.ParseArgsForCommand(tokens, phase)
	if err != nil {
		return nil, err
	}
	if phase == "startup" {
		for _, o := range parsed.Classify(parsedArgs.Args) {
			switch o := o.(type) {
			case *parsed.StartupOption:
				if _, ok := bazelrc.StartupFlagNoRc[o.Name()]; ok {
					return nil, fmt.Errorf("Can't specify %s in the .bazelrc file.", o.Name())
				}
			default:
				return nil, fmt.Errorf("Unknown startup option: '%s'", o.Arg().Format()[0])
			}
		}
	}
	return parsedArgs.Args, nil
}

// Convenience function to use the singleton parser's MakeOption function.
func MakeOption(optionName string, value *string) (option options.Option, err error) {
	p, err := GetParser()
	if err != nil {
		return nil, err
	}
	o, err := p.MakeOption(optionName, value)
	if err != nil {
		return nil, err
	}
	return o, nil
}

func (p *Parser) MakeOption(optionName string, value *string) (option options.Option, err error) {
	if len(optionName) == 1 {
		// assume length 1 is a short name
		option, err = p.parseShortNameOption(optionName)
		if err != nil {
			return nil, err
		}
	} else {
		option, err = p.parseLongNameOption(optionName)
		if err != nil {
			return nil, err
		}
	}
	option, err = options.NewOption(optionName, value, option.GetDefinition())
	if err != nil {
		return nil, err
	}
	if option.ExpectsValue() {
		return nil, fmt.Errorf("Required value option %s must have a value, but none was provided.", optionName)
	}
	return option, nil
}

// ExpandConfigs expands all the config options in the args, using the
// provided config parameters to resolve them. It also expands the
// `enable_platform_specific_config` option, if it exists.
//
// TODO(zoey): move this function into `parsed.go` and make it a method
// of `OrderedArgs`.
func ExpandConfigs(
	args *parsed.OrderedArgs,
	namedConfigs map[string]*parsed.Config,
	defaultConfig *parsed.Config,
) (*parsed.OrderedArgs, error) {
	command := args.GetCommand()
	expanded, err := expandConfigs(args, namedConfigs, defaultConfig)
	if err != nil {
		return nil, err
	}
	// Replace the last occurrence of `--enable_platform_specific_config` with
	// `--config=<bazelOS>`, so long as the last occurrence evaluates as true.
	opts := expanded.RemoveOptions(bazelrc.EnablePlatformSpecificConfigFlag)
	if len(opts) > 0 {
		enable := opts[len(opts)-1].Option
		index := opts[len(opts)-1].Index
		if b, ok := enable.(options.BoolLike); ok {
			if v, err := b.AsBool(); err == nil && v {
				bazelOS := bazelrc.GetBazelOS()
				if platformConfig, ok := namedConfigs[bazelOS]; ok {
					phases := bazelrc.GetPhases(command)
					expansion, err := appendArgsForConfig(platformConfig, nil, namedConfigs, phases, []string{bazelOS}, true)
					if err != nil {
						return nil, err
					}
					expanded.Args = slices.Insert(expanded.Args, index, expansion...)
					// Remove all occurrences of the enable platform-specific config flag
					// that may have been added when expanding the platform-specific config.
					expanded.RemoveOptions(bazelrc.EnablePlatformSpecificConfigFlag)
				}
			}
		}
	}
	return expanded, nil
}

// expandConfigs expands all the config options in the args, using the
// provided config parameters to resolve them.
//
// TODO(zoey): move this function into `parsed.go` and make it a method
// of `OrderedArgs`.
func expandConfigs(
	args *parsed.OrderedArgs,
	namedConfigs map[string]*parsed.Config,
	defaultConfig *parsed.Config,
) (*parsed.OrderedArgs, error) {
	// Expand startup args first, before any other args (including explicit
	// startup args).
	//
	// startup config is guaranteed to only be startup options.
	startupConfig := defaultConfig.ByPhase["startup"]

	commandIndex, command := parsed.Find[*parsed.Command](args.Args)
	if commandIndex == -1 {
		// No command is a help command that does not expand anything but the startup config.
		return &parsed.OrderedArgs{Args: slices.Concat(startupConfig, args.Args)}, nil
	}
	expanded := slices.Concat(startupConfig, args.Args[:commandIndex])
	expanded = append(expanded, command.PositionalArgument)

	// Always apply bazelrc rules in order of the precedence hierarchy. For
	// example, for the "test" command, apply options in order of "always",
	// then "common", then "build", then "test".
	phases := bazelrc.GetPhases(command.GetValue())
	log.Debugf("Bazel command: %q, rc rule classes: %v", command, phases)

	// We'll refer to args in bazelrc which aren't expanded from a --config
	// option as "default" args, like a .bazelrc line that just says
	// "common -c dbg" or "build -c dbg" as opposed to something qualified like
	// "build:dbg -c dbg".
	//
	// These default args take lower precedence than explicit command line args
	// so we expand those first just after the command.
	log.Debugf("Args before expanding default rc rules: %#v", arguments.FormatAll(expanded))
	var err error
	expanded, err = appendArgsForConfig(defaultConfig, expanded, namedConfigs, phases, []string{}, true)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate bazelrc configuration: %s", err)
	}
	log.Debugf("Args after expanding default rc rules: %v", arguments.FormatAll(expanded))
	expanded, err = appendExpansion(expanded, args.Args[commandIndex+1:], command.Value, namedConfigs, phases, []string{}, false)
	if err != nil {
		return nil, err
	}
	log.Debugf("Fully expanded args: %+v", arguments.FormatAll(expanded))

	// Append to new OrderedArgs to make sure `--` is handled correctly.
	expandedArgs := &parsed.OrderedArgs{}
	if err := expandedArgs.Append(expanded...); err != nil {
		return nil, err
	}
	return expandedArgs, err
}

// Expands and appends all applicable args from the provided Config to the
// provided argument slice and returns it.
//
// TODO(zoey): move this function into `parsed.go` and make it a method of
// `Config`.
func appendArgsForConfig(
	config *parsed.Config,
	expanded []arguments.Argument,
	namedConfigs map[string]*parsed.Config,
	phases []string,
	configStack []string,
	allowEmpty bool,
) ([]arguments.Argument, error) {
	empty := true
	for _, phase := range phases {
		toExpand, ok := config.ByPhase[phase]
		if !ok {
			continue
		}
		empty = false
		var err error
		expanded, err = appendExpansion(
			expanded,
			toExpand,
			phase,
			namedConfigs,
			phases,
			configStack,
			true,
		)
		if err != nil {
			return nil, err
		}
		log.Debugf("Expanded for phase %s: %#v", phase, arguments.FormatAll(expanded))
	}
	if empty && !allowEmpty {
		// empty config names do not require supported phases
		return nil, fmt.Errorf("config value not defined in any .rc file")
	}
	return expanded, nil
}

// appendExpansion expands and appends all args in `toExpand` to `expanded`.
//
// TODO(zoey): move  this function into `parsed.go`.
func appendExpansion(
	expanded []arguments.Argument,
	toExpand []arguments.Argument,
	phase string,
	namedConfigs map[string]*parsed.Config,
	phases []string,
	configStack []string,
	removeDoubleDash bool,
) ([]arguments.Argument, error) {
	for _, a := range toExpand {
		log.Debugf("Expanding '%+v'", a.Format())
		switch a := a.(type) {
		case *arguments.DoubleDash:
			if removeDoubleDash {
				continue
			}
			expanded = append(expanded, &arguments.DoubleDash{})
		case options.Option:
			// For the common phases, only append the arg if it's supported by
			// the command.
			if bazelrc.IsUnconditionalCommandPhase(phase) && !a.Supports(phases[len(phases)-1]) {
				if phase == "always" {
					log.Warnf("Inherited 'always' options: %v", arguments.FormatAll(toExpand))
					return nil, fmt.Errorf("%[1]s :: Unrecognized option %[1]s", a.Format()[0])
				}
				// TODO(zoey): return an error here if the option does not support any
				// command; unknown options are disallowed in rc files.
				log.Debugf("common rc rule: opt %q is unsupported by command %q; skipping", a.Name(), phases[len(phases)-1])
				continue
			}
			if a.Name() != "config" {
				expanded = append(expanded, a)
				continue
			}
			// This is a config option; expand it.
			if _, ok := a.(*options.RequiredValueOption); !ok {
				return nil, fmt.Errorf("config options must be of '*RequiredValueOption', but was of type '%T'.", a)
			}
			config, ok := namedConfigs[a.GetValue()]
			if !ok {
				return nil, fmt.Errorf("config value '%s' is not defined in any .rc file", a.GetValue())
			}
			if slices.Index(configStack, a.GetValue()) != -1 {
				return nil, fmt.Errorf("circular --config reference detected: %s", strings.Join(append(configStack, a.GetValue()), " -> "))
			}
			var err error
			if expanded, err = appendArgsForConfig(config, expanded, namedConfigs, phases, append(configStack, a.GetValue()), false); err != nil {
				return nil, fmt.Errorf("error expanding config '%s': %s", a.GetValue(), err)
			}
		case *arguments.PositionalArgument:
			expanded = append(expanded, a)
		}
	}
	return expanded, nil
}

// ConsumeAndParseRCFiles removes all rc-file related options from the provided
// args and appends an `ignore_all_rc_files` option to the startup options.
// Returns a map of all the named configs in those files and the default
// (unnamed) config from those files.
func (p *Parser) ConsumeAndParseRCFiles(args *parsed.OrderedArgs) (map[string]*parsed.Config, *parsed.Config, error) {
	ws, err := workspace.Path()
	if err != nil {
		log.Debugf("Could not determine workspace dir: %s", err)
	}
	return p.consumeAndParseRCFiles(args, ws)
}

// consumeAndParseRCFiles removes all rc-file related options from the provided
// args and appends an `ignore_all_rc_files` option to the startup options.
// Returns a map of all the named configs in those files and the default
// (unnamed) config from those files.
func (p *Parser) consumeAndParseRCFiles(args *parsed.OrderedArgs, workspaceDir string) (map[string]*parsed.Config, *parsed.Config, error) {
	rcFiles, err := ConsumeRCFileOptions(args, workspaceDir)
	if err != nil {
		return nil, nil, err
	}
	parsedNamedConfigs, defaultConfig, err := p.ParseRCFiles(workspaceDir, rcFiles...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse bazelrc file: %s", err)
	}

	// Ignore all RC files when actually running bazel, since the CLI has already
	// accounted for them.
	ignoreAllRCFilesOptionDefinition, ok := p.ByName["ignore_all_rc_files"]
	if !ok {
		return nil, nil, fmt.Errorf("`ignore_all_rc_files` was not present in the option definitions.")
	}
	opt, err := MakeOption(ignoreAllRCFilesOptionDefinition.Name(), nil)
	if err != nil {
		return nil, nil, err
	}
	if err := args.Append(opt); err != nil {
		return nil, nil, err
	}
	return parsedNamedConfigs, defaultConfig, nil
}

// ConsumeRCFileOptions removes all rc-file related options from the provided
// args and appends an `ignore_all_rc_files` option to the startup options.
// Returns a slice of all the rc files that should be parsed.
//
// TODO(zoey): move this function into `parsed.go` and make it a method of
// `OrderedArgs`.
func ConsumeRCFileOptions(parsedArgs *parsed.OrderedArgs, workspaceDir string) (rcFiles []string, err error) {
	if rcOptions := parsedArgs.RemoveOptions("ignore_all_rc_files"); len(rcOptions) > 0 {
		o := rcOptions[len(rcOptions)-1].Option
		if b, ok := o.(options.BoolLike); !ok {
			return nil, fmt.Errorf("%s must be a boolean option if it is present, but its type was %T", o.Name(), o)
		} else if v, err := b.AsBool(); err != nil {
			return nil, fmt.Errorf("%s must have a boolean value if it is present, but its value was %s", o.Name(), o.GetValue())
		} else if v {
			// Before we do anything, check whether --ignore_all_rc_files is already
			// set. If so, return an empty list of RC files, since bazel will do the
			// same.
			parsedArgs.RemoveOptions("system_rc", "workspace_rc", "home_rc")
			return nil, nil
		}
	}
	// Parse rc files in the order defined here:
	// https://bazel.build/run/bazelrc#bazelrc-file-locations
	for _, optName := range []string{"system_rc", "workspace_rc", " home_rc"} {
		if rcOptions := parsedArgs.RemoveOptions(optName); len(rcOptions) > 0 {
			o := rcOptions[len(rcOptions)-1].Option
			if b, ok := o.(options.BoolLike); !ok {
				return nil, fmt.Errorf("%s must be a boolean option if it is present, but its type was %T", o.Name(), o)
			} else if v, err := b.AsBool(); err != nil {
				return nil, fmt.Errorf("%s must have a boolean value if it is present, but its value was %s", o.Name(), o.GetValue())
			} else if !v {
				// When these flags are false, they have no effect on the list of
				// rcFiles we should parse.
				continue
			}
		}
		switch optName {
		case "system_rc":
			rcFiles = append(rcFiles, "/etc/bazel.bazelrc")
			rcFiles = append(rcFiles, `%ProgramData%\bazel.bazelrc`)
		case "workspace_rc":
			if workspaceDir != "" {
				rcFiles = append(rcFiles, filepath.Join(workspaceDir, ".bazelrc"))
			}
		case "home_rc":
			usr, err := user.Current()
			if err == nil {
				rcFiles = append(rcFiles, filepath.Join(usr.HomeDir, ".bazelrc"))
			}
		}
	}
	for _, indexedOption := range parsedArgs.RemoveOptions("bazelrc") {
		o := indexedOption.Option
		if o.GetValue() == "/dev/null" {
			// if we encounter --bazelrc=/dev/null, that means bazel will ignore
			// subsequent --bazelrc args, so we ignore them as well.
			break
		}
		rcFiles = append(rcFiles, o.GetValue())
		continue
	}
	return rcFiles, nil
}

// TODO: Return an empty string if the subcommand happens to come after
// a bb-specific command. For example, `bb install --path test` should
// return an empty string, not "test".
// TODO: More robust parsing of startup options. For example, this has a bug
// that passing `bazel --output_base build test ...` returns "build" as the
// bazel command, even though "build" is the argument to --output_base.
func GetBazelCommandAndIndex(args []string) (string, int) {
	for i, a := range args {
		if bazelrc.IsBazelCommand(a) {
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

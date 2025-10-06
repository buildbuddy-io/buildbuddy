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
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/cli_command"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/bazelrc"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/parsed"
	"github.com/buildbuddy-io/buildbuddy/cli/shortcuts"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"

	helpoptdef "github.com/buildbuddy-io/buildbuddy/cli/help/option_definitions"
	logoptdef "github.com/buildbuddy-io/buildbuddy/cli/log/option_definitions"
	watchoptdef "github.com/buildbuddy-io/buildbuddy/cli/watcher/option_definitions"

	bfpb "github.com/buildbuddy-io/buildbuddy/proto/bazel_flags"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	nativeDefinitions = map[string]*options.Definition{
		// Set to print debug output for the bb CLI.
		logoptdef.Verbose.Name(): logoptdef.Verbose,
		// Reinvokes the CLI as a subprocess on changes to source files.
		watchoptdef.Watch.Name(): watchoptdef.Watch,
		// Allow specifying --watcher_flags to forward args to the watcher.
		// Mostly useful for debugging, e.g. --watcher_flags='--verbose'
		watchoptdef.WatcherFlags.Name(): watchoptdef.WatcherFlags,
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
			for name, d := range nativeDefinitions {
				if err := parser.AddOptionDefinition(d); err != nil {
					log.Warnf("Error initializing command-line parser when adding bb-specific definiton for '%s': %s", name, err)
				}
			}
			parser.StartupOptionParser.Aliases = shortcuts.Shortcuts
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

// Subparser is a parser that recognizes one set of options, optionally followed by a subcommand.
type Subparser struct {
	ByName      map[string]*options.Definition
	ByShortName map[string]*options.Definition

	Subcommands set.Set[string]
	Aliases     map[string]string

	// Permissive specifies whether or not to error out when parsing an option
	// that does not support the current command.
	Permissive bool
}

func (m *Subparser) ForceAdd(d *options.Definition) {
	m.ByName[d.Name()] = d
	if d.HasNegative() {
		m.ByName["no"+d.Name()] = d
	}
	if d.ShortName() != "" {
		m.ByShortName[d.ShortName()] = d
	}
	if d.OldName() != "" {
		m.ByName[d.OldName()] = d
		if d.OldName() != "" {
			m.ByName["no"+d.OldName()] = d
		}
	}
}

func (m *Subparser) Add(d *options.Definition) error {
	if _, ok := m.ByName[d.Name()]; ok {
		return fmt.Errorf("Naming collision adding flag %s; flag already exists with that name.", d.Name())
	}
	if _, ok := m.ByShortName[d.ShortName()]; ok {
		return fmt.Errorf("Naming collision adding flag with short name %s; flag already exists with that short name.", d.ShortName())
	}
	if _, ok := m.ByName[d.OldName()]; ok {
		return fmt.Errorf("Naming collision adding flag %s; flag has old name %s, but another flag already exists with that name.", d.Name(), d.OldName())
	}
	if d.HasNegative() {
		if _, ok := m.ByName["no"+d.Name()]; ok {
			return fmt.Errorf("Naming collision adding flag %s; flag has negative form %s, but a flag already exists with that name.", d.Name(), "no"+d.Name())
		}
		if d.OldName() != "" {
			if _, ok := m.ByName["no"+d.OldName()]; ok {
				return fmt.Errorf("Naming collision adding flag %s; flag has old negative form %s, but another flag already exists with that name.", d.Name(), "no"+d.OldName())
			}
		}
	}
	m.ForceAdd(d)
	return nil
}

// Parser contains a set of OptionDefinitions (indexed for ease of parsing) and
// the known bazel commands.
type Parser struct {
	StartupOptionParser *Subparser
	CommandOptionParser *Subparser
}

func NewParser(optionDefinitions []*options.Definition, commands []string, aliases map[string]string) *Parser {
	p := &Parser{
		StartupOptionParser: &Subparser{
			ByName:      map[string]*options.Definition{},
			ByShortName: map[string]*options.Definition{},
			Subcommands: set.From(commands...),
			Aliases:     aliases,
		},
		CommandOptionParser: &Subparser{
			ByName:      map[string]*options.Definition{},
			ByShortName: map[string]*options.Definition{},
		},
	}
	for _, d := range optionDefinitions {
		p.AddOptionDefinition(d)
	}
	return p
}

// GetNativeParser can parse native bb options without needing to start the bazel
// client or server.
func GetNativeParser() *Parser {
	definitions := slices.Collect(maps.Values(nativeDefinitions))
	aliases := map[string]string{}
	for alias, command := range cli_command.Aliases {
		aliases[alias] = command.Name
	}
	return NewParser(
		definitions,
		slices.Collect(maps.Keys(cli_command.CommandsByName)),
		aliases,
	)
}

// GetHelpParser returns a parser that can parse bazel options, native options,
// and the help option.
// TODO: when we finally can support plugin options, make sure the help parser
// has access to those as well.
func GetHelpParser() (*Parser, error) {
	bazelParser, err := GetBazelParser()
	if err != nil {
		return nil, err
	}
	nativeParser := GetNativeParser()

	var helpOptionDefinitionsAsStartupOptions []*options.Definition
	for name, d := range bazelParser.CommandOptionParser.ByName {
		if d.Supports("help") {
			// help parser has to be able to recognize help options as startup options,
			// since when help is called with `-h` or `--help`, there technically is
			// no command for command options to follow.
			defOpts := []options.DefinitionOpt{
				options.WithSupportFor("startup"),
			}
			if d.ShortName() != "" {
				defOpts = append(defOpts, options.WithShortName(d.ShortName()))
			}
			if d.Multi() {
				defOpts = append(defOpts, options.WithMulti())
			}
			if d.HasNegative() {
				defOpts = append(defOpts, options.WithNegative())
			}
			if d.RequiresValue() {
				defOpts = append(defOpts, options.WithRequiresValue())
			}
			if d.PluginID() != "" {
				defOpts = append(defOpts, options.WithPluginID(d.PluginID()))
			}
			helpOptionDefinitionsAsStartupOptions = append(
				helpOptionDefinitionsAsStartupOptions,
				options.NewDefinition(
					name,
					defOpts...,
				),
			)
		}
	}

	option_definitions := slices.Concat(
		slices.Collect(maps.Values(nativeParser.StartupOptionParser.ByName)),
		slices.Collect(maps.Values(bazelParser.StartupOptionParser.ByName)),
		slices.Collect(maps.Values(bazelParser.CommandOptionParser.ByName)),
		helpOptionDefinitionsAsStartupOptions,
		[]*options.Definition{helpoptdef.Help},
	)

	commands := slices.Concat(
		slices.Collect(nativeParser.StartupOptionParser.Subcommands.All()),
		slices.Collect(bazelParser.StartupOptionParser.Subcommands.All()),
		// recognize -- to prevent parsing failure for cases like, for example,
		// `bb -h -- build
		[]string{"--"},
	)

	aliases := maps.Clone(bazelParser.StartupOptionParser.Aliases)
	maps.Insert(aliases, maps.All(nativeParser.StartupOptionParser.Aliases))

	return NewParser(option_definitions, commands, aliases), nil
}

// GetBazelParser can parse bazel options, bb CLI native options, and plugin
// options.
func GetBazelParser() (*Parser, error) {
	return GetParser()
}

func (p *Parser) ForceAddOptionDefinition(d *options.Definition) {
	// addOptionDefinition does not return an error if force is true.
	_ = p.addOptionDefinitionImpl(d, true)
}

func (p *Parser) AddOptionDefinition(d *options.Definition) error {
	return p.addOptionDefinitionImpl(d, false)
}

func (p *Parser) addOptionDefinitionImpl(d *options.Definition, force bool) error {
	if d.Supports("startup") {
		if force {
			p.StartupOptionParser.ForceAdd(d)
		} else {
			if err := p.StartupOptionParser.Add(d); err != nil {
				return nil
			}
		}
	}
	addedToCommonParser := false
	for cmd := range d.SupportedCommands() {
		if cmd != "startup" {
			p.StartupOptionParser.Subcommands.Add(cmd)
			if !addedToCommonParser {
				// non-startup flags support the "common" and "always" bazelrc classifiers
				d.AddSupportedCommand("common")
				d.AddSupportedCommand("always")
				if force {
					p.CommandOptionParser.ForceAdd(d)
				} else {
					if err := p.CommandOptionParser.Add(d); err != nil {
						return err
					}
				}
				addedToCommonParser = true
			}
		}
	}
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
	subparser := p.StartupOptionParser
	if command != "startup" {
		subparser = p.CommandOptionParser
	}
	for {
		opts, argIndex, err := subparser.ParseOptions(next, command)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s options: %s", command, err)
		}
		parsedArgs.Args = append(parsedArgs.Args, arguments.FromConcrete(opts)...)
		next = next[argIndex:]
		if len(next) == 0 {
			break
		}
		if next[0] == "--" {
			// -- as a positional argument always ends parsing, one way or another.
			if len(subparser.Subcommands) == 0 {
				parsedArgs.Args = append(parsedArgs.Args, &arguments.DoubleDash{})
				parsedArgs.Args = append(parsedArgs.Args, arguments.ToPositionalArguments(next[1:])...)
				break
			}
			if subparser.Subcommands.Contains("--") {
				// special case to support bb help edge cases; '--help' or '-h' implies
				// a 'help' subcommand, so `--` is not necessarily an error. Hence, the
				// parser for parsing help commands supports `--` as a command, though
				// it is still true that no arguments following it may be interpreted
				// as options.
				parsedArgs.Args = append(parsedArgs.Args, arguments.ToPositionalArguments(next)...)
				break
			}
			// Bazel does not recognize `--` until the command has been encountered.
			return nil, fmt.Errorf("unknown startup option '--'.")
		}
		if len(subparser.Subcommands) == 0 {
			// If the subparser does not support subcommands, this is just a normal
			// positional argument.
			parsedArgs.Args = append(parsedArgs.Args, &arguments.PositionalArgument{Value: next[0]})
			next = next[1:]
			continue
		}
		command = next[0]
		if !subparser.Subcommands.Contains(command) {
			if command == "" {
				// bazel treats a blank command as a help command that halts both option and
				// argument parsing and ignores all non-startup options in the rc file.
				break
			}
			// Expand command shortcuts like b=>build, t=>test, etc.
			aliased, ok := subparser.Aliases[command]
			if !ok {
				return nil, fmt.Errorf("Command '%s' not found. Try 'bb help'", command)
			}
			command = aliased
		}
		subparser = p.CommandOptionParser
		parsedArgs.Args = append(parsedArgs.Args, &arguments.PositionalArgument{Value: command})
		next = next[1:]
	}
	return parsedArgs, nil
}

// Parse options until we encounter a positional argument, and return the
// options and the index of the positional argument that terminated parsing, or
// the length of the input arguments array if no positional argument was
// encountered. If no command is provided, options will not be filtered by
// command.
func (p *Subparser) ParseOptions(args []string, command string) ([]options.Option, int, error) {
	var parsedOptions []options.Option
	// Iterate through the args, looking for a terminating token.
	for i := 0; i < len(args); {
		token := args[i]
		if token == "--" {
			// POSIX-specified (and bazel-supported) delimiter to end option parsing
			return parsedOptions, i, nil
		}
		option, next, err := p.Next(args, i)
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
			} else if !p.Permissive && !option.Supports(command) {
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
func (p *Subparser) Next(args []string, start int) (option options.Option, next int, err error) {
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
	if _, ok := option.(*options.UnknownOption); ok {
		log.Debugf("Unknown option %s", startToken)
		// Unknown option, possibly a plugin-specific argument. Apply a rough
		// heuristic to determine whether or not to have it consume the next
		// argument.
		if b := option.BoolLike(); b != nil && !option.HasValue() && !b.Negated() {
			// This could actually be a required-value-type option rather than a
			// boolean option; if the next argument doesn't look like an option or a
			// bazel command, let's assume that it is.
			if start+1 < len(args) {
				if nextArg := args[start+1]; !strings.HasPrefix(nextArg, "-") {
					if !p.Subcommands.Contains(nextArg) {
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

func (p *Subparser) parseLongNameOption(optName string) (options.Option, error) {
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

	for prefix := range options.StarlarkSkippedPrefixes {
		if strings.HasPrefix(optName, prefix) {
			// This is a new starlark definition; let's hang on to it.
			d := options.NewStarlarkOptionDefinition(optName)
			d.AddSupportedCommand(slices.Collect(bazelrc.BazelCommands().All())...)
			// No need to check if this option already exists since we never reach
			// this code if it does.
			p.ForceAdd(d)
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

func (p *Subparser) parseShortNameOption(optName string) (options.Option, error) {
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
func (p *Subparser) ParseOption(opt string) (option options.Option, err error) {
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
// OptionDefinitions, places each option definition into subparsers corresponding
// to the commands it supports, and returns the resulting parser.
func GenerateParser(flagCollection *bfpb.FlagCollection, commandsToPartition ...string) (*Parser, error) {
	p := NewParser(nil, nil, nil)
	for _, info := range flagCollection.FlagInfos {
		if err := p.AddOptionDefinition(options.DefinitionFrom(info)); err != nil {
			return nil, err
		}
	}
	return p, nil
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
		if !p.StartupOptionParser.Subcommands.Contains(args[0]) {
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
	opts := &bazelisk.RunOpts{Stdout: io.MultiWriter(tmp, buf), Stderr: errBuf}
	args := []string{
		"--ignore_all_rc_files",
		// Run in a temp output base to avoid messing with any running bazel
		// server in the current workspace.
		"--output_base=" + filepath.Join(tmpDir, "output_base"),
		// Make sure this server doesn't stick around for long.
		"--max_idle_secs=10",
		"help",
		topic,
	}
	// try with `--quiet` to avoid problems caused by log messages.
	exitCode, err := bazelisk.Run(append([]string{"--quiet"}, args...), opts)
	if exitCode == 2 {
		// try again without `--quiet`, which is only supported by bazel 8+
		exitCode, err = bazelisk.Run(args, opts)
	}
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
// those configs, and returns the result.
func ResolveArgs(parsedArgs *parsed.OrderedArgs) (*parsed.OrderedArgs, error) {
	ws, err := workspace.Path()
	if err != nil {
		log.Debugf("Could not determine workspace dir: %s", err)
	}
	return resolveArgs(parsedArgs, ws)
}

func resolveArgs(parsedArgs *parsed.OrderedArgs, ws string) (*parsed.OrderedArgs, error) {
	p, err := GetParser()
	if err != nil {
		return nil, err
	}
	return p.resolveArgs(parsedArgs, ws)
}

// ResolveArgs removes all rc-file options from the args, appends an
// `ignore_all_rc_files` option to the startup options, parses those rc-files
// into Configs, and expands all config options (as well as any
// `enable_platform_specific_config` option, if one exists) using
// those configs, and returns the result.
func (p *Parser) ResolveArgs(parsedArgs *parsed.OrderedArgs) (*parsed.OrderedArgs, error) {
	ws, err := workspace.Path()
	if err != nil {
		log.Debugf("Could not determine workspace dir: %s", err)
	}
	return p.resolveArgs(parsedArgs, ws)
}

func (p *Parser) resolveArgs(parsedArgs *parsed.OrderedArgs, ws string) (*parsed.OrderedArgs, error) {
	configs, defaultConfig, err := p.consumeAndParseRCFiles(parsedArgs, ws)
	if err != nil {
		return nil, err
	}
	return parsedArgs.ExpandConfigs(configs, defaultConfig)
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

// Convenience function to use the singleton parser's MakeStartupOption function.
func MakeStartupOption(optionName string, value *string) (option options.Option, err error) {
	p, err := GetParser()
	if err != nil {
		return nil, err
	}
	return p.MakeStartupOption(optionName, value)
}

// Convenience function to use the singleton parser's MakeCommandOption function.
func MakeCommandOption(optionName string, value *string) (option options.Option, err error) {
	p, err := GetParser()
	if err != nil {
		return nil, err
	}
	return p.MakeCommandOption(optionName, value)
}

func (p *Parser) MakeStartupOption(optionName string, value *string) (option options.Option, err error) {
	return p.StartupOptionParser.MakeOption(optionName, value)
}

func (p *Parser) MakeCommandOption(optionName string, value *string) (option options.Option, err error) {
	return p.CommandOptionParser.MakeOption(optionName, value)
}

func (p *Subparser) MakeOption(optionName string, value *string) (option options.Option, err error) {
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
	rcFiles, err := args.ConsumeRCFileOptions(workspaceDir)
	if err != nil {
		return nil, nil, err
	}
	parsedNamedConfigs, defaultConfig, err := p.ParseRCFiles(workspaceDir, rcFiles...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse bazelrc file: %s", err)
	}

	// Ignore all RC files when actually running bazel, since the CLI has already
	// accounted for them.
	ignoreAllRCFilesOptionDefinition, ok := p.StartupOptionParser.ByName["ignore_all_rc_files"]
	if !ok {
		return nil, nil, fmt.Errorf("`ignore_all_rc_files` was not present in the option definitions.")
	}
	opt, err := MakeStartupOption(ignoreAllRCFilesOptionDefinition.Name(), nil)
	if err != nil {
		return nil, nil, err
	}
	if err := args.Append(opt); err != nil {
		return nil, nil, err
	}
	return parsedNamedConfigs, defaultConfig, nil
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

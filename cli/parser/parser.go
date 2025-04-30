// package parser handles the logic for parsing and manipulating Bazel
// configuration (both flags and RC files).
package parser

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"maps"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
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
	bazelCommands = map[string]struct{}{
		"analyze-profile":    {},
		"aquery":             {},
		"build":              {},
		"canonicalize-flags": {},
		"clean":              {},
		"coverage":           {},
		"cquery":             {},
		"dump":               {},
		"fetch":              {},
		"help":               {},
		"info":               {},
		"license":            {},
		"mobile-install":     {},
		"print_action":       {},
		"query":              {},
		"run":                {},
		"shutdown":           {},
		"sync":               {},
		"test":               {},
		"version":            {},
	}
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
		if b, ok := unknownOption.Option.(options.Negatable); ok && !option.HasValue() && !b.Negated() {
			// This could actually be a required-value-type option rather than a
			// boolean option; if the next argument doesn't look like an option or a
			// bazel command, let's assume that it is.
			if start+1 < len(args) {
				if nextArg := args[start+1]; !strings.HasPrefix(nextArg, "-") {
					if _, ok := p.BazelCommands[nextArg]; !startup || !ok {
						innerOption, err := options.NewOption(
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
						unknownOption.Option = innerOption
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

func BazelCommands() (map[string]struct{}, error) {
	// TODO(zoey): figure out if we can get bazel help output without starting a
	// bazel server at any point.
	return bazelCommands, nil
}

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
		d := options.DefinitionFrom(info)
		for cmd := range d.SupportedCommands() {
			if cmd != "startup" {
				// startup is not a real command, just a flag classifier
				parser.BazelCommands[cmd] = struct{}{}
			}
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

func CanonicalizeStartupArgs(args []string) ([]string, error) {
	// Check for bazel command prior to running the parser to avoid the
	// performance cost of generating the parser, which runs bazel.
	bazelCommand, _ := GetBazelCommandAndIndex(args)
	if bazelCommand == "" {
		// Not a bazel command; no startup args to canonicalize.
		return args, nil
	}

	p, err := GetParser()
	if err != nil {
		return nil, err
	}
	return p.canonicalizeArgs(args, true)
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
	return p.canonicalizeArgs(args, false)
}

func (p *Parser) canonicalizeArgs(args []string, onlyStartupOptions bool) ([]string, error) {
	args, execArgs := arg.SplitExecutableArgs(args)
	// First pass: go through args, expanding short names, converting bool
	// values to 0 or 1, and converting "--name value" args to "--name=value"
	// form.
	var processedArgs []arguments.Argument
	lastOptionIndex := map[string]int{}
	i := 0
	command := "startup"
	for i < len(args) {
		token := args[i]
		option, next, err := p.Next(args, i, command == "startup")
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s options: %s", command, err)
		}
		i = next
		switch {
		case option == nil:
			// This is a positional argument
			processedArgs = append(processedArgs, &arguments.PositionalArgument{Value: token})
		case !option.HasSupportedCommands():
			// This option is unsupported by bazel, assume a plugin option
			fallthrough
		case option.Supports(command):
			lastOptionIndex[option.Name()] = len(processedArgs)
			processedArgs = append(processedArgs, option.Normalized())
		default:
			return nil, fmt.Errorf("option '%s' is not a '%s' option.", option.Name(), command)
		}
		if _, ok := p.BazelCommands[token]; ok {
			if onlyStartupOptions {
				return arg.JoinExecutableArgs(append(arguments.FormatAll(processedArgs), args[i:]...), execArgs), nil
			}
			// When we see the bazel command token, switch to parsing command
			// options instead of startup options.
			command = token
		}
	}
	// Second pass: loop through the canonical args so far, and remove any args
	// which are overridden by a later arg. Note that multi-args cannot be
	// overriden.
	var canonical []string
	for i, arg := range processedArgs {
		if opt, ok := arg.(options.Option); ok {
			if opt != nil && !opt.Multi() && lastOptionIndex[opt.Name()] > i {
				continue
			}
		}
		canonical = append(canonical, arg.Format()...)
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
	opts := make([]*RcRule, 0)
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
		opts, err = appendRcRulesFromFile(workspaceDir, file, opts, nil /*=importStack*/)
		if err != nil {
			return nil, err
		}
	}
	return opts, nil
}

func ExpandConfigs(args []string) ([]string, error) {
	p, err := GetParser()
	if err != nil {
		return nil, err
	}
	ws, err := workspace.Path()
	if err != nil {
		log.Debugf("Could not determine workspace dir: %s", err)
	}
	args, err = p.expandConfigs(ws, args)
	if err != nil {
		return nil, err
	}
	return p.canonicalizeArgs(args, false)
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

func (p *Parser) expandConfigs(workspaceDir string, args []string) ([]string, error) {
	bazelCommand, idx := GetBazelCommandAndIndex(args)
	if idx == -1 {
		// Not a bazel command; don't expand configs.
		return args, nil
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
	startupArgs, err := p.appendArgsForConfig("startup", rules, nil, "startup", nil, "" /*=config*/, nil)
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
		defaultArgs, err = p.appendArgsForConfig(bazelCommand, rules, defaultArgs, phase, phases, "" /*=config*/, nil)
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
		if strings.HasPrefix(enable, "-") {
			// It's likely that the "enable" value here is just another Bazel flag, such as `--config` or `-c`.
			// And our --enable_platform_specific_config flag is a standalone flag that does not come with a value.
			//
			// In this case, manually fix arg.FindLast results to avoid confusion.
			// TODO(sluongng): This is a hack. We should fix arg.FindLast to return the correct index.
			enable = "true"
			enableLength = 1
		}
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
			configArgs, err = p.appendArgsForConfig(bazelCommand, rules, configArgs, phase, phases, config, nil)
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

func (p *Parser) appendArgsForConfig(command string, rules *Rules, args []string, phase string, phases []string, config string, configStack []string) ([]string, error) {
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
					var err error
					args, err = p.appendArgsForConfig(command, rules, args, phase, phases, configArg, configStack)
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
				// If the opt is supported, we can do a proper parse to
				// determine how many args to consume in this iteration.
				// e.g., need to skip 2 args for "-c opt", 1 arg for
				// "--nocache_test_results", and 1 arg for "--curses=yes".
				option, next, err := p.Next(rule.Tokens, i, false)
				if err != nil {
					return nil, err
				}
				if option != nil {
					if option.Supports(command) {
						args = append(args, option.Format()...)
					} else {
						log.Debugf("common rc rule: opt %q is unsupported by command %q; skipping", tok, command)
					}
					i = next
				} else {
					// not an option; support positional arguments
					args = append(args, tok)
					i++
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

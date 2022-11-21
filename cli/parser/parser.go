package parser

import (
	"bufio"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
)

const (
	workspacePrefix = `%workspace%/`
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
)

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
	Phase   string
	Config  string
	Options []string
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

func appendRcRulesFromFile(workspaceDir string, f *os.File, opts []*RcRule, importStack []string) ([]*RcRule, error) {
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
			opts, err = appendRcRulesFromImport(workspaceDir, path, opts, isOptional, importStack)
			if err != nil {
				return nil, err
			}
			continue
		}

		opt, err := parseRcRule(line)
		if err != nil {
			log.Debugf("Error parsing bazelrc option: %s", err.Error())
			continue
		}
		// Bazel doesn't support configs for startup options and ignores them if
		// they appear in a bazelrc: https://bazel.build/run/bazelrc#config
		if opt.Phase == "startup" && opt.Config != "" {
			continue
		}
		opts = append(opts, opt)
	}
	return opts, scanner.Err()
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
	tokens := strings.Fields(line)
	if len(tokens) == 0 {
		return nil, fmt.Errorf("unexpected empty line")
	}
	if strings.HasPrefix(tokens[0], "-") {
		return &RcRule{
			Phase:   "common",
			Options: tokens,
		}, nil
	}
	if !strings.Contains(tokens[0], ":") {
		return &RcRule{
			Phase:   tokens[0],
			Options: tokens[1:],
		}, nil
	}
	phaseConfig := strings.Split(tokens[0], ":")
	if len(phaseConfig) != 2 {
		return nil, fmt.Errorf("invalid bazelrc syntax: %s", phaseConfig)
	}
	return &RcRule{
		Phase:   phaseConfig[0],
		Config:  phaseConfig[1],
		Options: tokens[1:],
	}, nil
}

func ParseRCFiles(workspaceDir string, filePaths ...string) ([]*RcRule, error) {
	options := make([]*RcRule, 0)
	for _, filePath := range filePaths {
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
	return expandConfigs(ws, args)
}

func expandConfigs(workspaceDir string, args []string) ([]string, error) {
	_, idx := GetBazelCommandAndIndex(args)
	if idx == -1 {
		// Not a bazel command; don't expand configs.
		return args, nil
	}

	args, rcFiles, err := consumeRCFileArgs(args, workspaceDir)
	if err != nil {
		return nil, err
	}
	r, err := ParseRCFiles(workspaceDir, rcFiles...)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bazelrc file: %s", err)
	}
	rules := StructureRules(r)

	// Expand startup args first, before any other args (including explicit
	// startup args).
	startupArgs, err := appendArgsForConfig(rules, nil, "startup", nil, "" /*=config*/, nil)
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

	// We'll refer to args in bazelrc which aren't expanded from a --config
	// option as "default" args, like a .bazelrc line that just says "-c dbg" or
	// "build -c dbg" as opposed to something qualified like "build:dbg -c dbg".
	//
	// These default args take lower precedence than explicit command line args
	// so we expand those first just after the command.
	var defaultArgs []string
	for _, phase := range phases {
		defaultArgs, err = appendArgsForConfig(rules, defaultArgs, phase, phases, "" /*=config*/, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate bazelrc configuration: %s", err)
		}
	}
	args = concat(args[:commandIndex+1], defaultArgs, args[commandIndex+1:])

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
			configArgs, err = appendArgsForConfig(rules, configArgs, phase, phases, config, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate bazelrc configuration: %s", err)
			}
		}

		args = concat(args[:configIndex], configArgs, args[configIndex+length:])
		offset = configIndex
	}

	log.Debugf("expanded args: %+v", args)

	return args, nil
}

func appendArgsForConfig(rules *Rules, args []string, phase string, phases []string, config string, configStack []string) ([]string, error) {
	var err error
	for _, c := range configStack {
		if c == config {
			return nil, fmt.Errorf("circular --config reference detected: %s -> %s", strings.Join(configStack, " -> "), config)
		}
	}
	configStack = append(configStack, config)
	for _, rule := range rules.ForPhaseAndConfig(phase, config) {
		for i := 0; i < len(rule.Options); i++ {
			opt := rule.Options[i]

			configArg := ""
			configArgCount := 0
			if strings.HasPrefix(opt, "--config=") {
				configArg = strings.TrimPrefix(opt, "--config=")
				configArgCount = 1
			} else if opt == "--config" && i+1 < len(rule.Options) {
				configArg = rule.Options[i+1]
				// Consume the following argument in this iteration too
				configArgCount = 2
			}
			// If we have a --config arg, expand it if it is defined in any rc
			// file. If it is not defined, let bazel show an error saying that
			// it is not defined.
			if configArg != "" && isConfigDefined(rules, config, phases) {
				for _, phase := range phases {
					args, err = appendArgsForConfig(rules, args, phase, phases, configArg, configStack)
					if err != nil {
						return nil, err
					}
				}
				i += (configArgCount - 1)
				continue
			}

			args = append(args, opt)
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
	for i, a := range args {
		if _, ok := bazelCommands[a]; ok {
			return a, i
		}
	}
	return "", -1
}

// getPhases returns the command's inheritance hierarchy in increasing order of
// precedence.
//
// Examples:
//
//     getPhases("run")      // {"common", "build", "run"}
//     getPhases("coverage") // {"common", "build", "test", "coverage"}
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

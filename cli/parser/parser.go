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
	rcFiles := make([]string, 0)
	// TODO: handle --nosystem_rc, --system_rc no, --system_rc=0, etc.
	if arg.Get(args, "system_rc") != "no" {
		rcFiles = append(rcFiles, "/etc/bazel.bazelrc")
		rcFiles = append(rcFiles, `%ProgramData%\bazel.bazelrc`)
	}
	if workspaceDir != "" && arg.Get(args, "workspace_rc") != "no" {
		rcFiles = append(rcFiles, filepath.Join(workspaceDir, ".bazelrc"))
	}
	if arg.Get(args, "home_rc") != "no" {
		usr, err := user.Current()
		if err == nil {
			rcFiles = append(rcFiles, filepath.Join(usr.HomeDir, ".bazelrc"))
		}
	}
	// TODO(siggisim): Handle multiple bazelrc params.
	if b := arg.Get(args, "bazelrc"); b != "" {
		rcFiles = append(rcFiles, b)
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

	command, commandIndex := arg.GetCommandAndIndex(args)

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

	for {
		config, i, length := arg.Find(args, "config")
		if i < 0 {
			break
		}

		var configArgs []string
		for _, phase := range phases {
			configArgs, err = appendArgsForConfig(rules, configArgs, phase, phases, config, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate bazelrc configuration: %s", err)
			}
		}

		args = concat(args[:i], configArgs, args[i+length:])
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
			if strings.HasPrefix(opt, "--config=") {
				configArg = strings.TrimPrefix(opt, "--config=")
			} else if opt == "--config" && i+1 < len(rule.Options) {
				configArg = rule.Options[i+1]
				// Consume the following argument in this iteration too
				i++
			}
			if configArg != "" {
				for _, phase := range phases {
					args, err = appendArgsForConfig(rules, args, phase, phases, configArg, configStack)
					if err != nil {
						return nil, err
					}
				}
				continue
			}

			args = append(args, opt)
		}
	}
	return args, nil
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

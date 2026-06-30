package bbrc

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/cli_command"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/bazelrc"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
)

const (
	ConfigFlag = "bb_config"
	rcFileName = ".bbrc"
)

type Config struct {
	ByPhase map[string][]string
}

type argElement struct {
	Token  string
	Config *string
}

func NewConfig() *Config {
	return &Config{ByPhase: make(map[string][]string)}
}

func ResolveArgs(command *cli_command.Command, args []string, startupConfigs []string) ([]string, error) {
	ws, err := workspace.Path()
	if err != nil {
		log.Debugf("Could not determine workspace dir for .bbrc: %s", err)
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Debugf("Could not determine home dir for .bbrc: %s", err)
	}
	return resolveArgs(command, args, startupConfigs, ws, homeDir)
}

func resolveArgs(command *cli_command.Command, args []string, startupConfigs []string, workspaceDir string, homeDir string) ([]string, error) {
	namedConfigs, defaultConfig, err := ParseRCFiles(workspaceDir, rcFilePaths(workspaceDir, homeDir)...)
	if err != nil {
		return nil, fmt.Errorf("failed to parse .bbrc file: %s", err)
	}
	return expandArgs(command, args, startupConfigs, namedConfigs, defaultConfig)
}

func expandArgs(command *cli_command.Command, args []string, startupConfigs []string, namedConfigs map[string]*Config, defaultConfig *Config) ([]string, error) {
	boundary := command.RCBoundary(args)
	if boundary > len(args) {
		boundary = len(args)
	}

	prefix := args[:boundary]
	suffix := slices.Clone(args[boundary:])

	explicitArgs, argsWithoutConfigs, err := splitExplicitArgs(prefix)
	if err != nil {
		return nil, err
	}

	commandPath, subcommandArgCount := command.ResolveRCPath(argsWithoutConfigs)
	phases := GetPhases(commandPath)
	log.Debugf("bb command: %q, rc rule classes: %v", commandPath, phases)

	subcommandTokens := slices.Clone(argsWithoutConfigs[:subcommandArgCount])
	if subcommandArgCount > 0 && len(explicitArgs) > 0 {
		remainingArgs := make([]argElement, 0, len(explicitArgs))
		skipped := 0
		for _, elem := range explicitArgs {
			if elem.Config == nil && skipped < subcommandArgCount {
				skipped += 1
				continue
			}
			remainingArgs = append(remainingArgs, elem)
		}
		explicitArgs = remainingArgs
	}

	expanded := subcommandTokens
	expanded, err = defaultConfig.appendArgsForConfig(expanded, namedConfigs, phases, []string{}, true)
	if err != nil {
		return nil, err
	}
	expanded, err = appendConfigSelections(expanded, startupConfigs, namedConfigs, phases, []string{})
	if err != nil {
		return nil, err
	}
	expanded, err = appendElements(expanded, explicitArgs, namedConfigs, phases, []string{})
	if err != nil {
		return nil, err
	}
	return slices.Concat(expanded, suffix), nil
}

func ParseRCFiles(workspaceDir string, filePaths ...string) (map[string]*Config, *Config, error) {
	seen := make(map[string]struct{}, len(filePaths))
	namedConfigs := map[string]*Config{}
	defaultConfig := NewConfig()
	for _, filePath := range filePaths {
		namedRcRules := map[string]map[string][]string{}
		defaultRcRules := map[string][]string{}
		realPath, err := bazelrc.Realpath(filePath)
		if err != nil {
			continue
		}
		if _, ok := seen[realPath]; ok {
			continue
		}
		seen[realPath] = struct{}{}

		if err := bazelrc.AppendRcRulesFromFile(workspaceDir, realPath, namedRcRules, defaultRcRules, nil /*=importStack*/, true); err != nil {
			return nil, nil, err
		}

		for phase, tokens := range defaultRcRules {
			if !IsPhase(phase) {
				log.Warnf("invalid command name '%s'", phase)
				continue
			}
			defaultConfig.ByPhase[phase] = append(defaultConfig.ByPhase[phase], tokens...)
		}
		for name, config := range namedRcRules {
			parsedConfig, ok := namedConfigs[name]
			if !ok {
				parsedConfig = NewConfig()
				namedConfigs[name] = parsedConfig
			}
			for phase, tokens := range config {
				if !IsPhase(phase) {
					log.Warnf("invalid command name '%s:%s'", phase, name)
					continue
				}
				parsedConfig.ByPhase[phase] = append(parsedConfig.ByPhase[phase], tokens...)
			}
		}
	}
	return namedConfigs, defaultConfig, nil
}

func GetPhases(commandPath string) []string {
	phases := []string{bazelrc.CommonPhase}
	tokens := strings.Fields(commandPath)
	for i := 1; i <= len(tokens); i++ {
		phases = append(phases, strings.Join(tokens[:i], " "))
	}
	return phases
}

func IsPhase(phase string) bool {
	_, ok := validPhases()[phase]
	return ok
}

func (c *Config) appendArgsForConfig(expanded []string, namedConfigs map[string]*Config, phases []string, configStack []string, allowEmpty bool) ([]string, error) {
	empty := true
	for _, phase := range phases {
		toExpand, ok := c.ByPhase[phase]
		if !ok {
			continue
		}
		empty = false
		var err error
		expanded, err = appendTokens(expanded, toExpand, namedConfigs, phases, configStack)
		if err != nil {
			return nil, err
		}
	}
	if empty && !allowEmpty {
		return nil, fmt.Errorf("config value not defined in any .bbrc file")
	}
	return expanded, nil
}

func appendTokens(expanded []string, tokens []string, namedConfigs map[string]*Config, phases []string, configStack []string) ([]string, error) {
	elements, _, err := splitExplicitArgs(tokens)
	if err != nil {
		return nil, err
	}
	return appendElements(expanded, elements, namedConfigs, phases, configStack)
}

func appendElements(expanded []string, elements []argElement, namedConfigs map[string]*Config, phases []string, configStack []string) ([]string, error) {
	for _, elem := range elements {
		if elem.Config == nil {
			expanded = append(expanded, elem.Token)
			continue
		}
		var err error
		expanded, err = appendNamedConfig(expanded, *elem.Config, namedConfigs, phases, configStack)
		if err != nil {
			return nil, err
		}
	}
	return expanded, nil
}

func appendConfigSelections(expanded []string, configNames []string, namedConfigs map[string]*Config, phases []string, configStack []string) ([]string, error) {
	for _, name := range configNames {
		var err error
		expanded, err = appendNamedConfig(expanded, name, namedConfigs, phases, configStack)
		if err != nil {
			return nil, err
		}
	}
	return expanded, nil
}

func appendNamedConfig(expanded []string, configName string, namedConfigs map[string]*Config, phases []string, configStack []string) ([]string, error) {
	config, ok := namedConfigs[configName]
	if !ok {
		return nil, fmt.Errorf("config value '%s' is not defined in any .bbrc file", configName)
	}
	if slices.Contains(configStack, configName) {
		return nil, fmt.Errorf("circular --%s reference detected: %s", ConfigFlag, strings.Join(append(configStack, configName), " -> "))
	}
	return config.appendArgsForConfig(expanded, namedConfigs, phases, append(configStack, configName), false)
}

func splitExplicitArgs(args []string) ([]argElement, []string, error) {
	elements := make([]argElement, 0, len(args))
	argsWithoutConfigs := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		value, consumed, ok, err := parseConfigSelection(args, i)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			elements = append(elements, argElement{Config: &value})
			i += consumed - 1
			continue
		}
		elements = append(elements, argElement{Token: args[i]})
		argsWithoutConfigs = append(argsWithoutConfigs, args[i])
	}
	return elements, argsWithoutConfigs, nil
}

func parseConfigSelection(args []string, index int) (string, int, bool, error) {
	token := args[index]
	prefix := "--" + ConfigFlag + "="
	if value, ok := strings.CutPrefix(token, prefix); ok {
		return value, 1, true, nil
	}
	if token != "--"+ConfigFlag {
		return "", 0, false, nil
	}
	if index+1 >= len(args) {
		return "", 0, false, fmt.Errorf("expected value after --%s", ConfigFlag)
	}
	return args[index+1], 2, true, nil
}

func rcFilePaths(workspaceDir string, homeDir string) []string {
	var rcFiles []string
	if workspaceDir != "" {
		rcFiles = append(rcFiles, filepath.Join(workspaceDir, rcFileName))
	}
	if homeDir != "" {
		rcFiles = append(rcFiles, filepath.Join(homeDir, rcFileName))
	}
	return rcFiles
}

func validPhases() map[string]struct{} {
	phases := map[string]struct{}{
		bazelrc.CommonPhase: {},
	}
	for _, path := range cli_command.AllRCPaths() {
		phases[path] = struct{}{}
	}
	return phases
}

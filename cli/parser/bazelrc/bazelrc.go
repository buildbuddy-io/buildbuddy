package bazelrc

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/bazel_command"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/parsed"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/rc_util"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
	"github.com/google/shlex"
)

const (
	EnablePlatformSpecificConfigFlag = "enable_platform_specific_config"
	workspacePrefix                  = `%workspace%/`
)

var (
	allPhases = set.FromSeq(
		set.Union(
			set.FromSeq(bazel_command.Commands().All()),
			set.From(rc_util.CommonPhase, rc_util.AlwaysPhase, "startup"),
		),
	)

	StartupFlagNoRc = map[string]struct{}{
		"ignore_all_rc_files": {},
		"home_rc":             {},
		"workspace_rc":        {},
		"system_rc":           {},
		"bazelrc":             {},
	}
)

// RcRule is a rule parsed from a bazelrc file.
type RcRule struct {
	Phase string
	// Make Config a pointer to a string so we can distinguish default configs
	// from configs with blank names.
	Config *string
	// Tokens contains the raw (non-canonicalized) tokens in the rule.
	Tokens []string
}

// getPhases returns the command's inheritance hierarchy in increasing order of
// precedence.
//
// Examples:
//
//	getPhases("run")      // {"always", "common", "build", "run"}
//	getPhases("coverage") // {"always", "common", "build", "test", "coverage"}
func GetPhases(command string) (out []string) {
	for {
		if command == "" {
			out = append(out, "common", "always")
			break
		}
		out = append(out, command)
		command = bazel_command.Parent(command)
	}
	slices.Reverse(out)
	return
}

func appendRcRulesFromImport(workspaceDir, path string, namedConfigs map[string]map[string][]string, defaultConfig map[string][]string, optional bool, importStack []string) error {
	if strings.HasPrefix(path, workspacePrefix) {
		path = filepath.Join(workspaceDir, path[len(workspacePrefix):])
	}
	realPath, err := Realpath(path)
	if err != nil {
		if optional {
			return nil
		}
		return fmt.Errorf("could not determine real path of bazelrc file: %s", err)
	}

	return AppendRcRulesFromFile(workspaceDir, realPath, namedConfigs, defaultConfig, importStack, optional)
}

// AppendRCRulesFromFile reads and lexes the provided rc file and appends the
// args to the provided configs based on the detected phase and name.
//
// configs is a map keyed by config name where the values are maps keyed by
// phase name where the values are lists containing all the rules for that
// config in the order they are encountered.
func AppendRcRulesFromFile(workspaceDir string, realPath string, namedConfigs map[string]map[string][]string, defaultConfig map[string][]string, importStack []string, optional bool) error {
	if slices.Contains(importStack, realPath) {
		return fmt.Errorf("circular import detected: %s -> %s", strings.Join(importStack, " -> "), realPath)
	}
	importStack = append(importStack, realPath)
	file, err := os.Open(realPath)
	if err != nil {
		if optional {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		// Handle line continuations (lines can end with "\" to effectively
		// continue the same line)
		for strings.HasSuffix(line, `\`) && scanner.Scan() {
			line = line[:len(line)-1] + scanner.Text()
		}
		lexer := shlex.NewLexer(strings.NewReader(line))
		tokens := []string{}
		for {
			token, err := lexer.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("Error parsing bazelrc: %s\nFailed to lex '%s'.", err, line)
			}
			tokens = append(tokens, token)
		}
		if len(tokens) == 0 {
			// blank line
			continue
		}
		if tokens[0] == "import" || tokens[0] == "try-import" {
			isOptional := tokens[0] == "try-import"
			path := strings.TrimSpace(strings.TrimPrefix(line, tokens[0]))
			if err := appendRcRulesFromImport(workspaceDir, path, namedConfigs, defaultConfig, isOptional, importStack); err != nil {
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
		if rule.Config == nil {
			defaultConfig[rule.Phase] = append(defaultConfig[rule.Phase], rule.Tokens...)
			continue
		}
		// Bazel doesn't support named configs for startup options and ignores them
		// if they appear in a bazelrc: https://bazel.build/run/bazelrc#config
		if rule.Phase == "startup" {
			continue
		}
		config, ok := namedConfigs[*rule.Config]
		if !ok {
			config = make(map[string][]string)
			namedConfigs[*rule.Config] = config
		}
		config[rule.Phase] = append(config[rule.Phase], rule.Tokens...)
	}
	log.Debugf("Added rc rules from %q; new configs: %#v", realPath, namedConfigs)
	return scanner.Err()
}

// Realpath evaluates any symlinks in the given path and then returns the
// absolute path.
func Realpath(path string) (string, error) {
	directPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return filepath.Abs(directPath)
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
	phase := tokens[0]
	var configName *string
	if colonIndex := strings.Index(tokens[0], ":"); colonIndex != -1 {
		phase = tokens[0][:colonIndex]
		v := tokens[0][colonIndex+1:]
		configName = &v
	}

	return &RcRule{
		Phase:  phase,
		Config: configName,
		Tokens: tokens[1:],
	}, nil
}

// GetBazelOS returns the os string that `enable_platform_specific_config` will
// expect based on the detected runtime.GOOS.
//
// Mirroring the behavior here:
// https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/runtime/ConfigExpander.java#L41
func GetBazelOS() string {
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

// ExpandConfigs expands .bazelrc --config flags.
func ExpandConfigs(
	args *parsed.OrderedArgs,
	namedConfigs map[string]*parsed.Config,
	defaultConfig *parsed.Config,
) (*parsed.OrderedArgs, error) {
	policy := &parsed.ConfigExpansionPolicy{FlagName: "config", GetPhases: GetPhases}
	expanded, err := args.ExpandConfigsWithPolicy(namedConfigs, defaultConfig, policy)
	if err != nil {
		return nil, err
	}
	return expandPlatformSpecificConfig(expanded, namedConfigs, policy)
}

// expandPlatformSpecificConfig replaces the last occurrence of
// --enable_platform_specific_config with --config=<bazelOS>, so long as the
// last occurrence evaluates to true.
func expandPlatformSpecificConfig(
	expanded *parsed.OrderedArgs,
	namedConfigs map[string]*parsed.Config,
	policy *parsed.ConfigExpansionPolicy,
) (*parsed.OrderedArgs, error) {
	opts := expanded.RemoveCommandOptions(EnablePlatformSpecificConfigFlag)
	if v, err := options.AccumulateValues[*parsed.IndexedOption](false, opts); err != nil {
		return nil, fmt.Errorf("failed to get value from %q option: %s", EnablePlatformSpecificConfigFlag, err)
	} else if v {
		index := opts[len(opts)-1].Index
		bazelOS := GetBazelOS()
		if _, ok := namedConfigs[bazelOS]; ok {
			phases := GetPhases(expanded.GetCommand())
			expansion, err := parsed.ExpandNamedConfig(bazelOS, namedConfigs, phases, policy)
			if err != nil {
				return nil, err
			}
			expanded.Args = slices.Insert(expanded.Args, index, expansion...)
			// Remove all occurrences of the enable platform-specific config flag
			// that may have been added when expanding the platform-specific config.
			expanded.RemoveCommandOptions(EnablePlatformSpecificConfigFlag)
		}
	}
	return expanded, nil
}

// IsPhase returns whether or not this is a valid phase for a bazel rc line.
func IsPhase(phase string) bool {
	_, ok := allPhases[phase]
	return ok
}

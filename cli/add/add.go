package add

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bzlmod"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/manifoldco/promptui"
)

var (
	flags = flag.NewFlagSet("add", flag.ContinueOnError)
	Flags = flags
	usage = `
usage: bb ` + flags.Name() + ` <module>[@<version>]

Adds the given dependency to your MODULE.bazel (or WORKSPACE) file, e.g.
"bb add rules_go" or "bb add github/bazel-contrib/rules_go@0.50.1".
Does nothing if the workspace already depends on it.
`
	headerTemplate = "###### Begin auto-generated section for %s ######"
	footerTemplate = "###### End auto-generated section for %s ######"

	headerRegex = regexp.MustCompile(`##### Begin auto-generated section for \[https://registry\.build/(.+?)@(.+?)\]`)
)

const (
	registryEndpoint = "https://registry.build/%s/data.json"
)

func HandleAdd(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return 1, err
	}

	if len(flags.Args()) != 1 {
		log.Print(usage)
		return 1, nil
	}

	result, err := Add(flags.Args()[0])
	if err != nil {
		return 1, err
	}
	if result.AlreadyPresent {
		log.Printf("%s already depends on %s; nothing to do.", result.File, result.Module)
	} else if result.Added {
		log.Printf("Added %s@%s to %s.", result.Module, result.Version, result.File)
	}
	return 0, nil
}

// Result describes what Add did.
type Result struct {
	// File is the basename of the MODULE.bazel or WORKSPACE file.
	File string
	// Module is the module name (for MODULE.bazel) or registry path (for
	// WORKSPACE).
	Module  string
	Version string
	// Added is true if a dependency was added.
	Added bool
	// AlreadyPresent is true if the workspace already had the dependency, so
	// nothing was changed.
	AlreadyPresent bool
}

// Add adds the dependency described by input (a registry.build module path or
// name, optionally with an @version suffix, and optionally prefixed with ~ to
// only add it to WORKSPACE files) to the workspace's MODULE.bazel or WORKSPACE
// file. It's not an error if the workspace already has the dependency, unless
// a different version was explicitly requested.
func Add(input string) (*Result, error) {
	transitive := strings.HasPrefix(input, "~")
	if transitive {
		input = strings.TrimPrefix(input, "~")
	}

	module, version, resp, err := FetchModuleOrDisambiguate(input)
	if err != nil {
		return nil, err
	}

	workspacePath, basename, err := workspace.CreateModuleIfNotExists()
	if err != nil {
		return nil, err
	}

	if strings.HasPrefix(strings.ToUpper(basename), "MODULE") {
		if transitive {
			// Bzlmod resolves transitive deps on its own.
			return &Result{File: basename}, nil
		}
		return addToModule(workspacePath, basename, version, resp)
	}
	return addToWorkspace(filepath.Join(workspacePath, basename), module, version, resp)
}

func addToWorkspace(path, module, requestedVersion string, resp *RegistryResponse) (*Result, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	version := requestedVersion
	if version == "" {
		version = resp.LatestReleaseWithWorkspaceSnippet
	}
	result := &Result{File: filepath.Base(path), Module: module, Version: version}

	matches := headerRegex.FindAllStringSubmatch(string(contents), -1)
	for _, m := range matches {
		existingModule := m[1]
		existingVersion := m[2]
		if module != existingModule {
			continue
		}
		if requestedVersion != "" && existingVersion != requestedVersion {
			return nil, fmt.Errorf("%s already contains %s at version %s (the requested version is %s)",
				result.File, existingModule, existingVersion, requestedVersion)
		}
		result.Version = existingVersion
		result.AlreadyPresent = true
		return result, nil
	}
	if strings.Contains(string(contents), resp.Repo.FullName) {
		// Likely installed by hand.
		result.Version = ""
		result.AlreadyPresent = true
		return result, nil
	}

	addition := GenerateWorkspaceSnippet(module, version, resp)
	if err := appendToFile(path, addition); err != nil {
		return nil, err
	}
	log.Debugf("Added the following snippet to %s:\n%s\n\n", result.File, addition)
	result.Added = true
	return result, nil
}

func addToModule(workspacePath, basename, requestedVersion string, resp *RegistryResponse) (*Result, error) {
	snippet := resp.ModuleSnippet
	if strings.TrimSpace(snippet) == "" {
		return nil, fmt.Errorf("the registry has no MODULE.bazel snippet for %s", resp.Repo.FullName)
	}
	name, _, err := bzlmod.ParseBazelDep(snippet)
	if err != nil {
		return nil, fmt.Errorf("parse MODULE.bazel snippet for %s: %w", resp.Repo.FullName, err)
	}
	result := &Result{File: basename, Module: name}

	// Look at MODULE.bazel and everything it include()s.
	module, err := bzlmod.Load(workspacePath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", basename, err)
	}
	if existingVersion, ok := module.BazelDep(name); ok {
		if requestedVersion != "" && existingVersion != "" &&
			existingVersion != requestedVersion && existingVersion != strings.TrimPrefix(requestedVersion, "v") {
			return nil, fmt.Errorf("%s already depends on %s at version %s (the requested version is %s)",
				basename, name, existingVersion, requestedVersion)
		}
		result.Version = existingVersion
		result.AlreadyPresent = true
		return result, nil
	}

	// The registry's snippet uses the module's latest release, which may be a
	// pre-release, so pick the version from the BCR instead.
	version, err := pickBCRVersion(name, requestedVersion)
	if err != nil {
		return nil, err
	}
	snippet, err = bzlmod.SetBazelDepVersion(snippet, name, version)
	if err != nil {
		return nil, err
	}
	if err := appendToFile(filepath.Join(workspacePath, basename), "\n"+snippet); err != nil {
		return nil, err
	}
	log.Debugf("Added the following snippet to %s:\n%s\n\n", basename, snippet)
	result.Version = version
	result.Added = true
	return result, nil
}

func appendToFile(path, contents string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(contents)
	return err
}

func FetchModuleOrDisambiguate(moduleInput string) (string, string, *RegistryResponse, error) {
	moduleAndVersion := strings.Replace(moduleInput, "https://", "", 1)
	moduleAndVersion = strings.Replace(moduleAndVersion, "github.com/", "github/", 1)
	moduleAndVersion = strings.TrimRight(moduleAndVersion, "/")
	moduleParts := strings.Split(moduleAndVersion, "@")
	moduleName := moduleParts[0]
	moduleVersion := ""
	if len(moduleParts) > 1 {
		moduleVersion = moduleParts[1]
	}
	res, err := fetch(moduleName)
	if err != nil {
		return "", "", nil, err
	}
	if len(res.Disambiguation) == 0 && res.Name == "" {
		return "", "", nil, fmt.Errorf("module %q not found", moduleName)
	}
	if len(res.Disambiguation) == 1 && res.Name == "" {
		moduleName = res.Disambiguation[0].Path
		res, err = fetch(moduleName)
		if err != nil {
			return "", "", nil, err
		}
	}
	if len(res.Disambiguation) > 1 && res.Name == "" {
		pickedModule, err := showPicker(res.Disambiguation)
		if err != nil {
			return "", "", nil, err
		}
		moduleName = pickedModule
		res, err = fetch(moduleName)
		if err != nil {
			return "", "", nil, err
		}
	}
	return moduleName, moduleVersion, res, nil
}

func GenerateWorkspaceSnippet(module, version string, resp *RegistryResponse) string {
	versionKey := fmt.Sprintf("[https://registry.build/%s@%s]", module, version)
	header := fmt.Sprintf(headerTemplate, versionKey)
	footer := fmt.Sprintf(footerTemplate, versionKey)
	snippet := resp.WorkspaceSnippet
	for _, r := range resp.Releases {
		if r.Name == "v"+version || r.Name == version {
			snippet = r.WorkspaceSnippet
			break
		}
	}
	return fmt.Sprintf("\n%s\n\n%+v\n\n%s\n", header, strings.TrimSpace(snippet), footer)
}

func GenerateModuleSnippet(module, version string, resp *RegistryResponse) string {
	snippet := resp.ModuleSnippet
	return fmt.Sprintf("%s\n", snippet)
}

func fetch(module string) (*RegistryResponse, error) {
	resp, err := http.Get(fmt.Sprintf(registryEndpoint, module))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("module %q not found in registry, code: %d", module, resp.StatusCode)
	}

	response := &RegistryResponse{}
	err = json.Unmarshal(body, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func showPicker(modules []Disambiguation) (string, error) {
	// If not running interactively, we can't show a prompt.
	if !terminal.IsTTY(os.Stdin) || !terminal.IsTTY(os.Stderr) {
		return "", fmt.Errorf("ambiguous module name, not running in interactive mode")
	}

	items := []string{}
	for _, m := range modules {
		items = append(items, fmt.Sprintf("%s [%d stars]", m.Path, m.Stars))
	}

	// If there is more than one module, show a picker.
	prompt := promptui.Select{
		Label:             "Select the module you want",
		Items:             items,
		Stdout:            &bellSkipper{},
		Size:              10,
		Searcher:          searcher(modules),
		StartInSearchMode: true,
		Keys: &promptui.SelectKeys{
			Prev:     promptui.Key{Code: promptui.KeyPrev, Display: promptui.KeyPrevDisplay},
			Next:     promptui.Key{Code: promptui.KeyNext, Display: promptui.KeyNextDisplay},
			PageUp:   promptui.Key{Code: promptui.KeyBackward, Display: promptui.KeyBackwardDisplay},
			PageDown: promptui.Key{Code: promptui.KeyForward, Display: promptui.KeyForwardDisplay},
			Search:   promptui.Key{Code: '?', Display: "?"},
		},
	}
	index, _, err := prompt.Run()
	if err != nil {
		return "", fmt.Errorf("failed to select module: %v", err)
	}
	return modules[index].Path, nil
}

type RegistryResponse struct {
	Name                              string           `json:"name"`
	Owner                             string           `json:"owner"`
	WorkspaceSnippet                  string           `json:"workspace_snippet"`
	ModuleSnippet                     string           `json:"module_snippet"`
	LatestReleaseWithWorkspaceSnippet string           `json:"latest_release_with_workspace_snippet"`
	LatestReleaseWithModuleSnippet    string           `json:"latest_release_with_module_snippet"`
	Disambiguation                    []Disambiguation `json:"disambiguation"`
	Repo                              Repo             `json:"repo"`
	Releases                          []Release        `json:"releases"`
}

type Disambiguation struct {
	Path  string `json:"path"`
	Stars int    `json:"stars"`
}

type Repo struct {
	FullName string `json:"full_name"`
}

type Release struct {
	WorkspaceSnippet string `json:"workspace_snippet"`
	Name             string `json:"name"`
}

// This is a workaround for the bell issue documented in
// https://github.com/manifoldco/promptui/issues/49.
type bellSkipper struct{}

func (bs *bellSkipper) Write(b []byte) (int, error) {
	const charBell = 7 // c.f. readline.CharBell
	if len(b) == 1 && b[0] == charBell {
		return 0, nil
	}
	return os.Stderr.Write(b)
}

func (bs *bellSkipper) Close() error {
	return os.Stderr.Close()
}

func searcher(targets []Disambiguation) func(input string, index int) bool {
	return func(input string, index int) bool {
		return strings.Contains(targets[index].Path, input)
	}
}

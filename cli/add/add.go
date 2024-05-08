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
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/manifoldco/promptui"
)

var (
	flags = flag.NewFlagSet("add", flag.ContinueOnError)
	usage = `
usage: bb ` + flags.Name() + ` rules_go

Adds the given dependency to your WORKSPACE file.
`
	headerTemplate = "###### Begin auto-generated section for %s ######"
	footerTemplate = "###### End auto-generated section for %s ######"

	headerRegex = regexp.MustCompile(`##### Begin auto-generated section for \[https://registry\.build/(.+?)@(.+?)\]`)
	moduleRegex = regexp.MustCompile(`bazel_dep\(name = "([^"]+?)", version = "([^"]+?)".*?\)`)
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

	input := flags.Args()[0]
	transitive := strings.HasPrefix(input, "~")
	if transitive {
		input = strings.TrimPrefix(input, "~")
	}

	module, version, resp, err := FetchModuleOrDisambiguate(input)
	if err != nil {
		return 1, err
	}

	if version == "" {
		version = resp.LatestReleaseWithWorkspaceSnippet
	}

	f, err := openOrCreateWorkspaceFile()
	if err != nil {
		return 1, err
	}
	defer f.Close()

	if strings.HasPrefix(strings.ToUpper(filepath.Base(f.Name())), "MODULE") {
		if transitive {
			return 0, nil
		}
		if err := addToModule(f, module, version, resp); err != nil {
			return 1, err
		}
	} else {
		if err := addToWorkspace(f, module, version, resp); err != nil {
			return 1, err
		}
	}

	return 0, nil
}

func addToWorkspace(f *os.File, module, version string, resp *RegistryResponse) error {
	contents, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	matches := headerRegex.FindAllStringSubmatch(string(contents), -1)
	for _, m := range matches {
		existingModule := m[1]
		existingVersion := m[2]
		if module == existingModule && version == existingVersion {
			return fmt.Errorf("WORKSPACE already contains %s at the requested version (%s)",
				existingModule, existingVersion)
		}
		if module == existingModule {
			return fmt.Errorf("WORKSPACE already contains %s at version %s (the requested version is %s)",
				existingModule, existingVersion, version)
		}
	}
	if strings.Contains(string(contents), resp.Repo.FullName) {
		return fmt.Errorf("WORKSPACE already contains %s which is likely %s manually installed",
			resp.Repo.FullName, module)
	}

	addition := GenerateWorkspaceSnippet(module, version, resp)

	if _, err := f.WriteString(addition); err != nil {
		return err
	}

	log.Debugf("Added the following snippet to %s:\n%s\n\n", filepath.Base(f.Name()), addition)
	return nil
}

func addToModule(f *os.File, module, version string, resp *RegistryResponse) error {
	contents, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	moduleSnippet := GenerateModuleSnippet(module, version, resp)
	registryMatches := moduleRegex.FindStringSubmatch(moduleSnippet)
	if registryMatches == nil {
		return fmt.Errorf("MODULE %s not found: %s", module, moduleSnippet)
	}
	newModule := registryMatches[1]
	newVersion := registryMatches[2]

	matches := moduleRegex.FindAllStringSubmatch(string(contents), -1)
	for _, m := range matches {
		existingModule := m[1]
		existingVersion := m[2]
		if newModule == existingModule && newVersion == existingVersion {
			return fmt.Errorf("MODULE already contains %s at the requested version (%s)",
				existingModule, existingVersion)
		}
		if newModule == existingModule {
			return fmt.Errorf("MODULE already contains %s at version %s (the requested version is %s)",
				existingModule, existingVersion, newVersion)
		}
	}

	if _, err := f.WriteString(moduleSnippet); err != nil {
		return err
	}

	log.Debugf("Added the following snippet to %s:\n%s\n\n", filepath.Base(f.Name()), moduleSnippet)
	return nil
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

func openOrCreateWorkspaceFile() (*os.File, error) {
	workspacePath, basename, err := workspace.CreateModuleIfNotExists()
	if err != nil {
		return nil, err
	}
	return os.OpenFile(filepath.Join(workspacePath, basename), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
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

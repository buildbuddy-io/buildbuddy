package plugin

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/creack/pty"
	"gopkg.in/yaml.v2"
)

const (
	// Path where we expect to find the plugin configuration, relative to the
	// root of the Bazel workspace in which the CLI is invoked.
	workspaceRelativeConfigPath = "buildbuddy.yaml"

	// Path where we expect to find the user's plugin configuration, relative
	// to the user's home directory.
	homeRelativeUserConfigPath = "buildbuddy.yaml"

	// Path under the CLI storage dir where plugins are saved.
	pluginsStorageDirName = "plugins"

	installCommandUsage = `
Usage: bb install [REPO[@VERSION]][:PATH] [--user]

Installs a remote or local CLI plugin for the current bazel workspace.

The --user flag installs the plugin globally for your user in ~/buildbuddy.yaml,
instead of just for the current workspace.

A local plugin can be installed by omitting the repo argument and specifying
just :PATH, or the flag --path=PATH.

Examples:
  # Install the latest version of "github.com/example-inc/example-bb-plugin"
  bb install example-inc/example-bb-plugin

  # Install "github.com/example-inc/example-bb-plugin" at version tag "v1.2.3"
  bb install example-inc/example-bb-plugin@v1.2.3

  # Install "example.com/example-bb-plugin" at commit SHA "abc123",
  # where the plugin is located in the "src" directory in that repo.
  bb install example.com/example-bb-plugin@abc123:src

  # Use the local plugin located at "./plugins/local_plugin".
  bb install :plugins/local_plugin
  # or:
  bb install --path plugins/local_plugin
`
)

var (
	installCmd     = flag.NewFlagSet("install", flag.ContinueOnError)
	installPath    = installCmd.String("path", "", "Path under the repo root where the plugin directory is located.")
	installForUser = installCmd.Bool("user", false, "Whether to install globally for the user.")

	repoPattern = regexp.MustCompile(`` +
		`^` + // Start marker
		`(https?://)?` + // Optional scheme. TODO: Support SSH, git@, etc.
		`(?P<repo>.+?)` + // Required repo spec
		`(@(?P<version>.*?))?` + // Optional version spec
		`(:(?P<path>.*))?` + // Optional path
		`$`, // End marker
	)
)

// HandleInstall handles the "bb install" subcommand, which allows adding
// plugins to buildbuddy.yaml.
func HandleInstall(args []string) (exitCode int, err error) {
	command, idx := arg.GetCommandAndIndex(args)
	if command != "install" {
		return -1, nil
	}
	if idx != 0 {
		log.Debugf("Unexpected flag: %s", args[0])
		log.Print(installCommandUsage)
		return 1, nil
	}
	if err := arg.ParseFlagSet(installCmd, args[1:]); err != nil {
		if err != flag.ErrHelp {
			log.Printf("Failed to parse flags: %s", err)
		}
		log.Print(installCommandUsage)
		return 1, nil
	}
	if len(installCmd.Args()) == 0 && *installPath == "" {
		log.Print("Error: either a repo or a --path= is expected.")
		log.Print(installCommandUsage)
		return 1, nil
	}
	if len(installCmd.Args()) > 1 {
		log.Print("Error: unexpected positional arguments")
		log.Print(installCommandUsage)
		return 1, nil
	}
	pluginCfg := &PluginConfig{
		Repo: "",
		Path: *installPath,
	}
	if len(installCmd.Args()) == 1 {
		repo := installCmd.Args()[0]
		cfg, err := parsePluginSpec(repo, *installPath)
		if err != nil {
			log.Printf("Failed to parse repo: %s", repo)
			return 1, nil
		}
		pluginCfg = cfg
	}

	configPath := ""
	if *installForUser {
		home := os.Getenv("HOME")
		if home == "" {
			log.Printf("Could not locate user config path: $HOME not set")
			return 1, nil
		}
		configPath = filepath.Join(home, homeRelativeUserConfigPath)
	} else {
		ws, err := workspace.Path()
		if err != nil {
			log.Printf("Could not locate workspace config path: %s", err)
			return 1, nil
		}
		configPath = filepath.Join(ws, workspaceRelativeConfigPath)
	}

	if err := installPlugin(pluginCfg, configPath); err != nil {
		log.Printf("Failed to install plugin: %s", err)
		return 1, nil
	}
	log.Printf("Plugin installed successfully and added to %s", configPath)
	return 0, nil
}

func parsePluginSpec(spec, pathArg string) (*PluginConfig, error) {
	var repoSpec, versionSpec, pathSpec string
	if strings.HasPrefix(spec, ":") {
		pathSpec = strings.TrimPrefix(spec, ":")
	} else {
		m := repoPattern.FindStringSubmatch(spec)
		if len(m) == 0 {
			return nil, fmt.Errorf("invalid plugin spec %q: does not match REPO[@VERSION][:PATH]", spec)
		}
		repoSpec = m[repoPattern.SubexpIndex("repo")]
		if v := m[repoPattern.SubexpIndex("version")]; v != "" {
			versionSpec = "@" + v
		}
		pathSpec = m[repoPattern.SubexpIndex("path")]
	}

	if pathArg != "" {
		if pathSpec != "" {
			return nil, fmt.Errorf("ambiguous path: positional argument specifies %q but --path flag specifies %q", pathSpec, pathArg)
		}
		pathSpec = pathArg
	}

	return &PluginConfig{
		Repo: repoSpec + versionSpec,
		Path: pathSpec,
	}, nil
}

func installPlugin(plugin *PluginConfig, configPath string) error {
	configFile, err := readConfig(configPath)
	if err != nil {
		return err
	}
	if configFile == nil {
		configFile = &ConfigFile{
			Path:             configPath,
			BuildBuddyConfig: &BuildBuddyConfig{},
		}
	}

	// Load the plugin so that an invalid version, path etc. doesn't wind
	// up getting added to buildbuddy.yaml.
	p := &Plugin{configFile: configFile, config: plugin}
	if err := p.load(); err != nil {
		return err
	}

	// Make sure the plugin is not already installed.
	pluginPath, err := p.Path()
	if err != nil {
		return err
	}
	for _, p := range configFile.Plugins {
		existingPath, err := (&Plugin{configFile: configFile, config: p}).Path()
		if err != nil {
			return fmt.Errorf("invalid config: failed to determine existing plugin path: %s", err)
		}
		if pluginPath == existingPath {
			return fmt.Errorf("plugin is already installed")
		}
	}

	// Init the config file if it doesn't exist already.
	if _, err := os.Stat(configPath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		f, err := os.Create(configPath)
		if err != nil {
			return fmt.Errorf("failed to initialize config file: %s", err)
		}
		f.Close()
	}

	// Getting go-yaml to update a YAML file while still preserving formatting
	// is currently a pain; see: https://github.com/go-yaml/yaml/issues/899
	// To avoid messing with buildbuddy.yaml too much,
	// look for the "plugins:" section of the yaml file and mutate only that
	// part.
	b, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read existing config contents: %s", err)
	}
	lines := strings.Split(string(b), "\n")
	pluginSection := struct{ start, end int }{-1, -1}
	anyMapKeyRE := regexp.MustCompile(`^\w.*?:`)
	for i, line := range lines {
		if strings.HasPrefix(line, "plugins:") {
			pluginSection.start = i
			continue
		}
		if pluginSection.start >= 0 && anyMapKeyRE.MatchString(line) {
			pluginSection.end = i
			break
		}
	}
	if pluginSection.start >= 0 && pluginSection.end == -1 {
		pluginSection.end = len(lines)
	}

	// Compute the updated config file contents.
	// head, tail are the original config lines before/after the plugins
	// section.
	var head, tail []string
	if pluginSection.start >= 0 {
		head, tail = lines[:pluginSection.start], lines[pluginSection.end:]
	}
	configFile.Plugins = append(configFile.Plugins, plugin)
	b, err = yaml.Marshal(configFile.BuildBuddyConfig)
	if err != nil {
		return err
	}
	pluginSectionLines := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	// go-yaml doesn't properly indent lists :'(
	// Apply a fix for that here.
	for i := 1; i < len(pluginSectionLines); i++ {
		pluginSectionLines[i] = "  " + pluginSectionLines[i]
	}
	var newCfgLines []string
	newCfgLines = append(newCfgLines, head...)
	newCfgLines = append(newCfgLines, pluginSectionLines...)
	newCfgLines = append(newCfgLines, tail...)

	// Write the new config to a temp file then replace the old config once it's
	// fully written.
	tmp, err := os.CreateTemp("", "buildbuddy-*.yaml")
	if err != nil {
		return err
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}()
	for _, line := range newCfgLines {
		if _, err := io.WriteString(tmp, line+"\n"); err != nil {
			return err
		}
	}
	if err := disk.MoveFile(tmp.Name(), configPath); err != nil {
		return fmt.Errorf("failed to move temp config to %s: %s", configPath, err)
	}
	return nil
}

// ConfigFile represents a decoded config file along its file metadata.
type ConfigFile struct {
	Path string
	*BuildBuddyConfig
}

type BuildBuddyConfig struct {
	Plugins []*PluginConfig `yaml:"plugins,omitempty"`
}

type PluginConfig struct {
	// Repo where the plugin should be loaded from.
	// If empty, use the local workspace.
	Repo string `yaml:"repo,omitempty"`

	// Path relative to the repo where the plugin is defined.
	// Optional. If unspecified, it behaves the same as "." (the repo root).
	Path string `yaml:"path,omitempty"`
}

func readWorkspaceConfig(workspaceDir string) (*ConfigFile, error) {
	return readConfig(filepath.Join(workspaceDir, workspaceRelativeConfigPath))
}

func readUserConfig() (*ConfigFile, error) {
	homeDir := os.Getenv("HOME")
	if homeDir == "" {
		log.Debugf("not reading ~/%s: $HOME environment variable is not set", homeRelativeUserConfigPath)
		return nil, nil
	}
	return readConfig(filepath.Join(homeDir, homeRelativeUserConfigPath))
}

func readConfig(path string) (*ConfigFile, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Debugf("%s not found", path)
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	log.Debugf("Reading %s", f.Name())
	cfg := &BuildBuddyConfig{}
	if err := yaml.NewDecoder(f).Decode(cfg); err != nil {
		return nil, status.UnknownErrorf("failed to parse %s: %s", f.Name(), err)
	}
	return &ConfigFile{Path: path, BuildBuddyConfig: cfg}, nil
}

// getConfigFiles returns a list of parsed buildbuddy.yaml files from which to
// load plugins, in increasing order of precedence.
func getConfigFiles(workspaceDir string) ([]*ConfigFile, error) {
	var configs []*ConfigFile
	cfg, err := readUserConfig()
	if err != nil {
		return nil, err
	}
	if cfg != nil {
		configs = append(configs, cfg)
	}
	cfg, err = readWorkspaceConfig(workspaceDir)
	if err != nil {
		return nil, err
	}
	if cfg != nil {
		configs = append(configs, cfg)
	}
	return configs, nil
}

// dedupe returns a modified list of plugins such that if there are multiple
// occurrences of the same plugin in the list, only the last occurrence appears
// in the final list. Version info is ignored when determining whether two
// plugins are the same.
func dedupe(plugins []*Plugin) ([]*Plugin, error) {
	ids := map[string]struct{}{}
	var out []*Plugin
	// Iterate in reverse order so that IDs appearing latest get the highest
	// precedence.
	for i := len(plugins) - 1; i >= 0; i-- {
		p := plugins[i]
		id, err := p.NonVersionedID()
		if err != nil {
			return nil, err
		}
		if _, ok := ids[id]; ok {
			continue
		}
		ids[id] = struct{}{}
		out = append(out, p)
	}
	// Reverse to undo the reverse-iteration. This ensures plugin hooks are
	// run with the same relative ordering as they appear in buildbuddy.yaml.
	reversePlugins(out)
	return out, nil
}

func reversePlugins(a []*Plugin) {
	for i := 0; i < len(a)/2; i++ {
		j := len(a) - i - 1
		a[i], a[j] = a[j], a[i]
	}
}

// Plugin represents a CLI plugin. Plugins can exist locally or remotely
// (if remote, they will be fetched).
type Plugin struct {
	// config is the raw config spec for this plugin.
	config *PluginConfig
	// configFile is the ConfigFile that defined this plugin.
	configFile *ConfigFile
	// tempDir is a directory where the plugin can store temporary files.
	// This dir lasts only for the current CLI invocation and is visible
	// to all hooks.
	tempDir string
}

// LoadAll loads all plugins from the combined user and workspace configs, and
// ensures that any remote plugins are downloaded.
func LoadAll(tempDir string) ([]*Plugin, error) {
	ws, err := workspace.Path()
	if err != nil {
		return nil, err
	}
	return loadAll(ws, tempDir)
}

// loadAll gets all of the configured plugins from the user's ~/buildbuddy.yaml
// and workspace buildbuddy.yaml, and prepares them for use by ensuring all
// files and directories needed for correct operation are present on disk.
func loadAll(workspaceDir, tempDir string) ([]*Plugin, error) {
	plugins, err := getConfiguredPlugins(workspaceDir)
	if err != nil {
		return nil, err
	}
	for _, plugin := range plugins {
		pluginTempDir, err := os.MkdirTemp(tempDir, "plugin-tmp-*")
		if err != nil {
			return nil, status.InternalErrorf("failed to create plugin temp dir: %s", err)
		}
		plugin.tempDir = pluginTempDir
		if err := plugin.load(); err != nil {
			return nil, err
		}
	}
	return plugins, nil
}

// getConfiguredPlugins parses the user's ~/buildbuddy.yaml as well as the
// workspace buildbuddy.yaml and returns the set of plugins. The returned
// plugins are not yet ready to use.
func getConfiguredPlugins(workspaceDir string) ([]*Plugin, error) {
	configFiles, err := getConfigFiles(workspaceDir)
	if err != nil {
		return nil, err
	}
	var plugins []*Plugin
	for _, f := range configFiles {
		for _, p := range f.BuildBuddyConfig.Plugins {
			plugin := &Plugin{config: p, configFile: f}
			plugins = append(plugins, plugin)
		}
	}
	return dedupe(plugins)
}

// PrepareEnv sets environment variables for use in plugins.
func PrepareEnv() error {
	ws, err := workspace.Path()
	if err != nil {
		return err
	}
	if err := os.Setenv("BUILD_WORKSPACE_DIRECTORY", ws); err != nil {
		return err
	}
	cfg, err := os.UserConfigDir()
	if err != nil {
		return err
	}
	if err := os.Setenv("USER_CONFIG_DIR", cfg); err != nil {
		return err
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return err
	}
	if err := os.Setenv("USER_CACHE_DIR", cache); err != nil {
		return err
	}
	return nil
}

// RepoURL returns the normalized repo URL. It does not include the ref part of
// the URL. For example, a "repo" spec of "foo/bar@abc123" returns
// "https://github.com/foo/bar".
func (p *Plugin) RepoURL() string {
	if p.config.Repo == "" {
		return ""
	}
	repo, _ := p.splitRepoRef()
	segments := strings.Split(repo, "/")
	// Auto-convert owner/repo to https://github.com/owner/repo
	if len(segments) == 2 {
		repo = "https://github.com/" + repo
	}
	u, err := git.NormalizeRepoURL(repo)
	if err != nil {
		return ""
	}
	return u.String()
}

func (p *Plugin) splitRepoRef() (string, string) {
	refParts := strings.Split(p.config.Repo, "@")
	if len(refParts) == 2 {
		return refParts[0], refParts[1]
	}
	if len(refParts) > 0 {
		return refParts[0], ""
	}
	return "", ""
}

// VersionedID returns a human-readable, unique representation of the plugin
// which includes its versioning info.
func (p *Plugin) VersionedID() (string, error) {
	if p.config.Repo == "" {
		return p.ResolveLocalPath()
	}
	id := p.RepoURL()
	_, ref := p.splitRepoRef()
	if ref != "" {
		id += "@" + ref
	}
	if p.config.Path != "" {
		id += ":" + filepath.Clean(p.config.Path)
	}
	return id, nil
}

// NonVersionedID returns a human-readable, unique representation of the plugin
// which does not include its versioning info. Plugins with the same repo and
// path information have identical IDs, even if their version is different.
func (p *Plugin) NonVersionedID() (string, error) {
	if p.config.Repo == "" {
		return p.ResolveLocalPath()
	}
	id := p.RepoURL()
	if p.config.Path != "" {
		id += ":" + filepath.Clean(p.config.Path)
	}
	return id, nil
}

// ResolveLocalPath returns the absolute path on the local filesystem for the
// plugin if it's a local plugin. Otherwise, it returns an error.
func (p *Plugin) ResolveLocalPath() (string, error) {
	if p.RepoURL() != "" {
		return "", fmt.Errorf("could not resolve local path for remote plugin %s", p.RepoURL())
	}
	// If already an abs path, return it directly.
	if strings.HasPrefix(p.config.Path, "/") {
		return realpath(p.config.Path)
	}
	// Otherwise, resolve relative to the config file which defined it.
	configParentDir := filepath.Dir(p.configFile.Path)
	return realpath(filepath.Join(configParentDir, p.config.Path))
}

func (p *Plugin) repoClonePath() (string, error) {
	storagePath, err := storage.CacheDir()
	if err != nil {
		return "", err
	}
	u, err := url.Parse(p.RepoURL())
	if err != nil {
		return "", err
	}
	_, ref := p.splitRepoRef()
	refSubdir := "latest"
	if ref != "" {
		refSubdir = filepath.Join("ref", ref)
	}
	fullPath := filepath.Join(storagePath, pluginsStorageDirName, u.Host, u.Path, refSubdir)
	return fullPath, nil
}

// load downloads the plugin from the specified repo, if applicable.
func (p *Plugin) load() error {
	if p.config.Repo == "" {
		return nil
	}

	if p.RepoURL() == "" {
		return status.InvalidArgumentErrorf(`could not parse plugin repo URL %q: expecting "[HOST/]OWNER/REPO[@REVISION]"`, p.config.Repo)
	}
	path, err := p.repoClonePath()
	if err != nil {
		return err
	}
	exists, err := disk.FileExists(context.TODO(), filepath.Join(path, ".git"))
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	log.Printf("Downloading %q", p.RepoURL())
	// Clone into a temp dir so that if the ref does not exist, we don't
	// actually create the plugin directory.
	tempPath, err := os.MkdirTemp("", "buildbuddy-git-clone-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %s", err)
	}
	defer os.RemoveAll(tempPath) // intentionally ignoring error

	stderr := &bytes.Buffer{}
	clone := exec.Command("git", "clone", p.RepoURL(), tempPath)
	clone.Stderr = stderr
	clone.Stdout = io.Discard
	if err := clone.Run(); err != nil {
		log.Printf("%s", stderr.String())
		return err
	}
	_, ref := p.splitRepoRef()
	if ref != "" {
		stderr := &bytes.Buffer{}
		checkout := exec.Command("git", "checkout", ref)
		checkout.Stderr = stderr
		checkout.Stdout = io.Discard
		checkout.Dir = tempPath
		if err := checkout.Run(); err != nil {
			log.Printf("%s", stderr.String())
			return err
		}
	}

	// We cloned the repo and checked out the ref successfully; move it to
	// the cache dir.
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create repo dir: %s", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("failed to add cloned repo to plugins dir: %s", err)
	}

	if err := p.validate(); err != nil {
		return err
	}

	return nil
}

// validate makes sure the plugin's path spec points to a valid path within the
// source repository.
func (p *Plugin) validate() error {
	path, err := p.Path()
	if err != nil {
		return err
	}
	exists, err := disk.FileExists(context.TODO(), path)
	if err != nil {
		return err
	}
	if !exists {
		if p.config.Repo != "" {
			return status.FailedPreconditionErrorf("plugin path %q does not exist in %s", p.config.Path, p.config.Repo)
		}
		return status.FailedPreconditionErrorf("plugin path %q was not found in this workspace", p.config.Path)
	}
	return nil
}

// Path returns the absolute root path of the plugin.
func (p *Plugin) Path() (string, error) {
	if p.config.Repo != "" {
		repoPath, err := p.repoClonePath()
		if err != nil {
			return "", err
		}
		return filepath.Join(repoPath, p.config.Path), nil
	}
	return p.ResolveLocalPath()
}

func (p *Plugin) commandEnv() []string {
	env := os.Environ()
	env = append(env, fmt.Sprintf("PLUGIN_TEMPDIR=%s", p.tempDir))
	return env
}

// PreBazel executes the plugin's pre-bazel hook if it exists, allowing the
// plugin to return a set of transformed bazel arguments.
//
// Plugins receive as their first argument a path to a file containing the
// arguments to be passed to bazel. The plugin can read and write that file to
// modify the args (most commonly, just appending to the file), which will then
// be fed to the next plugin in the pipeline, or passed to Bazel if this is the
// last plugin.
//
// See cli/example_plugins/ping-remote/pre_bazel.sh for an example.
func (p *Plugin) PreBazel(args []string) ([]string, error) {
	// Write args to a file so the plugin can manipulate them.
	argsFile, err := os.CreateTemp("", "bazelisk-args-*")
	if err != nil {
		return nil, status.InternalErrorf("failed to create args file for pre-bazel hook: %s", err)
	}
	defer func() {
		argsFile.Close()
		os.Remove(argsFile.Name())
	}()
	_, err = disk.WriteFile(context.TODO(), argsFile.Name(), []byte(strings.Join(args, "\n")+"\n"))
	if err != nil {
		return nil, err
	}

	path, err := p.Path()
	if err != nil {
		return nil, err
	}
	scriptPath := filepath.Join(path, "pre_bazel.sh")
	exists, err := disk.FileExists(context.TODO(), scriptPath)
	if err != nil {
		return nil, err
	}
	if !exists {
		log.Debugf("Bazel hook not found at %s", scriptPath)
		return args, nil
	}
	log.Debugf("Running pre-bazel hook for %s/%s", p.config.Repo, p.config.Path)
	// TODO: support "pre_bazel.<any-extension>" as long as the file is
	// executable and has a shebang line
	cmd := exec.Command("/usr/bin/env", "bash", scriptPath, argsFile.Name())
	// TODO: Prefix output with "output from [plugin]" ?
	cmd.Dir = path
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.Env = p.commandEnv()
	if err := cmd.Run(); err != nil {
		return nil, status.InternalErrorf("Pre-bazel hook for %s/%s failed: %s", p.config.Repo, p.config.Path, err)
	}

	newArgs, err := readArgsFile(argsFile.Name())
	if err != nil {
		return nil, err
	}

	log.Debugf("New bazel args: %s", newArgs)

	// Canonicalize args after each plugin is run, so that every plugin gets
	// canonicalized args as input.
	return parser.CanonicalizeArgs(newArgs)
}

// PostBazel executes the plugin's post-bazel hook if it exists, allowing it to
// respond to the result of the invocation.
//
// Currently the invocation data is fed as plain text via a file. The file path
// is passed as the first argument.
//
// See cli/example_plugins/go-deps/post_bazel.sh for an example.
func (p *Plugin) PostBazel(bazelOutputPath string) error {
	path, err := p.Path()
	if err != nil {
		return err
	}
	scriptPath := filepath.Join(path, "post_bazel.sh")
	exists, err := disk.FileExists(context.TODO(), scriptPath)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	log.Debugf("Running post-bazel hook for %s/%s", p.config.Repo, p.config.Path)
	cmd := exec.Command("/usr/bin/env", "bash", scriptPath, bazelOutputPath)
	// TODO: Prefix stderr output with "output from [plugin]" ?
	cmd.Dir = path
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.Stdin = os.Stdin
	cmd.Env = p.commandEnv()
	if err := cmd.Run(); err != nil {
		return status.InternalErrorf("Post-bazel hook for %s/%s failed: %s", p.config.Repo, p.config.Path, err)
	}
	return nil
}

// Pipe streams the combined console output (stdout + stderr) from the previous
// plugin in the pipeline to this plugin, returning the output of this plugin.
// If there is no previous plugin in the pipeline then the original bazel output
// is piped in.
//
// It returns the original reader if no output handler is configured for this
// plugin.
//
// See cli/example_plugins/go-highlight/handle_bazel_output.sh for an example.
func (p *Plugin) Pipe(r io.Reader) (io.Reader, error) {
	path, err := p.Path()
	if err != nil {
		return nil, err
	}
	scriptPath := filepath.Join(path, "handle_bazel_output.sh")
	exists, err := disk.FileExists(context.TODO(), scriptPath)
	if err != nil {
		return nil, err
	}
	if !exists {
		return r, nil
	}
	cmd := exec.Command("/usr/bin/env", "bash", scriptPath)
	pr, pw := io.Pipe()
	cmd.Dir = path
	// Write command output to a pty to ensure line buffering.
	ptmx, tty, err := pty.Open()
	if err != nil {
		return nil, status.InternalErrorf("failed to open pty: %s", err)
	}
	cmd.Stdout = tty
	cmd.Stderr = tty
	cmd.Stdin = r
	// Prevent output handlers from receiving Ctrl+C, to prevent things like
	// "KeyboardInterrupt" being printed in Python plugins. Instead, plugins
	// will just receive EOF on stdin and exit normally.
	// TODO: Should we still have a way for plugins to listen to Ctrl+C?
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	cmd.Env = p.commandEnv()
	if err := cmd.Start(); err != nil {
		return nil, status.InternalErrorf("failed to start plugin bazel output handler: %s", err)
	}
	go func() {
		// Copy pty output to the next pipeline stage.
		io.Copy(pw, ptmx)
	}()
	go func() {
		defer tty.Close()
		defer ptmx.Close()
		defer pw.Close()
		log.Debugf("Running bazel output handler for %s/%s", p.config.Repo, p.config.Path)
		if err := cmd.Wait(); err != nil {
			log.Debugf("Command failed: %s", err)
		} else {
			log.Debugf("Command %s completed", cmd.Args)
		}
		// Flush any remaining data from the preceding stage, to prevent
		// the output writer for the preceding stage from getting stuck.
		io.Copy(io.Discard, r)
	}()
	return pr, nil
}

// PipelineWriter returns a WriteCloser that sends written bytes through all the
// plugins in the given pipeline and then finally to the given writer. It is the
// caller's responsibility to close the returned WriteCloser. Closing the
// returned writer will inform the first plugin in the pipeline that there is no
// more input to be flushed, causing each plugin to terminate once it has
// finished processing any remaining input.
func PipelineWriter(w io.Writer, plugins []*Plugin) (io.WriteCloser, error) {
	pr, pw := io.Pipe()

	var pluginOutput io.Reader = pr
	for _, p := range plugins {
		r, err := p.Pipe(pluginOutput)
		if err != nil {
			return nil, err
		}
		pluginOutput = r
	}
	doneCopying := make(chan struct{})
	go func() {
		defer close(doneCopying)
		io.Copy(w, pluginOutput)
	}()
	out := &overrideCloser{
		WriteCloser: pw,
		// Make Close() block until we're done copying the plugin output,
		// otherwise we might miss some output lines.
		AfterClose: func() { <-doneCopying },
	}
	return out, nil
}

func RunBazeliskWithPlugins(args []string, outputPath string, plugins []*Plugin) (int, error) {
	// Build the pipeline of bazel output handlers
	wc, err := PipelineWriter(os.Stdout, plugins)
	if err != nil {
		return -1, err
	}
	// Note, it's important that the Close() here happens just after the
	// bazelisk run completes and before we run post_bazel hooks, since this
	// waits for all plugins to finish writing to stdout. Otherwise, the
	// post_bazel output will get intermingled with bazel output.
	defer wc.Close()

	log.Debugf("Calling bazelisk with %+v", args)

	// Create the output path where the original bazel output will be written,
	// for post-bazel plugins to read.
	output, err := os.Create(outputPath)
	if err != nil {
		return -1, fmt.Errorf("failed to create output file: %s", err)
	}
	defer output.Close()

	// If bb's output is connected to a terminal, then allocate a pty so that
	// bazel thinks it's writing to a terminal, even though we're capturing its
	// output via a Writer that is not a terminal. This enables ANSI colors,
	// proper truncation of progress messages, etc.
	isStdoutTTY, err := terminal.IsTTY(os.Stdout)
	if err != nil {
		return -1, fmt.Errorf("failed to determine whether stdout is a terminal: %s", err)
	}
	w := io.MultiWriter(output, wc)
	opts := &bazelisk.RunOpts{Stdout: w, Stderr: w}
	if isStdoutTTY {
		ptmx, tty, err := pty.Open()
		if err != nil {
			return -1, fmt.Errorf("failed to allocate pty: %s", err)
		}
		defer func() {
			// 	Close pty/tty (best effort).
			_ = tty.Close()
			_ = ptmx.Close()
		}()
		if err := pty.InheritSize(os.Stdout, tty); err != nil {
			return -1, fmt.Errorf("failed to inherit terminal size: %s", err)
		}
		// Note: we don't listen to resize events (SIGWINCH) and re-inherit the
		// size, because Bazel itself doesn't do that currently. So it wouldn't
		// make a difference either way.
		opts.Stdout = tty
		opts.Stderr = tty
		go io.Copy(w, ptmx)
	} else {
		opts.Stdout = w
		opts.Stderr = w
	}

	return bazelisk.Run(args, opts)
}

type overrideCloser struct {
	io.WriteCloser
	AfterClose func()
}

func (o *overrideCloser) Close() error {
	defer o.AfterClose()
	return o.WriteCloser.Close()
}

// readArgsFile reads the arguments from the file at the given path.
// Each arg is expected to be placed on its own line.
// Blank lines are ignored.
func readArgsFile(path string) ([]string, error) {
	b, err := disk.ReadFile(context.TODO(), path)
	if err != nil {
		return nil, status.InternalErrorf("failed to read arguments: %s", err)
	}

	lines := strings.Split(string(b), "\n")
	// Construct args from non-blank lines.
	newArgs := make([]string, 0, len(lines))
	for _, line := range lines {
		if line != "" {
			newArgs = append(newArgs, line)
		}
	}

	return newArgs, nil
}

func realpath(path string) (string, error) {
	directPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return filepath.Abs(directPath)
}

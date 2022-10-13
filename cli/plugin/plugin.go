package plugin

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
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
	configPath = "buildbuddy.yaml"

	// Path under the CLI storage dir where plugins are saved.
	pluginsStorageDirName = "plugins"
)

type BuildBuddyConfig struct {
	Plugins []*PluginConfig `yaml:"plugins"`
}

type PluginConfig struct {
	// Repo where the plugin should be loaded from.
	// If empty, use the local workspace.
	Repo string `yaml:"repo"`

	// Path relative to the repo where the plugin is defined.
	// Optional. If unspecified, it behaves the same as "." (the repo root).
	Path string `yaml:"path"`
}

func readConfig() (*BuildBuddyConfig, error) {
	ws, err := workspace.Path()
	if err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(ws, configPath))
	if err != nil {
		if os.IsNotExist(err) {
			log.Debugf("%s not found in %s", configPath, ws)
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
	return cfg, nil
}

// Plugin represents a CLI plugin. Plugins can exist locally or remotely
// (if remote, they will be fetched).
type Plugin struct {
	config *PluginConfig
	// tempDir is a directory where the plugin can store temporary files.
	// This dir lasts only for the current CLI invocation and is visible
	// to all hooks.
	tempDir string
}

// LoadAll loads all plugins from the config, and ensures that any remote
// plugins are downloaded.
func LoadAll(tempDir string) ([]*Plugin, error) {
	cfg, err := readConfig()
	if err != nil {
		return nil, err
	}
	plugins := make([]*Plugin, 0, len(cfg.Plugins))
	for _, p := range cfg.Plugins {
		pluginTempDir, err := os.MkdirTemp(tempDir, "plugin-tmp-*")
		if err != nil {
			return nil, status.InternalErrorf("failed to create plugin temp dir: %s", err)
		}
		plugin := &Plugin{config: p, tempDir: pluginTempDir}
		if err := plugin.load(); err != nil {
			return nil, err
		}
		if err := plugin.validate(); err != nil {
			return nil, err
		}
		plugins = append(plugins, plugin)
	}
	return plugins, nil
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

func (p *Plugin) repoClonePath() (string, error) {
	storagePath, err := storage.Dir()
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
	stderr := &bytes.Buffer{}
	clone := exec.Command("git", "clone", p.RepoURL(), path)
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
		if err := checkout.Run(); err != nil {
			log.Printf("%s", stderr.String())
			return err
		}
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

	ws, err := workspace.Path()
	if err != nil {
		return "", err
	}
	return filepath.Join(ws, p.config.Path), nil
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
// See cli/example_plugins/ping_remote/pre_bazel.sh for an example.
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
	// TODO: if pre_bazel.sh is not executable and does not contain a shebang
	// line, wrap it with "/usr/bin/env bash"
	// TODO: support "pre_bazel.<any-extension>" as long as the file is
	// executable and has a shebang line
	cmd := exec.Command(scriptPath, argsFile.Name())
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
	return newArgs, nil
}

// PostBazel executes the plugin's post-bazel hook if it exists, allowing it to
// respond to the result of the invocation.
//
// Currently the invocation data is fed as plain text via a file. The file path
// is passed as the first argument.
//
// See cli/example_plugins/go_deps/post_bazel.sh for an example.
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
	cmd := exec.Command(scriptPath, bazelOutputPath)
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
// See cli/example_plugins/go_highlight/handle_bazel_output.sh for an example.
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
	cmd := exec.Command(scriptPath)
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
	cmd.Env = p.commandEnv()
	go func() {
		// Copy pty output to the next pipeline stage.
		io.Copy(pw, ptmx)
	}()
	go func() {
		defer tty.Close()
		defer ptmx.Close()
		defer pw.Close()
		log.Debugf("Running bazel output handler for %s/%s", p.config.Repo, p.config.Path)
		if err := cmd.Run(); err != nil {
			log.Debugf("Command failed: %s", err)
		} else {
			log.Debugf("Command %s completed", cmd.Args)
		}
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

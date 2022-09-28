package plugin

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"gopkg.in/yaml.v2"
)

const (
	// Path where we expect to find the plugin configuration, relative to the
	// root of the Bazel workspace in which the CLI is invoked.
	configPath = "buildbuddy.yaml"
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
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

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
}

// LoadAll loads all plugins from the config, and ensures that any remote
// plugins are downloaded.
func LoadAll() ([]*Plugin, error) {
	cfg, err := readConfig()
	if err != nil {
		return nil, err
	}
	plugins := make([]*Plugin, 0, len(cfg.Plugins))
	for _, p := range cfg.Plugins {
		plugin := &Plugin{config: p}
		if err := plugin.load(); err != nil {
			return nil, err
		}
		plugins = append(plugins, plugin)
	}
	return plugins, nil
}

// load downloads the plugin from the specified repo, if applicable.
func (p *Plugin) load() error {
	if p.config.Repo == "" {
		return nil
	}
	return status.UnimplementedError("repository plugins are not yet implemented")
}

// Path returns the root path of the plugin.
func (p *Plugin) Path() string {
	// TODO: If remote, join config path with fetched repo path.
	return p.config.Path
}

// PreBazel executes the plugin's pre-bazel hook if it exists, allowing the
// plugin to return a set of transformed bazel arguments.
//
// Plugins are currently expected to output the new arguments on stdout. To
// simplify handling of whitespace, each line is expected to contain exactly one
// argument.
//
// Example plugin that automatically disables remote cache if it takes longer
// than 1s to ping buildbuddy:
//
//     pre_bazel.sh
//       #!/usr/bin/env bash
//       # echo original args, one per line
//       for arg in "$@"; do
//         echo "$arg"
//       done
//       if ! timeout 1 ping -c1 remote.buildbuddy.io; then
//           # Network is spotty; don't use cache
//           echo "--remote_cache="
//       fi
func (p *Plugin) PreBazel(args []string) ([]string, error) {
	scriptPath := filepath.Join(p.Path(), "pre_bazel.sh")
	exists, err := disk.FileExists(context.TODO(), scriptPath)
	if err != nil {
		return nil, err
	}
	if !exists {
		return args, nil
	}
	// TODO: if pre_bazel.sh is not executable and does not contain a shebang
	// line, wrap it with "/usr/bin/env bash"
	// TODO: support "pre_bazel.<any-extension>" as long as the file is
	// executable and has a shebang line
	buf := &bytes.Buffer{}
	cmd := exec.Command(scriptPath, args...)
	// TODO: Prefix stderr output with "output from [plugin]" ?
	cmd.Stderr = os.Stderr
	cmd.Stdout = buf
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return strings.Split(buf.String(), "\n"), nil
}

// PostBazel executes the plugin's post-bazel hook if it exists, allowing it to
// respond to the result of the invocation.
//
// Currently the invocation data is fed as plain text via a file. The file path
// is passed as the first argument.
//
// Example plugin that highlights lines containing source locations (like
// "/path/to/foo.go:23:42: undefined variable") so they are easier to visually
// spot in the build output:
//
//     post_bazel.sh
//       #!/usr/bin/env bash
//       perl -nle 'if (/^(.*\.\w+:\d+:\d+:)(.*)/) {
//         print "\x1b[33m" . $1 . "\x1b[m" . $2
//       }' < "$1"
func (p *Plugin) PostBazel(bazelOutputPath string) error {
	scriptPath := filepath.Join(p.Path(), "post_bazel.sh")
	exists, err := disk.FileExists(context.TODO(), scriptPath)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	cmd := exec.Command(scriptPath, bazelOutputPath)
	// TODO: Prefix stderr output with "output from [plugin]" ?
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	return cmd.Run()
}

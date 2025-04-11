package config

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/docker/go-units"

	yaml "gopkg.in/yaml.v2"
)

const (
	// Path where we expect to find the plugin configuration, relative to the
	// root of the Bazel workspace in which the CLI is invoked.
	WorkspaceRelativeConfigPath = "buildbuddy.yaml"

	// Path where we expect to find the user's plugin configuration, relative
	// to the user's home directory.
	HomeRelativeUserConfigPath = "buildbuddy.yaml"

	// Environment variable that is set to "1" if we are a sidecar.
	BbIsSidecar = "_BB_IS_SIDECAR"
)

// File represents a decoded config file along with its metadata.
type File struct {
	Path string
	*RootConfig
}

// RootConfig is the top-level config object in buildbuddy.yaml
type RootConfig struct {
	Plugins    []*PluginConfig   `yaml:"plugins,omitempty"`
	LocalCache *LocalCacheConfig `yaml:"local_cache,omitempty"`
}

type PluginConfig struct {
	// Repo where the plugin should be loaded from.
	// If empty, use the local workspace.
	Repo string `yaml:"repo,omitempty"`

	// Path relative to the repo where the plugin is defined.
	// Optional. If unspecified, it behaves the same as "." (the repo root).
	Path string `yaml:"path,omitempty"`
}

type LocalCacheConfig struct {
	// Enabled specifies whether the local cache is enabled.
	// Defaults to true.
	Enabled *bool `yaml:"enabled,omitempty"`

	// MaxSize is the max local cache size.
	// This can be a number of bytes, a size like 100GB, or a percentage of the
	// filesystem capacity where the cache directory resides, like '20%'.
	MaxSize any `yaml:"max_size,omitempty"`

	// RootDirectory is the local cache root directory.
	// Environment variables like ${HOME} are expanded.
	RootDirectory string `yaml:"root_directory,omitempty"`
}

func loadWorkspaceConfig(workspaceDir string) (*File, error) {
	return LoadFile(filepath.Join(workspaceDir, WorkspaceRelativeConfigPath))
}

func loadUserConfig() (*File, error) {
	homeDir := os.Getenv("HOME")
	if homeDir == "" {
		log.Debugf("not reading ~/%s: $HOME environment variable is not set", HomeRelativeUserConfigPath)
		return nil, nil
	}
	return LoadFile(filepath.Join(homeDir, HomeRelativeUserConfigPath))
}

// LoadFile loads a single buildbuddy.yaml from the given path.
func LoadFile(path string) (*File, error) {
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
	cfg := &RootConfig{}

	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	s := os.ExpandEnv(string(b))
	// Decode YAML but ignore EOF errors, which happen when the file is empty.
	if err := yaml.NewDecoder(strings.NewReader(s)).Decode(cfg); err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to parse %s: %w", f.Name(), err)
	}
	return &File{Path: path, RootConfig: cfg}, nil
}

// LoadAllFiles returns a list of parsed buildbuddy.yaml files from which to
// load plugins, in increasing order of precedence.
//
// TODO: only return a single "merged" config here.
func LoadAllFiles(workspaceDir string) ([]*File, error) {
	var configs []*File
	cfg, err := loadUserConfig()
	if err != nil {
		return nil, err
	}
	if cfg != nil {
		configs = append(configs, cfg)
	}
	cfg, err = loadWorkspaceConfig(workspaceDir)
	if err != nil {
		return nil, err
	}
	if cfg != nil {
		configs = append(configs, cfg)
	}
	return configs, nil
}

// ParseDiskCapacityBytes parses a YAML value to a size in bytes:
//   - Integers are treated as a number of bytes, and returned directly
//   - Strings like "50%" are parsed as a percentage of the capacity
//     of the disk where the given directory is located
//   - Strings like "50GB" are expanded to the corresponding byte count,
//     in bytes, kibibytes, mebibytes, gibibytes, or tebibytes
//     (powers of 1024).
func ParseDiskCapacityBytes(size any, directory string) (int64, error) {
	switch v := size.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case string:
		if strings.HasSuffix(v, "%") {
			percentage, err := strconv.Atoi(strings.TrimSuffix(v, "%"))
			if err != nil {
				return 0, fmt.Errorf("parse percentage as int: %w", err)
			}
			stat := new(syscall.Statfs_t)
			err = syscall.Statfs(directory, stat)
			if err != nil {
				return 0, fmt.Errorf("statfs %q: %w", directory, err)
			}
			diskSize := stat.Blocks * uint64(stat.Bsize)
			return int64(diskSize * uint64(percentage) / 100), nil
		}
		bytes, err := units.RAMInBytes(v)
		if err != nil {
			return 0, err
		}
		return bytes, nil
	default:
		return 0, fmt.Errorf("invalid type %T", size)
	}
}

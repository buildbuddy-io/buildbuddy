package config

import (
	"flag"

	"github.com/buildbuddy-io/buildbuddy/server/util/file"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

const pathFlagName = "config_file"

var (
	configPath = flag.String(pathFlagName, "/config.yaml", "The path to a buildbuddy config file")

	// If you wish to extract a section of the config to the Config struct as opposed to parsing into flags:
	// 1. Set the section name in configKeysToExtract so the flag parser will know to ignore it
	// 2. Define the section in the Config struct using `yaml` tags
	configKeysToExtract = []string{"migration"}
)

type Config struct {
	CacheBlock *CacheBlock `yaml:"cache"`
}

type CacheBlock struct {
	MigrationConfig *MigrationConfig `yaml:"migration"`
}

func init() {
	// As this flag determines the YAML file we read the config from, it can't
	// meaningfully be specified in the YAML config file.
	flagyaml.IgnoreFlagForYAML(pathFlagName)

	for _, key := range configKeysToExtract {
		flagyaml.IgnoreKeyForYAML(key)
	}
}

func Path() string {
	return *configPath
}

// ParseConfig parses a config file
// There are two ways to extract data from the config file:
// 1. By default, this function populates flags with the config data
// 2. For fields defined in the Config struct, it also returns a parsed Config object
func ParseConfig(configPath string) (*Config, error) {
	log.Infof("Reading buildbuddy config from '%s'", configPath)
	fileBytes, err := file.ReadFile(configPath)
	if err != nil {
		// If the file does not exist then skip it.
		if status.IsNotFoundError(err) {
			log.Warningf("No config file found at %s.", configPath)
			return nil, nil
		}
		return nil, err
	}

	if err := flagyaml.PopulateFlagsFromData(fileBytes); err != nil {
		return nil, errors.WithMessagef(err, "error populating flags from config file %s", configPath)
	}
	flagyaml.RereadFlagsOnDisconnect(configPath)

	cfg := &Config{}
	if err = yaml.Unmarshal(fileBytes, cfg); err != nil {
		return nil, errors.WithMessagef(err, "error unmarshaling config file %s to config struct", configPath)
	}
	return cfg, nil
}

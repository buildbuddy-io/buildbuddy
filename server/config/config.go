package config

import (
	"flag"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

const pathFlagName = "config_file"

var configPath = flag.String(pathFlagName, "/config.yaml", "The path to a buildbuddy config file")

func init() {
	// As this flag determines the YAML file we read the config from, it can't
	// meaningfully be specified in the YAML config file.
	flagutil.IgnoreFlagForYAML(pathFlagName)
}

func Path() string {
	return *configPath
}

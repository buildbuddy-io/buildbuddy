package config

import (
	"flag"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

const pathFlagName = "config_file"

var configPath = flag.String(pathFlagName, "/config.yaml", "The path to a buildbuddy config file")

func init() {
	flagutil.IgnoreFlag(pathFlagName)
}

func Path() string {
	return *configPath
}

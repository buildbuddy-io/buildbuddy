package config

import (
	"flag"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/signalutil"

	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

const pathFlagName = "config_file"

var configPath = flag.String(pathFlagName, "/config.yaml", "The path to a buildbuddy config file")

func init() {
	// As this flag determines the YAML file we read the config from, it can't
	// meaningfully be specified in the YAML config file.
	flagyaml.IgnoreFlagForYAML(pathFlagName)
}

func Path() string {
	return *configPath
}

// Load parses the flags and loads the config file specified by config.Path().
func Load() error {
	flag.Parse()
	return flagyaml.PopulateFlagsFromFile(Path())
}

// Reload resets the flags to their default values, re-parses the flags and
// loads the config file specified by config.Path().
func Reload() error {
	flagutil.ResetFlags()
	return Load()
}

// ReloadOnSIGHUP registers a signal handler for syscall.SIGHUP that resets the
// flags to their default values, re-parses the flags and loads the config file
// specified by config.Path().
func ReloadOnSIGHUP() {
	// Setup a listener to re-read the config file on SIGHUP.
	signalutil.OnSignal(
		syscall.SIGHUP,
		func() error {
			log.Infof("Re-reading buildbuddy config from '%s'", Path())
			return Reload()
		},
		func(err error) { log.Warningf("SIGHUP handler err: %s", err) },
	)
}

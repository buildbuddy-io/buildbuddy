package config

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

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

// ReloadOnSIGHUP registers a signal handler (as a goroutine) for syscall.SIGHUP
// that resets the flags to their default values, re-parses the flags and loads
// the config file specified by config.Path(). This function is only intended to
// be called once per process (probably near the top of main), as it otherwise
// could leak goroutines.
func ReloadOnSIGHUP() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)

	// Setup a listener to re-read the config file on SIGHUP.
	go func() {
		for range c {
			log.Infof("Re-reading buildbuddy config from '%s'", Path())
			if err := Reload(); err != nil {
				log.Warningf("SIGHUP handler err: %s", err)
			}
		}
	}()
}

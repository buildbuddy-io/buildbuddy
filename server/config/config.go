package config

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

const (
	// The placeholder prefix we look for to identify external secret references
	// when parsing config file. Any placeholders in format ${SECRET:foo} will
	// be replaced with the resolved content from the external secret store.
	externalSecretPrefix = "SECRET:"
)

// As this flag determines the YAML file we read the config from, it can't
// meaningfully be specified in the YAML config file.
var (
	configPath = flag.String("config_file", "/config.yaml", "The path to a buildbuddy config file", flag.YAMLIgnore)

	// This may be optionally set by a configured provider.
	SecretProvider interfaces.ConfigSecretProvider
)

func Path() string {
	return *configPath
}

func expandStringValue(value string) (string, error) {
	ctx := context.Background()
	var expandErr error
	expandedValue := os.Expand(value, func(s string) string {
		if strings.HasPrefix(s, externalSecretPrefix) {
			if SecretProvider == nil {
				expandErr = status.UnavailableError("config references an external secret but no secret provider is available")
			} else {
				name := strings.TrimPrefix(s, externalSecretPrefix)
				secret, err := SecretProvider.GetSecret(ctx, name)
				if err != nil {
					expandErr = status.UnavailableErrorf("could not retrieve config secret %q: %s", name, err)
				}
				return string(secret)
			}
		}
		return os.Getenv(s)
	})
	if expandErr != nil {
		return "", expandErr
	}
	return expandedValue, nil
}

// LoadFromFile parses the flags and loads the config from a string.
func LoadFromData(data string) error {
	if err := flagyaml.PopulateFlagsFromData(data); err != nil {
		return err
	}
	var lastErr error
	common.DefaultFlagSet.VisitAll(func(f *flag.Flag) {
		if err := types.Expand(f.Value, expandStringValue); err != nil {
			lastErr = err
		}
	})
	if lastErr != nil {
		return lastErr
	}
	return nil
}

// LoadFromFile parses the flags and loads the config file specified by
// configFile.
func LoadFromFile(configFile string) error {
	fileBytes, err := os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("error reading config file: %s", err)
	}
	return LoadFromData(string(fileBytes))
}

// Load parses the flags and loads the config file specified by config.Path().
func Load() error {
	configFile := Path()

	log.Infof("Reading buildbuddy config from '%s'", configFile)

	_, err := os.Stat(configFile)

	// If the file does not exist then skip it.
	if os.IsNotExist(err) {
		log.Warningf("No config file found at %s.", configFile)
		return nil
	}

	return LoadFromFile(configFile)
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

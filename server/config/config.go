package config

import (
	"flag"
	"fmt"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	configFile = flag.String("config_file", "/config.yaml", "The path to a buildbuddy config file")
)

func PopulateFlagsFromData(data []byte) error {
	return flagutil.PopulateFlagsFromData(data)
}

func PopulateFlagsFromFile() error {
	log.Infof("Reading buildbuddy config from '%s'", *configFile)

	_, err := os.Stat(*configFile)

	// If the file does not exist then skip it.
	if os.IsNotExist(err) {
		log.Warningf("No config file found at %s.", *configFile)
		return nil
	}

	fileBytes, err := os.ReadFile(*configFile)
	if err != nil {
		return fmt.Errorf("Error reading config file: %s", err)
	}

	return PopulateFlagsFromData(fileBytes)
}

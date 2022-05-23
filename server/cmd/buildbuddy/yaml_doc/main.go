package main

import (
	"flag"
	"log"
	"os"

	_ "github.com/buildbuddy-io/buildbuddy/server/cmd/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
)

const flagName = "yaml_documented_defaults_out_file"

var yamlDefaultsOutFile = flag.String(flagName, "buildbuddy_server_documented_defaults.yaml", "Path to a file to write the default YAML config (with docs) to.")

func init() {
	flagutil.IgnoreFlagForYAML(flagName)
}

func main() {
	flag.Parse()

	b, err := flagutil.SplitDocumentedYAMLFromFlags()
	if err != nil {
		log.Fatalf("Encountered error generating documented default YAML file: %s", err)
	}

	if err := os.WriteFile(*yamlDefaultsOutFile, b, 0644); err != nil {
		log.Fatalf("Encountered error writing the documented default YAML file: %s", err)
	}
}

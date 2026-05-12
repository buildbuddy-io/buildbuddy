// Package version provides access to the BuildBuddy CLI version string. The
// version is embedded at build time from a Bazel flag and is used throughout
// the CLI for version reporting, tool tagging, and update checking.
package version

import (
	_ "embed"
)

// The bb cli "version" var is generated in this package according to the
// Bazel flag value --//cli/version:cli_version
//
//go:embed version_flag.txt
var cliVersionFlag string

func String() string {
	return cliVersionFlag
}

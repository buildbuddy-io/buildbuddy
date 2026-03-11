package version

import (
	_ "embed"
)

const unknownValue = "unknown"

// The bb cli "version" var is generated in this package according to the
// Bazel flag value --//cli/version:cli_version
//
//go:embed version_flag.txt
var cliVersionFlag string

// Fallback version derived from the stamped workspace status.
//
//go:embed stamped_version.txt
var stampedVersion string

func String() string {
	if cliVersionFlag != "" && cliVersionFlag != unknownValue {
		return cliVersionFlag
	}
	if stampedVersion != "" && stampedVersion != "{STABLE_VERSION_TAG}" {
		return stampedVersion
	}
	return unknownValue
}

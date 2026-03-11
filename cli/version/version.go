package version

import (
	_ "embed"
	"strings"
)

const unknownValue = "unknown"
const stampedUnknownValue = "{STABLE_CLI_VERSION}"

// The bb CLI version is populated either from the Bazel flag
// --//cli/version:cli_version or, in stamped builds, from STABLE_CLI_VERSION.
//
//go:embed version.txt
var cliVersion string

func String() string {
	v := strings.TrimSpace(cliVersion)
	if v == "" || v == stampedUnknownValue {
		return unknownValue
	}
	return v
}

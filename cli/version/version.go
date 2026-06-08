package version

import (
	_ "embed"
)

// This is populated by the populate_version genrule.
//
//go:embed version.txt
var versionTag string

func String() string {
	if versionTag == "" {
		return "unknown"
	}
	return versionTag
}

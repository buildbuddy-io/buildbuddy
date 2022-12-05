package version

import (
	"fmt"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

const (
	unknownValue = "unknown"
)

// This is set by x_defs in the BUILD file.
//
//	x_defs = {
//	    "commitSha": "{COMMIT_SHA}",
//	},
var commitSha string

func Print() {
	appVersion := fmt.Sprintf("BuildBuddy %s", AppVersion())
	if commitHash := Commit(); commitHash != unknownValue {
		appVersion = fmt.Sprintf("%s (%s)", appVersion, commitHash)
	}
	log.Infof("%s compiled with %s", appVersion, GoVersion())
}

func AppVersion() string {
	if versionTag != "" {
		return versionTag
	}
	return unknownValue
}

func GoVersion() string {
	return runtime.Version()
}

func Commit() string {
	if commitSha != "" && commitSha != "{COMMIT_SHA}" {
		return commitSha
	}
	return unknownValue
}

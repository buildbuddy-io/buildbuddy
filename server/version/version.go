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
var commitSha string
var versionTag string

func Print() {
	appVersion := fmt.Sprintf("BuildBuddy %s", AppVersion())
	if commitHash := Commit(); commitHash != unknownValue {
		appVersion = fmt.Sprintf("%s (%s)", appVersion, commitHash)
	}
	log.Infof("%s compiled with %s", appVersion, GoVersion())
}

func AppVersion() string {
	if versionTag != "" && versionTag != "{STABLE_VERSION_TAG}" {
		return versionTag
	}
	return unknownValue
}

func GoVersion() string {
	return runtime.Version()
}

func Commit() string {
	if commitSha != "" && commitSha != "{STABLE_COMMIT_SHA}" {
		return commitSha
	}
	return unknownValue
}

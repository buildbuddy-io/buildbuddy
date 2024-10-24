package version

import (
	_ "embed"
	"fmt"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	unknownValue = "unknown"
)

// The text files are populated by the populate_* genrules
//
//go:embed commit.txt
var commitSha string

//go:embed version.txt
var versionTag string

func init() {
	metrics.Version.With(prometheus.Labels{
		metrics.VersionLabel: Tag(),
		metrics.CommitLabel:  Commit(),
	}).Set(1)
}

func Print(name string) {
	appVersion := fmt.Sprintf("%s %s", name, Tag())
	if commitHash := Commit(); commitHash != unknownValue {
		appVersion = fmt.Sprintf("%s (%s)", appVersion, commitHash)
	}
	log.Infof("%s compiled with %s", appVersion, GoVersion())
}

func Tag() string {
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

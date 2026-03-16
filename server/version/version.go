package version

import (
	_ "embed"
	"fmt"
	"os"
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
//go:embed version.txt
var versionTag string

func init() {
	labels := prometheus.Labels{
		metrics.VersionLabel: MetricVersion(),
		metrics.CommitLabel:  MetricCommit(),
	}
	metrics.Version.With(labels).Set(1)
	metrics.BuildInfo.With(labels).Set(1)
}

func Print(name string) {
	appVersion := fmt.Sprintf("%s %s", name, MetricVersion())
	if commitHash := MetricCommit(); commitHash != unknownValue {
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
	return getenvOrUnknown("BUILD_COMMIT_SHA", "COMMIT_SHA")
}

func MetricVersion() string {
	return getenvOrDefault(Tag(), "BUILD_VERSION", "VERSION")
}

func MetricCommit() string {
	return getenvOrDefault(Commit(), "BUILD_COMMIT_SHA", "COMMIT_SHA")
}

func getenvOrDefault(fallback string, keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	if fallback != "" {
		return fallback
	}
	return unknownValue
}

func getenvOrUnknown(keys ...string) string {
	return getenvOrDefault(unknownValue, keys...)
}

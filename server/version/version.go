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

//go:embed latest_public_release.txt
var latestPublicReleaseTag string

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

// LatestPublicRelease returns the latest public release (vX.Y.Z) that this
// binary's code contains. Builds that don't stamp it separately are public
// builds, whose version tag is that release.
func LatestPublicRelease() string {
	return selectLatestPublicRelease(latestPublicReleaseTag, Tag())
}

func selectLatestPublicRelease(stamped, tag string) string {
	if stamped != "" && stamped != "{STABLE_LATEST_PUBLIC_RELEASE}" {
		return stamped
	}
	return tag
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

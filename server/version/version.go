package version

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	bundle "github.com/buildbuddy-io/buildbuddy"
)

const (
	unknownValue    = "unknown"
	versionFilename = "VERSION"
)

// This is set by x_defs in the BUILD file.
//    x_defs = {
//        "version": "{VERSION}",
//        "commitSha": "{COMMIT_SHA}",
//    },
var commitSha string
var version string

func Print() {
	appVersion := fmt.Sprintf("BuildBuddy %s", AppVersion())
	if commitHash := Commit(); commitHash != unknownValue {
		appVersion = fmt.Sprintf("%s (%s)", appVersion, commitHash)
	}
	log.Infof("%s compiled with %s", appVersion, GoVersion())
}

func AppVersion() string {
	if commitSha != "{VERSION}" {
		return commitSha
	}
	return unknownValue
}

func GoVersion() string {
	return runtime.Version()
}

func Commit() string {
	if commitSha != "{COMMIT_SHA}" {
		return commitSha
	}
	return unknownValue
}

package version

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
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
	if version != "" && version != "{VERSION}" {
		return version
	}
	var versionBytes []byte
	if rfp, err := bazel.RunfilesPath(); err == nil {
		versionFile := filepath.Join(rfp, versionFilename)
		if b, err := os.ReadFile(versionFile); err == nil {
			versionBytes = b
		}
	}
	if versionBytes != nil {
		return strings.TrimSpace(string(versionBytes))
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

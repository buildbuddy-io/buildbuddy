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
//        "commitSha": "{COMMIT_SHA}",
//    },
var commitSha string

func Print() {
	appVersion := fmt.Sprintf("BuildBuddy %s", AppVersion())
	if commitHash := Commit(); commitHash != unknownValue {
		appVersion = fmt.Sprintf("%s (%s)", appVersion, commitHash)
	}
	log.Infof("%s compiled with %s", appVersion, GoVersion())
}

func AppVersion(in ...[]byte) string {
	var versionBytes []byte
	if len(in) > 0 {
		versionBytes = in[0]
	} else if rfp, err := bazel.RunfilesPath(); err == nil {
		versionFile := filepath.Join(rfp, versionFilename)
		if b, err := os.ReadFile(versionFile); err == nil {
			versionBytes = b
		}
	} else {
		if bundleFS, err := bundle.Get(); err == nil {
			if data, err := fs.ReadFile(bundleFS, versionFilename); err == nil {
				versionBytes = data
			}
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
	if commitSha != "{COMMIT_SHA}" {
		return commitSha
	}
	return unknownValue
}

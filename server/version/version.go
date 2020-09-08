package version

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
)

const (
	unknownValue    = "unknown"
	versionFilename = "VERSION"
)

var commitSha string

func init() {
	appVersion := fmt.Sprintf("BuildBuddy %s", AppVersion())
	if commitHash := Commit(); commitHash != unknownValue {
		appVersion = fmt.Sprintf("%s (%s)", appVersion, commitHash)
	}
	log.Printf("%s compiled with go: %s", appVersion, GoVersion())
}

func AppVersion() string {
	if rfp, err := bazel.RunfilesPath(); err == nil {
		versionFile := filepath.Join(rfp, versionFilename)
		if versionBytes, err := ioutil.ReadFile(versionFile); err == nil {
			return strings.TrimSpace(string(versionBytes))
		}
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

package version

import (
	"context"
	"fmt"
	"runtime"
	"strings"

	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/filepath"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/os"
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

func Print(ctx context.Context) {
	appVersion := fmt.Sprintf("BuildBuddy %s", AppVersion(ctx))
	if commitHash := Commit(); commitHash != unknownValue {
		appVersion = fmt.Sprintf("%s (%s)", appVersion, commitHash)
	}
	log.Infof("%s compiled with %s", appVersion, GoVersion())
}

func AppVersion(ctx context.Context) string {
	if version != "" && version != "{VERSION}" {
		return version
	}
	var versionBytes []byte
	if rfp, err := bazel.RunfilesPath(); err == nil {
		versionFile := filepath.Join(rfp, versionFilename)
		if b, err := os.ReadFile(ctx, versionFile); err == nil {
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

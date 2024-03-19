package workspace

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
)

const (
	WorkspaceFileName    = "WORKSPACE"
	WorkspaceAltFileName = "WORKSPACE.bazel"

	ModuleFileName          = "MODULE.bazel"
	ModuleWorkspaceFileName = "WORKSPACE.bzlmod"
)

var (
	pathOnce sync.Once
	pathVal  string
	pathErr  error
	basename string

	WorkspaceIndicatorFiles = []string{WorkspaceFileName, WorkspaceAltFileName, ModuleFileName}
)

// Path returns the current Bazel workspace path by traversing upwards until
// we see a WORKSPACE, WORKSPACE.bazel or MODULE.bazel file.
func Path() (string, error) {
	pathOnce.Do(func() {
		pathVal, basename, pathErr = path()
	})
	return pathVal, pathErr
}

func PathAndBasename() (string, string, error) {
	_, _ = Path()
	return pathVal, basename, pathErr
}

func path() (string, string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", "", err
	}
	for {
		for _, file := range WorkspaceIndicatorFiles {
			ex, err := disk.FileExists(context.TODO(), filepath.Join(dir, file))
			if err != nil {
				return "", "", err
			}
			if ex {
				return dir, file, nil
			}
		}
		next := filepath.Dir(dir)
		if dir == next {
			// We've reached the root dir without finding a WORKSPACE file
			return "", "", fmt.Errorf("not within a bazel workspace (could not find WORKSPACE, WORKSPACE.bazel, MODULE, or MODULE.bazel file)")
		}
		dir = next
	}
}

// TODO: Take workspace dir as a param everywhere so that this isn't needed.
func SetForTest(path string) {
	_, _ = Path()
	pathVal, pathErr = path, nil
}

func CreateWorkspaceIfNotExists() (string, string, error) {
	log.Debugf("Checking if workspace file exists")
	path, base, err := PathAndBasename()
	if err == nil {
		return path, base, nil
	}

	useModules, err := useModules()
	if err != nil {
		return "", "", err
	}

	fileName := ModuleFileName
	if !useModules {
		fileName = WorkspaceFileName // gazelle doesn't like WORKSPACE.bazel...
	}

	log.Debugf("Creating %s file", fileName)

	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return "", "", err
	}
	defer f.Close()
	workspacePath, err := os.Getwd()
	if err != nil {
		return "", "", err
	}
	contents := ""
	if useModules {
		contents = `module(name = "` + filepath.Base(workspacePath) + `")` + "\n"
	} else {
		contents = `workspace(name = "` + filepath.Base(workspacePath) + `")` + "\n"
	}
	if _, err := f.WriteString(contents); err != nil {
		return "", "", err
	}
	pathVal = workspacePath
	basename = fileName
	pathErr = nil
	log.Debugf("Created %s file at %s/%s", fileName, pathVal, basename)
	return pathVal, basename, nil
}

func useModules() (bool, error) {
	bazelArgs := []string{"info", "release", "starlark-semantics"}
	stdout := &bytes.Buffer{}
	opts := &bazelisk.RunOpts{
		Stdout: stdout,
	}
	_, err := bazelisk.Run(bazelArgs, opts)
	if err != nil {
		return false, fmt.Errorf("could not run bazel info: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	var release, starlarkSemantics string
	for _, line := range lines {
		halves := strings.Split(line, ": ")
		if halves[0] == "release" {
			release = halves[1]
			continue
		}
		if halves[0] == "starlark-semantics" {
			starlarkSemantics = halves[1]
		}
	}

	// It's possible that bzlmod is enabled/disabled explicitly inside a bazelrc file.
	// If that's the case, starlark-semantics should show the status if the value is not the default value
	if strings.Contains(starlarkSemantics, "enable_bzlmod=true") {
		return true, nil
	}
	if strings.Contains(starlarkSemantics, "enable_bzlmod=false") {
		return false, nil
	}

	version := strings.TrimLeft(release, "release ")
	versionParts := strings.Split(version, ".")
	majorVersion, err := strconv.Atoi(versionParts[0])
	if err != nil {
		return false, fmt.Errorf("could not parse Bazel release version: %v", err)
	}

	// From Bazel 7 on-ward, bzlmod is enabled by default and will not be shown
	// in starlark-semantics setting.
	if majorVersion <= 6 {
		// If current version is before Bazel 7, then bzlmod is disabled by default.
		return false, nil
	}
	return true, nil
}

func GetBuildFileContents(dir string) (string, string) {
	for _, basename := range []string{"BUILD", "BUILD.bazel"} {
		path := filepath.Join(dir, basename)
		info, err := os.Stat(path)
		if err != nil || info.IsDir() {
			continue
		}
		bytes, err := os.ReadFile(path)
		if err == nil {
			return string(bytes), path
		}
	}
	return "", filepath.Join(dir, "BUILD.bazel")
}

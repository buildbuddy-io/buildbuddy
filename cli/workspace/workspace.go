package workspace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

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

func CreateWorkspaceIfNotExists(bzlmodEnabled bool) (string, string, error) {
	log.Debugf("Checking if workspace file exists")
	path, base, err := PathAndBasename()
	if err == nil {
		return path, base, nil
	}

	fileName := ModuleFileName
	if !bzlmodEnabled {
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
	if bzlmodEnabled {
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

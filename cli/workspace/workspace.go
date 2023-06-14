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

var (
	pathOnce sync.Once
	pathVal  string
	pathErr  error
	basename string
)

// Path returns the current Bazel workspace path by traversing upwards until
// we see a WORKSPACE or WORKSPACE.bazel file.
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
		for _, basename := range []string{"WORKSPACE", "WORKSPACE.bazel"} {
			ex, err := disk.FileExists(context.TODO(), filepath.Join(dir, basename))
			if err != nil {
				return "", "", err
			}
			if ex {
				return dir, basename, nil
			}
		}
		next := filepath.Dir(dir)
		if dir == next {
			// We've reached the root dir without finding a WORKSPACE file
			return "", "", fmt.Errorf("not within a bazel workspace (could not find WORKSPACE or WORKSPACE.bazel file)")
		}
		dir = next
	}
}

// TODO: Take workspace dir as a param everywhere so that this isn't needed.
func SetForTest(path string) {
	_, _ = Path()
	pathVal, pathErr = path, nil
}

func CreateWorkspaceFileIfNotExists() (string, string, error) {
	log.Debugf("Checking if workspace file exists")
	path, base, err := PathAndBasename()
	if err == nil {
		return path, base, nil
	}
	log.Debugf("Creating workspace file")
	fileName := "WORKSPACE" // gazelle doesn't like WORKSPACE.bazel...
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return "", "", err
	}
	defer f.Close()
	workspacePath, err := os.Getwd()
	if err != nil {
		return "", "", err
	}
	if _, err := f.WriteString(`workspace(name = "` + filepath.Base(workspacePath) + `")` + "\n"); err != nil {
		return "", "", err
	}
	pathVal = workspacePath
	basename = fileName
	pathErr = nil
	log.Debugf("Created workspace file at %s/%s", pathVal, basename)
	return pathVal, basename, nil
}

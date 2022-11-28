package workspace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
)

var (
	pathOnce sync.Once
	pathVal  string
	pathErr  error
)

// Path returns the current Bazel workspace path by traversing upwards until
// we see a WORKSPACE or WORKSPACE.bazel file.
func Path() (string, error) {
	pathOnce.Do(func() {
		pathVal, pathErr = path()
	})
	return pathVal, pathErr
}

func path() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		for _, basename := range []string{"WORKSPACE", "WORKSPACE.bazel"} {
			ex, err := disk.FileExists(context.TODO(), filepath.Join(dir, basename))
			if err != nil {
				return "", err
			}
			if ex {
				return dir, nil
			}
		}
		next := filepath.Dir(dir)
		if dir == next {
			// We've reached the root dir without finding a WORKSPACE file
			return "", fmt.Errorf("not within a bazel workspace (could not find WORKSPACE or WORKSPACE.bazel file)")
		}
		dir = next
	}
}

// TODO: Take workspace dir as a param everywhere so that this isn't needed.
func SetForTest(path string) {
	_, _ = Path()
	pathVal, pathErr = path, nil
}

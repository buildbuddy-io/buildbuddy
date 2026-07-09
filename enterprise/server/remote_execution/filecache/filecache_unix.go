//go:build (linux && !android) || (darwin && !ios)

package filecache

import (
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// syncDir fsyncs the directory at path to ensure any pending metadata changes
// (e.g. file creation or deletion) are durable on disk.
func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	start := time.Now()
	err = dir.Sync()
	log.Infof("filecache: syncDir(%q) took %s (err: %v)", path, time.Since(start), err)
	return err
}

func syncLockFileCreation(path string) error {
	return syncDir(path)
}

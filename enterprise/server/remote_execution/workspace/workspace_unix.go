//go:build !windows

package workspace

import (
	"github.com/google/uuid"
)

func newRandomBuildDirCandidate() string {
	return uuid.Must(uuid.NewRandom()).String()
}

func maybeCreatePlatformSpecificSubDir(dir string) (string, error) {
	return dir, nil
}

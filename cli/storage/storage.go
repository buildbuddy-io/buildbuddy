package storage

import (
	"os"
	"path/filepath"
)

func Dir() (string, error) {
	// Make sure we have a home directory to work in.
	bbHome := os.Getenv("BUILDBUDDY_HOME")
	if len(bbHome) == 0 {
		userCacheDir, err := os.UserCacheDir()
		if err != nil {
			return "", err
		}
		bbHome = filepath.Join(userCacheDir, "buildbuddy")
	}
	if err := os.MkdirAll(bbHome, 0755); err != nil {
		return "", err
	}
	return bbHome, nil
}

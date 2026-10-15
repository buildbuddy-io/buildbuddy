//go:build !linux || android

package workspace

import (
	"errors"
)

func lstatDir(path string) (dirFingerprint, bool, error) {
	return dirFingerprint{}, false, errors.ErrUnsupported
}

func waitForTimestampTick(t int64) error {
	return errors.ErrUnsupported
}

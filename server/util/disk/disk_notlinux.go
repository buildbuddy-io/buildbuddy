//go:build !linux || android

package disk

import "os"

// openAnonymousTmpFile is a stub for platforms without O_TMPFILE support.
// It always returns ok=false so callers fall back to a named temp file.
func openAnonymousTmpFile(dir string) (*os.File, bool, error) {
	return nil, false, nil
}

// linkAnonymousTmpFile is unreachable on platforms without O_TMPFILE support
// because openAnonymousTmpFile always returns ok=false.
func linkAnonymousTmpFile(f *os.File, linkPath string) error {
	return os.ErrInvalid
}

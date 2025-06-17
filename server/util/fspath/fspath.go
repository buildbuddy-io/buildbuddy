package fspath

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// Key is a representation of a filesystem path suitable for use as a map key.
// It properly handles case-insensitive filesystems, in which paths with
// different cases should be considered equal.
type Key struct {
	value string
}

// NewKey returns a new Key for the given path.
func NewKey(path string, isCaseInsensitive bool) Key {
	if isCaseInsensitive {
		return Key{value: strings.ToLower(path)}
	}
	return Key{value: path}
}

// NormalizedString returns a string representation of the key that does not
// necessarily match the original path.
func (k *Key) NormalizedString() string {
	return k.value
}

// IsParent returns true if c is a child of the parent.
func IsParent(parent, c string, isCaseInsensitive bool) bool {
	parent = filepath.Clean(parent)
	c = filepath.Clean(c)
	if isCaseInsensitive {
		parent = strings.ToLower(parent)
		c = strings.ToLower(c)
	}
	return strings.HasPrefix(c, parent+string(os.PathSeparator))
}

// IsCaseInsensitiveFS returns true if the filesystem containing the given
// directory path ignores case differences when checking whether file paths are
// equal.
//
// It uses the given directory to create a file in order to test for case
// sensitivity. The directory must exist and must be writable, or an error will
// be returned.
func IsCaseInsensitiveFS(dirPath string) (bool, error) {
	// Create a test file with a globally unique name and which includes
	// uppercase characters.
	nameUpper := fmt.Sprintf(".CASE_SENSITIVITY_CHECK_%d", rand.Intn(1e18))
	pathUpper := filepath.Join(dirPath, nameUpper)
	if err := os.WriteFile(pathUpper, nil, 0644); err != nil {
		return false, fmt.Errorf("write test file: %w", err)
	}
	defer func() {
		if err := os.Remove(pathUpper); err != nil {
			log.Warningf("Failed to remove FS case sensitivity test file %q: %s", nameUpper, err)
		}
	}()
	// Check whether the lowercase version of the path exists.
	_, err := os.Stat(filepath.Join(dirPath, strings.ToLower(nameUpper)))
	if err != nil {
		// Lowercase version does not exist. Assuming the file was not
		// concurrently deleted, this means the filesystem is case-sensitive.
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat test file: %w", err)
	}
	// Lowercase version exists. This means the filesystem is case-insensitive.
	return true, nil
}

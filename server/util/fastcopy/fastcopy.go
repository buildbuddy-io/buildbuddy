// +build !darwin

package fastcopy

import (
	"os"
)

func FastCopy(source, destination string) error {
	err := os.Link(path, outputPath)
	if !os.IsExist(err) {
		return err
	}
	return nil
}

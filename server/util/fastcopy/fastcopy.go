//go:build !darwin
// +build !darwin

package fastcopy

import (
	"os"
)

func FastCopy(source, destination string) error {
	err := os.Link(source, destination)
	if !os.IsExist(err) {
		return err
	}
	return nil
}

//go:build windows

package fastcopy

import "os"

func Clone(source, destination string) error {
	return FastCopy(source, destination)
}

func FastCopy(source, destination string) error {
	err := os.Link(source, destination)
	if !os.IsExist(err) {
		return err
	}
	return nil
}

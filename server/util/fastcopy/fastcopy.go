//go:build !darwin
// +build !darwin

package fastcopy

import (
	"flag"
	"os"
)

var enableFastcopyReflinking = flag.Bool("executor.enable_fastcopy_reflinking", false, "If true, attempt to use `cp --reflink=auto` to link files")

func Clone(source, destination string) error {
	if *enableFastcopyReflinking {
		return reflink(source, destination)
	}
	return FastCopy(source, destination)
}

func FastCopy(source, destination string) error {
	err := os.Link(source, destination)
	if !os.IsExist(err) {
		return err
	}
	return nil
}

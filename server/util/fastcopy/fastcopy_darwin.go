//go:build darwin
// +build darwin

package fastcopy

/*
#include <stdint.h> // for uint32_t
#include <stdlib.h> // for free
#include <sys/clonefile.h>
*/
import "C"

import (
	"syscall"
	"unsafe"
)

func FastCopy(source, destination string) error {
	src := C.CString(source)
	defer C.free(unsafe.Pointer(src))
	dst := C.CString(destination)
	defer C.free(unsafe.Pointer(dst))

	ret, err := C.clonefile(src, dst, C.uint32_t(0))
	if errno, ok := err.(syscall.Errno); ret == -1 && ok && errno != syscall.EEXIST {
		return err
	}
	return nil
}

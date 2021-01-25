// +build darwin

package fastcopy

/*
#include <stdint.h> // for uint32_t
#include <sys/clonefile.h>
*/
import "C"

import (
	"fmt"
	"syscall"
)

func FastCopy(source, destination string) error {
	ret, err := C.clonefile(C.CString(source), C.CString(destination), C.uint32_t(0))
	if errno, ok := err.(syscall.Errno); ret == -1 && ok && errno != syscall.EEXIST {
		return err
	}
	return nil
}

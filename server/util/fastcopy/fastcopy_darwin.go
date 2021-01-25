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
	if err := C.clonefile(C.CString(source), C.CString(destination), C.uint32_t(0)); err != 0 {
		return fmt.Errorf("Error cloning file: %v", syscall.Errno(err))
	}
	return nil
}

// This package exists only because gazelle complains if we try to use cgo
// directly in copy_on_write_test.go
package cow_cgo_testutil

/*
#define _GNU_SOURCE
#include <fcntl.h>

int get_F_OFD_SETLKW() {
#ifdef F_OFD_SETLKW
	return F_OFD_SETLKW;
#else
	return 0;
#endif
}
*/
import "C"

var (
	F_OFD_SETLKW = int(C.get_F_OFD_SETLKW())
)

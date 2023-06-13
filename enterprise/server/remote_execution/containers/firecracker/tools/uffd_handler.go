package main

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

/*
#include <linux/userfaultfd.h> // For UFFD_API, UFFDIO_API
*/
import "C"

const (
	// From https://blog.rchapman.org/posts/Linux_System_Call_Table_for_x86_64/
	SYS_USERFAULTFD = 323

	// Printed from a C script
	// printf("(hex)%x\n", _IOWR(UFFDIO, _UFFDIO_API, struct uffdio_api));
	UFFDIO_API = 0xc018aa3f
)

type uffdioApi struct {
	Api      uint64
	Features uint64
	Ioctls   uint64
}

type uffdioRange struct {
	Start uintptr
	Len   uint64
}
type uffdioRegister struct {
	Range   uffdioRange
	Mode    uint32
	ioctlop uintptr
}

type uffdMsg struct {
	Event uint64
	Addr  uint64
	Flags uint64
	Ptid  int32
	Fd    int32
	Pid   int32
}

func main() {
	// Create a UFFD page fault handler
	uffd, _, err := syscall.Syscall(SYS_USERFAULTFD, syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0, 0)
	if err != 0 {
		fmt.Printf("Failed to create uffd: %v\n", err)
		os.Exit(1)
	}

	// After the userfaultfd object is created with userfaultfd(), the
	//       application must enable it using the UFFDIO_API ioctl(2)
	//       operation.  This operation allows a handshake between the kernel
	//       and user space to determine the API version and supported
	//       features.
	uffdioAPI := uffdioApi{
		Api:      C.UFFD_API,
		Features: 0,
	}

	// Perform an operation on a uffd object by using ioctl
	// UFFDIO allows a handshake between the kernel and user space to enable the uffd object
	// https://manpages.ubuntu.com/manpages/bionic/man2/ioctl_userfaultfd.2.html
	_, _, err = syscall.Syscall(syscall.SYS_IOCTL, uffd, UFFDIO_API, uintptr(unsafe.Pointer(&uffdioAPI)))
	if err != 0 {
		fmt.Printf("Failed to call UFDIO ioctl: %v\n", err)
		os.Exit(1)
	}

	//// Allocate virtual memory that will be allocated by userfaultfde
	//pagesToAllocate := 5
	//// TODO: Should this be the system page size?
	//pageSize := 2048
	//addr, mmapErr := syscall.Mmap(0, 0, pageSize*pagesToAllocate, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS)
	//if mmapErr != nil {
	//	fmt.Printf("Failed to allocate virtual memory: %v\n", err)
	//	os.Exit(1)
	//}
	//startMemRange := uintptr(unsafe.Pointer(&addr[0]))
	//
	//// Register userfaultfd to listen on the allocated memory range
	//uffdioRegister := uffdioRegister{
	//	Range: uffdioRange{
	//		Start: startMemRange,
	//		Len:   uint64(pageSize * pagesToAllocate),
	//	},
	//	Mode: 0,
	//}
	//
	//// After a successful UFFDIO_API operation, the application then
	////       registers memory address ranges using the UFFDIO_REGISTER
	////       ioctl(2) operation.  After successful completion of a
	////       UFFDIO_REGISTER operation, a page fault occurring in the
	////       requested memory range, and satisfying the mode defined at the
	////       registration time, will be forwarded by the kernel to the user-
	////       space application.  The application can then use the UFFDIO_COPY
	////       or UFFDIO_ZEROPAGE ioctl(2) operations to resolve the page fault.
	//registerMacro := iowr(C.UFFDIO, C._UFFDIO_REGISTER, unsafe.Sizeof(uffdioRegister))
	//_, _, err = syscall.Syscall(syscall.SYS_IOCTL, uffd, registerMacro, uintptr(unsafe.Pointer(&uffdioRegister)))
	//if err != 0 {
	//	fmt.Printf("Failed to call UFDIO ioctl: %v\n", err)
	//	os.Exit(1)
	//}
	//
	//// Background thread to handle page faults
	//go func() {
	//	for {
	//		var event uffdMsg
	//		_, _, err := syscall.Syscall(syscall.SYS_READ, uffd, uintptr(unsafe.Pointer(&event)), unsafe.Sizeof(event))
	//		if err != 0 {
	//			fmt.Printf("Failed to read event: %v\n", err)
	//			os.Exit(1)
	//		}
	//
	//		fmt.Printf("Event is %v", event.Event)
	//	}
	//}()
}

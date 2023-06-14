package main

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

/*
#include <linux/userfaultfd.h> // For UFFD_API, UFFDIO_API
#include <linux/poll.h> // For POLLIN
*/
import "C"

const (
	// From https://blog.rchapman.org/posts/Linux_System_Call_Table_for_x86_64/
	SYS_USERFAULTFD = 323

	// Printed from a C script
	/*
		#include <stdio.h>
		#include <fcntl.h>
		#include <linux/ioctl.h>
		#include <linux/userfaultfd.h>
	*/
	// printf("(hex)%x\n", _IOWR(UFFDIO, _UFFDIO_API, struct uffdio_api));
	UFFDIO_API      = 0xc018aa3f
	UFFDIO_REGISTER = 0xc020aa00
	UFFDIO_COPY     = 0xc028aa03
)

type uffdioApi struct {
	Api      uint64
	Features uint64
	Ioctls   uint64
}

type uffdioRange struct {
	Start uint64
	Len   uint64
}
type uffdioRegister struct {
	Range  uffdioRange
	Mode   uint64
	Ioctls uint64
}

type uffdMsg struct {
	Event uint8

	Reserved1 uint8
	Reserved2 uint16
	Reserved3 uint32

	PageFault struct {
		Flags   uint64
		Address uint64
		Ptid    uint32
	}
}

type uffdioCopy struct {
	Dst  uint64 // Source of copy
	Src  uint64 // Destination of copy
	Len  uint64 // Number of bytes to copy
	Mode uint64 // Flags controlling behavior of copy
	Copy int64  // Number of bytes copied, or negated error
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
	// https://manpages.ubuntu.com/manpages/bionic/man2/ioctl_userfaultfd.2.html
	uffdioAPI := uffdioApi{
		Api:      C.UFFD_API,
		Features: 0,
	}
	_, _, err = syscall.Syscall(syscall.SYS_IOCTL, uffd, UFFDIO_API, uintptr(unsafe.Pointer(&uffdioAPI)))
	if err != 0 {
		fmt.Printf("Failed to call UFFDIO_API: %v\n", err)
		os.Exit(1)
	}

	// Create virtual memory that will be allocated by userfaultfd
	pagesToAllocate := 5
	pageSize := os.Getpagesize()
	lenToManage := pagesToAllocate * pageSize
	addr, mmapErr := syscall.Mmap(-1, 0, lenToManage, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS)
	if mmapErr != nil {
		fmt.Printf("Failed to create virtual memory: %v\n", err)
		os.Exit(1)
	}
	defer syscall.Munmap(addr)
	startAddr := &addr[0]
	fmt.Printf("Address returned by mmap is %p", startAddr)

	// After a successful UFFDIO_API operation, the application then
	//       registers memory address ranges using the UFFDIO_REGISTER
	//       ioctl(2) operation.
	registerData := uffdioRegister{
		Range: uffdioRange{
			Start: uint64(uintptr(unsafe.Pointer(startAddr))),
			Len:   uint64(lenToManage),
		},
		Mode: C.UFFDIO_REGISTER_MODE_MISSING,
	}
	_, _, err = syscall.Syscall(syscall.SYS_IOCTL, uffd, UFFDIO_REGISTER, uintptr(unsafe.Pointer(&registerData)))
	if err != 0 {
		fmt.Printf("Failed to call UFFDIO_REGISTER: %v\n", err)
		os.Exit(1)
	}

	// Background thread to handle page faults
	go func() {
		// Create a page that will be copied into the faulting region
		pageToCopy, mmapErr := syscall.Mmap(-1, 0, pageSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS)
		if mmapErr != nil {
			fmt.Printf("Failed to create virtual memory: %v\n", err)
			os.Exit(1)
		}
		defer syscall.Munmap(pageToCopy)

		for i := range pageToCopy {
			pageToCopy[i] = 'M'
		}

		pollFDs := []unix.PollFd{{
			Fd:     int32(uffd),
			Events: C.POLLIN,
		}}

		for {
			nready, pollErr := unix.Poll(pollFDs, -1)
			if pollErr != nil {
				fmt.Printf("Failed to poll UFFD: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Num ready is %d", nready)

			var event uffdMsg
			_, _, err := syscall.Syscall(syscall.SYS_READ, uffd, uintptr(unsafe.Pointer(&event)), unsafe.Sizeof(event))
			if err != 0 {
				fmt.Printf("Failed to read event: %v\n", err)
				os.Exit(1)
			}

			fmt.Printf("Address is %v", event.Event)

			copyData := uffdioCopy{
				Dst:  event.PageFault.Address,
				Src:  uint64(uintptr(unsafe.Pointer(&pageToCopy[0]))),
				Len:  uint64(pageSize),
				Mode: 0,
				Copy: 0,
			}

			_, _, err = syscall.Syscall(syscall.SYS_IOCTL, uffd, UFFDIO_COPY, uintptr(unsafe.Pointer(&copyData)))
			if err != 0 {
				fmt.Printf("Failed to call UFFDIO_COPY: %v\n", err)
				os.Exit(1)
			}
		}
	}()

	fmt.Println("Down here")
	l := 0xf
	c := addr[l]
	fmt.Printf("Read value %v in main(): ", c)
	l += 1024
	c = addr[l]
	fmt.Printf("Read value %v in main(): ", c)
	for {
	}
}

package main

import (
	"fmt"
	"net"
	"os"
	"syscall"
)

/*
#include <linux/userfaultfd.h> // For UFFD_API, UFFDIO_API
#include <linux/poll.h> // For POLLIN
*/
import "C"

const UFFDIO_COPY = 0xc028aa03

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
	// Remove any existing socket file
	socketPath := "/tmp/uffd_socket.sock"
	os.RemoveAll(socketPath)

	// Create a Unix domain socket listener
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		fmt.Println("Error creating Unix domain socket listener:", err)
		return
	}
	defer listener.Close()

	// Set the permissions of the socket file
	if err := os.Chmod(socketPath, 0777); err != nil {
		fmt.Println("Error setting perms on socket:", err)
		return
	}

	// Wait for firecracker to connect to the socket
	fmt.Println("Listening for connection")
	conn, err := listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection:", err)
		return
	}
	unixConn := conn.(*net.UnixConn)
	fmt.Println("Connection made")

	// Get the underlying socket
	socketFile, err := unixConn.File()
	if err != nil {
		fmt.Println("Error getting FD for socket:", err)
		return
	}
	socket := int(socketFile.Fd())
	defer socketFile.Close()

	// Read data sent from firecracker
	buf := make([]byte, syscall.CmsgSpace(4))
	_, _, _, _, err = syscall.Recvmsg(socket, nil, buf, 0)
	if err != nil {
		fmt.Println("Error accepting data over the socket:", err)
		return
	}
	fmt.Println("Connection made")
	fmt.Println(string(buf))

	//// For now, don't parse the rest of the data from the socket - just see if I can receive the socket
	//var uffd uintptr
	//uffd = file.Fd()
	//
	//// Background thread to handle page faults
	//go func() {
	//	// Create a page that will be copied into the faulting region
	//	pageSize := os.Getpagesize()
	//	pageToCopy, mmapErr := syscall.Mmap(-1, 0, pageSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS)
	//	if mmapErr != nil {
	//		fmt.Printf("Failed to create virtual memory: %v\n", err)
	//		os.Exit(1)
	//	}
	//	defer syscall.Munmap(pageToCopy)
	//
	//	for i := range pageToCopy {
	//		pageToCopy[i] = 'M'
	//	}
	//
	//	pollFDs := []unix.PollFd{{
	//		Fd:     int32(uffd),
	//		Events: C.POLLIN,
	//	}}
	//
	//	for {
	//		nready, pollErr := unix.Poll(pollFDs, -1)
	//		if pollErr != nil {
	//			fmt.Printf("Failed to poll UFFD: %v\n", err)
	//			os.Exit(1)
	//		}
	//		fmt.Printf("Num ready is %d", nready)
	//
	//		var event uffdMsg
	//		_, _, err := syscall.Syscall(syscall.SYS_READ, uffd, uintptr(unsafe.Pointer(&event)), unsafe.Sizeof(event))
	//		if err != 0 {
	//			fmt.Printf("Failed to read event: %v\n", err)
	//			os.Exit(1)
	//		}
	//
	//		fmt.Printf("Address is %v", event.Event)
	//
	//		copyData := uffdioCopy{
	//			Dst:  event.PageFault.Address,
	//			Src:  uint64(uintptr(unsafe.Pointer(&pageToCopy[0]))),
	//			Len:  uint64(pageSize),
	//			Mode: 0,
	//			Copy: 0,
	//		}
	//
	//		_, _, err = syscall.Syscall(syscall.SYS_IOCTL, uffd, UFFDIO_COPY, uintptr(unsafe.Pointer(&copyData)))
	//		if err != 0 {
	//			fmt.Printf("Failed to call UFFDIO_COPY: %v\n", err)
	//			os.Exit(1)
	//		}
	//	}
	//}()
	//
	//for {
	//
	//}
}

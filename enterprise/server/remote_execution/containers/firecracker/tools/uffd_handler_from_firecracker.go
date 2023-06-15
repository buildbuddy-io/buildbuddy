package main

import (
	"fmt"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"syscall"
	"unsafe"
)

/*
#include <linux/userfaultfd.h> // For UFFD_API, UFFDIO_API
#include <linux/poll.h> // For POLLIN
*/
import "C"

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

	// Accept a UFFD handler from the socket
	conn, err := listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection:", err)
		return
	}
	unixConn := conn.(*net.UnixConn)

	file, err := unixConn.File()
	if err != nil {
		fmt.Println("Error accepting fd over the socket:", err)
		return
	}

	// For now, don't parse the rest of the data from the socket - just see if I can receive the socket
	var uffd uintptr
	uffd = file.Fd()

	// Background thread to handle page faults
	go func() {
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
		}
	}()

	for {

	}
}

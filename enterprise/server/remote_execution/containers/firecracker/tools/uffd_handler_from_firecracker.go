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
	// When you try to load a firecracker snapshot with UFFD backend, it will connect to this socket and send some data over it
	// (See below for more info)
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
	// Firecracker sends the following over the socket (https://github.com/firecracker-microvm/firecracker/blob/main/src/vmm/src/persist.rs#L672)
	// - UFFD object it created
	//		- an open FD is a special type of data, considered a "control message" or "ancillary data", which is why we must process the two types of data sent separately
	// - mappings of VM virtual memory to backend memory file offsets
	//		- GuestRegionUffdMapping struct - https://github.com/firecracker-microvm/firecracker/blob/main/src/vmm/src/persist.rs#LL144C12-L144C34
	//		- Ex. -VM A creates a memory snapshot mem.snap. Its virtual memory address 0x100 gets saved to page 3 in mem.snapshot
	//			  - GuestRegionUffdMapping would contain { base_address: 0x100, offset: 3 }
	//			  - Tells UFFD to return page 3 of mem.snapshot when we get a memory fault for address 0x100
	bufMemoryMappings := make([]byte, 1024)
	// Each FD is 4B - only 1 should be sent (the UFFD object)
	bufUFFD := make([]byte, syscall.CmsgSpace(4))

	numBytesMappings, numBytesFD, _, _, err := unixConn.ReadMsgUnix(bufMemoryMappings, bufUFFD)
	if err != nil {
		fmt.Println("Error receiving data:", err)
		return
	}

	// Parse memory mappings
	bufMemoryMappings = bufMemoryMappings[:numBytesMappings]
	fmt.Printf("Received data: %s\n", string(bufMemoryMappings))

	// Parse UFFD object
	controlMsgs, err := syscall.ParseSocketControlMessage(bufUFFD[:numBytesFD])
	if err != nil {
		fmt.Println("Error parsing control messages:", err)
		return
	}
	if len(controlMsgs) != 1 {
		fmt.Println("Expected 1 control message containing UFFD, found %d", len(controlMsgs))
		return
	}
	fds, err := syscall.ParseUnixRights(&controlMsgs[0])
	if len(fds) != 1 {
		fmt.Println("Expected 1 FD containing UFFD, found %d", len(fds))
		return
	}
	uffd := uintptr(fds[0])

	// Background thread to handle page faults
	go func() {
		// Create a page that will be copied into the faulting region
		pageSize := os.Getpagesize()
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

	for {

	}
}

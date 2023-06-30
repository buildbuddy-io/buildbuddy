package main

/*
This example resolves page faults for a firecracker VM when it is loading a snapshot

In the LoadSnapshot call, Firecracker creates a UFFD object and sends it via a socket, with additional data about the VM's memory mappings
This handler then uses a memory snapshot file to serve the underlying memory

Instructions to create a memory snapshot file to serve as the underlying memory for UFFD testing:

1. Follow the instructions here to start a firecracker VM: https://github.com/firecracker-microvm/firecracker/blob/main/docs/getting-started.md#running-firecracker
	- By the end, you should be able to SSH into the VM with `sudo ssh -i ./ubuntu-18.04.id_rsa 172.16.0.2`
2. Pause the VM to prepare for taking a snapshot

curl --unix-socket ~/firecracker.socket -i \
    -X PATCH 'http://localhost/vm' \
    -H 'Accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
            "state": "Paused"
    }'

3. Take a snapshot

curl --unix-socket ~/firecracker.socket -i \
    -X PUT 'http://localhost/snapshot/create' \
    -H  'Accept: application/json' \
    -H  'Content-Type: application/json' \
    -d '{
            "snapshot_type": "Full",
            "snapshot_path": "./snapshot_file",
            "mem_file_path": "./mem_file"
    }'

** This handler assumes mem_file lives in root, and uses it as the backing memory file

Instructions to test:
1. Run the uffd handler: go run enterprise/server/remote_execution/containers/firecracker/tools/uffd_handler_from_firecracker.go
	- The socket path is hard coded to /home/maggie/sock/uffd_socket.sock
2. Run firecracker from root: Copy commands in run_firecracker.sh
	- You must download the firecracker repo: git clone https://github.com/firecracker-microvm/firecracker
	- The socket path is hardcoded to ~/firecracker.socket
	- You may not be able to run the bash script directly, because it executes in a sub-process
3. Configure networking for a firecracker VM: ./prep_vm.sh
4. Try to start a VM by loading a snapshot with UFFD configured as the backend

curl --unix-socket ~/firecracker.socket -i     -X PUT 'http://localhost/snapshot/load'     -H  'Accept: application/json'     -H  'Content-Type: application/json'     -d '{
            "snapshot_path": "./snapshot_file",
            "mem_backend": {
                "backend_path": "/home/maggielou/uffd.sock",
                "backend_type": "Uffd"
            },
            "enable_diff_snapshots": true,
            "resume_vm": true
    }'

*/

import (
	"encoding/json"
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
const UFFDIO_WRITEPROTECT = 0xc018aa06

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

type uffdioRange struct {
	Start uint64
	Len   uint64
}

type uffdioWriteProtect struct {
	Range uffdioRange
	Mode  uint64
}

// Corresponds to firecracker GuestRegionUffdMapping
type snapshottedMemoryMapping struct {
	BaseHostVirtAddr uint64  `json:"base_host_virt_addr"`
	Size             uintptr `json:"size"`
	Offset           uint64  `json:"offset"`
}

func main() {
	// Remove any existing socket file
	socketPath := "/home/maggielou/uffd.sock"
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
	var mappings []snapshottedMemoryMapping
	err = json.Unmarshal(bufMemoryMappings, &mappings)
	if err != nil {
		fmt.Printf("Could not parse memory mapping data: %s", err)
		return
	}
	fmt.Printf("Received data: %v\n", mappings)

	vmStartMemory := mappings[0].BaseHostVirtAddr
	vmMemorySize := mappings[0].Size

	// Parse UFFD object
	controlMsgs, err := syscall.ParseSocketControlMessage(bufUFFD[:numBytesFD])
	if err != nil {
		fmt.Println("Error parsing control messages:", err)
		return
	}
	if len(controlMsgs) != 1 {
		fmt.Printf("Expected 1 control message containing UFFD, found %d", len(controlMsgs))
		return
	}
	fds, err := syscall.ParseUnixRights(&controlMsgs[0])
	if len(fds) != 1 {
		fmt.Printf("Expected 1 FD containing UFFD, found %d", len(fds))
		return
	}
	uffd := uintptr(fds[0])

	pollFDs := []unix.PollFd{{
		Fd:     int32(uffd),
		Events: C.POLLIN,
	}}

	// Map backing file to memory (so you can access contents of the file as if they were RAM)
	backingMemorySnapshotFile := "/home/maggielou/mem_file"
	file, err := os.OpenFile(backingMemorySnapshotFile, os.O_RDWR, 0)
	if err != nil {
		fmt.Println("Failed to open file:", err)
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	backingMemoryAddr, mmapErr := syscall.Mmap(int(file.Fd()), 0, int(fileInfo.Size()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE)
	if mmapErr != nil {
		fmt.Printf("Failed to mmap backing memory file: %v\n", err)
		os.Exit(1)
	}

	// Write-protect memory
	// In order for this to work, you need the UFFD object to be initialized with write protection enabled
	// Firecracker patch : https://github.com/maggie-lou/firecracker/pull/1/files
	//writeProtectData := uffdioWriteProtect{
	//	Range: uffdioRange{
	//		Start: vmStartMemory,
	//		Len:   uint64(vmMemorySize),
	//	},
	//	Mode: C.UFFDIO_WRITEPROTECT_MODE_WP,
	//}
	//_, _, errNo := syscall.Syscall(syscall.SYS_IOCTL, uffd, UFFDIO_WRITEPROTECT, uintptr(unsafe.Pointer(&writeProtectData)))
	//if errNo != 0 {
	//	fmt.Printf("Failed to call UFFDIO_WRITEPROTECT: %v\n", err)
	//	os.Exit(1)
	//}

	go func() {
		for {
			// Poll UFFD for messages
			_, pollErr := unix.Poll(pollFDs, -1)
			if pollErr != nil {
				fmt.Printf("Failed to poll UFFD: %v\n", err)
				os.Exit(1)
			}

			var event uffdMsg
			_, _, err := syscall.Syscall(syscall.SYS_READ, uffd, uintptr(unsafe.Pointer(&event)), unsafe.Sizeof(event))
			if err != 0 {
				fmt.Printf("Failed to read event: %v\n", err)
				os.Exit(1)
			}

			if event.Event != C.UFFD_EVENT_PAGEFAULT {
				fmt.Printf("Unsupported UFFD event type %v", event.Event)
				continue
			}

			if event.PageFault.Flags&C.UFFD_PAGEFAULT_FLAG_WP != 0 {
				fmt.Printf("Captured a write!")
				continue
			}

			// Handle page fault by using a memory snapshot file created by snapshotting a different VM

			// Map requested address from page fault -> page of backing memory file
			// Align the address to the nearest lower multiple of pageSize by masking the least significant bits.
			// From https://github.com/firecracker-microvm/firecracker/blob/main/tests/host_tools/uffd/src/uffd_utils.rs#LL134C8-L134C84
			//pageSize := os.Getpagesize()
			//memoryPage := event.PageFault.Address & ^(uint64(pageSize - 1))

			// Copy memory from backing snapshot file
			copyData := uffdioCopy{
				Dst:  vmStartMemory,
				Src:  uint64(uintptr(unsafe.Pointer(&backingMemoryAddr[0]))),
				Len:  uint64(vmMemorySize),
				Mode: C.UFFDIO_COPY_MODE_WP,
				Copy: 0,
			}
			_, _, err = syscall.Syscall(syscall.SYS_IOCTL, uffd, UFFDIO_COPY, uintptr(unsafe.Pointer(&copyData)))
			if err != 0 {
				fmt.Printf("Failed to call UFFDIO_COPY: %v\n", err)
				os.Exit(1)
			}
		}
	}()

	//var character byte = 'A'
	//vmMemPtr := (*byte)(unsafe.Pointer(uintptr(vmStartMemory)))
	//*vmMemPtr = character
	//
	//// Read the character from memory
	//readCharacter := *vmMemPtr
	//fmt.Printf("Character written to memory: %c\n", readCharacter)
}

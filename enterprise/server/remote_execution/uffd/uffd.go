package uffd

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"syscall"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/blockio"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sys/unix"
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

func (c *uffdioCopy) String() string {
	return fmt.Sprintf("uffdio_copy{0x%x => 0x%x, len=0x%x}", c.Src, c.Dst, c.Len)
}

type uffdioRange struct {
	Start uint64
	Len   uint64
}

type uffdioWriteProtect struct {
	Range uffdioRange
	Mode  uint64
}

// GuestRegionUFFDMapping represents a VM memory region mapped to a memory
// region in the UFFD backing memory file.
type GuestRegionUFFDMapping struct {
	BaseHostVirtAddr uintptr `json:"base_host_virt_addr"`
	Size             uintptr `json:"size"`
	Offset           uintptr `json:"offset"`
}

func (g *GuestRegionUFFDMapping) ContainsGuestAddr(addr uintptr) bool {
	return addr >= g.BaseHostVirtAddr && addr < g.BaseHostVirtAddr+g.Size
}

// Initial message sent on the socket by firecracker.
type setupMessage struct {
	// Mappings contains the guest region memory mappings.
	Mappings []GuestRegionUFFDMapping
	// Fd is the UFFD file descriptor.
	Fd uintptr
}

type Handler struct {
	lis net.Listener
}

func NewHandler() (*Handler, error) {
	return &Handler{}, nil
}

func (h *Handler) Start(ctx context.Context, socketPath string, memoryStore *blockio.COWStore) error {
	lis, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return status.WrapError(err, "listen on socket")
	}
	h.lis = lis
	log.CtxDebugf(ctx, "userfaultfd handler listening on unix://%s", socketPath)

	// Set the permissions of the socket file
	if err := os.Chmod(socketPath, 0777); err != nil {
		return status.WrapError(err, "set socket permissions")
	}

	go func() {
		if err := h.handle(ctx, memoryStore); err != nil {
			log.CtxErrorf(ctx, "Failed to accept firecracker connection: %s", err)
		}
	}()
	return nil
}

func (h *Handler) receiveSetupMsg(ctx context.Context) (*setupMessage, error) {
	// Wait for firecracker to connect to the socket
	log.CtxDebugf(ctx, "Waiting for firecracker to connect to uffd socket")
	conn, err := h.lis.Accept()
	if err != nil {
		return nil, status.WrapError(err, "accept firecracker connection")
	}
	unixConn := conn.(*net.UnixConn)
	log.CtxDebugf(ctx, "Firecracker connected to uffd socket")

	// Read data sent from firecracker.
	//
	// Firecracker sends the following over the socket
	// (https://github.com/firecracker-microvm/firecracker/blob/main/src/vmm/src/persist.rs#L672)
	//
	// 1. Mappings of VM virtual memory to backing memory file offsets. These
	// are GuestRegionUFFDMapping structs, serialized as JSON. See
	// GuestRegionUFFDMapping for more info on what these are for.
	//
	// 2. The UFFD object it created, as "out-of-band" data.
	mappingsBuf := make([]byte, 1024)
	// Each FD is 4B - only 1 should be sent (the UFFD object)
	uffdBuf := make([]byte, syscall.CmsgSpace(4))

	numBytesMappings, numBytesFD, _, _, err := unixConn.ReadMsgUnix(mappingsBuf, uffdBuf)
	if err != nil {
		return nil, status.WrapError(err, "failed to read unix msg from connection")
	}

	// Parse memory mappings
	mappingsBuf = mappingsBuf[:numBytesMappings]
	var mappings []GuestRegionUFFDMapping
	err = json.Unmarshal(mappingsBuf, &mappings)
	if err != nil {
		return nil, status.WrapError(err, "parse memory mapping data")
	}
	log.CtxDebugf(ctx, "Received memory region mappings: %s", string(mappingsBuf))

	// Parse UFFD object
	controlMsgs, err := syscall.ParseSocketControlMessage(uffdBuf[:numBytesFD])
	if err != nil {
		return nil, status.WrapError(err, "parse control messages")
	}
	if len(controlMsgs) != 1 {
		return nil, status.InternalErrorf("expected 1 control message containing UFFD, found %d", len(controlMsgs))
	}
	fds, err := syscall.ParseUnixRights(&controlMsgs[0])
	if len(fds) != 1 {
		return nil, status.InternalErrorf("expected 1 fd containing uffd, found %d", len(fds))
	}
	uffd := uintptr(fds[0])

	return &setupMessage{
		Fd:       uffd,
		Mappings: mappings,
	}, nil
}

func (h *Handler) handle(ctx context.Context, memoryStore *blockio.COWStore) error {
	setup, err := h.receiveSetupMsg(ctx)
	if err != nil {
		return status.WrapError(err, "receive setup message from firecracker")
	}
	uffd := setup.Fd
	mappings := setup.Mappings

	pollFDs := []unix.PollFd{{Fd: int32(uffd), Events: C.POLLIN}}
	pageSize := os.Getpagesize()
	storeLength, err := memoryStore.SizeBytes()
	if err != nil {
		return status.WrapError(err, "get memory store size bytes")
	}
	for {
		// Poll UFFD for messages
		_, pollErr := unix.Poll(pollFDs, -1)
		if pollErr != nil {
			return status.WrapError(err, "poll uffd")
		}

		var event uffdMsg
		_, _, errno := syscall.Syscall(syscall.SYS_READ, uffd, uintptr(unsafe.Pointer(&event)), unsafe.Sizeof(event))
		if errno != 0 {
			return status.WrapError(errno, "read event from uffd")
		}

		if event.Event != C.UFFD_EVENT_PAGEFAULT {
			return status.InternalErrorf("unsupported uffd event type %v", event.Event)
		}

		if event.PageFault.Flags&C.UFFD_PAGEFAULT_FLAG_WP != 0 {
			return status.InternalErrorf("got message with WP flag, but write protection is not yet supported")
		}

		// Map the requested address from page fault address -> page of backing
		// memory file.
		// Align the address to the nearest lower multiple of pageSize by masking the least significant bits.
		// From https://github.com/firecracker-microvm/firecracker/blob/main/tests/host_tools/uffd/src/uffd_utils.rs#LL134C8-L134C84
		guestPageAddr := uintptr(event.PageFault.Address & ^(uint64(pageSize) - 1))
		faultStoreOffset, err := guestMemoryAddrToStoreOffset(guestPageAddr, mappings)
		if err != nil {
			return status.WrapError(err, "translate to store offset")
		}
		hostPageAddr, err := memoryStore.GetPageAddress(uintptr(faultStoreOffset), false /*=write*/)
		if err != nil {
			return status.WrapError(err, "get backing page address")
		}

		// Should never map a partial page at the end of the file, but just
		// warn in this case for now.
		if remainder := storeLength - int64(faultStoreOffset); remainder < int64(pageSize) {
			log.CtxWarningf(ctx, "uffdio_copy range extends past store length")
		}

		// DO NOT SUBMIT
		log.Debugf("faultAddr=0x%x, faultPageAddr=0x%x, storeOffset=0x%x", event.PageFault.Address, guestPageAddr, faultStoreOffset)

		copyData := uffdioCopy{
			Dst: uint64(guestPageAddr),
			Src: uint64(hostPageAddr),
			// For now, copy just the one page to the VM memory range. TODO:
			// It's probably much more efficient to copy the whole part of the
			// chunk that overlaps with the mapped memory region.
			Len:  uint64(pageSize),
			Mode: 0,
			Copy: 0,
		}

		// DO NOT SUBMIT
		log.Debugf("Sending %s", &copyData)

		_, _, errno = syscall.Syscall(syscall.SYS_IOCTL, uffd, UFFDIO_COPY, uintptr(unsafe.Pointer(&copyData)))
		if errno != 0 {
			return status.WrapError(errno, "UFFDIO_COPY")
		}

		log.Debugf("UFFDIO_COPY completed successfully")
	}
}

func (h *Handler) Stop() {
	h.lis.Close()
}

// Translate the fault page address in the guest to a persisted store offset
// based on the memory mappings.
func guestMemoryAddrToStoreOffset(addr uintptr, mappings []GuestRegionUFFDMapping) (uintptr, error) {
	for _, m := range mappings {
		if !m.ContainsGuestAddr(addr) {
			continue
		}
		relativeOffset := addr - m.BaseHostVirtAddr
		return m.Offset + relativeOffset, nil
	}
	return 0, status.InternalErrorf("page address 0x%x not found in guest region UFFD mappings", addr)
}

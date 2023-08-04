//go:build linux && !android

package uffd

import (
	"context"
	"encoding/json"
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

// UFFD macros - see README for more info
const UFFDIO_COPY = 0xc028aa03

// uffdMsg is a notification from the userfaultfd object about a change in the virtual memory layout of the
// faulting process
// It's defined in the linux header file userfaultfd.h
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

// uffdioCopy contains input/output data to the UFFDIO_COPY syscall
// It's defined in the linux header file userfaultfd.h
type uffdioCopy struct {
	Dst  uint64 // Address of the faulting region that memory should be copied to
	Src  uint64 // Source of copy
	Len  uint64 // Number of bytes to copy
	Mode uint64 // Flags controlling behavior of copy
	Copy int64  // After the syscall has completed, contains the number of bytes copied, or a negative errno to indicate failure
}

// GuestRegionUFFDMapping represents the mapping between a VM memory address to the offset in the corresponding
// memory snapshot file.
//
// # This data is sent by firecracker to tell the UFFD handler how to handle page faults
//
// Ex. If the VM tries to read memory at `BaseHostVirtAddr` and triggers a page fault, the UFFD handler can populate it
// by copying `size` bytes from `offset` in the memory snapshot file.
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
	Mappings []GuestRegionUFFDMapping
	Uffd     uintptr
}

// When loading a firecracker memory snapshot, this userfaultfd handler can be used to handle page faults for the VM.
// This handler uses a blockio.COWStore to manage the snapshot - allowing it to be served remotely, compressed, etc.
type Handler struct {
	lis      net.Listener
	quitChan chan struct{}

	earlyTerminationReader *os.File
	earlyTerminationWriter *os.File
}

func NewHandler() (*Handler, error) {
	return &Handler{}, nil
}

// Start starts a goroutine to listen on the given socket path for Firecracker's
// UFFD initialization message, and then starts fulfilling UFFD requests using
// the given memory store.
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

	// Initialize quitChan. Stop() will wait until this is closed to verify the handler has completed handling
	// open requests
	h.quitChan = make(chan struct{}, 0)

	// Create a FD that can be used to terminate Poll early
	pipeRead, pipeWrite, err := os.Pipe()
	if err != nil {
		return status.WrapError(err, "create early-termination fd")
	}
	h.earlyTerminationReader = pipeRead
	h.earlyTerminationWriter = pipeWrite

	go func() {
		if err := h.handle(ctx, memoryStore); err != nil {
			log.CtxErrorf(ctx, "Failed to handle firecracker memory requests: %s", err)
		}
	}()
	return nil
}

// When UFFD is set as the memory backend, firecracker will create a UFFD object and send it to
// the UFFD handler over a unix socket. It also sends GuestRegionUFFDMapping data so the handler knows
// how to resolve the page faults
func (h *Handler) receiveSetupMsg(ctx context.Context) (*setupMessage, error) {
	defer h.lis.Close()

	log.CtxDebugf(ctx, "Waiting for firecracker to connect to uffd socket")
	conn, err := h.lis.Accept()
	if err != nil {
		return nil, status.WrapError(err, "accept firecracker connection")
	}
	unixConn := conn.(*net.UnixConn)
	log.CtxDebugf(ctx, "Firecracker connected to uffd socket")

	// Read data sent from firecracker.
	// The GuestRegionUFFDMapping data is serialized as JSON. The UFFD object it created is sent as "out-of-band" data.
	mappingsBuf := make([]byte, 1024)
	// The size of a FD is 4B - only 1 should be sent (the UFFD object)
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
		return nil, status.InternalErrorf("expected 1 fd (the uffd object), found %d", len(fds))
	}
	uffd := uintptr(fds[0])

	return &setupMessage{
		Uffd:     uffd,
		Mappings: mappings,
	}, nil
}

func (h *Handler) handle(ctx context.Context, memoryStore *blockio.COWStore) error {
	defer close(h.quitChan)

	// Get uffd sent from firecracker
	setup, err := h.receiveSetupMsg(ctx)
	if err != nil {
		return status.WrapError(err, "receive setup message from firecracker")
	}
	uffd := setup.Uffd
	mappings := setup.Mappings

	pollFDs := []unix.PollFd{
		{Fd: int32(uffd), Events: C.POLLIN},
		{Fd: int32(h.earlyTerminationReader.Fd()), Events: C.POLLIN},
	}
	pageSize := os.Getpagesize()
	storeLength, err := memoryStore.SizeBytes()
	if err != nil {
		return status.WrapError(err, "get memory store size bytes")
	}

	for {
		// Poll UFFD for messages
		_, pollErr := unix.Poll(pollFDs, -1)
		if pollErr != nil {
			if pollErr == unix.EINTR {
				// Poll call was interrupted by another signal - retry
				continue
			}
			return status.WrapError(pollErr, "poll uffd")
		}

		// Check for an early termination message
		earlyTerminationFd := pollFDs[1]
		if earlyTerminationFd.Revents&C.POLLIN != 0 {
			return nil
		}

		// Receive a page fault notification
		guestFaultingAddr, err := readFaultingAddress(uffd)
		if err != nil {
			if err == unix.EAGAIN {
				// Try again code
				continue
			}
			return status.InternalErrorf("read event from uffd failed with errno(%d)", err)
		}
		guestPageAddr := pageStartAddress(guestFaultingAddr, pageSize)

		// Find the memory data in the store that should be used to handle the page fault
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

		// TODO(Maggie): Copy entire chunk, as opposed to just a page
		_, err = resolvePageFault(uffd, uint64(guestPageAddr), uint64(hostPageAddr), uint64(pageSize))
		if err != nil {
			return err
		}
	}
}

// resolvePageFault copies `size` bytes of memory from a `Src` address to the faulting region `Dst`
//
// When complete, the UFFDIO_COPY call wakes up the process that triggered the page fault (When a process
// attempts to access unallocated memory, it triggers a page fault and hangs until it has been resolved)
//
// Returns the number of bytes copied
func resolvePageFault(uffd uintptr, faultingRegion uint64, src uint64, size uint64) (int64, error) {
	copyData := uffdioCopy{
		Dst: faultingRegion,
		Src: src,
		Len: uint64(size),
	}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uffd, UFFDIO_COPY, uintptr(unsafe.Pointer(&copyData)))
	if errno != 0 {
		if errno == unix.ENOENT {
			// The faulting process changed its virtual memory layout simultaneously with an outstanding UFFDIO_COPY
			// The UFFDIO_COPY will have aborted and the handler should continue to serve page faults
			return 0, nil
		}
		return 0, status.InternalErrorf("UFFDIO_COPY failed with errno(%d)", errno)
	}
	return int64(copyData.Copy), nil
}

// readFaultingAddress reads a notification from the uffd object and returns the faulting address
// (i.e. the memory location the VM tried to access that triggered the page fault)
func readFaultingAddress(uffd uintptr) (uint64, error) {
	var event uffdMsg
	_, _, errno := syscall.Syscall(syscall.SYS_READ, uffd, uintptr(unsafe.Pointer(&event)), unsafe.Sizeof(event))
	if errno != 0 {
		return 0, errno
	}

	if event.Event != C.UFFD_EVENT_PAGEFAULT {
		return 0, status.InternalErrorf("unsupported uffd event type %v", event.Event)
	}
	if event.PageFault.Flags&C.UFFD_PAGEFAULT_FLAG_WP != 0 {
		return 0, status.InternalErrorf("got message with WP flag, but write protection is not yet supported")
	}

	return event.PageFault.Address, nil
}

// Gets the address of the start of the memory page containing `addr`
func pageStartAddress(addr uint64, pageSize int) uintptr {
	// Align the address to the nearest lower multiple of pageSize by masking the least significant bits.
	return uintptr(addr & ^(uint64(pageSize) - 1))
}

func (h *Handler) Stop() error {
	if h.earlyTerminationWriter == nil || h.earlyTerminationReader == nil || h.quitChan == nil {
		log.Info("UFFD handler was already stopped when Stop() was called")
		return nil
	}

	log.Info("UFFD handler beginning shut down")
	_, err := h.earlyTerminationWriter.Write([]byte{0})
	if err != nil {
		return status.WrapError(err, "write to early terminator")
	}

	// Wait for handle() to finish processing open requests
	<-h.quitChan
	h.quitChan = nil

	h.earlyTerminationReader.Close()
	h.earlyTerminationWriter.Close()
	h.earlyTerminationReader = nil
	h.earlyTerminationWriter = nil
	return nil
}

// Translate the faulting memory address in the guest to a persisted store offset
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

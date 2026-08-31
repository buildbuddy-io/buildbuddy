//go:build openbsd

// Package processlist provides native access to the OpenBSD process table.
package processlist

import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	ctlKern     = 1
	kernProc    = 66
	kernProcAll = 0

	// OpenBSD's kinfo_proc ABI is append-only and permits callers to request
	// only the leading portion of each record. The prefix below ends after the
	// PID and parent PID fields, which are the only fields we need.
	kinfoProcPIDOffset  = int(unsafe.Offsetof(kinfoProcPrefix{}.PID))
	kinfoProcPPIDOffset = int(unsafe.Offsetof(kinfoProcPrefix{}.PPID))
	kinfoProcPrefixSize = kinfoProcPPIDOffset + int(unsafe.Sizeof(kinfoProcPrefix{}.PPID))

	// Leave room for processes created between the sizing and retrieval
	// sysctl calls. We still retry if the table outgrows this headroom.
	processTableHeadroom = 16
	maxListAttempts      = 4
)

// kinfoProcPrefix mirrors the stable prefix of OpenBSD's struct kinfo_proc.
// All pointer fields in the kernel ABI are represented as fixed-width 64-bit
// integers, regardless of the userspace architecture.
type kinfoProcPrefix struct {
	Pointers [12]uint64
	Eflag    int32
	ExitSig  int32
	Flag     int32
	PID      int32
	PPID     int32
}

// Process contains the process relationship fields used by the executor.
type Process struct {
	PID  int
	PPID int
}

// List returns all user processes visible to the calling process.
func List() ([]Process, error) {
	for attempt := 0; attempt < maxListAttempts; attempt++ {
		required, err := readProcessTable(nil)
		if err != nil {
			return nil, fmt.Errorf("size process table: %w", err)
		}
		if required%kinfoProcPrefixSize != 0 {
			return nil, fmt.Errorf("process table size %d is not a multiple of record size %d", required, kinfoProcPrefixSize)
		}

		recordCapacity := required/kinfoProcPrefixSize + processTableHeadroom
		buf := make([]byte, recordCapacity*kinfoProcPrefixSize)
		used, err := readProcessTable(buf)
		if errors.Is(err, unix.ENOMEM) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read process table: %w", err)
		}
		if used > len(buf) {
			return nil, fmt.Errorf("process table returned %d bytes into a %d-byte buffer", used, len(buf))
		}
		if used%kinfoProcPrefixSize != 0 {
			return nil, fmt.Errorf("process table returned %d bytes, which is not a multiple of record size %d", used, kinfoProcPrefixSize)
		}

		count := used / kinfoProcPrefixSize
		processes := make([]Process, 0, count)
		for i := 0; i < count; i++ {
			offset := i * kinfoProcPrefixSize
			pid := *(*int32)(unsafe.Pointer(&buf[offset+kinfoProcPIDOffset]))
			ppid := *(*int32)(unsafe.Pointer(&buf[offset+kinfoProcPPIDOffset]))
			processes = append(processes, Process{
				PID:  int(pid),
				PPID: int(ppid),
			})
		}
		return processes, nil
	}
	return nil, fmt.Errorf("process table changed during %d consecutive read attempts", maxListAttempts)
}

// readProcessTable invokes OpenBSD's kern.proc sysctl. If buf is nil, it
// returns the required buffer size. Otherwise it returns the number of bytes
// written to buf.
func readProcessTable(buf []byte) (int, error) {
	recordCapacity := len(buf) / kinfoProcPrefixSize
	mib := [...]int32{
		ctlKern,
		kernProc,
		kernProcAll,
		0,
		int32(kinfoProcPrefixSize),
		int32(recordCapacity),
	}

	var data unsafe.Pointer
	size := uintptr(len(buf))
	if len(buf) > 0 {
		data = unsafe.Pointer(&buf[0])
	}
	_, _, errno := unix.Syscall6(
		unix.SYS___SYSCTL,
		uintptr(unsafe.Pointer(&mib[0])),
		uintptr(len(mib)),
		uintptr(data),
		uintptr(unsafe.Pointer(&size)),
		0,
		0,
	)
	runtime.KeepAlive(mib)
	runtime.KeepAlive(buf)
	if errno != 0 {
		return int(size), errno
	}
	return int(size), nil
}

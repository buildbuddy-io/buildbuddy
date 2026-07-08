package uffd

import (
	"errors"
	"os"
	"syscall"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type fakeResolver struct {
	onZeropage func(uffd uintptr, z *uffdIoZeropage) syscall.Errno
	onCopy     func(uffd uintptr, c *uffdioCopy) syscall.Errno
}

func (f *fakeResolver) copy(uffd uintptr, c *uffdioCopy) syscall.Errno {
	return f.onCopy(uffd, c)
}

func (f *fakeResolver) zeropage(uffd uintptr, z *uffdIoZeropage) syscall.Errno {
	return f.onZeropage(uffd, z)
}

func newTestHandler(r faultResolver) *Handler {
	return &Handler{
		removedAddresses: make(map[int64]struct{}),
		faultResolver:    r,
	}
}

func newTestMemoryStore(t *testing.T, totalPages int64) *copy_on_write.COWStore {
	pageSize := int64(os.Getpagesize())
	store, err := copy_on_write.NewCOWStore(t.Context(), nil, "test", nil, copy_on_write.COWOptions{
		ChunkSizeBytes:   pageSize,
		TotalSizeBytes:   totalPages * pageSize,
		DataDir:          t.TempDir(),
		MaxMmappedChunks: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func TestRemoveEventsAreHandledBeforePageFaults(t *testing.T) {
	pageSize := uintptr(os.Getpagesize())
	faultingAddress := uintptr(0x100000)
	uffd := uintptr(10)

	zeroCalls := 0
	copyCalls := 0
	h := newTestHandler(&fakeResolver{
		onZeropage: func(gotUffd uintptr, z *uffdIoZeropage) syscall.Errno {
			require.Equal(t, uffd, gotUffd)
			require.Equal(t, uint64(faultingAddress), z.Range.Start)

			// The two consecutive pages that were removed should be zeroed.
			require.Equal(t, uint64(2*pageSize), z.Range.Len)
			zeroCalls++
			return 0
		},
		onCopy: func(gotUffd uintptr, c *uffdioCopy) syscall.Errno {
			copyCalls++
			return 0
		},
	})
	mappings := []GuestRegionUFFDMapping{{
		BaseHostVirtAddr: faultingAddress,
		Size:             4 * pageSize,
	}}
	memoryStore := newTestMemoryStore(t, 4)

	events := []*uffdEvent{
		// This page fault should be deferred until after the remove events are handled.
		{pagefault: &Pagefault{Address: uint64(faultingAddress)}},
		// Simulate a remove event for the first two pages.
		{remove: &Remove{Start: uint64(faultingAddress), End: uint64(faultingAddress + 2*pageSize)}},
		// Simulate another remove event.
		// Leave a one-page gap so the removed pages are not consecutive.
		{remove: &Remove{Start: uint64(faultingAddress + 3*pageSize), End: uint64(faultingAddress + 4*pageSize)}},
	}

	deferred, err := h.processEvents(uffd, memoryStore, mappings, events)

	require.NoError(t, err)
	require.Empty(t, deferred)

	// Because the page fault was deferred until after a remove event on the same address,
	// it should be zeroed when it is faulted in.
	require.Equal(t, 1, zeroCalls)
	require.Equal(t, 0, copyCalls)

	// Because there were 2 consecutive removed pages, they should both have been zeroed and not
	// tracked in the map.
	require.NotContains(t, h.removedAddresses, int64(faultingAddress))
	require.NotContains(t, h.removedAddresses, int64(faultingAddress+pageSize))
	// The non-consecutive page that was removed should still be in the map.
	require.Contains(t, h.removedAddresses, int64(faultingAddress+3*pageSize))
}

func TestEAGAIN_DefersRemainingPageFaults(t *testing.T) {
	pageSize := uintptr(os.Getpagesize())
	baseAddress := uintptr(0x100000)

	// Simulate 3 page faults on non-consecutive pages.
	pageFaults := []*Pagefault{
		{Address: uint64(baseAddress)},
		{Address: uint64(baseAddress + 2*pageSize)},
		{Address: uint64(baseAddress + 4*pageSize)},
	}
	events := make([]*uffdEvent, 0, 2*len(pageFaults))
	for _, pageFault := range pageFaults {
		events = append(events, &uffdEvent{pagefault: pageFault})
	}
	// Simulate remove events for all pages that were faulted in.
	for _, pageFault := range pageFaults {
		events = append(events, &uffdEvent{remove: &Remove{
			Start: pageFault.Address,
			End:   pageFault.Address + uint64(pageSize),
		}})
	}

	var zeroedAddresses []uint64
	h := newTestHandler(&fakeResolver{
		onZeropage: func(uffd uintptr, z *uffdIoZeropage) syscall.Errno {
			zeroedAddresses = append(zeroedAddresses, z.Range.Start)
			// Simulate an EAGAIN error for the second page fault.
			if z.Range.Start == pageFaults[1].Address {
				return unix.EAGAIN
			}
			return 0
		},
	})
	mappings := []GuestRegionUFFDMapping{{
		BaseHostVirtAddr: baseAddress,
		Size:             6 * pageSize,
	}}
	memoryStore := newTestMemoryStore(t, 6)

	deferred, err := h.processEvents(0 /*=uffd*/, memoryStore, mappings, events)

	require.NoError(t, err)
	// The first two pages should have triggered the zeropage ioctl.
	require.Equal(t, []uint64{pageFaults[0].Address, pageFaults[1].Address}, zeroedAddresses)
	// The second and third page faults should have been deferred because of the EAGAIN error from the second page fault.
	require.Equal(t, pageFaults[1:], deferred)
}

func TestZero_Error(t *testing.T) {
	pageSize := uintptr(os.Getpagesize())
	faultingAddress := uintptr(0x100000)

	h := newTestHandler(&fakeResolver{
		onZeropage: func(uffd uintptr, z *uffdIoZeropage) syscall.Errno {
			return unix.EAGAIN
		},
	})
	h.removedAddresses[int64(faultingAddress)] = struct{}{}
	h.removedAddresses[int64(faultingAddress+pageSize)] = struct{}{}

	err := h.copyZeroes(0 /*=uffd*/, faultingAddress)

	require.True(t, errors.Is(err, unix.EAGAIN), "error should wrap EAGAIN, got %v", err)
	// The pages should still be in the map because they were never successfully zeroed.
	require.Contains(t, h.removedAddresses, int64(faultingAddress))
	require.Contains(t, h.removedAddresses, int64(faultingAddress+pageSize))
}

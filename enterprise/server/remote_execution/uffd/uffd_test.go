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
			z.Zeropage = int64(z.Range.Len)
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
			z.Zeropage = int64(z.Range.Len)
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

func TestCopyRange_ContinuesAfterPartialEAGAIN(t *testing.T) {
	pageSize := uint64(os.Getpagesize())
	uffd := uintptr(10)
	destAddress := uint64(0x100000)
	hostAddress := uint64(0x200000)

	copyCalls := 0
	h := newTestHandler(&fakeResolver{
		onCopy: func(gotUffd uintptr, c *uffdioCopy) syscall.Errno {
			require.Equal(t, uffd, gotUffd)
			copyCalls++
			switch copyCalls {
			case 1:
				// Simulate the first UFFDIO_COPY successfully copying 1 page,
				// then returning EAGAIN.
				require.Equal(t, destAddress, c.Dst)
				require.Equal(t, hostAddress, c.Src)
				require.Equal(t, 3*pageSize, c.Len)
				c.Copy = int64(pageSize)
				return unix.EAGAIN
			case 2:
				// Simulate the second UFFDIO_COPY successfully copying the remaining 2 pages.
				require.Equal(t, destAddress+pageSize, c.Dst)
				require.Equal(t, hostAddress+pageSize, c.Src)
				require.Equal(t, 2*pageSize, c.Len)
				c.Copy = int64(2 * pageSize)
				return 0
			default:
				require.FailNow(t, "unexpected copy call")
				return 0
			}
		},
	})

	err := h.copyRange(uffd, uintptr(destAddress), uintptr(hostAddress), int64(3*pageSize))

	require.NoError(t, err)
	require.Equal(t, 2, copyCalls)
}

func TestCopyRange_ReturnsEAGAINIfNoProgressMade(t *testing.T) {
	pageSize := uint64(os.Getpagesize())
	uffd := uintptr(10)
	destAddress := uint64(0x100000)
	hostAddress := uint64(0x200000)

	copyCalls := 0
	h := newTestHandler(&fakeResolver{
		onCopy: func(gotUffd uintptr, c *uffdioCopy) syscall.Errno {
			// Simulate a successful copy of the first page.
			if copyCalls == 0 {
				c.Copy = int64(pageSize)
			}
			copyCalls++
			return unix.EAGAIN
		},
	})

	err := h.copyRange(uffd, uintptr(destAddress), uintptr(hostAddress), int64(3*pageSize))

	require.True(t, errors.Is(err, unix.EAGAIN), "error should wrap EAGAIN, got %v", err)
	require.Equal(t, 2, copyCalls)
}

func TestCopyRange_SkipsAlreadyMappedPages(t *testing.T) {
	pageSize := uint64(os.Getpagesize())
	uffd := uintptr(10)
	destAddress := uint64(0x100000)
	hostAddress := uint64(0x200000)

	copyCalls := 0
	h := newTestHandler(&fakeResolver{
		onCopy: func(gotUffd uintptr, c *uffdioCopy) syscall.Errno {
			defer func() { copyCalls++ }()
			switch copyCalls {
			case 0:
				// Simulate the first page being already mapped, which returns
				// EEXIST with 0 bytes copied.
				return unix.EEXIST
			case 1:
				// On the second copy request, we expect the first page to be
				// skipped, because it was already mapped.
				require.Equal(t, destAddress+pageSize, c.Dst)
				require.Equal(t, hostAddress+pageSize, c.Src)
				require.Equal(t, 2*pageSize, c.Len)
				c.Copy = int64(2 * pageSize)
				return 0
			default:
				require.FailNow(t, "unexpected copy call")
				return 0
			}
		},
	})

	err := h.copyRange(uffd, uintptr(destAddress), uintptr(hostAddress), int64(3*pageSize))

	require.NoError(t, err)
	require.Equal(t, 2, copyCalls)
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
	mapping := &GuestRegionUFFDMapping{
		BaseHostVirtAddr: faultingAddress,
		Size:             2 * pageSize,
	}

	err := h.copyZeroes(0 /*=uffd*/, faultingAddress, mapping)

	require.True(t, errors.Is(err, unix.EAGAIN), "error should wrap EAGAIN, got %v", err)
	// The pages should still be in the map because they were never successfully zeroed.
	require.Contains(t, h.removedAddresses, int64(faultingAddress))
	require.Contains(t, h.removedAddresses, int64(faultingAddress+pageSize))
}

func TestZero_PartialEAGAIN(t *testing.T) {
	pageSize := uintptr(os.Getpagesize())
	faultingAddress := uintptr(0x100000)

	zeroCalls := 0
	h := newTestHandler(&fakeResolver{
		onZeropage: func(uffd uintptr, z *uffdIoZeropage) syscall.Errno {
			zeroCalls++
			// Simulate successfully zeroing the first page, then returning EAGAIN.
			z.Zeropage = int64(pageSize)
			return unix.EAGAIN
		},
	})
	// Simulate that all faulting addresses were previously removed by the balloon.
	for addr := faultingAddress; addr < faultingAddress+3*pageSize; addr += pageSize {
		h.removedAddresses[int64(addr)] = struct{}{}
	}
	mapping := &GuestRegionUFFDMapping{
		BaseHostVirtAddr: faultingAddress,
		Size:             3 * pageSize,
	}

	err := h.copyZeroes(0 /*=uffd*/, faultingAddress, mapping)

	// Even though the entire range was not successfully eagerly zeroed,
	// as long as mapping the first page succeeded, we should not return an error.
	require.NoError(t, err)
	require.Equal(t, 1, zeroCalls)
	require.NotContains(t, h.removedAddresses, int64(faultingAddress))
	require.Contains(t, h.removedAddresses, int64(faultingAddress+pageSize))
	require.Contains(t, h.removedAddresses, int64(faultingAddress+2*pageSize))
}

func TestZero_EEXIST(t *testing.T) {
	pageSize := uintptr(os.Getpagesize())
	faultingAddress := uintptr(0x100000)

	h := newTestHandler(&fakeResolver{
		onZeropage: func(uffd uintptr, z *uffdIoZeropage) syscall.Errno {
			return unix.EEXIST
		},
	})
	// Simulate that two pages were previously removed by the balloon.
	h.removedAddresses[int64(faultingAddress)] = struct{}{}
	h.removedAddresses[int64(faultingAddress+pageSize)] = struct{}{}

	// Try to zero 2 pages.
	mapping := &GuestRegionUFFDMapping{
		BaseHostVirtAddr: faultingAddress,
		Size:             2 * pageSize,
	}

	err := h.copyZeroes(0 /*=uffd*/, faultingAddress, mapping)

	// If the EEXIST error was returned by the IOCTL, we should not return an error.
	require.NoError(t, err)
	// The first page should be removed from the map because EEXIST indicates that
	// it's already been handled.
	require.NotContains(t, h.removedAddresses, int64(faultingAddress))
	// The second page should still be in the map because it was not successfully zeroed.
	require.Contains(t, h.removedAddresses, int64(faultingAddress+pageSize))
}

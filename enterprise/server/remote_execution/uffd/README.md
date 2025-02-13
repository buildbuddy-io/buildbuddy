# Userfaultfd (UFFD) Handler

When loading a firecracker memory snapshot, this userfaultfd handler can be used to handle page faults for the VM.

## Overview of Virtual Memory

When a process first starts up, the kernel allocates _virtual memory_ to it. The process will treat it like memory, but
there is no actual physical memory on the machine allocated to it. The first time the process tries to access a virtual
memory address, it will trigger a _page fault_.

A page fault pauses the initial memory access, and typically transfers control to the kernel. The kernel prepares
physical memory, creates a mapping between virtual -> physical memory, then resumes the request. Now that the
physical memory actually exists, the process can proceed to use it.

## Overview of UFFD

Rather than having the kernel handle page faults, we can use UFFD to handle page faults ourselves, allowing us
to implement custom memory management code.

For example, when a firecracker VM loads a memory snapshot file, by default it will rely on the kernel to handle
page faults by loading the contents of the snapshot file into physical memory. The snapshot file must exist locally
for this to work.

By using a UFFD handler, we can add custom logic - we can stream the memory data from a remote cache, compress it,
modify it in place with copy-on-write mechanics etc.

## UFFD and Firecracker

To initialize, set `Uffd` as the `mem_backend.backend_type` when loading a firecracker memory snapshot.

Firecracker creates the UFFD object, as it knows the virtual memory range allocated to the VM that the UFFD object
should be registered to. When you set `Uffd` as the backend_type, you also must pass a unix socket to firecracker.
Firecracker will send the UFFD object it created on this socket, so the UFFD handler should listen on the socket
to receive it.

## Memory Ballooning

A memory balloon is a mechanism to control the amount of memory available to a VM.

You can set a target size for the balloon. The balloon will then try to reach that
target size by allocating memory in the VM. When used in conjunction with UFFD,
UFFD will receive notifications (EVENT_REMOVE) containing the addresses of pages that the balloon
has expanded into.

The balloon guarantees that the VM is not using these removed memory pages.
Typically, ballooning allows the host to re-allocate removed memory to other processes.
In our case, we use ballooning to manage our memory snapshot size. Because we know
that removed pages are unused by the VM, we do not need to cache them in our memory
snapshots.

After a page has been removed, the next time the VM tries to read it and triggers
a page fault, the page should be 0'd. This can be done with UFFDIO_ZEROPAGE. You should
not page fault in data from the original snapshot file, as the guest is expecting
an empty page.

Remove events are considered non-cooperative. Unlike page faults, which cause the faulting
thread in the guest to sleep until the UFFD handler resolves them with
UFFDIO_COPY or UFFDIO_ZEROPAGE, no direct action is required by the UFFD handler.
It only needs to update its internal accounting so subsequent page faults on
removed pages are handled correctly.

Unfortunately there is no easy way to control the speed of inflation/deflation of the
balloon, or which memory addresses are removed; this is controlled by the guest kernel driver.
Also note that inflating the balloon can be a CPU-intensive process in the guest, as the balloon
may need to allocate a lot of memory. Thus it's recommended to set a realistic target size.

## Performing UFFD operations (UFFD Macros)

To perform UFFD operations, you make an IOCTL syscall with the address of a macro defined in the
linux header file userfaultfd.h

These macros aren't imported correctly from the C package, so you can manually compute their address to use them.

Example to get the address of the UFFDIO_COPY macro:

1. Find the definition of UFFDIO_COPY in linux/userfaultfd.h

```
#define UFFDIO_COPY             _IOWR(UFFDIO, _UFFDIO_COPY, struct uffdio_copy)
```

2. Copy the definition to the following C script address.c

```
#include <stdio.h>
#include <linux/ioctl.h>
#include <linux/userfaultfd.h>
	int main() {
		printf("(hex)%x\n", _IOWR(UFFDIO, _UFFDIO_COPY, struct uffdio_copy));
	}
```

3. Run the script:

```
gcc -o address address.c
./address
```

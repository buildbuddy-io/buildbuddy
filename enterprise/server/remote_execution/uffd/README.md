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

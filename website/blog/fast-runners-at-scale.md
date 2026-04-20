---
slug: fast-runners-at-scale
title: "Snapshot, Chunk, Clone: Fast Runners at Scale"
authors: maggie
date: 2026-01-26 12:00:00
image: /img/fast-runners-at-scale.png
tags: [performance, infrastructure]
---

At BuildBuddy, our core mission is to make builds faster. We use Firecracker snapshots to keep warmed-up workers available on demand.

With as little as a curl request, you can run any bash command in a warm worker.

<!-- truncate -->

## Painting the scene

Imagine if every morning you had to re-initialize the developer environment on your laptop from scratch - re-installing tools, cloning git repos, downloading container images and starting up various applications. I’m shuddering just imagining the time and effort.

Not only would the setup be slow, but all initial workloads would also be slower. Applications and the kernel would have to repopulate various memory and disk caches, and download tools and dependencies.

Cold starts are a familiar problem in computing. Many CI platforms, like GitHub Actions, initialize a new worker for each workload, and AI coding agents typically run on relatively cold runners. This ensures each environment is clean, consistent, and isolated from other customers’ workloads. Unfortunately it also often means each workload takes minutes longer to run.

With snapshotted runners, you can pay the slow setup cost of initializing a cold worker once, and clone the warmed up worker for future workloads.

These workers have many valuable properties:

- **Flexibility**: Each worker has its own kernel, increasing security and enabling complex workloads that need Docker or custom images.
- **Copy on write**: Parallel workers can be cloned from a shared baseline while changes are copied on write, allowing different workloads to run without interference.
- **Scalability**: Can be cloned over the network, giving us an infinitely scalable fleet of fast runners.
- **Persistence**: Can be stored on disk, making workers resilient to machine restarts and auto-scaling.
- **High-capacity**: Can handle large workloads requiring up to 100GB of disk and substantial memory, which is often necessary for monorepos or memory-heavy tasks like running ML models.
- **Incremental updates**: Can be incrementally updated with each workload, reducing environment drift, even across weeks of development.
- **Memory persistence**: Can preserve in-memory state across workloads, allowing expensive initialization work - like Bazel’s analysis graph and JVM heap state - to be reused.

For our repo, a fully cached Bazel build takes about 6 minutes. On GitHub Actions, 3.5 minutes are spent initializing the worker - installing tools, checking out a sparse GitHub repo, and building Bazel’s analysis graph before the build even starts. That means over a third of CI time is spent on setup alone!

With snapshotted runners, our median CI run is now only 30 seconds! For smaller repos, we've seen tests run in 6 seconds.

This snapshot-based runner architecture underpins both **BuildBuddy Workflows**, our CI product, and **Remote Bazel**, our general-purpose remote runners. (Note: Even though the product name Remote Bazel mentions bazel, you can run any bash command on our snapshotted runners.)

Looking forward, we’re especially excited to integrate **Remote Bazel** with AI coding agents. As agents make developers faster, our runners will make the agents themselves faster.

## Limitations with Firecracker

Firecracker is a virtualization technology that lets you snapshot a running VM. A snapshot serializes the entire machine to a file - this includes everything from its disk to its memory to its internal kernel state.

(Technically the snapshot files only contain the memory and kernel state, but for simplicity’s sake we are referring to both the contents of the VM’s memory and disk, as both must be serialized, cached, and transferred in a similar manner.)

If you resume a snapshot file in a different Firecracker process, you will get an exact copy of your original VM, and the original VM will not be affected. From that point on, the original VM and its clone are unique and can be used for different purposes.

Cloning Firecracker VMs worked remarkably well to resolve the cold start problem. When workloads landed on a warm runner, they were often several minutes faster.

However, managing snapshot files quickly became a challenge.

Let’s say your workload requires 20GB of memory and 80GB of disk (common with Bazel builds). That directly translates to a 20GB memory snapshot file, and a 80GB file-backed disk image. All 100GB of data must be present on the host for the entire lifetime of the running VM. If you wanted to run this snapshot on a different machine, you’d need to transfer all 100GB of data over the network.

Resuming multiple workloads from the same snapshot file was another technical challenge. Our host machines use the ext4 filesystem, which doesn’t support copy-on-write. We needed workloads to be isolated from each other, so we couldn’t directly modify the disk images. But duplicating an 80GB disk image for each clone was also prohibitively expensive.

## Chunked snapshots to the rescue!

We realized that if we could split each snapshot into smaller chunks, it would be much easier to manage. We could store the chunks in a remote cache and easily share them across multiple machines. We could compress the chunks for more efficient storage, replicate them across zones for redundancy, and deduplicate data across workloads.

When taking a snapshot, we split the memory and disk files into chunks. We also maintain a mapping from addresses in the snapshot file to a unique hash identifying each chunk of data.

For example, the metadata for the memory snapshot might look like:

```
- Hash `abc` => \{ memory addresses 0x00 - 0x100 }
- Hash `def` => \{ memory addresses 0x101 - 0x200 }
- …
```

The metadata for the disk snapshot might look like:

```
- Hash `111` => \{ offsets 0 - 100 }
- Hash `222` => \{ offsets 101 - 200 }
- …
```

At snapshot generation time, chunks are compressed and saved in a remote cache. Later, when a clone tries to read this snapshot data, each chunk is lazily downloaded. This allows for quicker startup than downloading the entire snapshot file in advance.

Of course, saving chunked data is only half the battle. The kernel expects memory and disk to look a certain way. By default, Firecracker expects the entire memory and disk snapshot files to live on disk at VM startup. It isn’t expecting to do fancy things like serve compressed memory from a remote cache.

We realized we’d need to manage disk and memory access ourselves. Around this time, we came upon this [excellent blog series from Code Sandbox](https://codesandbox.io/blog/how-we-clone-a-running-vm-in-2-seconds) about Userfaultfd (UFFD). UFFD lets users implement custom handlers for managing memory. Using UFFD, we could load memory data from wherever we wanted! Similarly, we could use FUSE to implement a custom handler for managing disk access.

## Trip down memory lane

Typically, when a process requests memory, the kernel assigns _virtual memory_ to it. This memory isn’t guaranteed to exist in RAM. Often its contents will live on disk, letting a machine “use more memory” than the physical limits of its RAM chip, which is typically much more expensive than disk.

When a process tries to access virtual memory that isn’t in RAM, it’s the kernel’s job to load the data from disk into RAM. This process is called a _page fault_.

For example, let’s say a process tries to access memory page 0x100 that isn’t in RAM:

1. The kernel receives a page fault notification for address 0x100.
1. The kernel will use internal data structures to map this address to a location on disk. It will then read the data from disk.
1. The kernel will copy that data from disk into RAM.
1. Now the process can read and write the data to/from RAM.

When resuming from a Firecracker snapshot, things look mostly similar. Normally, when a process accesses memory that isn’t in RAM, the kernel loads it from wherever it was previously stored on disk. When resuming from a Firecracker snapshot, the kernel will load the data directly from the snapshot file.

Using the same example from above:

1. The kernel receives a page fault notification for address 0x100.
1. This address will be mapped to a location within the snapshot file.
1. The kernel will copy the data from the snapshot file, which lives on disk, into RAM.
1. Now the process can read and write the data to/from RAM.

## Custom memory management with UFFD

UFFD, which stands for userfault file descriptor, is a kernel feature that lets you define custom behavior for handling page faults.

In other words, rather than relying on the default kernel behavior to handle page faults, developers can write custom programs that load memory data. Rather than loading memory data from a snapshot file on disk, our UFFD handler loads it chunked and compressed from a remote cache.

Firecracker has built-in support for resuming from a snapshot with UFFD. When you configure Firecracker to use UFFD, the following happens:

1. Firecracker will create a UFFD file descriptor registered for the memory ranges used by the VM.

- Let’s say the VM uses 1GB of memory starting from address 0x100.
- Firecracker would initialize UFFD using the UFFDIO_REGISTER ioctl with parameters \{ `start=0x100`, `len=1e9` }.
- Now if there is a page fault within that memory range, the kernel will delegate its resolution to the custom UFFD handler, as opposed to handling it with the default behavior.

2. Firecracker will send the UFFD file descriptor over a configured Unix socket.

On our side, we run a Go program that functions as the UFFD handler:

1. The handler listens for the UFFD file descriptor that Firecracker sent on the configured Unix socket.
1. Once received, it polls the file descriptor for page fault events.

- Each event indicates the VM faulted when trying to access a memory address, and includes the faulting address.

3. It is now the handler’s responsibility to supply the requested memory data to the guest.
1. Using the [snapshot metadata file](#chunked-snapshots-to-the-rescue), the handler will map the faulting address to a snapshot chunk.

- In the example above, address `0x100` is mapped to chunk `abc`.

5. It then fetches and decompresses the requested chunk from the remote cache.
1. It then uses the `UFFDIO_COPY` operation to copy the data into the faulting memory address.

- `UFFDIO_COPY` copies data from a user-space buffer into the faulting address, resolving the page fault.
- Once the data has been copied over, the kernel wakes up the process that originally faulted and allows it to continue execution.

7. Now the process can read the requested data from RAM!

- From the VM’s perspective, this looks exactly like a normal page fault being handled by the kernel.

In step #5, snapshot chunks are cached locally on the host. Other VMs on the same host can reuse these chunks without re-downloading them, improving performance.

Memory writes don’t have to be explicitly handled by the UFFD handler. After a page fault is resolved, the page is owned by the VM. The VM can then read and write to that page freely without triggering additional page faults, and the UFFD handler is no longer involved in managing that memory address.

## FUSE for disk management

On the disk side, we use FUSE, a kernel feature that lets you define custom behavior for handling filesystem operations.

Firecracker expects disk images to be attached as files. For example, if not using FUSE, you might attach a disk image file `rootfs.img` to `/dev/vdx `on the VM. Any reads from `/dev/vdx` on the VM will read the underlying data from the `rootfs.img` file.

If you wanted multiple clones to start with the same disk contents, you could attach `rootfs.img` to each VM.

Remember, transferring and duplicating a massive disk image file is very costly. So rather than attaching a monolithic disk image file on our VMs, we attach a FUSE-backed virtual disk. This is allowed by Firecracker as long as the FUSE device adheres to the file interface (i.e. supports `Read`, `Write`, and `Fsync`).

When initializing a FUSE-backed disk, you configure the path it should be attached to and a FUSE handler. Our FUSE handler is a Go program that implements the `Reader` and `Writer` interfaces.

Let’s say a FUSE block device is attached at `/dev/vdx` and a process in the VM tries to read from offset 150 of `/dev/vdx`:

1. Because `/dev/vdx` was configured with FUSE, the kernel forwards the request to the FUSE server.
1. The FUSE server calls the configured FUSE handler - a Go program that implements the `Read` method.

`Read(ctx context.Context, p []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) \{...}`

3. Within `Read`, the handler uses the [snapshot metadata file](#chunked-snapshots-to-the-rescue) to map the offset to a snapshot chunk.

- In our earlier metadata example, offset 150 corresponds to chunk `222`.

4. It fetches and decompresses the requested chunk from the remote cache.
1. It then populates the requested data in the read buffer (`p` in the function signature from above).

For writes, we use _copy-on-write semantics_ to avoid modifying the original snapshot chunk, which may be used by other clones running in parallel.

Let’s say a process tries to write to offset 150 on `/dev/vdx`:

1. Because `/dev/vdx` was configured with FUSE, the kernel forwards the request to the FUSE server.
1. The FUSE server calls the configured FUSE handler - a Go program that implements the `Write` method.

`Write(ctx context.Context, p []byte, off int64) (uint32, syscall.Errno) \{...}`

3. Within `Write`, the handler uses the snapshot metadata file to map the offset to a snapshot chunk.

- In our earlier metadata example, offset 150 corresponds to chunk `222`.

4. It fetches and decompresses the requested chunk from the remote cache.
1. Rather than writing directly to the fetched chunk, the handler duplicates the chunk to a new chunk with a `.dirty` suffix.

- Chunk `222` would be duplicated to `222.dirty`.

6. New data is written to the `.dirty` chunk, and the original chunk is untouched.
7. The handler tracks all `.dirty` chunks, and ensures that all future reads and writes to this offset use the `.dirty` chunk.

At the end of the workload, when taking a new snapshot, only dirtied chunks are uploaded to the cache. A new hash is generated for each dirtied chunk, and the metadata file is updated accordingly. Unmodified chunks are not re-uploaded, minimizing network transfer.

## Network configuration for clones

The final part of the puzzle is networking. A VM snapshot includes a fixed network configuration (such as an IP address and interface name). If you ran two clones from the same snapshot file on the same host machine, they’d encounter IP and routing conflicts. Each VM needs a unique network identity without changing the underlying snapshot file.

As recommended in the Firecracker documentation, we handle this using _Linux network namespaces_. Each network namespace has its own network interfaces, routing table, iptables rules, and port space, all managed by the host kernel.

Although every cloned VM uses the same IP address and interface configuration from the shared snapshot, each runs in a separate network namespace. From the VM’s perspective, it is the only machine on the network. The network configurations will remain fully isolated and won’t conflict.

To support external network connectivity, we enable NAT masquerading on the host. NAT masquerading lets multiple VMs share the host’s public IP address for outbound traffic, eliminating the need to assign a unique public IP to each VM. The host kernel tracks these connections using a connection table called _conntrack_, which ensures that return traffic is routed back to the correct VM and network namespace.

## How do I use this?

Running commands on a snapshotted Firecracker runner is as easy as issuing a CURL request.

To get started, you can create a free BuildBuddy account [here](https://www.buildbuddy.io/) to generate an API key. A sample CURL request might look like:

```
curl --data '{
"steps": [{ "run": "echo ‘HELLO WORLD!!!’" }]
}' \
--header 'x-buildbuddy-api-key: YOUR_API_KEY' \
--header 'Content-Type: application/json' \
https://app.buildbuddy.io/api/v1/Run
```

The bash script in the `run` block will be executed in the VM.

If you issue a second CURL request, or multiple subsequent CURL requests in parallel, they will reuse the same runner that was created from the first request.

We support configuring the operating system, architecture, machine size (number of cores, disk size, and memory), container image, and more on our runners.

As long as the machine configuration stays the same, future requests will use the same snapshotted runner. Runners can even be shared across teammates who all need the same startup environment.

We have two products built on top of this snapshot-based architecture:

With [BuildBuddy Workflows](https://www.buildbuddy.io/docs/workflows-setup/), our CI product, you can run commands on snapshotted runners with every push and pull request to your GitHub repository.

[Remote Bazel](https://www.buildbuddy.io/docs/remote-bazel/), the general-purpose remote runner product showcased above, is more flexible. You can run commands on a snapshotted runner via CURL request or using our `bb` CLI.

## What’s next?

One area we’re particularly excited about is teaching AI coding agents to use Remote Bazel to run expensive commands that would benefit from warm workspaces.

Remote Bazel helps overcome constraints running builds locally or on the resource-constrained runners provided by AI providers.

Agents can be taught to use Remote Bazel by including the CURL request in an agent’s [skills file](https://platform.claude.com/docs/en/agents-and-tools/agent-skills/overview?utm_source=chatgpt.com) or developing a lightweight MCP server that makes the CURL request. I hope to write future blog posts outlining how to do this.

If you have any questions, comments, or feedback on where you’d like to see us take this technology, please reach out! Especially if you have thoughts on the best way to integrate this with AI agents, I’d love to hear from you.

For audio-visual learners, I’ve also spoken about this technology at a couple conferences. Here’s a [technical overview](https://www.youtube.com/watch?v=k30xZfiRZYo&list=PLbzoR-pLrL6rUiqylH-kumoZCWntG1vjp&index=33) of how we implement snapshotted Firecracker runners, and here’s a [product overview](https://www.youtube.com/watch?v=BM2gsH2Ao04&list=PLbzoR-pLrL6ptKfAQNZ5RS4HMdmeilBcw&index=14) with more examples of how to take advantage of the technology.

You can find me on our open source Slack channel @maggie or by email at maggie@buildbuddy.io.

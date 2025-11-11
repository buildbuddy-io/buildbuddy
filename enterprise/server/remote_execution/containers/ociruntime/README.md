# ociruntime Package

The `ociruntime` package provides OCI (Open Container Initiative) container runtime support for BuildBuddy's remote execution system. It manages the full lifecycle of pulling container images, preparing filesystems, and executing commands in isolated OCI containers using runtimes like crun, runc, or runsc.

## What OCI Image Data Does ociruntime Read?

### 1. OCI Image Manifests
- **Source**: Container registries (Docker Hub, GCR, GHCR, etc.)
- **Content**: Image manifests containing layer references, config file references, and metadata
- **Location**: Retrieved via the `Resolver` in `enterprise/server/util/oci/oci.go`
- **Function**: `ImageStore.pull()` at `ociruntime.go:1554`

### 2. Image Configuration Files
- **Format**: JSON files conforming to OCI Image Spec
- **Content**:
  - Environment variables
  - User/UID settings
  - Entrypoint and CMD directives
  - Working directory
  - Exposed ports
- **Storage**: Cached in-memory in `Image.ConfigFile` (type `ctr.ConfigFile`)
- **Function**: Retrieved concurrently with layers at lines 1612-1620

### 3. Image Layers (Compressed Tarballs)
- **Source**: Container registry blob storage
- **Format**: Compressed tar archives (gzip, zstd, etc.)
- **Content**: Filesystem deltas containing:
  - Regular files
  - Directories
  - Symlinks
  - Hard links
  - File permissions and ownership
  - Whiteout markers (for deleted files: `.wh.*` prefix files)
  - Opaque directory markers (`.wh..wh..opq`)
- **Function**: `downloadLayer()` at line 1633
- **Processing**: Each layer is:
  1. Downloaded via `layer.Uncompressed()` (automatic decompression)
  2. Extracted as a tar archive
  3. Whiteout files converted to overlayfs markers

## What Gets Written to Disk?

### 1. Image Layer Cache

**Directory Structure**:
```
{cacheRoot}/images/oci/v2/{algorithm}/{hash}/
  ├── bin/
  ├── usr/
  ├── lib/
  └── ... (complete extracted layer filesystem)
```

- **Location**: `{cacheRoot}/images/oci/v2/{algorithm}/{hash}/`
- **Version**: Currently "v2" (`imageCacheVersion` constant)
- **Content**: Each layer extracted as a complete directory tree
- **Purpose**: Reusable read-only layers for overlayfs mounts
- **Key Details**:
  - Empty directories are skipped (optimization)
  - Whiteout files become character devices (overlayfs deletion markers)
  - Opaque directories marked with `trusted.overlay.opaque` xattr
  - File ownership (UID/GID) and permissions preserved
- **Functions**:
  - `layerPath()` at line 1480
  - `downloadLayer()` at line 1633-1754

### 2. Container Bundle Directory

**Directory Structure** (per container):
```
{containersRoot}/{containerID}/
  ├── config.json               # OCI runtime spec
  ├── rootfs/                   # Overlayfs mount point
  ├── tmp/
  │   ├── rootfs.work/          # Overlayfs workdir
  │   ├── rootfs.upper/         # Overlayfs upperdir (writable layer)
  │   ├── merged0/              # Intermediate merged mounts (for >20 layers)
  │   ├── merged0.work/
  │   ├── merged0.upper/
  │   └── ...
  ├── hostname                  # "localhost"
  ├── hosts                     # /etc/hosts content
  ├── resolv.conf               # DNS configuration
  └── proc_cgroups              # Fake /proc/cgroups content
```

#### Bundle Components:

#### a. config.json (OCI Runtime Specification)
- **Location**: `{bundlePath}/config.json`
- **Format**: JSON file conforming to OCI Runtime Spec v1.1.0-rc.3
- **Content**:
  - Process configuration (user, args, env, cwd, capabilities)
  - Root filesystem path
  - Mount specifications (~15-20 standard mounts)
  - Linux namespace configuration (PID, IPC, UTS, Mount, Cgroup, Network)
  - Cgroup settings
  - Security settings (seccomp profile, masked paths, readonly paths)
- **Function**: `createBundle()` at line 510, `createSpec()` at line 1046

#### b. rootfs/ (Container Root Filesystem)
- **Location**: `{bundlePath}/rootfs/`
- **Type**: Overlayfs mount point
- **Mount Structure**:
  ```
  overlayfs
    ├── lowerdir: layer1:layer2:layer3:...:layerN (read-only image layers)
    ├── upperdir: {bundlePath}/tmp/rootfs.upper (writable layer)
    └── workdir: {bundlePath}/tmp/rootfs.work (overlayfs working directory)
  ```
- **Layer Ordering**: Reversed from manifest (uppermost layer first in lowerdir)
- **Function**: `createRootfs()` at line 942-1044
- **Special Handling**:
  - For images with >20 layers, intermediate merged mounts are created to stay within kernel mount option length limits (4095 bytes)
  - Mount options include: `userxattr,volatile` for performance

#### c. Supplementary Files
- **hostname**: Contains "localhost"
- **hosts**: `/etc/hosts` content with container name mapping
- **resolv.conf**: DNS nameserver configuration (default: 8.8.8.8)
- **proc_cgroups**: Fake `/proc/cgroups` file for container compatibility

### 3. Container Mounts

The following get mounted into the container rootfs:

#### Standard Mounts:
- `/proc` - proc filesystem
- `/dev` - tmpfs (65MB) with standard devices
- `/dev/pts` - devpts for pseudo-terminals
- `/dev/shm` - tmpfs (64MB) for shared memory
- `/dev/mqueue` - mqueue for POSIX message queues
- `/sys` - sysfs (read-only)
- `/sys/fs/cgroup` - cgroup filesystem (read-only)
- `/etc/hosts` - bind mount from bundle
- `/etc/hostname` - bind mount from bundle
- `/etc/resolv.conf` - bind mount from bundle or host
- `/proc/cgroups` - bind mount of fake file

#### Work Directory Mount:
- `{execrootPath}` (default: `/buildbuddy-execroot`) - bind mount of actual workdir

#### Optional Mounts:
- Tini binary at `/usr/local/buildbuddy-container-tools/tini` (if `--executor.oci.enable_tini=true`)
- LXCFS mounts for virtualized `/proc/cpuinfo`, `/proc/meminfo`, etc. (if `--executor.oci.enable_lxcfs=true`)
- Persistent volumes (if configured)
- Custom mounts from `--executor.oci.mounts` flag

### 4. Cgroup Filesystem
- **Location**: Managed by the cgroup package
- **Settings Written**:
  - `cpu.max` - CPU quota limits
  - `memory.max` - Memory limits
  - `pids.max` - Process ID limits
  - `memory.oom.group` - OOM killer behavior
- **Function**: `setupCgroup()` (referenced at line 544)

### 5. Network Namespaces
- **Location**: Network namespace files created by the networking package
- **Referenced**: In OCI spec at lines 1174-1176

## Key Functions

### Reading Operations:

#### `ImageStore.Pull()` (line 1526)
Main entry point for pulling images from registries.

#### `ImageStore.pull()` (line 1554)
Core pull logic that coordinates manifest retrieval, layer downloads, and config parsing.

#### `downloadLayer()` (line 1633)
Downloads and extracts individual layers. Handles:
- Automatic decompression
- Tar extraction
- Whiteout conversion to overlayfs format
- Xattr setting for opaque directories

#### `ImageStore.CachedImage()` (line 1546)
Retrieves cached image metadata without pulling from registry.

### Writing Operations:

#### `createBundle()` (line 510)
Creates the OCI bundle directory structure with all necessary files:
- config.json (OCI runtime spec)
- rootfs/ mount point
- Supplementary files (hostname, hosts, resolv.conf)

#### `createRootfs()` (line 942)
Mounts overlayfs for the container filesystem:
- Combines all image layers as lowerdir
- Creates writable upperdir
- Handles >20 layer case with intermediate merges

#### `createSpec()` (line 1046)
Generates the OCI runtime specification JSON file with:
- Process configuration
- Mount table
- Namespace setup
- Security settings

#### `setupCgroup()` (referenced line 544)
Configures cgroup limits for the container.

## Layer Storage and Management

### Layer Caching Strategy
- **Deduplication**: Layers identified by DiffID (uncompressed hash)
- **Concurrent Downloads**: Single-flight pattern prevents duplicate downloads
- **Reusability**: Multiple containers share the same cached layers
- **Version Management**: Old cache versions automatically cleaned up

### Overlayfs Layering
- Image layers mounted as read-only `lowerdir` entries (colon-separated list)
- Writable `upperdir` captures all container modifications
- `workdir` used by overlayfs for atomic operations
- For high layer counts (>20), intermediate merged mounts prevent kernel mount option length limits

### Whiteout Handling (OCI Standard)
- `.wh.{filename}` → Character device created with major/minor 0/0 (overlayfs deletion marker)
- `.wh..wh..opq` → Directory marked opaque with `trusted.overlay.opaque` xattr set to "y"

## Container Runtime Spec Details

The OCI runtime spec (`config.json`) includes:
- **Version**: OCI 1.1.0-rc.3
- **Process**: User, environment variables, arguments, capabilities, rlimits
- **Root**: Path to rootfs, read/write mode
- **Mounts**: Complete mount table (~15-20 standard mounts)
- **Linux**: Namespaces (PID, IPC, UTS, Mount, Cgroup, Network)
- **Security**: Seccomp profile, masked paths, readonly paths
- **Devices**: Standard devices (/dev/null, /dev/zero, /dev/random, /dev/urandom, /dev/full, /dev/tty)

## Architecture Benefits

This comprehensive architecture allows BuildBuddy to:
- Efficiently run containerized builds with full OCI image support
- Deduplicate layers across multiple containers
- Provide proper filesystem isolation using overlayfs
- Support multiple OCI-compliant runtimes (crun, runc, runsc)
- Cache image layers for fast container startup
- Handle complex images with many layers
- Maintain compatibility with the OCI standards

## File Locations

**Main implementation**: `enterprise/server/remote_execution/containers/ociruntime/ociruntime.go`

**Key dependencies**:
- `enterprise/server/util/oci`: Registry authentication and image resolution
- `enterprise/server/remote_execution/cgroup`: Cgroup management
- `github.com/google/go-containerregistry`: OCI image manipulation
- Container runtimes: crun, runc, or runsc

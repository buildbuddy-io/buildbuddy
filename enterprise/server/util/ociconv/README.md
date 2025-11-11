# ociconv Package

The `ociconv` package converts OCI (Open Container Initiative) container images into ext4 filesystem disk images for use with Firecracker VMs and OCI runtimes. It serves as a bridge between container registries and disk-based container execution environments.

## What Data Does ociconv Read?

### 1. OCI Container Images from Container Registries
- **Source**: Remote container registries (Docker Hub, etc.)
- **Protocol**: Uses OCI registry API via `oci.Resolver`
- **Authentication**: Supports credentials (username/password)
- **Format**: Standard OCI container image format
- **Location**: `ociconv.go:186` - `resolver.Resolve()`

### 2. Container Image Layer Tarballs
- **Source**: Extracted from OCI images via `mutate.Extract(img)`
- **Format**: Tar archives containing filesystem layers
- **Processing**: Piped directly to tar command for extraction (`ociconv.go:199-206`)

### 3. Cached Disk Images (Checking)
- **Location**: `{cacheRoot}/images/ext4/{hashedContainerName}/{imageHash}/containerfs.ext4`
- **Purpose**: Check if conversion already exists before re-downloading
- **Key Functions**:
  - `CachedDiskImagePath()` (lines 68-101)
  - `getDiskImagesPath()` (lines 60-63)

### 4. Overlayfs Layer Directories
- **Source**: Local filesystem directories with overlayfs format
- **Format**: Directory structure with overlayfs whiteout markers
- **Special Files Read**:
  - `.wh.*` prefix files (whiteout markers for deleted files)
  - `.wh..wh..opq` (opaque directory markers)
  - Extended attributes: `trusted.overlay.opaque` (line 263)
  - Character devices with device number 0 (whiteout markers, line 289)
- **Key Function**: `OverlayfsLayerToTarball()` (lines 222-310)

### 5. File Metadata and Contents
- Regular files, directories, symlinks, character devices
- File permissions, ownership, and extended attributes
- Used for walking directories and creating tar headers

## What Does ociconv Write to Disk?

### 1. ext4 Disk Images (Primary Output)
- **Format**: ext4 filesystem image files
- **Filename**: `containerfs.ext4`
- **Location**: `{cacheRoot}/images/ext4/{hashedContainerName}/{imageHash}/containerfs.ext4`
- **Process**:
  1. Creates temporary file `containerfs-*.ext4` (line 209)
  2. Writes ext4 image via `ext4.DirectoryToImageAutoSize()` (line 215)
  3. Renames to final location with hash-based directory (lines 171-178)
- **Key Functions**: `createExt4Image()` and `convertContainerToExt4FS()`

### 2. Temporary Extraction Directories
- **Location**: `{workspaceDir}/unpack-*` (temporary)
- **Purpose**: Intermediate storage for tar extraction before conversion
- **Cleanup**: Automatically removed via `defer os.RemoveAll()` (line 197)
- **Created at**: Lines 193-196

### 3. Tar Archive Data
- **Format**: Uncompressed OCI layer tarball format
- **Destination**: Provided `io.Writer` (could be file, network, memory)
- **Contents**:
  - File headers with metadata
  - File contents
  - Overlayfs whiteout markers converted to `.wh.*` format
  - Directory opaque markers as `.wh..wh..opq` files
- **Key Function**: `OverlayfsLayerToTarball()` (lines 222-310)

### 4. Cache Directory Structure
- **Path Pattern**: `{cacheRoot}/images/ext4/{hash(containerImage)}/{hash(ext4file)}/`
- **Example**: `/cache/images/ext4/abc123def456/789ghi012jkl/containerfs.ext4`
- **Organization**:
  - First hash: Based on container image reference
  - Second hash: SHA256 of the actual ext4 file contents
  - Allows multiple versions of same tag (e.g., `:latest`)

## Key Functions

### Reading Operations

#### `CachedDiskImagePath()` (lines 68-101)
Checks if disk image already exists in cache. Reads directory listings to find most recent cached image. Returns path or empty string if not found.

#### `CreateDiskImage()` (lines 111-145)
Main entry point for getting/creating disk images. Reads from cache if available. Authenticates with registry even when cached. Deduplicates concurrent conversions via singleflight.

#### `convertContainerToExt4FS()` (lines 185-220)
Resolves and downloads OCI image from registry. Extracts container layers using tar command. Reads extracted filesystem.

#### `OverlayfsLayerToTarball()` (lines 222-310)
Walks overlayfs layer directory structure. Reads file metadata, contents, and extended attributes. Handles special overlayfs markers.

#### `hashFile()` (lines 44-55)
Reads entire file to compute SHA256 hash. Used for content-addressable storage.

### Writing Operations

#### `createExt4Image()` (lines 158-181)
Creates temporary ext4 image file. Writes to final cache location with hash-based naming. Uses `os.Rename()` for atomic file placement.

#### `convertContainerToExt4FS()` (lines 185-220)
Writes temporary extraction directory. Writes final ext4 image file via `ext4.DirectoryToImageAutoSize()`.

#### `OverlayfsLayerToTarball()` (lines 222-310)
Writes tar headers and file contents to provided writer. Converts overlayfs format to OCI tarball format.

## Usage Context

The package is used by:
- **Firecracker runtime** (`firecracker.go`): Converts container images to ext4 for VM execution
- **OCI runtime** (`ociruntime.go`): Converts overlayfs layers back to tarballs for image operations

## Key Dependencies

- `enterprise/server/util/ext4`: Creates and extracts ext4 filesystem images
- `enterprise/server/util/oci`: Resolves and authenticates with container registries
- `google/go-containerregistry`: OCI image manipulation
- `tar` command: Extracts container layers
- `unix.Getxattr`: Reads overlayfs extended attributes

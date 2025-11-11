# ocimaterialize Package

The `ocimaterialize` package provides pure data transformation functions for materializing OCI container image layers to filesystem directories. It is designed to operate downstream of image resolution, taking explicit data streams as input rather than handling registry operations or authentication.

## Design Philosophy

This package focuses solely on **data transformation**:
- Takes standard library types (`io.ReadCloser`) as input
- No dependencies on go-containerregistry interfaces
- No knowledge of OCI registries, authentication, or image resolution
- Clear contracts about data formats (compressed vs uncompressed)
- Caller controls all upstream operations (resolution, caching, decompression)

## Package Interface

### Core Functions

#### ExtractLayer

```go
func ExtractLayer(ctx context.Context, tarStream io.ReadCloser, destDir string) error
```

Extracts a single OCI layer to a directory with overlayfs whiteout handling.

**DATA FORMAT REQUIREMENTS:**
- `tarStream`: UNCOMPRESSED tar archive
  - Must contain valid tar headers
  - Must follow OCI layer tar format
  - Caller is responsible for decompression
  - Will be closed by this function (defer not needed)

**WHITEOUT HANDLING:**
- Files named `.wh.<filename>` are converted to character devices (major:minor 0:0)
- Files named `.wh..wh..opq` mark directories opaque (sets `trusted.overlay.opaque` xattr)

**BEHAVIOR:**
- `destDir` will be created if it doesn't exist
- Files are extracted with their original permissions and ownership
- Existing files in `destDir` are overwritten

**ERRORS:**
- Returns `InvalidArgumentError` for malformed tar data
- Returns `FailedPreconditionError` if destDir parent doesn't exist
- Returns `PermissionDeniedError` for filesystem permission issues

#### ExtractLayerWithOptions

```go
func ExtractLayerWithOptions(
    ctx context.Context,
    tarStream io.ReadCloser,
    destDir string,
    opts *ExtractOptions,
) error

type ExtractOptions struct {
    // WhiteoutFormat controls how .wh.* files are handled
    WhiteoutFormat WhiteoutFormat

    // PreserveOwnership controls whether to preserve UID/GID from tar headers
    // If false, files are created with the current user's ownership
    PreserveOwnership bool

    // VerifyHash optionally validates the extracted content
    // If set, the uncompressed tar stream will be hashed and compared
    VerifyHash *Hash  // nil to skip verification
}

type Hash struct {
    Algorithm string  // e.g., "sha256"
    Hex       string  // hex-encoded digest
}

type WhiteoutFormat int
const (
    // WhiteoutOverlayfs converts .wh.* to char devices (for overlayfs)
    WhiteoutOverlayfs WhiteoutFormat = iota

    // WhiteoutAUFS keeps .wh.* files as regular files (for AUFS)
    WhiteoutAUFS

    // WhiteoutNone ignores whiteout files (for fully merged filesystems)
    WhiteoutNone
)
```

Provides more control over extraction behavior.

#### FlattenLayers

```go
func FlattenLayers(ctx context.Context, layerDirs []string, destDir string) error
```

Merges multiple overlayfs-format layer directories into a single filesystem.

**REQUIREMENTS:**
- `layerDirs` must be ordered from base (oldest) to top (newest)
- Layer directories must be in overlayfs format (from `ExtractLayer`)

**OVERLAYFS SEMANTICS:**
- Files in upper layers override files in lower layers
- Character devices with major:minor 0:0 indicate deletions (file is removed from merge)
- Directories with `trusted.overlay.opaque="y"` xattr are opaque (lower contents hidden)

**BEHAVIOR:**
- `destDir` will contain the final merged filesystem
- Original layer directories are not modified

#### CreateExt4FromDirectory

```go
func CreateExt4FromDirectory(
    ctx context.Context,
    srcDir string,
    outputPath string,
    sizeBytes int64,
) error
```

Creates an ext4 filesystem image from a directory.

**PARAMETERS:**
- `srcDir`: Directory containing a complete filesystem (e.g., from `FlattenLayers`)
- `outputPath`: Where the ext4 image file will be written
- `sizeBytes`: Size for the ext4 image (0 for automatic sizing based on content)

**BEHAVIOR:**
- Creates a standard ext4 filesystem image
- All files and directories from `srcDir` are included
- File permissions, ownership, and attributes are preserved

#### LayerToTarball

```go
func LayerToTarball(ctx context.Context, layerDir string, tarWriter io.Writer) error
```

Converts an overlayfs-format layer directory back to an OCI tar stream.

**INVERSE OPERATION:**
- This is the inverse of `ExtractLayer`
- Converts character devices with major:minor 0:0 → `.wh.<filename>` files
- Converts directories with `trusted.overlay.opaque="y"` → `.wh..wh..opq` files

**OUTPUT:**
- `tarWriter` receives UNCOMPRESSED tar data
- Caller is responsible for compression if needed

## Usage Examples

### Example 1: Basic Layer Extraction

```go
// Caller resolves the image
resolver := oci.NewResolver(env)
img, err := resolver.Resolve(ctx, "gcr.io/project/image:tag", platform, creds)
if err != nil {
    return err
}

// Caller gets layers
layers, err := img.Layers()
if err != nil {
    return err
}

// Extract each layer using ocimaterialize
for _, layer := range layers {
    // Caller gets UNCOMPRESSED stream
    rc, err := layer.Uncompressed()
    if err != nil {
        return err
    }

    // Caller decides destination
    diffID, _ := layer.DiffID()
    destDir := filepath.Join("/cache/layers", diffID.Algorithm, diffID.Hex)

    // ocimaterialize just extracts
    if err := ocimaterialize.ExtractLayer(ctx, rc, destDir); err != nil {
        return err
    }
}
```

### Example 2: Extraction with Verification

```go
for _, layer := range layers {
    rc, err := layer.Uncompressed()
    if err != nil {
        return err
    }

    diffID, _ := layer.DiffID()
    destDir := filepath.Join("/cache/layers", diffID.Algorithm, diffID.Hex)

    // Verify hash during extraction
    err = ocimaterialize.ExtractLayerWithOptions(ctx, rc, destDir, &ocimaterialize.ExtractOptions{
        WhiteoutFormat: ocimaterialize.WhiteoutOverlayfs,
        PreserveOwnership: true,
        VerifyHash: &ocimaterialize.Hash{
            Algorithm: diffID.Algorithm,
            Hex:       diffID.Hex,
        },
    })
    if err != nil {
        return err
    }
}
```

### Example 3: From Compressed File on Disk

```go
// Caller handles decompression
compressedFile, err := os.Open("/path/to/layer.tar.gz")
if err != nil {
    return err
}
defer compressedFile.Close()

// Decompress before passing to ocimaterialize
gzReader, err := gzip.NewReader(compressedFile)
if err != nil {
    return err
}
defer gzReader.Close()

// Extract the decompressed stream
err = ocimaterialize.ExtractLayer(ctx, gzReader, "/output/layer")
```

### Example 4: Complete Image Materialization Pipeline

```go
// 1. Resolve image (caller's responsibility)
resolver := oci.NewResolver(env)
img, err := resolver.Resolve(ctx, "gcr.io/project/image:tag", platform, creds)
if err != nil {
    return err
}

// 2. Get layers (caller's responsibility)
layers, err := img.Layers()
if err != nil {
    return err
}

// 3. Extract each layer to separate directories
var extractedDirs []string
for _, layer := range layers {
    rc, err := layer.Uncompressed()
    if err != nil {
        return err
    }

    diffID, _ := layer.DiffID()
    destDir := filepath.Join("/cache/layers", diffID.Algorithm, diffID.Hex)

    // Extract using ocimaterialize
    if err := ocimaterialize.ExtractLayer(ctx, rc, destDir); err != nil {
        return err
    }

    extractedDirs = append(extractedDirs, destDir)
}

// 4. Flatten all layers into a single filesystem
rootfsDir := "/tmp/materialized-rootfs"
if err := ocimaterialize.FlattenLayers(ctx, extractedDirs, rootfsDir); err != nil {
    return err
}

// 5. Create ext4 disk image
ext4Path := "/output/containerfs.ext4"
if err := ocimaterialize.CreateExt4FromDirectory(ctx, rootfsDir, ext4Path, 0); err != nil {
    return err
}

fmt.Println("Image materialized to:", ext4Path)
```

### Example 5: Converting Back to Tarball

```go
// Convert an overlayfs layer directory back to OCI format
layerDir := "/cache/layers/sha256/abc123..."

outputFile, err := os.Create("/output/layer.tar")
if err != nil {
    return err
}
defer outputFile.Close()

// Write uncompressed tar
if err := ocimaterialize.LayerToTarball(ctx, layerDir, outputFile); err != nil {
    return err
}

// Optionally compress
compressedFile, err := os.Create("/output/layer.tar.gz")
if err != nil {
    return err
}
defer compressedFile.Close()

gzWriter := gzip.NewWriter(compressedFile)
defer gzWriter.Close()

// Re-read and compress
layerFile, _ := os.Open("/output/layer.tar")
defer layerFile.Close()
io.Copy(gzWriter, layerFile)
```

## Data Formats

### Input: Uncompressed Tar Stream

The `ExtractLayer` function expects an **uncompressed** tar stream following OCI layer format:

```
tar stream (uncompressed):
  file: bin/bash
  file: usr/bin/ls
  file: etc/passwd
  file: .wh.oldfile              # Deletion marker
  directory: etc/
  file: etc/.wh..wh..opq         # Opaque directory marker
  file: app/myapp
  ...
```

**Caller's Responsibility:**
- Decompress gzip, zstd, or other compression formats before passing to `ExtractLayer`
- Ensure tar stream is valid and complete
- Handle any registry-specific blob formats

### Output: Overlayfs-Compatible Directory

The `ExtractLayer` function produces a directory suitable for overlayfs mounting:

```
/cache/layers/sha256/abc123.../
  bin/
    bash
  usr/
    bin/
      ls
  etc/
    passwd
  oldfile                        # char device (0:0) - deletion marker
  etc/                           # xattr: trusted.overlay.opaque=y
  app/
    myapp
```

**Overlayfs Markers:**
- **Deletion marker**: Character device with major:minor 0:0 (indicates file should be deleted in merged view)
- **Opaque directory**: Extended attribute `trusted.overlay.opaque="y"` (hides lower layer contents)

### Output: Flattened Filesystem

The `FlattenLayers` function produces a single merged directory:

```
/tmp/rootfs/
  bin/
    bash
  usr/
    bin/
      ls
  etc/
    passwd
  app/
    myapp
```

**Merging Rules:**
- Files in upper layers override lower layers
- Deletion markers remove files from final result
- Opaque directories hide lower layer contents
- Final result is a standard filesystem hierarchy

## Whiteout Handling

### OCI Whiteout Format (Input)

OCI layers use special tar entries to indicate deletions:

1. **File deletion**: `.wh.<filename>`
   - Example: `.wh.oldfile` means `oldfile` should be deleted

2. **Opaque directory**: `.wh..wh..opq`
   - In directory `dir/`, the file `dir/.wh..wh..opq` means all contents from lower layers are hidden

### Overlayfs Whiteout Format (Output)

Overlayfs uses filesystem features:

1. **File deletion**: Character device with major:minor 0:0
   - Example: `oldfile` becomes a char device node

2. **Opaque directory**: Extended attribute
   - Directory has xattr: `trusted.overlay.opaque="y"`

### Conversion

`ExtractLayer` automatically converts:
```
OCI Format              →  Overlayfs Format
─────────────────────────────────────────────
.wh.oldfile            →  oldfile (char 0:0)
dir/.wh..wh..opq       →  dir/ (with xattr)
```

`LayerToTarball` converts back:
```
Overlayfs Format        →  OCI Format
─────────────────────────────────────────────
oldfile (char 0:0)     →  .wh.oldfile
dir/ (with xattr)      →  dir/.wh..wh..opq
```

## Output Directory Structures

### For Layer Cache (Overlayfs)

Typical cache layout for extracted layers:

```
{cacheRoot}/layers/
  ├── sha256/
  │   ├── abc123.../              # Layer 1 (base layer)
  │   │   ├── bin/
  │   │   ├── usr/
  │   │   └── lib/
  │   ├── def456.../              # Layer 2
  │   │   ├── etc/
  │   │   ├── app/
  │   │   └── oldfile             # char device (deletion)
  │   └── ghi789.../              # Layer 3 (top layer)
  │       └── app/
  │           └── config.json
```

**Usage with overlayfs:**
```bash
mount -t overlay overlay \
  -o lowerdir=ghi789...:def456...:abc123...,\
     upperdir=container-upper,\
     workdir=container-work \
  /container/rootfs
```

### For Flattened Filesystem

Output from `FlattenLayers`:

```
{destDir}/
  ├── bin/
  │   ├── bash
  │   └── sh
  ├── usr/
  │   ├── bin/
  │   └── lib/
  ├── etc/
  │   ├── passwd
  │   └── group
  └── app/
      ├── myapp
      └── config.json
```

### For Ext4 Image

Output from `CreateExt4FromDirectory`:

```
{outputPath}/containerfs.ext4    # Single ext4 file
```

Can be mounted:
```bash
mount -o loop containerfs.ext4 /mnt/container
```

## Benefits of This Design

### 1. Separation of Concerns

```
┌─────────────────────────────────────────────────────┐
│ Caller (e.g., ociruntime, ociconv)                  │
│ - Resolves image from registry                      │
│ - Handles authentication                            │
│ - Manages caching and deduplication                 │
│ - Provides data streams                             │
└─────────────────────────────────────────────────────┘
                        │
                        │ io.ReadCloser (uncompressed tar)
                        ▼
┌─────────────────────────────────────────────────────┐
│ ocimaterialize Package                              │
│ - Extracts tar streams to disk                      │
│ - Handles whiteout conversion                       │
│ - Merges layers                                     │
│ - Creates ext4 images                               │
│ - No knowledge of registries or authentication      │
└─────────────────────────────────────────────────────┘
```

### 2. Maximum Flexibility

Callers control:
- **Data sources**: Registry, disk, network, memory, test fixtures
- **Caching strategy**: How and where to cache layers
- **Decompression**: Which algorithms, when to decompress
- **Authentication**: How to authenticate with registries
- **Concurrency**: Parallel downloads, deduplication

### 3. Easy Testing

```go
// Simple test with in-memory tar
func TestExtractLayer(t *testing.T) {
    // Create a tar in memory
    var buf bytes.Buffer
    tw := tar.NewWriter(&buf)

    // Add test files
    tw.WriteHeader(&tar.Header{Name: "test.txt", Mode: 0644, Size: 4})
    tw.Write([]byte("test"))
    tw.Close()

    // Test extraction
    rc := io.NopCloser(&buf)
    err := ExtractLayer(ctx, rc, t.TempDir())

    // Verify files exist
    // ...
}
```

### 4. No External Dependencies

Only standard library types:
- `context.Context`
- `io.ReadCloser`
- `io.Writer`
- `string` for paths

### 5. Clear Contracts

Documentation explicitly states:
- What format data should be in (compressed vs uncompressed)
- What the caller must provide
- What the package guarantees
- What gets written to disk

## Error Handling

The package uses the `status` package for gRPC-style errors:

- **`status.InvalidArgumentError`**: Malformed tar data, invalid paths
- **`status.FailedPreconditionError`**: Missing parent directories, invalid state
- **`status.PermissionDeniedError`**: Filesystem permission issues
- **`status.InternalError`**: Unexpected errors (filesystem corruption, etc.)
- **`status.UnavailableError`**: Transient failures (disk full, etc.)

Example:
```go
err := ocimaterialize.ExtractLayer(ctx, rc, destDir)
if err != nil {
    if status.IsInvalidArgumentError(err) {
        // Bad tar data
    } else if status.IsPermissionDeniedError(err) {
        // Permission issue
    }
}
```

## Related Packages

- **ociconv**: Converts OCI images to ext4 for Firecracker (uses ocimaterialize internally)
- **ociruntime**: Manages OCI container execution with overlayfs (uses ocimaterialize internally)
- **util/oci**: Resolves images from registries (upstream of ocimaterialize)
- **util/ext4**: Low-level ext4 image creation (used by `CreateExt4FromDirectory`)

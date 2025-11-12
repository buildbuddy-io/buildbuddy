# OCI Interfaces: Image Fetching and Disk Writing

This document traces the call chain from the high-level `CommandContainer` interface down through the implementation layers to the actual disk writing operations.

---

## Layer 1: CommandContainer Interface

**Package**: `container`
**File**: `enterprise/server/remote_execution/container/container.go`

### Interface Definition

```go
type CommandContainer interface {
    // IsImageCached checks if the container image is cached locally
    IsImageCached(ctx context.Context) (bool, error)

    // PullImage pulls the container image if not cached
    PullImage(ctx context.Context, creds oci.Credentials) error

    Run(ctx context.Context, command *repb.Command, workingDir string, creds oci.Credentials) *interfaces.CommandResult
    Create(ctx context.Context, workingDir string) error
    Exec(ctx context.Context, command *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult
    Remove(ctx context.Context) error
    Stats(ctx context.Context) (*repb.UsageStats, error)
}
```

### Orchestration Function

**`PullImageIfNecessary`** - Lines 521-564

```go
func PullImageIfNecessary(ctx context.Context, env environment.Env, ctr CommandContainer,
                          creds oci.Credentials, imageRef string) error
```

**Flow**:
1. Acquires per-(isolation_type, imageRef) mutex to serialize concurrent pulls
2. Calls `ctr.IsImageCached(ctx)` to check if image exists locally
3. Creates token via `NewImageCacheToken(ctx, env, creds, imageRef)`
4. **Fast path**: If image is cached AND `IsAuthorized(token)` returns true → skip pull
5. **Slow path**: Calls `ctr.PullImage(ctx, creds)` to download/extract image
6. On success, calls `Refresh(token)` to mark authorized for 15 minutes

---

## Layer 2: ImageCacheAuthenticator

**Package**: `container`
**File**: `enterprise/server/remote_execution/container/container.go`

### Purpose
Maintains a 15-minute token cache to avoid re-authenticating with the remote registry on every action. Provides massive performance improvement for cached images.

### Token Structure

**File**: `server/interfaces/interfaces.go:1634-1639`

```go
type ImageCacheToken struct {
    // GroupID is the authenticated group ID
    GroupID string

    // ImageRef is the remote image ref, e.g. "gcr.io/foo/bar:v1.0"
    ImageRef string
}
```

**Important**: Token is just (GroupID, ImageRef) - credentials are NOT stored in the token.

### Interface

**File**: `server/interfaces/interfaces.go:1641-1651`

```go
type ImageCacheAuthenticator interface {
    // IsAuthorized returns whether the given token is valid
    IsAuthorized(token ImageCacheToken) bool

    // Refresh extends the TTL of the given token (default: 15 minutes)
    Refresh(token ImageCacheToken)
}
```

### Implementation

**Lines 588-631**

```go
type imageCacheAuthenticator struct {
    opts             ImageCacheAuthenticatorOpts
    mu               sync.Mutex // protects tokenExpireTimes
    tokenExpireTimes map[interfaces.ImageCacheToken]time.Time
}
```

**Default TTL**: 15 minutes (lines 38-40)

### Key Functions

**`NewImageCacheToken`** - Lines 566-583

```go
func NewImageCacheToken(ctx context.Context, env environment.Env, creds oci.Credentials,
                        imageRef string) (interfaces.ImageCacheToken, error)
```

- Extracts `GroupID` from authenticated user context (empty string for anonymous)
- Returns deterministic token for (GroupID, ImageRef) pair
- Used to check if this group has recently authenticated for this image

**`IsAuthorized`** - Lines 611-617

```go
func (a *imageCacheAuthenticator) IsAuthorized(token interfaces.ImageCacheToken) bool
```

- Checks if token exists in the expiration map and hasn't expired
- Opportunistically purges expired tokens during check
- Returns true if token is valid (authenticated within last 15 minutes)

**`Refresh`** - Lines 619-623

```go
func (a *imageCacheAuthenticator) Refresh(token interfaces.ImageCacheToken)
```

- Updates token expiration to now + 15 minutes
- Called after successful image pull (which validates credentials with registry)
- Allows subsequent actions to skip registry authentication

### Security Model

- **Group-isolated**: Different groups must re-authenticate even with same credentials
- **Time-bound**: Tokens expire after 15 minutes, forcing periodic re-validation
- **Multi-tenant safe**: Group A authenticating doesn't grant Group B access

---

## Layer 3: Container Implementations

### 3A: ociruntime (Layer-Based)

**Package**: `ociruntime`
**File**: `enterprise/server/remote_execution/containers/ociruntime/ociruntime.go`

#### Container Implementation

**Lines 554, 582, 587**

```go
type ociContainer struct {
    imageStore *ImageStore  // Shared across all containers in provider
    imageRef   string       // e.g., "gcr.io/org/image:v1"
    // ... other fields
}

func (c *ociContainer) IsImageCached(ctx context.Context) (bool, error) {
    image, ok := c.imageStore.CachedImage(c.imageRef)
    return ok && image != nil, nil
}

func (c *ociContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
    if _, err := c.imageStore.Pull(ctx, c.imageRef, creds); err != nil {
        return err
    }
    return nil
}
```

#### ImageStore

**Lines 1486-1494**

```go
type ImageStore struct {
    resolver       *oci.Resolver
    layersDir      string  // e.g., "/cache/images/oci"
    imagePullGroup singleflight.Group[string, *Image]  // Dedupe image pulls
    layerPullGroup singleflight.Group[string, any]     // Dedupe layer downloads

    mu           sync.RWMutex
    cachedImages map[string]*Image  // In-memory metadata cache
}
```

**`NewImageStore`** - Lines 1513-1519

```go
func NewImageStore(resolver *oci.Resolver, layersDir string) (*ImageStore, error)
```

- Creates store with empty cachedImages map
- One instance shared across all ociruntime containers in the provider
- layersDir typically points to `{cacheRoot}/images/oci`

**`CachedImage`** - Lines 1547-1553

```go
func (s *ImageStore) CachedImage(imageName string) (image *Image, ok bool)
```

- Thread-safe read from in-memory cachedImages map
- Returns (image, true) if previously pulled, (nil, false) otherwise
- **Does NOT check disk** - only checks in-memory metadata

**`Pull`** - Lines 1527-1542

```go
func (s *ImageStore) Pull(ctx context.Context, imageName string, creds oci.Credentials) (*Image, error)
```

- **Always re-authenticates** with remote registry (comment line 1524)
- Uses singleflight with key: `hash.Strings(imageName, creds.Username, creds.Password)`
- Dedupes concurrent pulls of same image with same credentials
- Calls internal `pull()` method to do actual work
- Stores resolved Image in cachedImages map on success

#### Image Structure

**Lines 1496-1511**

```go
type Image struct {
    // Layers from lowermost to uppermost (overlayfs order)
    Layers []*ImageLayer

    // ConfigFile holds image settings (user, env, etc.)
    ConfigFile ctr.ConfigFile
}

type ImageLayer struct {
    // DiffID is the uncompressed digest
    DiffID ctr.Hash
}
```

---

### 3B: firecracker (Ext4-Based)

**Package**: `firecracker`
**File**: `enterprise/server/remote_execution/containers/firecracker/firecracker.go`

#### Container Implementation

**Lines 2534-2560**

```go
type FirecrackerContainer struct {
    resolver       *oci.Resolver
    executorConfig *container.ExecutorConfig
    containerImage string  // e.g., "gcr.io/org/image:v1"
    // ... other fields
}

func (c *FirecrackerContainer) IsImageCached(ctx context.Context) (bool, error) {
    diskImagePath, err := ociconv.CachedDiskImagePath(ctx, c.executorConfig.CacheRoot, c.containerImage)
    if err != nil {
        return false, err
    }
    return diskImagePath != "", nil
}

func (c *FirecrackerContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
    _, err := ociconv.CreateDiskImage(ctx, c.resolver, c.executorConfig.CacheRoot,
                                       c.containerImage, creds)
    return err
}
```

**Key Difference**: Firecracker uses `ociconv` package to convert entire OCI image to a single ext4 filesystem image, rather than maintaining separate layer directories.

---

## Layer 4: Image Resolution

**Package**: `oci`
**File**: `enterprise/server/util/oci/oci.go`

### Resolver

```go
type Resolver struct {
    env                 environment.Env
    allowedPrivateIPs   []*net.IPNet
    mu                  sync.Mutex
    imageTagToDigestLRU *lru.LRU[tagToDigestEntry]  // Caches tag→digest mappings
    clock               clockwork.Clock
}
```

**`NewResolver`** - Creates resolver for fetching OCI images

```go
func NewResolver(env environment.Env) (*Resolver, error)
```

### Authentication

**`AuthenticateWithRegistry`**

```go
func (r *Resolver) AuthenticateWithRegistry(ctx context.Context, imageName string,
                                             platform *rgpb.Platform, credentials Credentials) error
```

- Makes HEAD request to remote registry with credentials
- Returns error if authentication fails
- Called by both ociruntime and firecracker before pulling

### Resolution

**`ResolveImageDigest`**

```go
func (r *Resolver) ResolveImageDigest(ctx context.Context, imageName string,
                                       platform *rgpb.Platform, credentials Credentials) (string, error)
```

- Resolves image tag to digest (e.g., "v1.0" → "sha256:abc123...")
- Uses LRU cache to reduce HEAD requests to registry
- Returns canonical image name with digest

**`Resolve`** - Main image fetching function

```go
func (r *Resolver) Resolve(ctx context.Context, imageName string,
                           platform *rgpb.Platform, credentials Credentials) (gcr.Image, error)
```

- Returns go-containerregistry Image interface
- Can fetch from cache or remote registry
- Handles both image manifests and image indices (multi-platform)

### Internal Fetching

**`fetchImageFromCacheOrRemote`**

```go
func fetchImageFromCacheOrRemote(ctx context.Context, digestOrTagRef gcrname.Reference,
                                  platform gcr.Platform, acClient repb.ActionCacheClient,
                                  bsClient bspb.ByteStreamClient, puller *remote.Puller,
                                  bypassRegistry bool) (gcr.Image, error)
```

**Flow**:
1. Tries to fetch manifest from cache via `ocicache.FetchManifestFromAC()`
2. On cache miss, fetches from remote via `puller.Get()`
3. Writes manifest to cache via `ocicache.WriteManifestToAC()`
4. Returns Image object that lazily fetches layers

### Image Implementation

```go
type imageFromRawManifest struct {
    repo        gcrname.Repository
    desc        gcr.Descriptor
    rawManifest []byte
    ctx         context.Context
    acClient    repb.ActionCacheClient
    bsClient    bspb.ByteStreamClient
    puller      *remote.Puller
    // ... other fields
}
```

**`Layers()`** - Returns list of layers in the image

```go
func (i *imageFromRawManifest) Layers() ([]gcr.Layer, error)
```

### Layer Implementation

```go
type layerFromDigest struct {
    repo   gcrname.Repository
    digest gcr.Hash
    image  *imageFromRawManifest
    puller *remote.Puller
    desc   *gcr.Descriptor
    // ... other fields
}
```

**`Compressed`** - Returns compressed layer bytes

```go
func (l *layerFromDigest) Compressed() (io.ReadCloser, error)
```

**Flow**:
1. Tries `fetchLayerFromCache()` via ByteStream
2. On cache miss, fetches from remote via `puller.Layer()`
3. Can use read-through caching via `ocicache.NewBlobReadThroughCacher()`
   - Writes to cache as bytes are read
   - Provides transparent caching during download

**`Uncompressed`** - Returns uncompressed layer bytes

```go
func (l *layerFromDigest) Uncompressed() (io.ReadCloser, error)
```

- Decompresses layer on the fly
- Used by ociruntime for tar extraction

---

## Layer 5: Cache Operations

**Package**: `ocicache`
**File**: `enterprise/server/util/ocicache/ocicache.go`

### Manifest Caching

**`FetchManifestFromAC`**

```go
func FetchManifestFromAC(ctx context.Context, acClient repb.ActionCacheClient,
                          repo gcrname.Repository, hash gcr.Hash,
                          originalRef gcrname.Reference) (*ocipb.OCIManifestContent, error)
```

- Fetches manifest from Action Cache
- Returns manifest content if cached, nil if not

**`WriteManifestToAC`**

```go
func WriteManifestToAC(ctx context.Context, raw []byte, acClient repb.ActionCacheClient,
                        repo gcrname.Repository, hash gcr.Hash, contentType string,
                        originalRef gcrname.Reference) error
```

- Writes manifest to Action Cache
- Stores as auxiliary metadata in ActionResult

### Blob (Layer) Caching

**`FetchBlobFromCache`**

```go
func FetchBlobFromCache(ctx context.Context, w io.Writer, bsClient bspb.ByteStreamClient,
                         hash gcr.Hash, contentLength int64) error
```

- Fetches blob (layer) from CAS via ByteStream
- Writes to provided writer
- Returns error if not in cache

**`WriteBlobToCache`**

```go
func WriteBlobToCache(ctx context.Context, r io.Reader, bsClient bspb.ByteStreamClient,
                       acClient repb.ActionCacheClient, repo gcrname.Repository,
                       hash gcr.Hash, contentType string, contentLength int64) error
```

- Writes blob (layer) to CAS via ByteStream
- Uses ZSTD compression
- Also writes metadata to Action Cache (size, content type)

### Streaming Operations

**`NewBlobUploader`**

```go
func NewBlobUploader(ctx context.Context, bsClient bspb.ByteStreamClient,
                      acClient repb.ActionCacheClient, repo gcrname.Repository,
                      hash gcr.Hash, contentType string, contentLength int64) (interfaces.CommittedWriteCloser, error)
```

- Creates uploader for streaming blob writes to cache
- Implements Write/Commit/Close pattern
- Allows incremental writing to cache

**`NewBlobReadThroughCacher`** - Most important for performance

```go
func NewBlobReadThroughCacher(ctx context.Context, rc io.ReadCloser, bsClient bspb.ByteStreamClient,
                               acClient repb.ActionCacheClient, repo gcrname.Repository,
                               hash gcr.Hash, contentType string, contentLength int64) (io.ReadCloser, error)
```

- Wraps an io.ReadCloser to write to cache as bytes are read
- Enables transparent caching during download
- **Key optimization**: Downloads from registry and writes to cache simultaneously
- Used by oci package when fetching layers from remote

### Storage Backend

- **Manifests**: Action Cache (as auxiliary metadata)
- **Blobs/Layers**: CAS via ByteStream (with ZSTD compression)
- **Metadata**: Action Cache (blob size, content type, etc.)

---

## Layer 6: Disk Writing

### 6A: ociruntime - Layer-Based Extraction

**Package**: `ociruntime`
**File**: `enterprise/server/remote_execution/containers/ociruntime/ociruntime.go`

#### pull Implementation

**Lines 1555-1626**

```go
func (s *ImageStore) pull(ctx context.Context, imageName string, creds oci.Credentials) (*Image, error)
```

**Flow**:
1. Resolves image via `resolver.Resolve(ctx, imageName, platform, creds)`
2. Gets layers via `img.Layers()`
3. Downloads/extracts layers **concurrently** using errgroup (max 8 or NumCPU)
4. For each layer:
   - Gets DiffID (uncompressed digest)
   - Calculates destination: `layerPath(layersDir, diffID)` → `{layersDir}/v2/{algorithm}/{hex}`
   - **Checks if layer exists**: `os.Stat(destDir)`
   - If exists, skips download (layer deduplication)
   - If not, downloads via singleflight: `layerPullGroup.Do()`
5. Concurrently fetches ConfigFile
6. Returns Image with all layers and config

#### downloadLayer

**Lines 1628-1745**

```go
func downloadLayer(ctx context.Context, layer ctr.Layer, destDir string) error
```

**Flow**:
1. Gets uncompressed layer reader: `layer.Uncompressed()`
2. Creates temporary directory: `destDir + tmpSuffix()`
3. Extracts tarball with **overlayfs semantics**:
   - **Whiteout files** (`.wh.` prefix): Creates character device with dev 0
   - **Opaque directories** (`.wh..wh..opq`): Sets `trusted.overlay.opaque` xattr
   - Preserves file modes, ownership (uid/gid)
   - Handles symlinks and hardlinks correctly
   - Uses `fsync` operations for durability
4. Atomically renames temp dir to final destination
5. If rename fails with "exists", assumes concurrent download completed

**Reference implementations** (from code comments):
- Podman: containers/storage archive.go
- Moby (Docker): moby/moby archive.go

#### Storage Structure

**Cache Version**: `v2` (line 100-106)

**Layer Path Function** - Lines 1481-1483:

```go
func layerPath(imageCacheRoot string, hash ctr.Hash) string {
    return filepath.Join(imageCacheRoot, imageCacheVersion, hash.Algorithm, hash.Hex)
}
```

**Example Directory Structure**:
```
{cacheRoot}/images/oci/
  v2/                           # Current cache version
    sha256/                     # Hash algorithm
      abc123.../                # Layer DiffID (uncompressed)
        bin/                    # Extracted layer contents
        usr/
        etc/
        lib/
      def456.../                # Another layer
        var/
        lib/
```

**Layer Deduplication**:
- Layers are stored by their uncompressed DiffID (content-addressable)
- Multiple images sharing the same layers automatically share disk storage
- No explicit metadata database - filesystem is source of truth
- Simple `os.Stat()` check determines if layer exists

#### Version Cleanup

**Lines 1849-1865**

```go
func cleanStaleImageCacheDirs(root string) error
```

- Called during provider initialization
- Removes old version directories (e.g., "v1") that don't match current version
- Allows backwards-incompatible cache format changes

#### Concurrency Control

**Multi-level deduplication**:

1. **Image-level singleflight** (lines 1529-1541):
   - Key: `hash.Strings(imageName, creds.Username, creds.Password)`
   - Dedupes entire image pull operations
   - Includes credentials for security isolation

2. **Layer-level singleflight** (lines 1603-1610):
   - Key: `hash.Strings(destDir, creds.Username, creds.Password)`
   - Dedupes individual layer downloads
   - Multiple images sharing layers benefit from this

3. **RWMutex for cachedImages** (lines 1535-1537):
   - Protects in-memory metadata map
   - Allows concurrent reads, exclusive writes

4. **Filesystem atomicity**:
   - Atomic `os.Rename()` from temp to final destination
   - Handles concurrent extracts gracefully (line 1736)

---

### 6B: firecracker - Ext4 Disk Image

**Package**: `ociconv`
**File**: `enterprise/server/util/ociconv/ociconv.go`

#### CachedDiskImagePath

**Lines 47-71**

```go
func CachedDiskImagePath(ctx context.Context, cacheRoot, containerImage string) (string, error)
```

**Flow**:
1. Hashes container image name to get consistent directory
2. Checks for existing disk image at: `{cacheRoot}/images/ext4/{imageHash}/*/containerfs.ext4`
3. Returns path if exists, empty string if not
4. Multiple hash subdirectories allow different versions of same image

#### CreateDiskImage

**Lines 111-145**

```go
func CreateDiskImage(ctx context.Context, resolver *oci.Resolver, cacheRoot,
                      containerImage string, creds oci.Credentials) (string, error)
```

**Flow**:
1. Checks for cached disk image via `CachedDiskImagePath()`
2. If found:
   - Authenticates with registry via `resolver.AuthenticateWithRegistry()`
   - Returns existing path (no download)
3. If not found:
   - Uses singleflight to dedupe concurrent conversions
   - Calls `convertContainerToExt4FS()` to create new disk image
4. Returns path to ext4 file

#### convertContainerToExt4FS

**Lines 147-221**

```go
func convertContainerToExt4FS(ctx context.Context, resolver *oci.Resolver, workspaceDir,
                                containerImage string, creds oci.Credentials) (string, error)
```

**Flow**:
1. Resolves image via `resolver.Resolve()`
2. Extracts all layers to temporary directory via `mutate.Extract(img)`
   - Merges all layers into single directory tree
   - Handles overlayfs semantics (whiteouts, opaque dirs)
3. Converts directory to ext4 image via `ext4.DirectoryToImageAutoSize()`
   - Automatically calculates required filesystem size
   - Creates ext4 filesystem in a file
4. Hashes the ext4 file to get content-based name
5. Stores at: `{cacheRoot}/images/ext4/{hash(imageRef)}/{hash(ext4file)}/containerfs.ext4`
6. Returns path to disk image

#### Storage Structure

**Example Directory Structure**:
```
{cacheRoot}/images/ext4/
  {hash(imageRef)}/                 # e.g., hash("gcr.io/org/image:v1")
    {hash(ext4_contents)}/           # SHA256 of ext4 file
      containerfs.ext4               # Actual disk image file
```

**Key Characteristics**:
- **Two-level hashing**:
  - First level: Image reference (enables lookup by name)
  - Second level: Content hash (enables deduplication)
- **Whole-image conversion**: Entire OCI image → single ext4 filesystem
- **Content deduplication**: Same image content reuses same ext4 file
- **No layer sharing**: Unlike ociruntime, layers aren't separately stored

#### Usage in Firecracker

**File**: `enterprise/server/remote_execution/containers/firecracker/firecracker.go`

The ext4 disk image is mounted as a block device in the Firecracker VM:
- Attached as virtio-block device
- Mounted read-only as root filesystem
- Provides fast, efficient VM boot times
- No overlayfs needed - entire filesystem in one file

---

## Key Data Structures

### ImageStore (ociruntime)

```go
type ImageStore struct {
    resolver       *oci.Resolver
    layersDir      string
    imagePullGroup singleflight.Group[string, *Image]
    layerPullGroup singleflight.Group[string, any]
    mu             sync.RWMutex
    cachedImages   map[string]*Image
}
```

- **Scope**: One per ociruntime provider (shared across all containers)
- **Storage**: Content-addressable by DiffID
- **In-memory cache**: Map of image name → Image metadata
- **Disk check**: `os.Stat()` to determine if layer exists

### Image and ImageLayer

```go
type Image struct {
    Layers     []*ImageLayer
    ConfigFile ctr.ConfigFile
}

type ImageLayer struct {
    DiffID ctr.Hash  // Uncompressed digest
}
```

- **Image**: Contains metadata about all layers and configuration
- **ImageLayer**: Just the DiffID, used to construct layer path on disk
- **ConfigFile**: Contains environment, entrypoint, user, working dir, etc.

### ImageCacheToken

```go
type ImageCacheToken struct {
    GroupID  string  // Authenticated group ID
    ImageRef string  // Remote image reference
}
```

- **Purpose**: Key for 15-minute authorization cache
- **Security**: Group-isolated - different groups can't share tokens
- **Deterministic**: Same (group, image) always produces same token
- **No credentials**: Credentials validated separately, not stored in token

### ImageCacheAuthenticator

```go
type imageCacheAuthenticator struct {
    opts             ImageCacheAuthenticatorOpts
    mu               sync.Mutex
    tokenExpireTimes map[interfaces.ImageCacheToken]time.Time
}
```

- **Scope**: One per executor (shared across all containers and isolation types)
- **TTL**: 15 minutes (configurable)
- **Cleanup**: Opportunistic on access (no background goroutine)
- **Thread-safe**: All operations protected by mutex

---

## Summary: Two-Layer Caching Strategy

BuildBuddy's OCI implementation uses **two complementary caching layers**:

### 1. Token Cache (15 minutes)
- **What**: Caches that a group has recently authenticated for an image
- **Benefit**: Avoids network roundtrip to registry on every action
- **Scope**: In-memory, per-executor
- **Invalidation**: Time-based (15 minutes)

### 2. Image/Layer Cache (persistent)
- **What**: Caches image manifests, layers, and disk images
- **Benefit**: Avoids downloading image data from registry
- **Scope**: On-disk, shared across all executors
- **Invalidation**: Manual cleanup or cache eviction

**Performance Impact**: Without token cache, every action would require registry authentication even for cached images. With it, only the first access within a 15-minute window requires network communication.
